package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"bankofanthos_prototype/eval_driver/dbclone"
	"bankofanthos_prototype/eval_driver/diff"
	"bankofanthos_prototype/eval_driver/service"
	"bankofanthos_prototype/eval_driver/utility"

	"github.com/gookit/color"
)

var (
	v1Bin                 = "./../bankofanthos/bankofanthos"
	v2Bin                 = "./../bankofanthos/bankofanthos"
	configPath            = "configs/"
	logPath               = "logs/"
	outPath               = "out/"
	snapshotPath          = "snapshot/"
	v1Config              = "../bankofanthos/weaver.toml"
	v2Config              = "../bankofanthos/weaver_experimental.toml"
	nonDeterministicField = "nondeterministic/"
	databaseType          = "database"
	responseType          = "response"
)

// requestsPorts generates traffic pattern, each request will be directed to either baseline service port
// or experimental service port
func requestsPorts(l service.ListOfReqs, numOfRuns int, baseListenPort, expListenPort string) ([][]string, error) {
	reqCount := len(l())
	allPorts := [][]string{}

	for r := 0; r < numOfRuns; r++ {
		ports := []string{}
		if r <= 1 {
			// for all baseline traffic
			for i := 0; i < reqCount; i++ {
				ports = append(ports, baseListenPort)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 2 {
			// for all experimental traffic
			for i := 0; i < reqCount; i++ {
				ports = append(ports, expListenPort)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 3 {
			// half to baseline, half to experimental
			for i := 0; i < reqCount/2; i++ {
				ports = append(ports, baseListenPort)
			}
			for i := reqCount / 2; i < reqCount; i++ {
				ports = append(ports, expListenPort)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 4 {
			// half to experimental, half to baseline
			for i := 0; i < reqCount/2; i++ {
				ports = append(ports, expListenPort)
			}
			for i := reqCount / 2; i < reqCount; i++ {
				ports = append(ports, baseListenPort)
			}
			allPorts = append(allPorts, ports)
		}
	}

	return allPorts, nil
}

func getDatabaseFromURL(databaseUrl string) (*service.Database, error) {
	posS := strings.LastIndex(databaseUrl, "/")
	posE := strings.Index(databaseUrl, "?")

	if posS == -1 {
		return nil, fmt.Errorf("database name not found in URL")
	}

	return &service.Database{Name: databaseUrl[posS+1 : posE], Url: databaseUrl}, nil
}

func main() {
	// parse flags
	var origListenPort, expListenPort, baseListenPort, dbUrls string
	var dropSnapshotDB, dropClonedTables bool
	flag.StringVar(&origListenPort, "origListenPort", "9000", "Listen port for original service.")
	flag.StringVar(&baseListenPort, "expListenPort", "9001", "Listen port for experimental service.")
	flag.StringVar(&expListenPort, "baseListenPort", "9002", "Listen port for baseline service.")
	flag.StringVar(&dbUrls, "dbUrls", "postgresql://admin:admin@localhost:5432/accountsdb?sslmode=disable,postgresql://admin:admin@localhost:5432/postgresdb?sslmode=disable", "database urls used for app; split by ,")
	flag.BoolVar(&dropSnapshotDB, "dropSnapshotDB", false, "Drop snapshot DB at the end of eval run")
	flag.BoolVar(&dropClonedTables, "dropClonedTables", true, "Drop cloned tables at the end of eval run, only set false for investigation purpose")
	flag.Parse()

	// create directories to store eval info
	dirs := []string{configPath, logPath, outPath, nonDeterministicField, snapshotPath}
	for _, dir := range dirs {
		err := os.RemoveAll(dir)
		if err != nil {
			log.Fatalf("Remove %s failed: %v", dir, err)
		}
		err = os.Mkdir(dir, 0700)
		if err != nil {
			log.Fatalf("Mkdir %s failed: %v", dir, err)
		}
	}

	// get prod database
	urlSlice := strings.Split(dbUrls, ",")
	prodDbs := map[string]*service.Database{}
	for _, url := range urlSlice {
		db, err := getDatabaseFromURL(url)
		if err != nil {
			log.Fatalf("Parse databse url %s failed: %v", url, err)
		}
		prodDbs[db.Name] = db
	}

	// take snapshot of prod database
	for dbName, db := range prodDbs {
		if err := service.TakeSnapshot(db, snapshotPath+dbName+".sql"); err != nil {
			log.Fatalf("Failed to take snapshot for %s: %v", dbName, err)
		}
	}

	// get the service running in prod
	baseProdService := service.ProdService{
		ConfigPath: v1Config,
		ListenPort: origListenPort,
		Bin:        v1Bin,
		Databases:  prodDbs,
	}
	experimentalProdService := service.ProdService{
		ConfigPath: v2Config,
		ListenPort: origListenPort,
		Bin:        v2Bin,
		Databases:  prodDbs,
	}

	// restore snapshot to a new sanpshotdb
	dbSnapshots := map[string]*service.Database{}
	for dbName, db := range prodDbs {
		snpashotDb, err := service.RestoreSnapshot(snapshotPath+dbName+".sql", db)
		if err != nil {
			log.Fatalf("Failed to restore snapshot for %s: %v", dbName, err)
		}
		dbSnapshots[snpashotDb.Name] = snpashotDb
	}

	ctx := context.Background()

	var allClonedDbs []*dbclone.ClonedDb
	runCnt := 0

	// generate traffic patterns
	allPorts, err := requestsPorts(service.ListOfReqs1, 5, baseListenPort, expListenPort)
	if err != nil {
		log.Fatalf("Failed to generate traffic patterns: %v", err)
	}

	// run baseline service
	var clonedDBBs []*dbclone.ClonedDb
	for _, snapshot := range dbSnapshots {
		db, err := service.CloneDB(ctx, snapshot, "B")
		if err != nil {
			log.Fatalf("Cloned snapshotDB %s failed: %v", snapshot.Name, err)
		}
		clonedDBBs = append(clonedDBBs, db)
	}

	baselineService, err := service.Init(runCnt, []string{baseListenPort}, []service.ProdService{baseProdService}, allPorts[runCnt], dbSnapshots)
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	baselineService.Run(service.ListOfReqs1)
	allClonedDbs = append(allClonedDbs, clonedDBBs...)
	for _, db := range clonedDBBs {
		if err = db.Reset(ctx); err != nil {
			log.Fatalf("Reset cloned database failed: %v", err)
		}
	}

	// run baseline service2
	var clonedDBBTwos []*dbclone.ClonedDb
	for _, snapshot := range dbSnapshots {
		db, err := service.CloneDB(ctx, snapshot, "BTWO")
		if err != nil {
			log.Fatalf("Cloned snapshotDB %s failed: %v", snapshot.Name, err)
		}
		clonedDBBTwos = append(clonedDBBTwos, db)
	}

	runCnt += 1
	baselineService2, err := service.Init(runCnt, []string{baseListenPort}, []service.ProdService{baseProdService}, allPorts[runCnt], dbSnapshots)
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	baselineService2.Run(service.ListOfReqs1)

	if err := diff.GetNonDeterministic(baselineService, baselineService2); err != nil {
		log.Fatalf("Get non deterministic error failed: %v", err)
	}

	allClonedDbs = append(allClonedDbs, clonedDBBTwos...)
	for _, db := range clonedDBBTwos {
		if err = db.Reset(ctx); err != nil {
			log.Fatalf("Reset cloned database failed: %v", err)
		}
	}

	// run experimental service
	var clonedDBEs []*dbclone.ClonedDb
	for _, snapshot := range dbSnapshots {
		db, err := service.CloneDB(ctx, snapshot, "E")
		if err != nil {
			log.Fatalf("Cloned snapshotDB %s failed: %v", snapshot.Name, err)
		}
		clonedDBEs = append(clonedDBEs, db)
	}

	runCnt += 1
	experientalService, err := service.Init(runCnt, []string{expListenPort}, []service.ProdService{experimentalProdService}, allPorts[runCnt], dbSnapshots)
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	experientalService.Run(service.ListOfReqs1)

	eq1, err := diff.OutputEq(baselineService.OutputPath, experientalService.OutputPath, responseType)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	eq2, err := diff.OutputEq(baselineService.DumpDbPath, experientalService.DumpDbPath, databaseType)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	if eq1 && eq2 {
		color.Greenf("run %s and run %s is equal.\n", baselineService.Runs, experientalService.Runs)
	}
	allClonedDbs = append(allClonedDbs, clonedDBEs...)
	for _, db := range clonedDBEs {
		if err = db.Reset(ctx); err != nil {
			log.Fatalf("Reset cloned database failed: %v", err)
		}
	}

	// run requests on both baseline and experiental
	var clonedDBBEs []*dbclone.ClonedDb
	for _, snapshot := range dbSnapshots {
		db, err := service.CloneDB(ctx, snapshot, "BE")
		if err != nil {
			log.Fatalf("Cloned snapshotDB %s failed: %v", snapshot.Name, err)
		}

		clonedDBBEs = append(clonedDBBEs, db)
	}

	runCnt += 1
	b1E1Service, err := service.Init(runCnt, []string{baseListenPort, expListenPort}, []service.ProdService{baseProdService, experimentalProdService}, allPorts[runCnt], dbSnapshots)
	if err != nil {
		log.Fatalf("Init B1E1 service failed: %v", err)
	}
	b1E1Service.Run(service.ListOfReqs1)

	eq1, err = diff.OutputEq(baselineService.OutputPath, b1E1Service.OutputPath, responseType)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}

	eq2, err = diff.OutputEq(baselineService.DumpDbPath, b1E1Service.DumpDbPath, databaseType)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	if eq1 && eq2 {
		color.Greenf("run %s and run %s is equal.\n", baselineService.Runs, b1E1Service.Runs)
	}

	allClonedDbs = append(allClonedDbs, clonedDBBEs...)
	for _, db := range clonedDBBEs {
		if err = db.Reset(ctx); err != nil {
			log.Fatalf("Reset cloned database failed: %v", err)
		}
	}

	// run requests on both experiental and baseline
	var clonedDBEBs []*dbclone.ClonedDb
	for _, snapshot := range dbSnapshots {
		db, err := service.CloneDB(ctx, snapshot, "EB")
		if err != nil {
			log.Fatalf("Cloned snapshotDB %s failed: %v", snapshot.Name, err)
		}
		clonedDBEBs = append(clonedDBEBs, db)
	}

	runCnt += 1

	e1B1Service, err := service.Init(runCnt, []string{baseListenPort, expListenPort}, []service.ProdService{baseProdService, experimentalProdService}, allPorts[runCnt], dbSnapshots)
	if err != nil {
		log.Fatalf("Init B1E1 service failed: %v", err)
	}
	e1B1Service.Run(service.ListOfReqs1)

	eq1, err = diff.OutputEq(baselineService.OutputPath, e1B1Service.OutputPath, responseType)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	eq2, err = diff.OutputEq(baselineService.DumpDbPath, e1B1Service.DumpDbPath, databaseType)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	if eq1 && eq2 {
		color.Greenf("run %s and run %s is equal.\n", baselineService.Runs, e1B1Service.Runs)
	}

	allClonedDbs = append(allClonedDbs, clonedDBEBs...)
	for _, db := range clonedDBEBs {
		if err = db.Reset(ctx); err != nil {
			log.Fatalf("Reset cloned database failed: %v", err)
		}
	}

	if dropClonedTables {
		for _, cloned := range allClonedDbs {
			if err = cloned.Close(ctx); err != nil {
				log.Fatalf("Close cloned database failed: %v", err)
			}
		}
	}

	if dropClonedTables && dropSnapshotDB {
		for dbName := range dbSnapshots {
			origDb := utility.GetProdDbNameBySnapshot(dbName)
			if err = service.CloseSnapshotDB(baseProdService.Databases[origDb], dbName); err != nil {
				log.Fatalf("Close snapshotDB %s failed: %v", dbName, err)
			}
		}
	}

	fmt.Println("Exiting program...")
}
