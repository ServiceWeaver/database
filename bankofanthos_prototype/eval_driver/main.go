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

	"github.com/gookit/color"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	v1Bin                 = "./../bankofanthos/bankofanthos"
	v2Bin                 = "./../bankofanthos/bankofanthos"
	configPath            = "configs/"
	logPath               = "logs/"
	outPath               = "out/"
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

func runTrail(ctx context.Context, namespace string, branchers map[string]*dbclone.Brancher, runCnt int, listenPorts []string, prodServices []service.ProdService, reqPorts []string) (map[string]*dbclone.Branch, *service.Service, error) {
	branchMap := map[string]*dbclone.Branch{}
	for name, brancher := range branchers {
		b, err := brancher.Branch(ctx, namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("branch %s failed: %v", namespace, err)
		}
		defer func() {
			err = b.Commit(ctx)
			if err != nil {
				log.Fatalf("Commit %s failed: %v", namespace, err)
			}
		}()

		branchMap[name] = b
	}
	trail, err := service.Init(runCnt, listenPorts, prodServices, reqPorts)
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	trail.Run(service.ListOfReqs1)
	return branchMap, trail, nil
}

func main() {
	// parse flags
	var origListenPort, expListenPort, baseListenPort, dbUrls string
	var dropClonedTables bool
	flag.StringVar(&origListenPort, "origListenPort", "9000", "Listen port for original service.")
	flag.StringVar(&baseListenPort, "expListenPort", "9001", "Listen port for experimental service.")
	flag.StringVar(&expListenPort, "baseListenPort", "9002", "Listen port for baseline service.")
	flag.StringVar(&dbUrls, "dbUrls", "postgresql://admin:admin@localhost:5432/accountsdb?sslmode=disable,postgresql://admin:admin@localhost:5432/postgresdb?sslmode=disable", "database urls used for app; split by ,")
	flag.BoolVar(&dropClonedTables, "dropClonedTables", true, "Drop cloned tables at the end of eval run, only set false for investigation purpose")
	flag.Parse()

	// create directories to store eval info
	dirs := []string{configPath, logPath, outPath, nonDeterministicField}
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

	// get the service running in prod
	baseProdService := service.ProdService{
		ConfigPath: v1Config,
		ListenPort: origListenPort,
		Bin:        v1Bin,
	}
	experimentalProdService := service.ProdService{
		ConfigPath: v2Config,
		ListenPort: origListenPort,
		Bin:        v2Bin,
	}

	ctx := context.Background()
	branchPerTrail := map[string]map[string]*dbclone.Branch{} // {namespace1:{dbname1:Branch,dbname2:Branch},namespace2...}
	runCnt := 0

	// generate traffic patterns
	allPorts, err := requestsPorts(service.ListOfReqs1, 5, baseListenPort, expListenPort)
	if err != nil {
		log.Fatalf("Failed to generate traffic patterns: %v", err)
	}

	branchers := map[string]*dbclone.Brancher{}
	for _, prodDb := range prodDbs {
		db, err := pgxpool.Connect(ctx, prodDb.Url)
		if err != nil {
			log.Fatalf("Connect to DB %s failed with %s: %v", prodDb.Name, prodDb.Url, err)
		}
		defer db.Close()
		brancher := dbclone.NewBrancher(db)
		branchers[prodDb.Name] = brancher
	}

	baselineBranches, baselineService, err := runTrail(ctx, "B", branchers, runCnt, []string{baseListenPort}, []service.ProdService{baseProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("trail run failed: %v", err)
	}
	for _, branch := range baselineBranches {
		defer func() {
			if dropClonedTables {
				err = branch.Delete(ctx)
				if err != nil {
					log.Fatalf("Delete failed: %v", err)
				}
			}
		}()
	}
	branchPerTrail["B"] = baselineBranches

	runCnt += 1

	baseline2Branches, baselineService2, err := runTrail(ctx, "BTWO", branchers, runCnt, []string{baseListenPort}, []service.ProdService{baseProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("trail run failed: %v", err)
	}
	for _, branch := range baseline2Branches {
		defer func() {
			if dropClonedTables {
				err = branch.Delete(ctx)
				if err != nil {
					log.Fatalf("Delete failed: %v", err)
				}
			}
		}()
	}

	branchPerTrail["BTWO"] = baseline2Branches

	if err := diff.GetNonDeterministic(baselineService, baselineService2); err != nil {
		log.Fatalf("Get non deterministic error failed: %v", err)
	}

	// run experimental service
	runCnt += 1
	experientalServiceBranches, experientalService, err := runTrail(ctx, "E", branchers, runCnt, []string{expListenPort}, []service.ProdService{experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("trail run failed: %v", err)
	}
	for _, branch := range experientalServiceBranches {
		defer func() {
			if dropClonedTables {
				err = branch.Delete(ctx)
				if err != nil {
					log.Fatalf("Delete failed: %v", err)
				}
			}
		}()
	}
	branchPerTrail["E"] = experientalServiceBranches

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

	// run requests on both baseline and experiental
	runCnt += 1
	b1E1ServiceBranches, b1E1Service, err := runTrail(ctx, "BE", branchers, runCnt, []string{baseListenPort, expListenPort}, []service.ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("trail run failed: %v", err)
	}
	for _, branch := range b1E1ServiceBranches {
		defer func() {
			if dropClonedTables {
				err = branch.Delete(ctx)
				if err != nil {
					log.Fatalf("Delete failed: %v", err)
				}
			}
		}()
	}
	branchPerTrail["BE"] = b1E1ServiceBranches

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

	// run requests on both experiental and baseline
	runCnt += 1
	e1B1ServiceBranches, e1B1Service, err := runTrail(ctx, "EB", branchers, runCnt, []string{baseListenPort, expListenPort}, []service.ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("trail run failed: %v", err)
	}
	for _, branch := range e1B1ServiceBranches {
		defer func() {
			if dropClonedTables {
				err = branch.Delete(ctx)
				if err != nil {
					log.Fatalf("Delete failed: %v", err)
				}
			}
		}()
	}
	branchPerTrail["EB"] = e1B1ServiceBranches

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

	fmt.Println("Exiting program...")
}
