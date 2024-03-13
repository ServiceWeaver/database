package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"bankofanthos_prototype/eval_driver/diff"
	"bankofanthos_prototype/eval_driver/service"

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

func main() {
	// parse flags
	var runs int
	var origListenPort, expListenPort, baseListenPort, databasePort string
	flag.IntVar(&runs, "runs", 5, "Total runs for the same request set.")
	flag.StringVar(&origListenPort, "origListenPort", "9000", "Listen port for original service.")
	flag.StringVar(&baseListenPort, "expListenPort", "9001", "Listen port for experimental service.")
	flag.StringVar(&expListenPort, "baseListenPort", "9002", "Listen port for baseline service.")
	flag.StringVar(&databasePort, "databasePort", "55432", "Listen port for experimental service.")
	flag.Parse()

	// create config dir
	err := os.RemoveAll(configPath)
	if err != nil {
		log.Fatalf("Remove %s failed: %v", configPath, err)
	}
	err = os.Mkdir(configPath, 0700)
	if err != nil {
		log.Fatalf("Mkdir %s failed: %v", configPath, err)
	}

	// create log dir
	err = os.RemoveAll(logPath)
	if err != nil {
		log.Fatalf("Remove %s failed: %v", logPath, err)
	}
	err = os.Mkdir(logPath, 0700)
	if err != nil {
		log.Fatalf("Mkdir %s failed: %v", logPath, err)
	}

	// create out dir
	err = os.RemoveAll(outPath)
	if err != nil {
		log.Fatalf("Remove %s failed: %v", outPath, err)
	}
	err = os.Mkdir(outPath, 0700)
	if err != nil {
		log.Fatalf("Mkdir %s failed: %v", outPath, err)
	}

	// create non-deterministic dir
	err = os.RemoveAll(nonDeterministicField)
	if err != nil {
		log.Fatalf("Remove %s failed: %v", nonDeterministicField, err)
	}
	err = os.Mkdir(nonDeterministicField, 0700)
	if err != nil {
		log.Fatalf("Mkdir %s failed: %v", nonDeterministicField, err)
	}

	// create snapshot dir
	err = os.RemoveAll(snapshotPath)
	if err != nil {
		log.Fatalf("Remove %s failed: %v", snapshotPath, err)
	}
	err = os.Mkdir(snapshotPath, 0700)
	if err != nil {
		log.Fatalf("Mkdir %s failed: %v", snapshotPath, err)
	}

	// get the service running in prod
	baseProdService := service.ProdService{
		ConfigPath: v1Config,
		DbPort:     databasePort,
		ListenPort: origListenPort,
		Bin:        v1Bin,
	}
	experimentalProdService := service.ProdService{
		ConfigPath: v2Config,
		DbPort:     databasePort,
		ListenPort: origListenPort,
		Bin:        v2Bin,
	}

	runCnt := 0

	// cloned prod database
	clonedDb, err := service.CloneNeonDatabase("replica", "main", false)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database %+v\n", clonedDb)

	// generate traffic patterns
	allPorts, err := requestsPorts(service.ListOfReqs1, runs, baseListenPort, expListenPort)
	if err != nil {
		log.Fatalf("Failed to generate traffic patterns: %v", err)
	}

	// run baseline service
	clonedDbB, err := service.CloneNeonDatabase("cloneB", clonedDb.Branch, false)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %v\n", clonedDbB)

	baselineService, err := service.Init(runCnt, []string{baseListenPort}, clonedDbB.Port, []service.ProdService{baseProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	baselineService.Run(service.ListOfReqs1)

	// run baseline service2
	clonedDbB2, err := service.CloneNeonDatabase("cloneB2", clonedDb.Branch, false)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %v\n", clonedDbB)

	runCnt += 1
	baselineService2, err := service.Init(runCnt, []string{baseListenPort}, clonedDbB2.Port, []service.ProdService{baseProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	baselineService2.Run(service.ListOfReqs1)

	if err := diff.GetNonDeterministic(baselineService, baselineService2); err != nil {
		log.Fatalf("Get non deterministic error failed: %v", err)
	}
	if runs == 2 {
		return
	}

	// run experimental service
	clonedDbE, err := service.CloneNeonDatabase("cloneE", clonedDb.Branch, true)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %+v\n", clonedDbE)

	runCnt += 1
	experientalService, err := service.Init(runCnt, []string{expListenPort}, clonedDbE.Port, []service.ProdService{experimentalProdService}, allPorts[runCnt])
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

	if runs == 3 {
		return
	}

	// run requests on both baseline and experiental
	clonedDbB1E1, err := service.CloneNeonDatabase("cloneB1E1", clonedDb.Branch, true)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %+v\n", clonedDbB1E1)
	runCnt += 1
	b1E1Service, err := service.Init(runCnt, []string{baseListenPort, expListenPort}, clonedDbB1E1.Port, []service.ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
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
	if runs == 4 {
		return
	}

	// run requests on both experiental and baseline
	clonedDbE1B1, err := service.CloneNeonDatabase("cloneE1B1", clonedDb.Branch, true)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %+v\n", clonedDbE1B1)
	runCnt += 1

	e1B1Service, err := service.Init(runCnt, []string{baseListenPort, expListenPort}, clonedDbE1B1.Port, []service.ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
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

	if runs == 5 {
		return
	}

	fmt.Println("Exiting program...")
	return
}
