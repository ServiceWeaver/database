package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	v1Bin      = "./../bankofanthos/bankofanthos"
	v2Bin      = "./../bankofanthos/bankofanthos_experimental"
	configPath = "configs/"
	logPath    = "logs/"
	outPath    = "out/"
	v1Config   = "../bankofanthos/weaver.toml"
	v2Config   = "../bankofanthos/weaver_experimental.toml"
)

// ProdService defines binary will be running in prod
type ProdService struct {
	configPath string
	dbPort     string
	bin        string
	listenPort string
}

// requestsPorts generates traffic pattern, each request will be directed to either baseline service port
// or experimental service port
func requestsPorts(l listOfReqs, numOfRuns int, baseListenPort, expListenPort string) ([][]string, error) {
	reqCount := len(l())
	allPorts := [][]string{}

	for r := 0; r < numOfRuns; r++ {
		ports := []string{}
		if r == 0 {
			// for all baseline traffic
			for i := 0; i < reqCount; i++ {
				ports = append(ports, baseListenPort)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 1 {
			// for all experimental traffic
			for i := 0; i < reqCount; i++ {
				ports = append(ports, expListenPort)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 2 {
			// half to baseline, half to experimental
			for i := 0; i < reqCount/2; i++ {
				ports = append(ports, baseListenPort)
			}
			for i := reqCount / 2; i < reqCount; i++ {
				ports = append(ports, expListenPort)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 3 {
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
	flag.IntVar(&runs, "runs", 4, "Total runs for the same request set.")
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

	// get the service running in prod
	baseProdService := ProdService{
		configPath: v1Config,
		dbPort:     databasePort,
		listenPort: origListenPort,
		bin:        v1Bin,
	}
	experimentalProdService := ProdService{
		configPath: v2Config,
		dbPort:     databasePort,
		listenPort: origListenPort,
		bin:        v2Bin,
	}

	runCnt := 0

	// cloned prod database
	clonedDb, err := cloneDatabase("replica", "main")
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database %+v\n", clonedDb)

	// generate traffic patterns
	allPorts, err := requestsPorts(listOfReqs1, runs, baseListenPort, expListenPort)
	if err != nil {
		log.Fatalf("Failed to generate traffic patterns: %v", err)
	}

	// run baseline service
	clonedDbB, err := cloneDatabase("cloneB", clonedDb.branch)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %v\n", clonedDbB)

	baselineService, err := Init(runCnt, []string{baseListenPort}, clonedDbB.port, []ProdService{baseProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	baselineService.run(listOfReqs1)
	eq1, err := outputEq(baselineService.outputPath, baselineService.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	eq2, err := outputEq(baselineService.dumpDbPath, baselineService.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}

	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, baselineService.runs, eq1 && eq2)

	if runs == 1 {
		return
	}
	// run experimental service
	clonedDbE, err := cloneDatabase("cloneE", clonedDb.branch)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %+v\n", clonedDbE)

	runCnt += 1
	experientalService, err := Init(runCnt, []string{expListenPort}, clonedDbE.port, []ProdService{experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init service failed: %v", err)
	}
	experientalService.run(listOfReqs1)

	eq1, err = outputEq(baselineService.outputPath, experientalService.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	eq2, err = outputEq(baselineService.dumpDbPath, experientalService.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, experientalService.runs, eq1 && eq2)

	if runs == 2 {
		return
	}

	// run requests on both baseline and experiental
	clonedDbB1E1, err := cloneDatabase("cloneB1E1", clonedDb.branch)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %+v\n", clonedDbB1E1)
	runCnt += 1
	b1E1Service, err := Init(runCnt, []string{baseListenPort, expListenPort}, clonedDbB1E1.port, []ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init B1E1 service failed: %v", err)
	}
	b1E1Service.run(listOfReqs1)

	eq1, err = outputEq(baselineService.outputPath, b1E1Service.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	eq2, err = outputEq(baselineService.dumpDbPath, b1E1Service.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, b1E1Service.runs, eq1 && eq2)

	// run requests on both experiental and baseline
	clonedDbE1B1, err := cloneDatabase("cloneE1B1", clonedDb.branch)
	if err != nil {
		log.Fatalf("Cloned database failed: %v", err)
	}
	fmt.Printf("Cloned database is: %+v\n", clonedDbE1B1)
	runCnt += 1

	e1B1Service, err := Init(runCnt, []string{baseListenPort, expListenPort}, clonedDbE1B1.port, []ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init B1E1 service failed: %v", err)
	}
	e1B1Service.run(listOfReqs1)

	if runs == 3 {
		return
	}

	eq1, err = outputEq(baselineService.outputPath, e1B1Service.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	eq2, err = outputEq(baselineService.dumpDbPath, e1B1Service.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two outputs: %v", err)
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, e1B1Service.runs, eq1 && eq2)

	if runs == 4 {
		return
	}

	fmt.Println("Exiting program...")
	return
}
