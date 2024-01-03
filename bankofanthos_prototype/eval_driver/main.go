package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	baseBin           = "./../bankofanthos/bankofanthos"
	experientalBin    = "./../bankofanthos/bankofanthos_experimental"
	configPath        = "configs/"
	logPath           = "logs/"
	outPath           = "out/"
	baseConfig        = "../bankofanthos/weaver.toml"
	experientalConfig = "../bankofanthos/weaver_experimental.toml"
)

type ProdService struct {
	configPath   string
	databasePort string
	bin          string
	listenPort   string
}

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
			// half to baseline, half to experimental
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
	flag.StringVar(&expListenPort, "baseListenPort", "9003", "Listen port for baseline service.")
	flag.StringVar(&databasePort, "databasePort", "55432", "Listen port for experimental service.")
	flag.Parse()

	// create config dir if not exists
	err := os.RemoveAll(configPath)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(configPath, 0700)
	if err != nil {
		panic(err)
	}

	// create log dir if not exists
	err = os.RemoveAll(logPath)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(logPath, 0700)
	if err != nil {
		panic(err)
	}

	// create out dir
	err = os.RemoveAll(outPath)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(outPath, 0700)
	if err != nil {
		panic(err)
	}

	// get the service running in prod
	baseProdService := ProdService{
		configPath:   baseConfig,
		databasePort: databasePort,
		listenPort:   origListenPort,
		bin:          baseBin,
	}
	experimentalProdService := ProdService{
		configPath:   experientalConfig,
		databasePort: databasePort,
		listenPort:   origListenPort,
		bin:          experientalBin,
	}
	runCnt := 0

	// cloned prod database
	clonedDatabase, err := cloneDatabase("clone", "main")
	if err != nil {
		log.Fatalf("Cloned database failed, error=%v", err)
	}
	fmt.Printf("cloned database %+v", clonedDatabase)

	// generate traffic patterns
	allPorts, err := requestsPorts(listOfReqs1, runs, baseListenPort, expListenPort)
	if err != nil {
		panic(err)
	}

	// run baseline service
	clonedDatabaseB, err := cloneDatabase("cloneBase", clonedDatabase.branch)
	if err != nil {
		log.Fatalf("Cloned database failed, error=%v", err)
	}
	fmt.Printf("cloned database is: %+v\n", clonedDatabaseB)

	baselineService, err := Init(runCnt, []string{baseListenPort}, clonedDatabaseB.port, []ProdService{baseProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init service failed, error=%v", err)
		return
	}
	baselineService.run(listOfReqs1)
	eq1, err := outputEq(baselineService.outputPath, baselineService.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	eq2, err := outputEq(baselineService.dumpDbPath, baselineService.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, baselineService.runs, eq1 && eq2)

	// run experimental service
	clonedDatabaseE, err := cloneDatabase("cloneExp", clonedDatabase.branch)
	if err != nil {
		log.Fatalf("Cloned database failed, error=%v", err)
	}
	fmt.Printf("cloned database is: %+v\n", clonedDatabaseE)

	runCnt += 1
	experientalService, err := Init(runCnt, []string{expListenPort}, clonedDatabaseE.port, []ProdService{experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init service failed, error=%v", err)
		return
	}
	experientalService.run(listOfReqs1)

	eq1, err = outputEq(baselineService.outputPath, experientalService.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	eq2, err = outputEq(baselineService.dumpDbPath, experientalService.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, experientalService.runs, eq1 && eq2)

	// run requests on both baseline and experiental
	clonedDatabaseB1E1, err := cloneDatabase("cloneB1E1", clonedDatabase.branch)
	if err != nil {
		log.Fatalf("Cloned database failed, error=%v", err)
	}
	fmt.Printf("cloned database is: %+v\n", clonedDatabaseB1E1)
	runCnt += 1
	fmt.Printf("all ports length: %d\n", len(allPorts))
	b1E1Service, err := Init(runCnt, []string{baseListenPort, expListenPort}, clonedDatabaseB1E1.port, []ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init B1E1 service failed, error=%v", err)
		return
	}
	b1E1Service.run(listOfReqs1)

	eq1, err = outputEq(baselineService.outputPath, b1E1Service.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	eq2, err = outputEq(baselineService.dumpDbPath, b1E1Service.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, b1E1Service.runs, eq1 && eq2)

	// run requests on both experiental and baseline
	clonedDatabaseE1B1, err := cloneDatabase("cloneE1B1", clonedDatabase.branch)
	if err != nil {
		log.Fatalf("Cloned database failed, error=%v", err)
	}
	fmt.Printf("cloned database is: %+v\n", clonedDatabaseE1B1)
	runCnt += 1
	fmt.Printf("all ports length: %d\n", len(allPorts))
	e1B1Service, err := Init(runCnt, []string{baseListenPort, expListenPort}, clonedDatabaseE1B1.port, []ProdService{baseProdService, experimentalProdService}, allPorts[runCnt])
	if err != nil {
		log.Fatalf("Init B1E1 service failed, error=%v", err)
		return
	}
	e1B1Service.run(listOfReqs1)

	eq1, err = outputEq(baselineService.outputPath, e1B1Service.outputPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	eq2, err = outputEq(baselineService.dumpDbPath, b1E1Service.dumpDbPath)
	if err != nil {
		log.Fatalf("Failed to compare two output, error=%v", err)
		return
	}
	fmt.Printf("run %s and run %s is equal: %v\n", baselineService.runs, e1B1Service.runs, eq1 && eq2)

	fmt.Printf("exiting program...\n")
	return
}
