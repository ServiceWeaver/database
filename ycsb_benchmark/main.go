package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type Metrics struct {
	Workload     string
	Plain        string
	Branch       string
	RecordCnt    int
	OperationCnt int
}

func RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload string, recordCnt, operationcount int) string {
	dbParams := map[string]string{"pg.host": dbHost, "pg.port": dbPort, "pg.user": dbUser, "pg.db": dbName, "pg.sslmode": "disable", "recordcount": strconv.Itoa(recordCnt), "operationcount": strconv.Itoa(operationcount)}
	runCmds := []string{"run", "postgresql", "-P", "workloads/" + workload, "--threads", "1"}

	for key, val := range dbParams {
		runCmds = append(runCmds, "-p")
		runCmds = append(runCmds, key+"="+val)
	}
	cmd := exec.Command(bin, runCmds...)

	cmd.Dir = "../../go-ycsb"
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(out))

	// Split the string into a slice of substrings
	result := strings.Split(string(out), "*\n")
	if len(result) > 0 {
		return result[len(result)-1]
	}
	return ""
}

func LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup string, recordCnt int) {
	RunSqlScript(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup)

	dbParams := map[string]string{"pg.host": dbHost, "pg.port": dbPort, "pg.user": dbUser, "pg.db": dbName, "pg.sslmode": "disable", "dropdata": "true", "recordcount": strconv.Itoa(recordCnt)}
	loadCmds := []string{"load", "postgresql", "-P", "workloads/" + workload, "--threads", "15"}

	for key, val := range dbParams {
		loadCmds = append(loadCmds, "-p")
		loadCmds = append(loadCmds, key+"="+val)
	}
	cmd := exec.Command(bin, loadCmds...)
	cmd.Dir = "../../go-ycsb"
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
		fmt.Println(string(out))
	}

	// Print the output.
	// fmt.Println(string(out))
}

func RunSqlScript(dbHost, dbUser, dbPort, dbName, bin, workload, script string) {
	dbParams := map[string]string{"-h": dbHost, "-p": dbPort, "-U": dbUser, "-d": dbName}
	branchCmds := []string{}

	for key, val := range dbParams {
		branchCmds = append(branchCmds, key)
		branchCmds = append(branchCmds, val)
	}
	branchCmds = append(branchCmds, "-f", script)

	cmd := exec.Command("psql", branchCmds...)
	fmt.Println(cmd)

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatal(err)
		fmt.Println(string(out))
	}

	// Print the output.
	// fmt.Println(string(out))
}

func main() {
	dbHost := "localhost"
	dbUser := "postgres"
	dbPort := "5433"
	dbName := "testdb"
	bin := "./bin/go-ycsb"

	recordCnts := []int{10000, 10000, 100000, 100000, 10000000}
	workloads := []string{"workloada", "workloadb", "workloadc", "workloadd", "workloade"}
	operationcounts := []int{100, 1000, 100, 1000, 1000}
	branchScript := "usertable.sql"
	cleanup := "cleanup.sql"
	metricsFile := "ycsb_metrics.json"

	if len(recordCnts) != len(operationcounts) {
		panic("record cnts and operation cnts does not match")
	}

	file, err := os.Create(metricsFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.WriteString("[\n")
	if err != nil {
		panic(err)
	}
	for j, workload := range workloads {
		for i, recordCnt := range recordCnts {
			var m Metrics
			m.RecordCnt = recordCnt
			m.Workload = workload
			m.OperationCnt = operationcounts[i]

			LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup, recordCnt)

			// Run on branched db
			RunSqlScript(dbHost, dbUser, dbPort, dbName, bin, workload, branchScript)
			m.Branch = RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, recordCnt, operationcounts[i])

			// Run on plain postgres
			LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup, recordCnt)
			m.Plain = RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, recordCnt, operationcounts[i])

			// write metrics into json file
			jsonData, err := json.MarshalIndent(m, "", "  ")
			if err != nil {
				panic(err)
			}

			// Write the JSON data to the file
			_, err = file.Write(jsonData)
			if err != nil {
				panic(err)
			}

			if i != len(recordCnts)-1 && j != len(workloads)-1 {
				_, err = file.WriteString(",\n")
				if err != nil {
					panic(err)
				}
			}
		}
	}

	_, err = file.WriteString("\n]")
	if err != nil {
		panic(err)
	}

}
