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
	Workload  string
	Plain     string
	Branch    string
	RecordCnt int
}

func RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload string, recordCnt int) string {
	dbParams := map[string]string{"pg.host": dbHost, "pg.port": dbPort, "pg.user": dbUser, "pg.db": dbName, "pg.sslmode": "disable", "recordcount": strconv.Itoa(recordCnt)}
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
	loadCmds := []string{"load", "postgresql", "-P", "workloads/" + workload, "--threads", "1"}

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

	recordCnts := []int{1000, 10000, 100000}
	workloads := []string{"workloada", "workloadb", "workloadc", "workloadd", "workloade"}
	branchScript := "usertable.sql"
	cleanup := "cleanup.sql"

	var metricsLst []Metrics
	for _, workload := range workloads {
		for _, recordCnt := range recordCnts {
			var m Metrics
			m.RecordCnt = recordCnt
			m.Workload = workload

			LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup, recordCnt)

			// Run on branched db
			RunSqlScript(dbHost, dbUser, dbPort, dbName, bin, workload, branchScript)
			m.Branch = RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, recordCnt)

			// Run on plain postgres
			LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup, recordCnt)
			m.Plain = RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, recordCnt)
			metricsLst = append(metricsLst, m)
		}
	}

	// write metrics into json file
	jsonData, err := json.MarshalIndent(metricsLst, "", "  ")
	if err != nil {
		panic(err)
	}

	file, err := os.Create("ycsb_metrics.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		panic(err)
	}
}
