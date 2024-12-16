package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type latency struct {
	times []time.Duration
	Sum   string
	Std   string // standard deviation
	Mean  string
}

type Metrics struct {
	Workload     string
	Plain        latency
	Branch       latency
	RecordCnt    int
	OperationCnt int
}

func RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload string, recordCnt, operationcount int) time.Duration {
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
	result = strings.Split(result[len(result)-1], "\n")

	// Regular expression to match the time with optional "m"
	re := regexp.MustCompile(`takes (\d+\.\d+)m?s`)
	match := re.FindStringSubmatch(result[0])

	var duration time.Duration
	if len(match) == 2 {
		timeString := match[1]
		timeFloat, err := strconv.ParseFloat(timeString, 64)
		if err != nil {
			panic(err)
		}

		// Check if the time is in seconds, milliseconds or minutes
		if len(match[0]) == len("takes "+timeString+"s") {
			duration = time.Duration(timeFloat * float64(time.Second))
		} else if len(match[0]) == len("takes "+timeString+"ms") {
			duration = time.Duration(timeFloat * float64(time.Millisecond))
		} else { // minutes
			duration = time.Duration(timeFloat * float64(time.Minute))
		}

		fmt.Println(duration) // Output: 16m27.511228s
	} else {
		fmt.Println("Time not found in the string")
	}
	// fmt.Printf("result:%s\n", result)
	// result = strings.Split(result[0], "\n")
	// fmt.Printf("result:%s\n", result)

	return duration
}

func LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup string, recordCnt int) {
	RunSqlScript(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup)

	dbParams := map[string]string{"pg.host": dbHost, "pg.port": dbPort, "pg.user": dbUser, "pg.db": dbName, "pg.sslmode": "disable", "dropdata": "true", "recordcount": strconv.Itoa(recordCnt)}
	loadCmds := []string{"load", "postgresql", "-P", "workloads/" + workload, "--threads", "20"}

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

func newLatency(durations []time.Duration) *latency {
	if len(durations) == 0 {
		return nil
	}
	var sum time.Duration
	for _, t := range durations {
		sum = sum + t
	}
	mean := sum / time.Duration(len(durations))

	var variance float64
	for _, d := range durations {
		variance += math.Pow(float64(d-mean), 2)
	}
	variance /= float64(len(durations))

	return &latency{times: durations, Sum: sum.String(), Std: time.Duration(math.Sqrt(variance)).String(), Mean: mean.String()}
}

func main() {
	dbHost := "localhost"
	dbUser := "postgres"
	dbPort := "5433"
	dbName := "testdb"
	bin := "./bin/go-ycsb"

	recordCnt := 50000000
	workloads := []string{"workloada", "workloadb", "workloadc", "workloadd", "workloade", "workloadf"}
	operationcount := 1000
	branchScript := "usertable.sql"
	cleanup := "cleanup.sql"
	metricsFile := "ycsb_metrics.json"

	file, err := os.Create(metricsFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.WriteString("[\n")
	if err != nil {
		panic(err)
	}

	for _, workload := range workloads {
		var m Metrics
		m.RecordCnt = recordCnt
		m.Workload = workload
		m.OperationCnt = operationcount

		var plainDuration []time.Duration
		var branchDuration []time.Duration

		for _ = range 1 {
			LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup, recordCnt)
			// Run on branched db
			RunSqlScript(dbHost, dbUser, dbPort, dbName, bin, workload, branchScript)
			branchDuration = append(branchDuration, RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, recordCnt, operationcount))

			// Run on plain postgres
			LoadBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, cleanup, recordCnt)
			plainDuration = append(plainDuration, RunBenchmark(dbHost, dbUser, dbPort, dbName, bin, workload, recordCnt, operationcount))
		}

		plainLatency := newLatency(plainDuration)
		branchLatency := newLatency(branchDuration)
		m.Plain = *plainLatency
		m.Branch = *branchLatency

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

		_, err = file.WriteString(",\n")
		if err != nil {
			panic(err)
		}
	}

	_, err = file.WriteString("]")
	if err != nil {
		panic(err)
	}
}
