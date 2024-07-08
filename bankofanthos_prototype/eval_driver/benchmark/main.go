package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const dumpDir = "dump"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "Debug / analyze R+/R- implementation")
	flag.Parse()

	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dumpDir, 0755); err != nil {
			log.Panicf("Error creating directory, err=%s", err)
		}
	}

	dbs := []string{"postgresql://postgres:postgres@localhost:5433/benchmark_1mb", "postgresql://postgres:postgres@localhost:5433/benchmark_20mb", "postgresql://postgres:postgres@localhost:5433/benchmark_100mb"}

	metricsStats := map[string]map[string]*metrics{} // {Database: {table:metrics, table_with_primary_key:metrics}}

	for _, dbUrl := range dbs {
		idx := strings.LastIndex(dbUrl, "/")
		dbName := dbUrl[idx+1:]
		doltPort := "3306"
		table := "users"
		table_pk := "users_pk"

		metrics, err := benchmarkBranching([]string{table, table_pk}, dbName, dbUrl, doltPort, debug)
		if err != nil {
			log.Panicf("benchmark branching failed, err=%s", err)
		}
		metricsStats[dbName] = metrics
	}

	// write metrics stats to json file
	jsonData, err := json.MarshalIndent(metricsStats, "", "    ")
	if err != nil {
		log.Panicf("Error marshalling json, err=%s", err)
	}

	// Write JSON data to a file
	err = os.WriteFile(filepath.Join(dumpDir, "metrics.json"), jsonData, 0644)
	if err != nil {
		log.Panicf("Error writing json file, err=%s", err)
	}
}
