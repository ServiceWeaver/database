package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const dumpDir = "dump"

type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *arrayFlags) Set(value string) error {
	if i == nil {
		return fmt.Errorf("array flag is null")
	}
	*i = append(*i, value)
	return nil
}

func main() {
	var debug bool
	var dbs arrayFlags

	flag.BoolVar(&debug, "debug", false, "Debug / analyze R+/R- implementation")
	flag.Var(&dbs, "dbUrlLists", "Database url lists for benchmark to run")
	flag.Parse()

	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dumpDir, 0755); err != nil {
			log.Panicf("Error creating directory, err=%s", err)
		}
	}

	metricsStats := map[string]map[string]*metrics{} // {Database: {table:metrics, table_with_primary_key:metrics}}

	for _, dbUrl := range dbs {
		idx := strings.LastIndex(dbUrl, "/")
		dbName := dbUrl[idx+1:]
		doltPort := "3306"
		table := "users"
		table_pk := "users_pk"

		metrics, err := benchmarkBranching([]string{table, table_pk}, dbName, dbUrl, doltPort, debug, true)
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
