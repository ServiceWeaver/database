package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

const dumpDir = "dump"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "Debug / analyze R+/R- implementation")
	flag.Parse()

	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dumpDir, 0755); err != nil {
			fmt.Println("Error creating directory:", err)
		}
	}

	dbs := []string{"postgresql://postgres:postgres@localhost:5433/benchmark2", "postgresql://postgres:postgres@localhost:5433/benchmark"}

	metricsStats := map[string]map[string]*metrics{} // {Database: {table:metrics, table_with_primary_key:metrics}}

	for _, dbUrl := range dbs {
		idx := strings.LastIndex(dbUrl, "/")
		dbName := dbUrl[idx+1:]
		doltPort := "3306"
		table := "users"
		table_pk := "users_pk"

		metrics, err := benchmarkBranching([]string{table, table_pk}, dbName, dbUrl, doltPort, debug)
		if err != nil {
			fmt.Printf("benchmark branching failed, err=%s", err)
		}
		metricsStats[dbName] = metrics
	}
}
