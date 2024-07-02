package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type operation struct {
	Time      map[string]time.Duration
	queries   []string
	QuerySize int
}

type branch struct {
	Time map[string]time.Duration
}

type diff struct {
	Time         map[string]time.Duration
	ModifiedRows int
}

type metrics struct {
	Table string
	DbUrl string

	Rows           int64
	TableSize      string            // MB unit
	DbSizeIncrease map[string]string // MB unit

	Branch *branch

	// performance
	Writes  []*operation
	Reads   []*operation
	Deletes []*operation
	Diffs   []*diff
}

func newMetrics(table string, dbUrl string) (*metrics, error) {
	insertQueries := [][]string{createInsertQueries(1, table), createInsertQueries(100, table), createInsertQueries(1000, table)}
	writes := []*operation{
		{queries: insertQueries[0], QuerySize: len(insertQueries[0]), Time: map[string]time.Duration{}},
		{queries: insertQueries[1], QuerySize: len(insertQueries[1]), Time: map[string]time.Duration{}},
		{queries: insertQueries[2], QuerySize: len(insertQueries[2]), Time: map[string]time.Duration{}},
	}

	var deletes []*operation
	deleteQueries := createDeleteQueries(table)
	delete := &operation{queries: deleteQueries, QuerySize: len(deleteQueries), Time: map[string]time.Duration{}}
	deletes = append(deletes, delete)

	var reads []*operation
	readQueries := createReadQueries(table)
	read := &operation{queries: readQueries, QuerySize: len(readQueries), Time: map[string]time.Duration{}}
	reads = append(reads, read)

	branch := &branch{Time: map[string]time.Duration{}}

	var diffs []*diff
	for _, w := range writes {
		diff := &diff{Time: map[string]time.Duration{}, ModifiedRows: len(w.queries)}
		diffs = append(diffs, diff)
	}

	m := &metrics{DbUrl: dbUrl, Table: table, Writes: writes, Deletes: deletes, Reads: reads, Branch: branch, Diffs: diffs, DbSizeIncrease: map[string]string{}}
	if err := m.getTableSize(); err != nil {
		return nil, err
	}

	return m, nil
}

func convertSizeToMb(size string) (float64, error) {
	i := strings.Index(size, "MB")
	if i != -1 {
		n, err := strconv.ParseFloat(size[:i-1], 64)
		return n, err
	}

	i = strings.Index(size, "kB")
	if i != -1 {
		n, err := strconv.ParseFloat(size[:i-1], 64)
		return n / 1024, err
	}

	i = strings.Index(size, "bytes")
	if i != -1 {
		n, err := strconv.ParseFloat(size[:i-1], 64)
		return n / 1024 / 1024, err
	}

	return 0, fmt.Errorf("unit does not match")
}

func (m *metrics) getTableSize() error {
	p, err := newPostgresClient(m.DbUrl)
	if err != nil {
		return err
	}

	var count int64
	if err := p.client.QueryRow(fmt.Sprintf("select count(*) from %s;", m.Table)).Scan(&count); err != nil {
		return err
	}
	m.Rows = count

	var size string
	if err := p.client.QueryRow(fmt.Sprintf("SELECT pg_size_pretty(pg_total_relation_size('%s'));", m.Table)).Scan(&size); err != nil {
		return err
	}

	s, err := convertSizeToMb(size)
	if err != nil {
		return err
	}
	m.TableSize = fmt.Sprintf("%.8f MB", s)

	p.close()
	return nil
}

func (m *metrics) printMetrics() error {
	fmt.Printf("Table %s,", m.Table)
	fmt.Printf("Table has %d rows,", m.Rows)
	fmt.Printf("Table size is %s\n", m.TableSize)

	types := []string{Postgres.String(), Dolt.String(), RPlusRMinus.String()}

	fmt.Println("Branching")
	for _, s := range types {
		t := m.Branch.Time[s]
		fmt.Printf("%s: %s, %s;\t", s, t, m.DbSizeIncrease[s])
	}

	fmt.Println()
	fmt.Println()
	for _, w := range m.Writes {
		fmt.Printf("Write %d rows\n", len(w.queries))
		for _, s := range types {
			t := w.Time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
		fmt.Println()
	}

	fmt.Println()

	for _, d := range m.Deletes {
		fmt.Printf("Delete %d queries\n", len(d.queries))
		for _, s := range types {
			t := d.Time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
		fmt.Println()
	}

	fmt.Println()
	for _, r := range m.Reads {
		fmt.Printf("Read %d queries\n", len(r.queries))
		for _, s := range types {
			t := r.Time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
	}

	fmt.Println()
	fmt.Println()
	for _, d := range m.Diffs {
		fmt.Printf("Diffing %d modified rows with table rows %d\n", d.ModifiedRows, m.Rows)
		for _, s := range types {
			t := d.Time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
		fmt.Println()
	}

	fmt.Println()
	return nil
}
