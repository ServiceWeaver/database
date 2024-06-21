package main

import (
	"fmt"
	"strconv"
	"time"
)

type operation struct {
	time    map[string]time.Duration
	queries []string
}

type branch struct {
	time map[string]time.Duration
}

type diff struct {
	time         map[string]time.Duration
	modifiedRows int
}

type metrics struct {
	table string
	dbUrl string

	rows   int64
	dbSize string

	branch *branch

	// performance
	writes  []*operation
	reads   []*operation
	deletes []*operation
	diffs   []*diff
}

func newMetrics(table string, dbUrl string) (*metrics, error) {
	writes := []*operation{
		{queries: createInsertQueries(1, table), time: map[string]time.Duration{}},
		{queries: createInsertQueries(10, table), time: map[string]time.Duration{}},
		{queries: createInsertQueries(100, table), time: map[string]time.Duration{}},
	}

	var deletes []*operation
	delete := &operation{queries: createDeleteQueries(table), time: map[string]time.Duration{}}
	deletes = append(deletes, delete)

	var reads []*operation
	read := &operation{queries: createReadQueries(table), time: map[string]time.Duration{}}
	reads = append(reads, read)

	branch := &branch{time: map[string]time.Duration{}}

	var diffs []*diff
	for _, w := range writes {
		diff := &diff{time: map[string]time.Duration{}, modifiedRows: len(w.queries)}
		diffs = append(diffs, diff)
	}

	m := &metrics{dbUrl: dbUrl, table: table, writes: writes, deletes: deletes, reads: reads, branch: branch, diffs: diffs}
	if err := m.getTableSize(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *metrics) getTableSize() error {
	p, err := newPostgresClient(m.dbUrl)
	if err != nil {
		return err
	}

	var count int64
	if err := p.client.QueryRow(fmt.Sprintf("select count(*) from %s;", m.table)).Scan(&count); err != nil {
		return err
	}
	m.rows = count

	var size string
	if err := p.client.QueryRow(fmt.Sprintf("SELECT pg_size_pretty(pg_total_relation_size('%s'));", m.table)).Scan(&size); err != nil {
		return err
	}

	if size[len(size)-2:] == "kB" {
		n, err := strconv.ParseFloat(size[:len(size)-3], 64)
		if err != nil {
			return fmt.Errorf("failed to convert string to int, err=%s", err)
		}

		m.dbSize = fmt.Sprintf("%.2f MB", n/1024)
	} else {
		m.dbSize = size
	}

	p.close()
	return nil
}

func (m *metrics) printMetrics() error {
	fmt.Printf("Table %s,", m.table)
	fmt.Printf("Table has %d rows,", m.rows)
	fmt.Printf("Table size is %s\n", m.dbSize)

	types := []string{Postgres.String(), Dolt.String(), RPlusRMinus.String()}

	fmt.Println("Branching")
	for _, s := range types {
		t := m.branch.time[s]
		fmt.Printf("%s: %s;\t", s, t)
	}

	fmt.Println()
	fmt.Println()
	for _, w := range m.writes {
		fmt.Printf("Write %d rows\n", len(w.queries))
		for _, s := range types {
			t := w.time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
		fmt.Println()
	}

	fmt.Println()

	for _, d := range m.deletes {
		fmt.Printf("Delete %d queries\n", len(d.queries))
		for _, s := range types {
			t := d.time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
		fmt.Println()
	}

	fmt.Println()
	for _, r := range m.reads {
		fmt.Printf("Read %d queries\n", len(r.queries))
		for _, s := range types {
			t := r.time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
	}

	fmt.Println()
	fmt.Println()
	for _, d := range m.diffs {
		fmt.Printf("Diffing %d modified rows with table rows %d\n", d.modifiedRows, m.rows)
		for _, s := range types {
			t := d.time[s]
			fmt.Printf("%s: %s;\t", s, t)
		}
		fmt.Println()
	}

	fmt.Println()
	return nil
}
