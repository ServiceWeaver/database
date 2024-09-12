package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type latency struct {
	times []time.Duration
	Sum   time.Duration
	Std   time.Duration // standard deviation
	Mean  time.Duration
}
type operation struct {
	Time      map[string]*latency //{R+R-/Dolt/Postgres: {t1,t2,....,tn}}
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
	Writes []*operation
	Reads  []map[string]*operation
	Diffs  []*diff
}

func newMetrics(table string, dbUrl string) (*metrics, error) {
	insertQueries := [][]string{createInsertQueries(1, table), createInsertQueries(100, table), createInsertQueries(1000, table), createInsertQueries(10000, table), createInsertQueries(100000, table)}
	writes := []*operation{
		{queries: insertQueries[0], QuerySize: len(insertQueries[0]), Time: map[string]*latency{}},
		{queries: insertQueries[1], QuerySize: len(insertQueries[1]), Time: map[string]*latency{}},
		{queries: insertQueries[2], QuerySize: len(insertQueries[2]), Time: map[string]*latency{}},
		{queries: insertQueries[3], QuerySize: len(insertQueries[3]), Time: map[string]*latency{}},
		{queries: insertQueries[4], QuerySize: len(insertQueries[4]), Time: map[string]*latency{}},
	}

	var reads []map[string]*operation
	readmap := map[string]*operation{}
	readQueries := createReadQueries(table)
	for _, q := range readQueries {
		read := &operation{queries: []string{q}, QuerySize: readCntPerQuery, Time: map[string]*latency{}}
		readmap[q] = read
	}

	reads = append(reads, readmap)

	branch := &branch{Time: map[string]time.Duration{}}

	var diffs []*diff
	for _, w := range writes {
		diff := &diff{Time: map[string]time.Duration{}, ModifiedRows: len(w.queries)}
		diffs = append(diffs, diff)
	}

	m := &metrics{DbUrl: dbUrl, Table: table, Writes: writes, Reads: reads, Branch: branch, Diffs: diffs, DbSizeIncrease: map[string]string{}}
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

func (m *metrics) printMetrics(plusMinusOnly bool) error {
	fmt.Printf("Table %s,", m.Table)
	fmt.Printf("Table has %d rows,", m.Rows)
	fmt.Printf("Table size is %s\n", m.TableSize)

	types := []string{Postgres.String(), Dolt.String(), RPlusRMinus.String()}
	if plusMinusOnly {
		types = []string{RPlusRMinus.String()}
	}

	fmt.Println("Branching")
	for _, s := range types {
		t := m.Branch.Time[s]
		fmt.Printf("%s: %s, %s;\t", s, t, m.DbSizeIncrease[s])
	}

	fmt.Println()
	fmt.Println()
	for _, w := range m.Writes {
		fmt.Printf("Write %d rows mean\n", len(w.queries))
		for _, s := range types {
			t := w.Time[s]
			fmt.Printf("%s: %s;\t", s, t.Mean)
		}
		fmt.Println()
	}

	fmt.Println()
	for _, reads := range m.Reads {
		for q, r := range reads {
			fmt.Printf("Read query %d times mean %s", len(r.Time[types[0]].times), q)
			for _, s := range types {
				t := r.Time[s]
				fmt.Printf("%s: %s;\t", s, t.Mean)
			}
			fmt.Println()
			fmt.Println()
		}
	}

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

	return &latency{times: durations, Sum: sum, Std: time.Duration(math.Sqrt(variance)), Mean: mean}
}
