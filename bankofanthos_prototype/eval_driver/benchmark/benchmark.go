package main

import (
	"bankofanthos_prototype/eval_driver/dbbranch"
	"bankofanthos_prototype/eval_driver/utility"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

type diffType int

const (
	RPlusRMinus diffType = iota + 1
	Postgres
	Dolt
	readCntPerQuery = 250
)

func (d diffType) String() string {
	return [...]string{"RPlusRMinus", "Postgres", "Dolt"}[d-1]
}

func benchmarkBranching(tables []string, dbName string, dbUrl string, doltPort string, debug bool) (map[string]*metrics, error) {
	ctx := context.Background()

	benchmarkDb := &utility.Database{Name: dbName, Url: dbUrl}

	metrics := map[string]*metrics{}

	for _, table := range tables {
		m, err := newMetrics(table, dbUrl)
		if err != nil {
			return nil, err
		}

		if err := m.plusMinusCloning(ctx, benchmarkDb, debug); err != nil {
			return nil, err
		}

		if err := m.baselineCloning(benchmarkDb, table); err != nil {
			return nil, err
		}

		if err := m.doltCloning(doltPort, table); err != nil {
			return nil, err
		}

		m.printMetrics()
		metrics[table] = m
	}

	return metrics, nil
}

func (m *metrics) plusMinusCloning(ctx context.Context, benchmarkDb *utility.Database, debug bool) error {
	db, err := pgxpool.Connect(ctx, benchmarkDb.Url)
	if err != nil {
		log.Panicf("Connect to DB %s failed with %s: %v", benchmarkDb.Name, benchmarkDb.Url, err)
	}
	defer db.Close()

	branchName := "n"
	brancher, err := dbbranch.NewBrancher(ctx, db)
	if err != nil {
		return fmt.Errorf("create new branch for DB %s failed with %s: %v", benchmarkDb.Name, benchmarkDb.Url, err)
	}
	main, err := brancher.Branch(ctx, "main")
	if err != nil {
		return fmt.Errorf("branch %s failed: %v", branchName, err)
	}
	defer main.Delete(ctx)
	if err := main.Commit(ctx); err != nil {
		return err
	}

	pg, err := newPostgresClient(benchmarkDb.Url)
	if err != nil {
		return fmt.Errorf("failed to create new postgres client, %s", err)
	}
	defer pg.close()

	start := time.Now()
	startSize, err := pg.getDbSize(benchmarkDb.Name)
	if err != nil {
		return err
	}

	b, err := brancher.Branch(ctx, branchName)
	if err != nil {
		return fmt.Errorf("branch %s failed: %v", branchName, err)
	}
	duration := time.Since(start)
	m.Branch.Time[RPlusRMinus.String()] = duration
	endSize, err := pg.getDbSize(benchmarkDb.Name)
	if err != nil {
		return err
	}
	m.DbSizeIncrease[RPlusRMinus.String()] = fmt.Sprintf("%.8f MB", endSize-startSize)

	operations := [][]*operation{m.Writes, m.Deletes}

	for _, op := range operations {
		for _, w := range op {
			durations := make([]time.Duration, w.QuerySize)
			for i, q := range w.queries {
				start = time.Now()
				if _, err := pg.client.Exec(q); err != nil {
					return err
				}
				durations[i] = time.Since(start)
			}

			w.Time[RPlusRMinus.String()] = newLatency(durations)
		}

	}

	// reads
	for _, rMap := range m.Reads {
		for _, r := range rMap {
			durations := make([]time.Duration, readCntPerQuery)
			for _, q := range r.queries {
				for i := 0; i < readCntPerQuery; i++ {
					start = time.Now()
					if _, err := pg.client.Exec(q); err != nil {
						return err
					}
					durations[i] = time.Since(start)
				}
			}
			r.Time[RPlusRMinus.String()] = newLatency(durations)
		}
	}

	if !debug {
		if err := b.Commit(ctx); err != nil {
			return err
		}
	}

	for _, d := range m.Diffs {
		start = time.Now()
		_, err = brancher.ComputeDiffAtN(ctx, main, b, 1)
		if err != nil {
			log.Panicf("failed to compute diff: %v", err)
		}
		duration = time.Since(start)
		d.Time[RPlusRMinus.String()] = duration
	}

	if !debug {
		if err := b.Delete(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *metrics) baselineCloning(benchmarkDb *utility.Database, table string) error {
	snapshotPath := filepath.Join(dumpDir, "postgres.sql")

	start := time.Now()
	if err := utility.TakeSnapshot(benchmarkDb, snapshotPath); err != nil {
		return err
	}
	snapshot, err := utility.RestoreSnapshot(snapshotPath, benchmarkDb)
	if err != nil {
		return err
	}
	duration := time.Since(start)
	m.Branch.Time[Postgres.String()] = duration
	defer utility.CloseSnapshotDB(benchmarkDb, snapshot.Name)

	pg, err := newPostgresClient(snapshot.Url)
	if err != nil {
		return fmt.Errorf("failed to create new postgres client, %s", err)
	}
	defer pg.close()

	s, err := pg.getDbSize(snapshot.Name)
	if err != nil {
		return fmt.Errorf("failed to get %s size, err=%s", snapshot.Name, err)
	}

	m.DbSizeIncrease[Postgres.String()] = fmt.Sprintf("%.8f MB", s)

	if err := pg.copyTable(table); err != nil {
		return err
	}

	operations := [][]*operation{m.Writes, m.Deletes}
	for _, op := range operations {
		for _, w := range op {
			durations := make([]time.Duration, w.QuerySize)
			for i, q := range w.queries {
				start = time.Now()
				if _, err := pg.client.Exec(q); err != nil {
					return err
				}
				durations[i] = time.Since(start)
			}
			w.Time[Postgres.String()] = newLatency(durations)
		}
	}

	// reads
	for _, rMap := range m.Reads {
		for _, r := range rMap {
			durations := make([]time.Duration, readCntPerQuery)
			for _, q := range r.queries {
				for i := 0; i < readCntPerQuery; i++ {
					start = time.Now()
					if _, err := pg.client.Exec(q); err != nil {
						return err
					}
					durations[i] = time.Since(start)
				}
			}
			r.Time[Postgres.String()] = newLatency(durations)
		}
	}

	for _, d := range m.Diffs {
		start = time.Now()
		if err := pg.postgresDiff(table); err != nil {
			return fmt.Errorf("failed to diff two tables. %s", err)
		}
		duration = time.Since(start)
		d.Time[Postgres.String()] = duration
	}

	return nil
}

func (m *metrics) doltCloning(port string, table string) error {
	d, err := newDoltClient(port, table)
	if err != nil {
		return err
	}
	if err := d.convertPostgres(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go d.start(ctx)
	defer d.stop(ctx, cancel)

	// wait for dolt service up
	time.Sleep(30 * time.Millisecond)

	if err := d.loadData(); err != nil {
		return fmt.Errorf("load data failed, err=%s", err)
	}

	if err := d.connect(); err != nil {
		return fmt.Errorf("connect to mysql client failed, %s", err)
	}

	if err := d.commit(); err != nil {
		return err
	}

	startSize, err := d.getDatabaseSize()
	if err != nil {
		return err
	}
	start := time.Now()
	d.createNewBranch("n")
	duration := time.Since(start)
	m.Branch.Time[Dolt.String()] = duration

	endSize, err := d.getDatabaseSize()
	if err != nil {
		return err
	}
	m.DbSizeIncrease[Dolt.String()] = fmt.Sprintf("%.8f MB", endSize-startSize)

	operations := [][]*operation{m.Writes, m.Deletes}
	for _, op := range operations {
		for _, w := range op {
			durations := make([]time.Duration, w.QuerySize)
			for i, q := range w.queries {
				start = time.Now()
				if _, err := d.client.Exec(q); err != nil {
					return err
				}
				durations[i] = time.Since(start)
			}
			w.Time[Dolt.String()] = newLatency(durations)
		}
	}

	// reads
	for _, rMap := range m.Reads {
		for _, r := range rMap {
			durations := make([]time.Duration, readCntPerQuery)
			for _, q := range r.queries {
				for i := 0; i < readCntPerQuery; i++ {
					start = time.Now()
					if _, err := d.client.Exec(q); err != nil {
						return err
					}
					durations[i] = time.Since(start)
				}
			}
			r.Time[Dolt.String()] = newLatency(durations)
		}
	}

	if err := d.commit(); err != nil {
		return err
	}

	for _, diff := range m.Diffs {
		start = time.Now()
		if err := d.diffBranch(); err != nil {
			return err
		}
		duration = time.Since(start)
		diff.Time[Dolt.String()] = duration
	}

	return nil
}
