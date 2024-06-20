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
	if err := main.Commit(ctx); err != nil {
		return err
	}

	start := time.Now()
	b, err := brancher.Branch(ctx, branchName)
	if err != nil {
		return fmt.Errorf("branch %s failed: %v", branchName, err)
	}
	duration := time.Since(start)
	m.branch.time[RPlusRMinus.String()] = duration

	pg, err := newPostgresClient(benchmarkDb.Url)
	if err != nil {
		return fmt.Errorf("failed to create new postgres client, %s", err)
	}

	for _, q := range m.reads[0].queries {
		if _, err := pg.client.Exec(q); err != nil {
			return err
		}
	}

	operations := [][]*operation{m.writes, m.deletes, m.reads}

	for _, op := range operations {
		for _, w := range op {
			start = time.Now()
			for _, q := range w.queries {
				if _, err := pg.client.Exec(q); err != nil {
					return err
				}
			}
			duration = time.Since(start)
			w.time[RPlusRMinus.String()] = duration
		}

	}

	if !debug {
		if err := b.Commit(ctx); err != nil {
			return err
		}
	}

	for _, d := range m.diffs {
		start = time.Now()
		_, err = brancher.ComputeDiffAtN(ctx, main, b, 1)
		if err != nil {
			log.Panicf("failed to compute diff: %v", err)
		}
		duration = time.Since(start)
		d.time[RPlusRMinus.String()] = duration
	}

	if !debug {
		if err := b.Delete(ctx); err != nil {
			return err
		}
	}
	if err := main.Delete(ctx); err != nil {
		return err
	}

	if err := pg.close(); err != nil {
		return err
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
	m.branch.time[Postgres.String()] = duration

	pg, err := newPostgresClient(snapshot.Url)
	if err != nil {
		return fmt.Errorf("failed to create new postgres client, %s", err)
	}

	if err := pg.copyTable(table); err != nil {
		return err
	}

	operations := [][]*operation{m.writes, m.deletes, m.reads}
	for _, op := range operations {
		start = time.Now()
		for _, w := range op {
			for _, q := range w.queries {
				if _, err := pg.client.Exec(q); err != nil {
					return err
				}
			}
			duration = time.Since(start)
			w.time[Postgres.String()] = duration
		}
	}

	for _, d := range m.diffs {
		start = time.Now()
		if err := pg.postgresDiff(table); err != nil {
			return fmt.Errorf("failed to diff two tables. %s", err)
		}
		duration = time.Since(start)
		d.time[Postgres.String()] = duration
	}

	if err := pg.close(); err != nil {
		return fmt.Errorf("failed to close postgres client, %s", err)
	}
	if err := utility.CloseSnapshotDB(benchmarkDb, snapshot.Name); err != nil {
		return fmt.Errorf("failed to close snapshot db, %s", err)
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
		return err
	}

	if err := d.connect(); err != nil {
		return fmt.Errorf("connect to mysql client failed, %s", err)
	}

	if err := d.commit(); err != nil {
		return err
	}

	start := time.Now()
	d.createNewBranch("n")
	duration := time.Since(start)
	m.branch.time[Dolt.String()] = duration

	operations := [][]*operation{m.writes, m.deletes, m.reads}
	for _, op := range operations {
		for _, w := range op {
			start = time.Now()
			for _, q := range w.queries {
				if _, err := d.client.Exec(q); err != nil {
					return err
				}
			}
			duration = time.Since(start)
			w.time[Dolt.String()] = duration
		}
	}

	if err := d.commit(); err != nil {
		return err
	}

	for _, diff := range m.diffs {
		start = time.Now()
		if err := d.diffBranch(); err != nil {
			return err
		}
		duration = time.Since(start)
		diff.time[Dolt.String()] = duration
	}

	return nil
}
