package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type postgres struct {
	client *sql.DB
}

func newPostgresClient(uri string) (*postgres, error) {
	db, err := sql.Open("postgres", uri)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("db.Ping(): %s", err)
	}
	return &postgres{client: db}, err
}

func (p *postgres) close() error {
	return p.client.Close()
}

func (p *postgres) copyTable(table string) error {
	if _, err := p.client.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s_copy ;", table)); err != nil {
		return err
	}
	_, err := p.client.Exec(fmt.Sprintf("CREATE TABLE %s_copy AS SELECT * FROM %s;", table, table))
	return err
}

func (p *postgres) postgresDiff(table string) error {
	_, err := p.client.Exec(fmt.Sprintf("SELECT * FROM %s EXCEPT ALL SELECT * FROM %s_copy;", table, table))

	return err
}
