package dbbranch

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
)

const snapshotSuffix = "snapshot"

func alterTableName(ctx context.Context, connPool *pgxpool.Pool, newName string, table *table) error {
	query := fmt.Sprintf("ALTER TABLE %s RENAME to %s;", table.Name, newName)
	_, err := connPool.Exec(ctx, query)
	if err != nil {
		return err
	}
	table.Name = newName

	return nil
}

func alterViewName(ctx context.Context, connPool *pgxpool.Pool, newName string, view *view) error {
	query := fmt.Sprintf("ALTER VIEW %s RENAME to %s;", view.Name, newName)

	_, err := connPool.Exec(ctx, query)
	if err != nil {
		return err
	}
	view.Name = newName

	return nil
}

func dropTable(ctx context.Context, connPool *pgxpool.Pool, name string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", name)

	_, err := connPool.Exec(ctx, query)

	return err
}

func dropView(ctx context.Context, connPool *pgxpool.Pool, name string) error {
	query := fmt.Sprintf("DROP VIEW IF EXISTS %s;", name)

	_, err := connPool.Exec(ctx, query)
	return err
}

func dropTrigger(ctx context.Context, connPool *pgxpool.Pool, triggerName, tableName string) error {
	query := fmt.Sprintf("DROP TRIGGER %s ON %s CASCADE;", triggerName, tableName)
	_, err := connPool.Exec(ctx, query)

	return err
}

func dropFunction(ctx context.Context, connPool *pgxpool.Pool, name string) error {
	query := fmt.Sprintf("DROP FUNCTION %s;", name)
	_, err := connPool.Exec(ctx, query)

	return err
}

func dropSchemaCascade(ctx context.Context, connPool *pgxpool.Pool, namespace string) error {
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE;", namespace)

	_, err := connPool.Exec(ctx, query)
	return err
}

func getAllSchemaNames(ctx context.Context, connPool *pgxpool.Pool) ([]string, error) {
	var schemaNames []string
	rows, err := connPool.Query(ctx, `
	SELECT schema_name
	FROM information_schema.schemata
	WHERE schema_name NOT LIKE 'pg_%' 
	AND schema_name NOT IN ('information_schema', 'public');`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, err
		}
		schemaNames = append(schemaNames, schemaName)
	}
	return schemaNames, nil
}

// for goroutine group
func NewGroup[K comparable, V any](ctx context.Context) *Group[K, V] {
	g, _ := errgroup.WithContext(ctx)
	return &Group[K, V]{
		group:   g,
		results: make(map[K]V),
	}
}

type Group[K comparable, V any] struct {
	group   *errgroup.Group
	mu      sync.Mutex
	results map[K]V
}

func (g *Group[K, V]) Go(f func() (K, V, error)) {
	g.group.Go(func() error {
		key, val, err := f()
		g.mu.Lock()
		defer g.mu.Unlock()
		g.results[key] = val
		return err
	})
}

func (g *Group[K, V]) Wait() (map[K]V, error) {
	return g.results, g.group.Wait()
}
