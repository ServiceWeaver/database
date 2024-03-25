package dbclone

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

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
	query := fmt.Sprintf("DROP TABLE %s;", name)

	_, err := connPool.Exec(ctx, query)

	return err
}

func dropView(ctx context.Context, connPool *pgxpool.Pool, name string) error {
	query := fmt.Sprintf("DROP VIEW %s;", name)

	_, err := connPool.Exec(ctx, query)
	return err
}
