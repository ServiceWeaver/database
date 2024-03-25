package dbclone

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type ClonedDb struct {
	connPool     *pgxpool.Pool
	clonedTables map[string]*clonedTable
	clonedDdl    *cloneDdl
}

// Clone takes a dbURL("postgresql://user:password@ip:port/dbname?sslmode=disable"), and connects to the database.
// After connecting to the database, it clones all the tables and implements query rewrite.
// application will run on the cloned database later.
func Clone(ctx context.Context, dbURL string, namespace string) (*ClonedDb, error) {
	connPool, err := pgxpool.Connect(ctx, dbURL)
	if err != nil {
		return nil, err
	}

	database, err := newDatabase(ctx, connPool)
	if err != nil {
		return nil, fmt.Errorf("failed to create new database: %w", err)
	}

	cloneDdl, err := newCloneDdl(ctx, database, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create new clone ddl: %w", err)
	}

	for _, clonedTable := range cloneDdl.clonedTables {
		err = createTriggers(ctx, connPool, clonedTable)
		if err != nil {
			return nil, fmt.Errorf("failed to create triggers: %w", err)
		}
	}

	fmt.Printf("Successfully created clone database %s\n", dbURL)

	return &ClonedDb{connPool, cloneDdl.clonedTables, cloneDdl}, nil
}

// Reset is called after each run, renames snapshot back to original prod table name, rename view to schemaname.view
func (c *ClonedDb) Reset(ctx context.Context) error {
	return c.clonedDdl.reset(ctx)
}

// Close drops all plus/minus tables and views, close db connection
func (c *ClonedDb) Close(ctx context.Context) error {
	if err := c.clonedDdl.close(ctx); err != nil {
		return err
	}
	c.connPool.Close()
	return nil
}
