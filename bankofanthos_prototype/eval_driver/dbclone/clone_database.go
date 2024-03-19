package dbclone

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type ClonedDb struct {
	connPool     *pgxpool.Pool
	clonedTables map[string]*clonedTable
}

// Clone takes a dbURL("postgresql://user:password@ip:port/dbname?sslmode=disable"), and connects to the database.
// After connecting to the database, it clones all the tables and implements query rewrite.
// application will run on the cloned database later.
func Clone(ctx context.Context, dbURL string) (*ClonedDb, error) {
	connPool, err := pgxpool.Connect(ctx, dbURL)
	if err != nil {
		return nil, err
	}

	database, err := newDatabase(ctx, connPool)
	if err != nil {
		return nil, fmt.Errorf("failed to create new database: %w", err)
	}

	cloneDdl, err := newCloneDdl(ctx, database)
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
	return &ClonedDb{connPool, cloneDdl.clonedTables}, nil
}

func (d *ClonedDb) Close() {
	//TODO: Drop all the tables and views
	d.connPool.Close()
}

func GetRowDiff(ctx context.Context, A *ClonedDb, B *ClonedDb) (map[string]map[DiffType]*RowDiff, error) {
	// For each two clonedDb, compare each table and get rowDiffs for each table
	return nil, nil
}
