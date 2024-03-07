package dbclone

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type ClonedDatabase struct {
	database *Database
	cloneDdl *CloneDdl
}

// Clone takes a dbURL("postgresql://user:password@ip:port/dbname?sslmode=disable"), and connects to the database.
// After connecting to the database, it clones all the tables and implements query rewrite.
// application will run on the cloned database later.
func Clone(ctx context.Context, dbURL string) (*ClonedDatabase, error) {
	connPool, err := pgxpool.Connect(ctx, dbURL)
	if err != nil {
		return nil, err
	}

	database, err := NewDatabase(ctx, connPool)
	if err != nil {
		return nil, fmt.Errorf("failed to create new database: %w", err)
	}

	cloneDdl, err := NewCloneDdl(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to create new clone ddl: %w", err)
	}

	for _, clonedTable := range cloneDdl.ClonedTables {
		err = createTriggers(ctx, connPool, clonedTable)
		if err != nil {
			return nil, fmt.Errorf("failed to create triggers: %w", err)
		}
	}

	fmt.Printf("Successfully created clone database %s\n", dbURL)
	clonedDatabase := &ClonedDatabase{
		database: database,
		cloneDdl: cloneDdl,
	}
	return clonedDatabase, nil
}

func (c *ClonedDatabase) Close() {
	c.database.connPool.Close()
}
