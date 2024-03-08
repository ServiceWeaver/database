package dbclone

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

// Clone takes a dbURL("postgresql://user:password@ip:port/dbname?sslmode=disable"), and connects to the database.
// After connecting to the database, it clones all the tables and implements query rewrite.
// application will run on the cloned database later.
func Clone(ctx context.Context, dbURL string) error {
	connPool, err := pgxpool.Connect(ctx, dbURL)
	if err != nil {
		return err
	}

	database, err := newDatabase(ctx, connPool)
	if err != nil {
		return fmt.Errorf("failed to create new database: %w", err)
	}

	cloneDdl, err := newCloneDdl(ctx, database)
	if err != nil {
		return fmt.Errorf("failed to create new clone ddl: %w", err)
	}

	for _, clonedTable := range cloneDdl.clonedTables {
		err = createTriggers(ctx, connPool, clonedTable)
		if err != nil {
			return fmt.Errorf("failed to create triggers: %w", err)
		}
	}

	fmt.Printf("Successfully created clone database %s\n", dbURL)
	connPool.Close()
	return nil
}
