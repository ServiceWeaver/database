package clonedatabase

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type CloneDatabase struct {
	Database *Database
	CloneDdl *CloneDdl
}

func NewClonedDatabase(ctx context.Context, dbURL string) (*CloneDatabase, error) {
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
		queryRewriter, err := NewQueryRewriter(connPool, clonedTable)
		if err != nil {
			return nil, fmt.Errorf("failed to create new query rewriter: %w", err)
		}
		err = queryRewriter.CreateTriggers(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create triggers: %w", err)
		}
	}

	fmt.Println("Successfully created clone database")
	cloneDatabase := &CloneDatabase{
		Database: database,
		CloneDdl: cloneDdl,
	}
	return cloneDatabase, nil
}

func (c *CloneDatabase) Close() {
	c.Database.connPool.Close()
}
