package dbclone

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

// Examples:
// db := pgxpool.Connect(...)
// brancher := NewBrancher(db)
// b1 := brancher.Branch(ctx, "b1")
// defer b1.Delete() // optionally
// defer b1.Commit()
//
// Only one branch can be active at a time.
// It should be hard to forget to Commit a branch.
// Delete should maybe behave nicely even if you forget to call Commit.
type Brancher struct {
	db            *pgxpool.Pool
	currentBranch *Branch
	branches      map[string]*Branch
}

type Branch struct {
	clonedDdl *cloneDdl
	namespace string
	commited  bool
}

func NewBrancher(db *pgxpool.Pool) *Brancher {
	branches := map[string]*Branch{}
	return &Brancher{db, nil, branches}
}

func (b *Brancher) Branch(ctx context.Context, namespace string) (*Branch, error) {
	if b.currentBranch != nil && !b.currentBranch.commited {
		return nil, fmt.Errorf("branch %s is still pending, please commit first", b.currentBranch.namespace)
	}
	database, err := newDatabase(ctx, b.db)
	if err != nil {
		return nil, fmt.Errorf("failed to create new database: %w", err)
	}

	cloneDdl, err := newCloneDdl(ctx, database, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create new clone ddl: %w", err)
	}

	for _, clonedTable := range cloneDdl.clonedTables {
		err = createTriggers(ctx, b.db, clonedTable)
		if err != nil {
			return nil, fmt.Errorf("failed to create triggers: %w", err)
		}
	}

	branch := &Branch{clonedDdl: cloneDdl, namespace: namespace, commited: false}
	b.currentBranch = branch
	b.branches[namespace] = branch
	return branch, nil
}

func (b *Branch) Delete(ctx context.Context) error {
	if !b.commited {
		fmt.Println("WARNING: this branch haven't commited yet. Droping all tables now...")
		if err := b.clonedDdl.reset(ctx); err != nil {
			return err
		}
		b.commited = true
	}
	if err := b.clonedDdl.close(ctx); err != nil {
		return err
	}
	return nil
}

func (b *Branch) Commit(ctx context.Context) error {
	if err := b.clonedDdl.reset(ctx); err != nil {
		return err
	}
	b.commited = true
	return nil
}

func ComputeDiff(ctx context.Context, A *Branch, B *Branch) (map[string]*Diff, error) {
	// For each two clonedDb, compare each table and get rowDiffs for each table
	return nil, nil
}
