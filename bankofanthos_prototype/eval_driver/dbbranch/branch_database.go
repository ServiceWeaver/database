package dbbranch

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/exp/maps"
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
	committed bool
}

func NewBrancher(db *pgxpool.Pool) *Brancher {
	branches := map[string]*Branch{}
	return &Brancher{db, nil, branches}
}

func (b *Brancher) Branch(ctx context.Context, namespace string) (*Branch, error) {
	if b.currentBranch != nil && !b.currentBranch.committed {
		return nil, fmt.Errorf("branch %s is still pending, please commit first", b.currentBranch.namespace)
	}
	if _, ok := b.branches[namespace]; ok {
		return nil, fmt.Errorf("branch %s already exists", namespace)
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

	branch := &Branch{clonedDdl: cloneDdl, namespace: namespace, committed: false}
	b.currentBranch = branch
	b.branches[namespace] = branch
	return branch, nil
}

func (b *Branch) Delete(ctx context.Context) error {
	if !b.committed {
		fmt.Println("WARNING: this branch hasn't committed yet. Dropping all tables now...")
		if err := b.clonedDdl.reset(ctx); err != nil {
			return err
		}
		b.committed = true
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
	b.committed = true
	return nil
}

func (b *Branch) IncrementReqId(ctx context.Context) error {
	return b.clonedDdl.incrementCounter(ctx)
}

// For each two branch, compare each table and get rowDiffs for each table
func (b *Brancher) ComputeDiffAtN(ctx context.Context, A *Branch, B *Branch, n int, skipCols map[string][]string) (map[string]*Diff, error) {
	aTables := maps.Keys(A.clonedDdl.clonedTables)
	bTables := maps.Keys(B.clonedDdl.clonedTables)
	sort.Strings(aTables)
	sort.Strings(bTables)
	if !slices.Equal(aTables, bTables) {
		return nil, fmt.Errorf("two branches have different tables %s and %s, cannot compare", maps.Keys(A.clonedDdl.clonedTables), maps.Keys(B.clonedDdl.clonedTables))
	}

	diffs := map[string]*Diff{}
	for tableName, clonedTableA := range A.clonedDdl.clonedTables {
		clonedTableB := B.clonedDdl.clonedTables[tableName]
		dbDiff := newDbDiff(b.db, clonedTableA.Counter.Colname, skipCols, clonedTableA.IdCol)

		diff, err := dbDiff.getClonedTableRowDiffAtNReqs(ctx, clonedTableA, clonedTableB, n)
		if err != nil {
			return nil, err
		}
		diffs[tableName] = diff
	}

	return diffs, nil
}

// For each two branch, compare each table and get rowDiffs for each table at each request id
func (b *Brancher) ComputeDiffPerReq(ctx context.Context, A *Branch, B *Branch, N int, skipCols map[string][]string) ([]map[string]*Diff, error) {
	var reqMaps []map[string]*Diff
	for n := 0; n < N; n++ {
		diffs, err := b.ComputeDiffAtN(ctx, A, B, n, skipCols)
		if err != nil {
			return nil, err
		}
		reqMaps = append(reqMaps, diffs)
	}

	return reqMaps, nil
}
