package dbclone

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/exp/maps"
)

type Row []any

// Diff shows rows for 3 way diff. Left, Middle and Right are the same length
// each corresponding position is one row for one way diff.
// if the row is not exists, value will be nil.
type Diff struct {
	Left     []*Row // control
	Middle   []*Row // baseline
	Right    []*Row // experimental
	ColNames []string
}

type diffType int

const (
	APlusOnly diffType = iota + 1
	BPlusOnly
	APlusBPlus
	AMinusOnly
	BMinusOnly
	AMinusBMinus
	PrimaryKey
)

func (d diffType) String() string {
	return [...]string{"APlusOnly", "BPlusOnly", "APlusBPlus", "AMinusOnly", "BMinusOnly", "AMinusBMinus", "PrimaryKey"}[d-1]
}

type dbDiff struct {
	connPool *pgxpool.Pool
}

func newDbDiff(connPool *pgxpool.Pool) *dbDiff {
	return &dbDiff{connPool: connPool}
}

// dump dumps rows in view without any order.
// if a view already be sorted, the function will dump ordered view.
func (d *dbDiff) dumpView(ctx context.Context, view *view) ([]*Row, []string, error) {
	var dumpRows []*Row
	var colNames []string

	for n := range view.Cols {
		colNames = append(colNames, n)
	}

	// TODO: sort the columns for where they defined. Sort the primary keys by orders
	sort.Strings(colNames)
	query := fmt.Sprintf("SELECT %s FROM %s;", strings.Join(colNames, ", "), view.Name)

	rows, err := d.connPool.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		rowVal := make([]any, len(colNames))
		rowPtr := make([]any, len(colNames))
		for i := range rowVal {
			rowPtr[i] = &rowVal[i]
		}
		err = rows.Scan(rowPtr...)
		if err != nil {
			return nil, nil, err
		}

		row := &Row{rowVal}
		dumpRows = append(dumpRows, row)
	}

	return dumpRows, colNames, rows.Err()
}

func (d *dbDiff) trimClonedTable(ctx context.Context, clonedTable *clonedTable) (*view, *view, error) {
	trimPlusName := clonedTable.Plus.Name + "trim"
	trimPlus, err := d.minus(ctx, clonedTable.Plus, clonedTable.Minus, trimPlusName)
	if err != nil {
		return nil, nil, err
	}

	trimMinusName := clonedTable.Minus.Name + "trim"
	trimMinus, err := d.minus(ctx, clonedTable.Minus, clonedTable.Plus, trimMinusName)
	if err != nil {
		return nil, nil, err
	}

	return trimPlus, trimMinus, nil
}

func (d *dbDiff) operationHelper(A interface{}, B interface{}) (string, string, map[string]column, error) {
	var colA, colB map[string]column
	var nameA, nameB string
	_, isViewA := A.(*view)
	_, isViewB := B.(*view)
	_, isTableA := A.(*table)
	_, isTableB := B.(*table)
	if isViewA && isViewB {
		colA = A.(*view).Cols
		colB = B.(*view).Cols
		nameA = A.(*view).Name
		nameB = B.(*view).Name
	} else if isTableA && isTableB {
		colA = A.(*table).Cols
		colB = B.(*table).Cols
		nameA = A.(*table).Name
		nameB = B.(*table).Name
	} else {
		return "", "", nil, fmt.Errorf("unsupported struct")
	}
	if !reflect.DeepEqual(colA, colB) {
		return "", "", nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot intersect", colA, colB)
	}

	return nameA, nameB, colA, nil
}

// A - B
func (d *dbDiff) minus(ctx context.Context, A interface{}, B interface{}, viewName string) (*view, error) {
	nameA, nameB, cols, err := d.operationHelper(A, B)
	if err != nil {
		return nil, err
	}
	colNames := maps.Keys(cols)
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	EXCEPT ALL
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), nameA, strings.Join(colNames, ", "), nameB, strings.Join(colNames, ", "))

	if _, err = d.connPool.Exec(ctx, query); err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: cols}, nil
}

// A intersect B
func (d *dbDiff) intersect(ctx context.Context, A interface{}, B interface{}, viewName string) (*view, error) {
	nameA, nameB, cols, err := d.operationHelper(A, B)
	if err != nil {
		return nil, err
	}
	colNames := maps.Keys(cols)
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	INTERSECT
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), nameA, strings.Join(colNames, ", "), nameB, strings.Join(colNames, ", "))

	if _, err = d.connPool.Exec(ctx, query); err != nil {
		return nil, err
	}
	return &view{Name: viewName, Cols: cols}, nil
}

// A union B
func (d *dbDiff) union(ctx context.Context, A interface{}, B interface{}, viewName string) (*view, error) {
	nameA, nameB, cols, err := d.operationHelper(A, B)
	if err != nil {
		return nil, err
	}
	colNames := maps.Keys(cols)
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	UNION ALL
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), nameA, strings.Join(colNames, ", "), nameB, strings.Join(colNames, ", "))

	if _, err := d.connPool.Exec(ctx, query); err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: cols}, nil
}

// getUniqueCols returns primary key if there is any, if cannot find, it returns empty list.
func (d *dbDiff) getPrimaryKeyCols(table *table) []string {
	for _, idx := range table.Indexes {
		if idx.IsUnique && strings.Contains(idx.Name, "pkey") {
			return idx.ColumnNames
		}
	}

	return nil
}

func (d *dbDiff) getPrimarKeyRows(ctx context.Context, aPlus *view, bPlus *view, aMinus *view, bMinus *view, clonedTableA *clonedTable, cloneTableB *clonedTable) (*view, error) {
	var colNames []string
	if !reflect.DeepEqual(clonedTableA.View.Cols, cloneTableB.View.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot union", clonedTableA.View.Cols, cloneTableB.View.Cols)
	}
	for n := range aPlus.Cols {
		colNames = append(colNames, n)
	}
	sort.Strings(colNames)

	viewName := "AUnionB"

	primaryCols := d.getPrimaryKeyCols(clonedTableA.Snapshot)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT DISTINCT(%s) FROM 
	(
		SELECT %s FROM %s
		UNION ALL
		SELECT %s FROM %s
		UNION ALL
		SELECT %s FROM %s
		UNION ALL
		SELECT %s FROM %s
	) as keys
	ORDER BY %s);
	`, viewName, strings.Join(primaryCols, ","), strings.Join(primaryCols, ","), aPlus.Name, strings.Join(primaryCols, ","), aMinus.Name, strings.Join(primaryCols, ","), bPlus.Name, strings.Join(primaryCols, ","), bMinus.Name, strings.Join(primaryCols, ","))

	if _, err := d.connPool.Exec(ctx, query); err != nil {
		return nil, err
	}

	cols := map[string]column{}
	for _, colName := range primaryCols {
		cols[colName] = clonedTableA.View.Cols[colName]
	}
	return &view{Name: viewName, Cols: cols}, nil
}

func (d *dbDiff) getRowsByPrimaryKey(ctx context.Context, primaryKeyView *view, sideView *view, viewName string) (*view, error) {
	var viewColQuery, primaryKeyColNames, primarySideColNames []string
	for n := range primaryKeyView.Cols {
		primaryKeyColNames = append(primaryKeyColNames, primaryKeyView.Name+"."+n)
		primarySideColNames = append(primarySideColNames, sideView.Name+"."+n)
		viewColQuery = append(viewColQuery, fmt.Sprintf("%s.%s AS %s_pkey", primaryKeyView.Name, n, n))
	}

	var sideColNames []string
	for n := range sideView.Cols {
		sideColNames = append(sideColNames, sideView.Name+"."+n)
	}
	viewColQuery = append(viewColQuery, sideColNames...)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
		SELECT %s
		FROM %s LEFT JOIN %s ON %s = %s
		ORDER BY %s);
	`, viewName, strings.Join(viewColQuery, ","), primaryKeyView.Name, sideView.Name, strings.Join(primaryKeyColNames, ","), strings.Join(primarySideColNames, ","), strings.Join(primaryKeyColNames, ","))

	if _, err := d.connPool.Exec(ctx, query); err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: sideView.Cols}, nil
}

func (d *dbDiff) fillRowSlices(val []any, length int) []*Row {
	var slice []*Row
	for range length {
		slice = append(slice, (&Row{val}))
	}
	return slice
}

func (d *dbDiff) getNonPrimaryKeyRowDiff(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable) (*Diff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableA.View.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot intersect", clonedTableA.View.Cols, clonedTableA.View.Cols)
	}
	rowDiffs := map[diffType]*Diff{}

	var views []*view
	// trim cloned table A
	aPlus, aMinus, err := d.trimClonedTable(ctx, clonedTableA)
	if err != nil {
		return nil, err
	}
	views = append(views, aPlus, aMinus)

	// trim cloned table B
	bPlus, bMinus, err := d.trimClonedTable(ctx, clonedTableB)
	if err != nil {
		return nil, err
	}
	views = append(views, bPlus, bMinus)

	// A+ - B+
	aPlusOnly, err := d.minus(ctx, aPlus, bPlus, "aPlusOnly")
	if err != nil {
		return nil, err
	}
	aPlusRows, colNames, err := d.dumpView(ctx, aPlusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, aPlusOnly)

	var nilRows []any
	for range colNames {
		nilRows = append(nilRows, nil)
	}

	nilSlices := d.fillRowSlices(nilRows, len(aPlusRows))
	aPlusDiff := &Diff{Left: aPlusRows, Middle: nilSlices, Right: nilSlices, ColNames: colNames}
	rowDiffs[APlusOnly] = aPlusDiff

	// A+ intersect B+
	aPlusBPlus, err := d.intersect(ctx, aPlus, bPlus, "aPlusBPlus")
	if err != nil {
		return nil, err
	}

	aPlusBPlusRows, colNames, err := d.dumpView(ctx, aPlusBPlus)
	if err != nil {
		return nil, err
	}
	views = append(views, aPlusBPlus)

	nilSlices = d.fillRowSlices(nilRows, len(aPlusBPlusRows))
	aPlusBPlusRowsRowDiff := &Diff{Left: aPlusBPlusRows, Middle: nilSlices, Right: aPlusBPlusRows, ColNames: colNames}
	rowDiffs[APlusBPlus] = aPlusBPlusRowsRowDiff

	// B+ - A+
	bPlusOnly, err := d.minus(ctx, bPlus, aPlus, "bPlusOnly")
	if err != nil {
		return nil, err
	}

	bPlusRows, colNames, err := d.dumpView(ctx, bPlusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, bPlusOnly)

	nilSlices = d.fillRowSlices(nilRows, len(bPlusRows))
	bPlusDiff := &Diff{Left: nilSlices, Middle: nilSlices, Right: bPlusRows, ColNames: colNames}
	rowDiffs[BPlusOnly] = bPlusDiff

	// B- - A-
	bMinusOnly, err := d.minus(ctx, bMinus, aMinus, "bMinusOnly")
	if err != nil {
		return nil, err
	}
	bMinusRows, colNames, err := d.dumpView(ctx, bMinusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, bMinusOnly)

	nilSlices = d.fillRowSlices(nilRows, len(bMinusRows))
	bMinusDiff := &Diff{Left: bMinusRows, Middle: bMinusRows, Right: nilSlices, ColNames: colNames}
	rowDiffs[BMinusOnly] = bMinusDiff

	// B- intersect A-
	aMinusBMinus, err := d.intersect(ctx, aMinus, bMinus, "aMinusBMinus")
	if err != nil {
		return nil, err
	}
	aMinusbMinusRows, colNames, err := d.dumpView(ctx, aMinusBMinus)
	if err != nil {
		return nil, err
	}
	views = append(views, aMinusBMinus)
	nilSlices = d.fillRowSlices(nilRows, len(aMinusbMinusRows))
	aMinusbMinusDiff := &Diff{Left: nilSlices, Middle: aMinusbMinusRows, Right: nilSlices, ColNames: colNames}
	rowDiffs[AMinusBMinus] = aMinusbMinusDiff

	// A- - B-
	aMinusOnly, err := d.minus(ctx, aMinus, bMinus, "aMinusOnly")
	if err != nil {
		return nil, err
	}
	aMinusRows, colNames, err := d.dumpView(ctx, aMinusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, aMinusOnly)
	nilSlices = d.fillRowSlices(nilRows, len(aMinusRows))
	aMinusDiff := &Diff{Left: nilSlices, Middle: aMinusRows, Right: aMinusRows, ColNames: colNames}
	rowDiffs[AMinusOnly] = aMinusDiff

	// drop all views in reverse order because of dependency
	for i := len(views) - 1; i >= 0; i-- {
		if err = dropView(ctx, d.connPool, views[i].Name); err != nil {
			return nil, err
		}
	}

	diff := &Diff{ColNames: colNames}
	diffTypes := []diffType{APlusOnly, BPlusOnly, APlusBPlus, AMinusOnly, BMinusOnly, AMinusBMinus}
	for _, d := range diffTypes {
		diff.Left = append(diff.Left, rowDiffs[d].Left...)
		diff.Middle = append(diff.Middle, rowDiffs[d].Middle...)
		diff.Right = append(diff.Right, rowDiffs[d].Right...)
	}

	return diff, nil
}

// left: A+, B- - A-
// middle: A- intersect B-, A- - B-, B- - A-
// right: B+, A- - B-
func (d *dbDiff) getPrimaryKeyRowDiff(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable) (*Diff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableB.View.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot diff", clonedTableA.View.Cols, clonedTableA.View.Cols)
	}
	var views []*view

	// trim cloned table A
	aPlus, aMinus, err := d.trimClonedTable(ctx, clonedTableA)
	if err != nil {
		return nil, err
	}
	views = append(views, aPlus, aMinus)

	// trim cloned table B
	bPlus, bMinus, err := d.trimClonedTable(ctx, clonedTableB)
	if err != nil {
		return nil, err
	}
	views = append(views, bPlus, bMinus)

	primaryKeyView, err := d.getPrimarKeyRows(ctx, aPlus, bPlus, aMinus, bMinus, clonedTableA, clonedTableB)
	if err != nil {
		return nil, err
	}
	views = append(views, primaryKeyView)

	bMinusAMinus, err := d.minus(ctx, bMinus, aMinus, "BMinusAMinus")
	if err != nil {
		return nil, err
	}

	aMinusBMinus, err := d.minus(ctx, aMinus, bMinus, "AMinusBMinus")
	if err != nil {
		return nil, err
	}

	aMinusIntersectBMinus, err := d.intersect(ctx, aMinus, bMinus, "AMinusIntersectBMinus")
	if err != nil {
		return nil, err
	}
	views = append(views, bMinusAMinus, aMinusBMinus, aMinusIntersectBMinus)

	// Left: A+, B- - A-
	leftSideView, err := d.union(ctx, aPlus, bMinusAMinus, "leftView")
	if err != nil {
		return nil, err
	}

	leftSideDiff, err := d.getRowsByPrimaryKey(ctx, primaryKeyView, leftSideView, "leftDiff")
	if err != nil {
		return nil, err
	}
	leftSideRows, colNames, err := d.dumpView(ctx, leftSideDiff)
	if err != nil {
		return nil, err
	}
	views = append(views, leftSideView, leftSideDiff)

	// Right: B+, A- - B-
	rightSideView, err := d.union(ctx, bPlus, aMinusBMinus, "rightView")
	if err != nil {
		return nil, err
	}
	rightSideDiff, err := d.getRowsByPrimaryKey(ctx, primaryKeyView, rightSideView, "rightDiff")
	if err != nil {
		return nil, err
	}
	rightSideRows, _, err := d.dumpView(ctx, rightSideDiff)
	if err != nil {
		return nil, err
	}
	views = append(views, rightSideView, rightSideDiff)

	// Middle: A- intersect B-, A- - B-, B- - A-
	halfMiddleSideView, err := d.union(ctx, aMinusIntersectBMinus, aMinusBMinus, "halfMiddle")
	if err != nil {
		return nil, err
	}
	middleSideView, err := d.union(ctx, halfMiddleSideView, bMinusAMinus, "middleView")
	if err != nil {
		return nil, err
	}
	middleSideDiff, err := d.getRowsByPrimaryKey(ctx, primaryKeyView, middleSideView, "middleDiff")
	if err != nil {
		return nil, err
	}
	middleSideRows, _, err := d.dumpView(ctx, middleSideDiff)
	if err != nil {
		return nil, err
	}
	views = append(views, halfMiddleSideView, middleSideView, middleSideDiff)

	rowDiff := &Diff{Left: leftSideRows, Right: rightSideRows, Middle: middleSideRows, ColNames: colNames}

	// drop all views in reverse order because of dependency
	for i := len(views) - 1; i >= 0; i-- {
		if err = dropView(ctx, d.connPool, views[i].Name); err != nil {
			return nil, err
		}
	}

	return rowDiff, nil
}

func (d *dbDiff) getClonedTableRowDiff(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable) (*Diff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableB.View.Cols) {
		return nil, fmt.Errorf("cannot get row diff for different cols, tableA %v, tableB %v ", clonedTableA.View.Cols, clonedTableB.View.Cols)
	}

	pkCols := d.getPrimaryKeyCols(clonedTableA.Snapshot)

	if len(pkCols) == 0 {
		return d.getNonPrimaryKeyRowDiff(ctx, clonedTableA, clonedTableB)
	}
	return d.getPrimaryKeyRowDiff(ctx, clonedTableA, clonedTableB)
}
