package dbbranch

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

// Diff shows rows for 3 way diff. Control, Baseline and Experimental are the same length
// each corresponding position is one row for one way diff.
// if the row is not exists, value will be nil.
type Diff struct {
	Control      []*Row // control
	Baseline     []*Row // baseline
	Experimental []*Row // experimental
	ColNames     []string
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

type clonedTableAtN struct {
	Snapshot *table
	Plus     *view
	Minus    *view
	View     *view
	Counter  *counter
}

type dbDiff struct {
	connPool   *pgxpool.Pool
	counterCol string
}

func newDbDiff(connPool *pgxpool.Pool, counterCol string) *dbDiff {
	return &dbDiff{connPool: connPool, counterCol: counterCol}
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

		row := Row(rowVal)
		dumpRows = append(dumpRows, &row)
	}

	return dumpRows, colNames, rows.Err()
}

func (d *dbDiff) trimClonedTable(ctx context.Context, clonedTable *clonedTableAtN) (*view, *view, error) {
	trimPlusName := clonedTable.Plus.Name + "trim"
	trimPlus, err := d.minusViews(ctx, clonedTable.Plus, clonedTable.Minus, trimPlusName)
	if err != nil {
		return nil, nil, err
	}

	trimMinusName := clonedTable.Minus.Name + "trim"
	trimMinus, err := d.minusViews(ctx, clonedTable.Minus, clonedTable.Plus, trimMinusName)
	if err != nil {
		return nil, nil, err
	}

	return trimPlus, trimMinus, nil
}

func (d *dbDiff) combine(ctx context.Context, name, a string, acolumns map[string]column, operation string, b string, bcolumns map[string]column) (*view, error) {
	if !reflect.DeepEqual(acolumns, bcolumns) {
		return nil, fmt.Errorf("relations %s and %s have different columns and cannot be combined", a, b)
	}

	columnNames := maps.Keys(acolumns)
	sort.Strings(columnNames)
	joined := strings.Join(columnNames, ", ")
	query := fmt.Sprintf(`                                                                                                                                                                           
	CREATE VIEW %s AS (                                                                                                                                                                              
		SELECT %s FROM %s                                                                                                                                                                        
		%s                                                                                  
		SELECT %s FROM %s                                                                                                                                                                        
		ORDER BY %s                                                                                                                                                                              
	);                                                                                                                                                                                               
	`, name, joined, a, operation, joined, b, joined)
	_, err := d.connPool.Exec(ctx, query)
	return &view{Name: name, Cols: acolumns}, err
}

func (d *dbDiff) minusViews(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	return d.combine(ctx, viewName, viewA.Name, viewA.Cols, "EXCEPT ALL", viewB.Name, viewB.Cols)
}

func (d *dbDiff) intersectViews(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	return d.combine(ctx, viewName, viewA.Name, viewA.Cols, "INTERSECT ALL", viewB.Name, viewB.Cols)
}

func (d *dbDiff) unionViews(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	return d.combine(ctx, viewName, viewA.Name, viewA.Cols, "UNION ALL", viewB.Name, viewB.Cols)
}

func (d *dbDiff) unionUniqueViews(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	return d.combine(ctx, viewName, viewA.Name, viewA.Cols, "UNION", viewB.Name, viewB.Cols)
}

func (d *dbDiff) minusTables(ctx context.Context, tableA *table, tableB *table, tableName string) (*view, error) {
	return d.combine(ctx, tableName, tableA.Name, tableA.Cols, "EXCEPT ALL", tableB.Name, tableB.Cols)
}

func (d *dbDiff) intersectTables(ctx context.Context, tableA *table, tableB *table, tableName string) (*view, error) {
	return d.combine(ctx, tableName, tableA.Name, tableA.Cols, "INTERSECT ALL", tableB.Name, tableB.Cols)
}

func (d *dbDiff) unionTables(ctx context.Context, tableA *table, tableB *table, tableName string) (*view, error) {
	return d.combine(ctx, tableName, tableA.Name, tableA.Cols, "UNION ALL", tableB.Name, tableB.Cols)
}

// getPrimaryKeyCols returns primary key if there is any, if cannot find, it returns empty list.
func (d *dbDiff) getPrimaryKeyCols(table *table) []string {
	for _, idx := range table.Indexes {
		if idx.IsUnique && strings.Contains(idx.Name, "pkey") {
			return idx.ColumnNames
		}
	}

	return nil
}

func (d *dbDiff) getPrimarKeyRows(ctx context.Context, aPlus *view, bPlus *view, aMinus *view, bMinus *view, clonedTableA *clonedTableAtN, cloneTableB *clonedTableAtN) (*view, error) {
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
	row := Row(val)
	slice := make([]*Row, length)
	for i := range slice {
		slice[i] = &row
	}
	return slice
}

// The diff of two versions, A and B, of a table can be divided into six
// sections:
//
//  1. A+ - B+: the rows inserted into A but not into B.
//  2. A+ & B+: the rows inserted into A and B.
//  3. B+ - A+: the rows inserted into B but not into A.
//  4. B- - A-: the rows deleted from B but not from A.
//  5. A- & B-: the rows deleted from A and B.
//  6. A- - B-: the rows deleted from A but not from B.
//
// Graphically, these six sections look like the following:
//
//	left    | middle     | right
//	A+ - B+ | nil        | nil
//	A+ & B+ | nil        | A+ & B+
//	nil     | nil        | B+ - A+
//	B- -A-  | B- - A-    | nil
//	nil     | A- \cap B- | nil
//	nil     | A- - B-    | A- - B-
func (d *dbDiff) getNonPrimaryKeyRowDiff(ctx context.Context, clonedTableA *clonedTableAtN, clonedTableB *clonedTableAtN) (*Diff, error) {
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
	aPlusOnly, err := d.minusViews(ctx, aPlus, bPlus, "aPlusOnly")
	if err != nil {
		return nil, err
	}
	aPlusRows, colNames, err := d.dumpView(ctx, aPlusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, aPlusOnly)

	// TODO: switch to a single nil value rather than a row of nils
	nilRow := make([]any, len(colNames))

	nilSlices := d.fillRowSlices(nilRow, len(aPlusRows))
	aPlusDiff := &Diff{Control: aPlusRows, Baseline: nilSlices, Experimental: nilSlices, ColNames: colNames}
	rowDiffs[APlusOnly] = aPlusDiff

	// A+ intersect B+
	aPlusBPlus, err := d.intersectViews(ctx, aPlus, bPlus, "aPlusBPlus")
	if err != nil {
		return nil, err
	}

	aPlusBPlusRows, colNames, err := d.dumpView(ctx, aPlusBPlus)
	if err != nil {
		return nil, err
	}
	views = append(views, aPlusBPlus)

	nilSlices = d.fillRowSlices(nilRow, len(aPlusBPlusRows))
	aPlusBPlusRowsRowDiff := &Diff{Control: aPlusBPlusRows, Baseline: nilSlices, Experimental: aPlusBPlusRows, ColNames: colNames}
	rowDiffs[APlusBPlus] = aPlusBPlusRowsRowDiff

	// B+ - A+
	bPlusOnly, err := d.minusViews(ctx, bPlus, aPlus, "bPlusOnly")
	if err != nil {
		return nil, err
	}

	bPlusRows, colNames, err := d.dumpView(ctx, bPlusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, bPlusOnly)

	nilSlices = d.fillRowSlices(nilRow, len(bPlusRows))
	bPlusDiff := &Diff{Control: nilSlices, Baseline: nilSlices, Experimental: bPlusRows, ColNames: colNames}
	rowDiffs[BPlusOnly] = bPlusDiff

	// B- - A-
	bMinusOnly, err := d.minusViews(ctx, bMinus, aMinus, "bMinusOnly")
	if err != nil {
		return nil, err
	}
	bMinusRows, colNames, err := d.dumpView(ctx, bMinusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, bMinusOnly)

	nilSlices = d.fillRowSlices(nilRow, len(bMinusRows))
	bMinusDiff := &Diff{Control: bMinusRows, Baseline: bMinusRows, Experimental: nilSlices, ColNames: colNames}
	rowDiffs[BMinusOnly] = bMinusDiff

	// B- intersect A-
	aMinusBMinus, err := d.intersectViews(ctx, aMinus, bMinus, "aMinusBMinus")
	if err != nil {
		return nil, err
	}
	aMinusbMinusRows, colNames, err := d.dumpView(ctx, aMinusBMinus)
	if err != nil {
		return nil, err
	}
	views = append(views, aMinusBMinus)
	nilSlices = d.fillRowSlices(nilRow, len(aMinusbMinusRows))
	aMinusbMinusDiff := &Diff{Control: nilSlices, Baseline: aMinusbMinusRows, Experimental: nilSlices, ColNames: colNames}
	rowDiffs[AMinusBMinus] = aMinusbMinusDiff

	// A- - B-
	aMinusOnly, err := d.minusViews(ctx, aMinus, bMinus, "aMinusOnly")
	if err != nil {
		return nil, err
	}
	aMinusRows, colNames, err := d.dumpView(ctx, aMinusOnly)
	if err != nil {
		return nil, err
	}
	views = append(views, aMinusOnly)
	nilSlices = d.fillRowSlices(nilRow, len(aMinusRows))
	aMinusDiff := &Diff{Control: nilSlices, Baseline: aMinusRows, Experimental: aMinusRows, ColNames: colNames}
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
		diff.Control = append(diff.Control, rowDiffs[d].Control...)
		diff.Baseline = append(diff.Baseline, rowDiffs[d].Baseline...)
		diff.Experimental = append(diff.Experimental, rowDiffs[d].Experimental...)
	}

	return diff, nil
}

// left: A+ UNION ALL B- - A-
// middle: A- UNION B-
// right: B+ UNION ALL A- - B-
// left we will show inserted in A, and deleted only from B
// middle we will show distinct deleted from both A and B
// right we will show inserted in B, and deleted only from A
// because primary key is unique, so for each way there should be only one row with same primary key
func (d *dbDiff) getPrimaryKeyRowDiff(ctx context.Context, clonedTableA *clonedTableAtN, clonedTableB *clonedTableAtN) (*Diff, error) {
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

	bMinusAMinus, err := d.minusViews(ctx, bMinus, aMinus, "BMinusAMinus")
	if err != nil {
		return nil, err
	}

	aMinusBMinus, err := d.minusViews(ctx, aMinus, bMinus, "AMinusBMinus")
	if err != nil {
		return nil, err
	}

	aMinusIntersectBMinus, err := d.intersectViews(ctx, aMinus, bMinus, "AMinusIntersectBMinus")
	if err != nil {
		return nil, err
	}
	views = append(views, bMinusAMinus, aMinusBMinus, aMinusIntersectBMinus)

	// Control: A+, B- - A-
	leftSideView, err := d.unionViews(ctx, aPlus, bMinusAMinus, "leftView")
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

	// Experimental: B+, A- - B-
	rightSideView, err := d.unionViews(ctx, bPlus, aMinusBMinus, "rightView")
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

	middleSideView, err := d.unionUniqueViews(ctx, aMinus, bMinus, "middleView")
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
	views = append(views, middleSideView, middleSideDiff)

	rowDiff := &Diff{Control: leftSideRows, Experimental: rightSideRows, Baseline: middleSideRows, ColNames: colNames}

	// drop all views in reverse order because of dependency
	for i := len(views) - 1; i >= 0; i-- {
		if err = dropView(ctx, d.connPool, views[i].Name); err != nil {
			return nil, err
		}
	}

	return rowDiff, nil
}

func (d *dbDiff) getClonedTableRowDiff(ctx context.Context, clonedTableA *clonedTableAtN, clonedTableB *clonedTableAtN) (*Diff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableB.View.Cols) {
		return nil, fmt.Errorf("cannot get row diff for different cols, tableA %v, tableB %v ", clonedTableA.View.Cols, clonedTableB.View.Cols)
	}

	pkCols := d.getPrimaryKeyCols(clonedTableA.Snapshot)

	if len(pkCols) == 0 {
		return d.getNonPrimaryKeyRowDiff(ctx, clonedTableA, clonedTableB)
	}
	return d.getPrimaryKeyRowDiff(ctx, clonedTableA, clonedTableB)
}

func (d *dbDiff) getclonedTablesAtNReqs(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable, n int) (*clonedTableAtN, *clonedTableAtN, error) {
	tables := []*table{clonedTableA.Plus, clonedTableA.Minus, clonedTableB.Plus, clonedTableB.Minus}
	for _, t := range tables {
		query := fmt.Sprintf(`                                                                                                                                                                           
		CREATE OR REPLACE VIEW %s AS (                                                                                                                                                                              
			SELECT * FROM %s                                                                                                                                                                                                                                                                                                                                           
			where %s <= %d
			ORDER BY %s                                                                                                                                                                              
		);                                                                                                                                                                                               
		`, fmt.Sprintf("%s%d", t.Name, n), t.Name, clonedTableA.Counter.Colname, n, clonedTableA.Counter.Colname)

		if _, err := d.connPool.Exec(ctx, query); err != nil {
			return nil, nil, err
		}
	}

	updatedA := &clonedTableAtN{
		Counter: clonedTableA.Counter,
		Plus: &view{
			Name: fmt.Sprintf("%s%d", clonedTableA.Plus.Name, n),
			Cols: clonedTableA.Plus.Cols,
		},
		Minus: &view{
			Name: fmt.Sprintf("%s%d", clonedTableA.Minus.Name, n),
			Cols: clonedTableA.Minus.Cols,
		},
		View:     clonedTableA.View,
		Snapshot: clonedTableA.Snapshot,
	}

	updatedB := &clonedTableAtN{
		Counter: clonedTableB.Counter,
		Plus: &view{
			Name: fmt.Sprintf("%s%d", clonedTableB.Plus.Name, n),
			Cols: clonedTableB.Plus.Cols,
		},
		Minus: &view{
			Name: fmt.Sprintf("%s%d", clonedTableB.Minus.Name, n),
			Cols: clonedTableB.Minus.Cols,
		},
		View:     clonedTableB.View,
		Snapshot: clonedTableB.Snapshot,
	}

	return updatedA, updatedB, nil
}

func (d *dbDiff) getClonedTableRowDiffAtNReqs(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable, n int) (*Diff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableB.View.Cols) {
		return nil, fmt.Errorf("cannot get row diff for different cols, tableA %v, tableB %v ", clonedTableA.View.Cols, clonedTableB.View.Cols)
	}
	updatedA, updatedB, err := d.getclonedTablesAtNReqs(ctx, clonedTableA, clonedTableB, n)
	if err != nil {
		return nil, fmt.Errorf("failed to get cloned tables at n reqs, %w", err)
	}

	return d.getClonedTableRowDiff(ctx, updatedA, updatedB)
}
