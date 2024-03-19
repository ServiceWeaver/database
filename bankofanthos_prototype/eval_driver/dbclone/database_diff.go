package dbclone

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Row []interface{}

type RowDiff struct {
	Left     []*Row // control
	Middle   []*Row // baseline
	Right    []*Row // experimental
	ColNames []string
}

type DiffType int

const (
	APlusOnly DiffType = iota + 1
	BPlusOnly
	APlusBPlus
	AMinusOnly
	BMinusOnly
	AMinusBMinus
	PrimaryKey
)

func (d DiffType) String() string {
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
	sort.Strings(colNames)
	query := fmt.Sprintf("SELECT %s FROM %s;", strings.Join(colNames, ", "), view.Name)

	rows, err := d.connPool.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		rowVal := make([]interface{}, len(colNames))
		rowPtr := make([]interface{}, len(colNames))
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

	return dumpRows, colNames, nil
}

// tableA - tableB
func (d *dbDiff) minusTable(ctx context.Context, tableA *table, tableB *table, viewName string) (*view, error) {
	var colNames []string
	if !reflect.DeepEqual(tableA.Cols, tableB.Cols) {
		return nil, fmt.Errorf("tableA %v and tableB %v have different columns, cannot minus", tableA.Cols, tableB.Cols)
	}
	for n := range tableA.Cols {
		colNames = append(colNames, n)
	}
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	EXCEPT ALL
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), tableA.Name, strings.Join(colNames, ", "), tableB.Name, strings.Join(colNames, ", "))

	_, err := d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: tableA.Cols}, nil
}

// tableA intersect tableB
func (d *dbDiff) intersectTable(ctx context.Context, tableA *table, tableB *table, viewName string) (*view, error) {
	var colNames []string
	if !reflect.DeepEqual(tableA.Cols, tableB.Cols) {
		return nil, fmt.Errorf("tableA %v and tableB %v have different columns, cannot intersect", tableA.Cols, tableB.Cols)
	}
	for n := range tableA.Cols {
		colNames = append(colNames, n)
	}
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	INTERSECT
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), tableA.Name, strings.Join(colNames, ", "), tableB.Name, strings.Join(colNames, ", "))

	_, err := d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: tableA.Cols}, nil
}

func (d *dbDiff) trimClonedTable(ctx context.Context, clonedTable *clonedTable) (*view, *view, error) {
	trimPlusName := clonedTable.Plus.Name + "trim"
	trimPlus, err := d.minusTable(ctx, clonedTable.Plus, clonedTable.Minus, trimPlusName)
	if err != nil {
		return nil, nil, err
	}

	trimMinusName := clonedTable.Minus.Name + "trim"
	trimMinus, err := d.minusTable(ctx, clonedTable.Minus, clonedTable.Plus, trimMinusName)
	if err != nil {
		return nil, nil, err
	}

	return trimPlus, trimMinus, nil
}

// viewA - viewB
func (d *dbDiff) minusView(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	var colNames []string
	if !reflect.DeepEqual(viewA.Cols, viewB.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot minus", viewB.Cols, viewB.Cols)
	}
	for n := range viewA.Cols {
		colNames = append(colNames, n)
	}
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	EXCEPT ALL
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), viewA.Name, strings.Join(colNames, ", "), viewB.Name, strings.Join(colNames, ", "))

	_, err := d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: viewA.Cols}, nil
}

// viewA intersect viewB
func (d *dbDiff) intersectView(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	var colNames []string
	if !reflect.DeepEqual(viewA.Cols, viewB.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot intersect", viewA.Cols, viewB.Cols)
	}
	for n := range viewA.Cols {
		colNames = append(colNames, n)
	}
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	INTERSECT
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), viewA.Name, strings.Join(colNames, ", "), viewB.Name, strings.Join(colNames, ", "))

	_, err := d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: viewA.Cols}, nil
}

// viewA union viewB
func (d *dbDiff) unionView(ctx context.Context, viewA *view, viewB *view, viewName string) (*view, error) {
	var colNames []string
	if !reflect.DeepEqual(viewA.Cols, viewB.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot union", viewA.Cols, viewB.Cols)
	}
	for n := range viewA.Cols {
		colNames = append(colNames, n)
	}
	sort.Strings(colNames)

	query := fmt.Sprintf(`
	CREATE VIEW %s AS (
	SELECT %s FROM %s
	UNION ALL
	SELECT %s FROM %s
	ORDER BY %s);
	`, viewName, strings.Join(colNames, ", "), viewA.Name, strings.Join(colNames, ", "), viewB.Name, strings.Join(colNames, ", "))

	_, err := d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: viewA.Cols}, nil
}

// getUniqueCols returns primary key if there is any, if cannot find, it returns unique columns. If there is no
// unique columns, this function will return empty list.
func (d *dbDiff) getPrimaryKeyCols(table *table) ([]string, error) {
	var uniqueCols []string
	for _, idx := range table.Indexes {
		if idx.IsUnique && strings.Contains(idx.Name, "pkey") {
			uniqueCols = idx.ColumnNames
			break
		}
	}

	return uniqueCols, nil
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

	primaryCols, err := d.getPrimaryKeyCols(clonedTableA.Snapshot)
	if err != nil {
		return nil, err
	}

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

	_, err = d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}
	cols := make(map[string]column)
	for _, colName := range primaryCols {
		cols[colName] = clonedTableA.View.Cols[colName]
	}
	return &view{Name: viewName, Cols: cols}, nil
}

func (d *dbDiff) getOnePrimaryRow(ctx context.Context, primaryKeyView *view, sideView *view, viewName string) (*view, error) {
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

	_, err := d.connPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &view{Name: viewName, Cols: sideView.Cols}, nil
}

func (d *dbDiff) getNonPrimaryKeyRowDiff(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable) (map[DiffType]*RowDiff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableA.View.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot intersect", clonedTableA.View.Cols, clonedTableA.View.Cols)
	}
	rowDiffs := make(map[DiffType]*RowDiff)
	// trim cloned table A
	aPlus, aMinus, err := d.trimClonedTable(ctx, clonedTableA)
	if err != nil {
		return nil, err
	}

	// trim cloned table B
	bPlus, bMinus, err := d.trimClonedTable(ctx, clonedTableB)
	if err != nil {
		return nil, err
	}

	// A+ - B+
	aPlusOnly, err := d.minusView(ctx, aPlus, bPlus, "aPlusOnly")
	if err != nil {
		return nil, err
	}
	aPlusRows, colNames, err := d.dumpView(ctx, aPlusOnly)
	if err != nil {
		return nil, err
	}
	err = dropView(ctx, d.connPool, aPlusOnly.Name)
	if err != nil {
		return nil, err
	}

	aPlusDiff := &RowDiff{Left: aPlusRows, Middle: nil, Right: nil, ColNames: colNames}
	rowDiffs[APlusOnly] = aPlusDiff

	// A+ intersect B+
	aPlusBPlus, err := d.intersectView(ctx, aPlus, bPlus, "aPlusBPlus")
	if err != nil {
		return nil, err
	}

	aPlusBPlusRows, colNames, err := d.dumpView(ctx, aPlusBPlus)
	if err != nil {
		return nil, err
	}
	err = dropView(ctx, d.connPool, aPlusBPlus.Name)
	if err != nil {
		return nil, err
	}

	aPlusBPlusRowsRowDiff := &RowDiff{Left: aPlusBPlusRows, Middle: nil, Right: aPlusBPlusRows, ColNames: colNames}
	rowDiffs[APlusBPlus] = aPlusBPlusRowsRowDiff

	// B+ - A+
	bPlusOnly, err := d.minusView(ctx, bPlus, aPlus, "bPlusOnly")
	if err != nil {
		return nil, err
	}

	bPlusRows, colNames, err := d.dumpView(ctx, bPlusOnly)
	if err != nil {
		return nil, err
	}
	err = dropView(ctx, d.connPool, bPlusOnly.Name)
	if err != nil {
		return nil, err
	}

	bPlusDiff := &RowDiff{Left: nil, Middle: nil, Right: bPlusRows, ColNames: colNames}
	rowDiffs[BPlusOnly] = bPlusDiff

	// B- - A-
	bMinusOnly, err := d.minusView(ctx, bMinus, aMinus, "bMinusOnly")
	if err != nil {
		return nil, err
	}
	bMinusRows, colNames, err := d.dumpView(ctx, bMinusOnly)
	if err != nil {
		return nil, err
	}
	err = dropView(ctx, d.connPool, bMinusOnly.Name)
	if err != nil {
		return nil, err
	}
	bMinusDiff := &RowDiff{Left: bMinusRows, Middle: nil, Right: nil, ColNames: colNames}
	rowDiffs[BMinusOnly] = bMinusDiff

	// B- intersect A-
	aMinusBMinus, err := d.intersectView(ctx, aMinus, bMinus, "aMinusBMinus")
	if err != nil {
		return nil, err
	}
	aMinusbMinusRows, colNames, err := d.dumpView(ctx, aMinusBMinus)
	if err != nil {
		return nil, err
	}
	err = dropView(ctx, d.connPool, aMinusBMinus.Name)
	if err != nil {
		return nil, err
	}
	aMinusbMinusDiff := &RowDiff{Left: aMinusbMinusRows, Middle: nil, Right: aMinusbMinusRows, ColNames: colNames}
	rowDiffs[AMinusBMinus] = aMinusbMinusDiff

	// A- - B-
	aMinusOnly, err := d.minusView(ctx, aMinus, bMinus, "aMinusOnly")
	if err != nil {
		return nil, err
	}
	aMinusRows, colNames, err := d.dumpView(ctx, aMinusOnly)
	if err != nil {
		return nil, err
	}
	err = dropView(ctx, d.connPool, aMinusOnly.Name)
	if err != nil {
		return nil, err
	}
	aMinusDiff := &RowDiff{Left: nil, Middle: nil, Right: aMinusRows, ColNames: colNames}
	rowDiffs[AMinusOnly] = aMinusDiff

	if err = dropView(ctx, d.connPool, aPlus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, aMinus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, bPlus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, bMinus.Name); err != nil {
		return nil, err
	}

	return rowDiffs, nil
}

// left: A+, B- - A-
// middle: A- intersect B-, A- - B-, B- - A-
// right: B+, A- - B-
func (d *dbDiff) getPrimaryKeyRowDiff(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable) (map[DiffType]*RowDiff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableA.View.Cols) {
		return nil, fmt.Errorf("viewA %v and viewB %v have different columns, cannot intersect", clonedTableA.View.Cols, clonedTableA.View.Cols)
	}
	rowDiffs := make(map[DiffType]*RowDiff)

	// trim cloned table A
	aPlus, aMinus, err := d.trimClonedTable(ctx, clonedTableA)
	if err != nil {
		return nil, err
	}

	// trim cloned table B
	bPlus, bMinus, err := d.trimClonedTable(ctx, clonedTableB)
	if err != nil {
		return nil, err
	}

	primaryKeyView, err := d.getPrimarKeyRows(ctx, aPlus, bPlus, aMinus, bMinus, clonedTableA, clonedTableB)
	if err != nil {
		return nil, err
	}

	bMinusAMinus, err := d.minusView(ctx, bMinus, aMinus, "BMinusAMinus")
	if err != nil {
		return nil, err
	}

	aMinusBMinus, err := d.minusView(ctx, aMinus, bMinus, "AMinusBMinus")
	if err != nil {
		return nil, err
	}

	aMinusIntersectBMinus, err := d.intersectView(ctx, aMinus, bMinus, "AMinusIntersectBMinus")
	if err != nil {
		return nil, err
	}

	// Left: A+, B- - A-
	leftSideView, err := d.unionView(ctx, aPlus, bMinusAMinus, "leftView")
	if err != nil {
		return nil, err
	}
	leftSideDiff, err := d.getOnePrimaryRow(ctx, primaryKeyView, leftSideView, "leftDiff")
	if err != nil {
		return nil, err
	}
	leftSideRows, colNames, err := d.dumpView(ctx, leftSideDiff)
	if err != nil {
		return nil, err
	}
	if err = dropView(ctx, d.connPool, leftSideDiff.Name); err != nil {
		return nil, err
	}
	if err = dropView(ctx, d.connPool, leftSideView.Name); err != nil {
		return nil, err
	}

	// Right: B+, A- - B-
	rightSideView, err := d.unionView(ctx, bPlus, aMinusBMinus, "rightView")
	if err != nil {
		return nil, err
	}
	rightSideDiff, err := d.getOnePrimaryRow(ctx, primaryKeyView, rightSideView, "rightDiff")
	if err != nil {
		return nil, err
	}
	rightSideRows, _, err := d.dumpView(ctx, rightSideDiff)
	if err != nil {
		return nil, err
	}
	if err = dropView(ctx, d.connPool, rightSideDiff.Name); err != nil {
		return nil, err
	}
	if err = dropView(ctx, d.connPool, rightSideView.Name); err != nil {
		return nil, err
	}

	// Middle: A- intersect B-, A- - B-, B- - A-
	halfMiddleSideView, err := d.unionView(ctx, aMinusIntersectBMinus, aMinusBMinus, "halfMiddle")
	if err != nil {
		return nil, err
	}
	middleSideView, err := d.unionView(ctx, halfMiddleSideView, bMinusAMinus, "middleView")
	if err != nil {
		return nil, err
	}
	middleSideDiff, err := d.getOnePrimaryRow(ctx, primaryKeyView, middleSideView, "middleDiff")
	if err != nil {
		return nil, err
	}
	middleSideRows, _, err := d.dumpView(ctx, middleSideDiff)
	if err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, middleSideDiff.Name); err != nil {
		return nil, err
	}
	if err = dropView(ctx, d.connPool, middleSideView.Name); err != nil {
		return nil, err
	}
	if err = dropView(ctx, d.connPool, halfMiddleSideView.Name); err != nil {
		return nil, err
	}

	rowDiff := &RowDiff{Left: leftSideRows, Right: rightSideRows, Middle: middleSideRows, ColNames: colNames}
	rowDiffs[PrimaryKey] = rowDiff

	// drop all views
	if err = dropView(ctx, d.connPool, primaryKeyView.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, aMinusBMinus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, bMinusAMinus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, aMinusIntersectBMinus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, aPlus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, aMinus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, bPlus.Name); err != nil {
		return nil, err
	}

	if err = dropView(ctx, d.connPool, bMinus.Name); err != nil {
		return nil, err
	}

	return rowDiffs, nil
}

func (d *dbDiff) getClonedTableRowDiff(ctx context.Context, clonedTableA *clonedTable, clonedTableB *clonedTable) (map[DiffType]*RowDiff, error) {
	if !reflect.DeepEqual(clonedTableA.View.Cols, clonedTableB.View.Cols) {
		return nil, fmt.Errorf("cannot get row diff for different cols, tableA %v, tableB %v ", clonedTableA.View.Cols, clonedTableB.View.Cols)
	}

	pkCols, err := d.getPrimaryKeyCols(clonedTableA.Snapshot)
	if err != nil {
		return nil, err
	}
	if len(pkCols) == 0 {
		return d.getNonPrimaryKeyRowDiff(ctx, clonedTableA, clonedTableB)
	}
	return d.getPrimaryKeyRowDiff(ctx, clonedTableA, clonedTableB)
}
