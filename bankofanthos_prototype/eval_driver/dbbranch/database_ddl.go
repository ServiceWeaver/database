// This file creates R+/R-/R' clone database based on database metadata

package dbbranch

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	counterColName = "rid"
	counterName    = "rid"
)

type counter struct {
	Name    string
	Colname string
}
type clonedTable struct {
	Snapshot *table
	Plus     *table
	Minus    *table
	View     *view
	Counter  *counter

	Functions []string
	Triggers  []string
}

type cloneDdl struct {
	clonedTables map[string]*clonedTable
	database     *database
	namespace    string
	counter      *counter // tables in same database share the same counter table

	mu sync.Mutex
}

func newCloneDdl(ctx context.Context, Database *database, namespace string) (*cloneDdl, error) {
	database := &cloneDdl{
		clonedTables: map[string]*clonedTable{},
		database:     Database,
		namespace:    namespace,
	}

	err := database.createClonedTables(ctx)
	if err != nil {
		return nil, err
	}

	return database, nil
}

func (c *cloneDdl) createClonedTables(ctx context.Context) error {
	err := c.createSchema(ctx, c.namespace)
	if err != nil {
		return err
	}

	err = c.createCounter(ctx)
	if err != nil {
		return err
	}

	clonedTableChan := make(chan struct {
		tablename string
		cloned    *clonedTable
		err       error
	})

	var wg sync.WaitGroup

	for tablename, t := range c.database.Tables {
		wg.Add(1)
		go func(tablename string, t *table) {
			defer wg.Done()
			cloned, err := c.createClonedTable(ctx, t)
			clonedTableChan <- struct {
				tablename string
				cloned    *clonedTable
				err       error
			}{tablename, cloned, err}
		}(tablename, t)
	}
	go func() {
		wg.Wait()
		close(clonedTableChan)
	}()
	for result := range clonedTableChan {
		if result.err != nil {
			return fmt.Errorf("table %s: %s", result.tablename, result.err)
		}
		c.clonedTables[result.tablename] = result.cloned
	}

	return nil
}

func (c *cloneDdl) reset(ctx context.Context) error {
	for tablename, table := range c.clonedTables {
		// drop all created triggers
		for _, trigger := range table.Triggers {
			if err := dropTrigger(ctx, c.database.connPool, trigger, tablename); err != nil {
				return err
			}
		}

		for _, function := range table.Functions {
			if err := dropFunction(ctx, c.database.connPool, function); err != nil {
				return err
			}
		}

		if err := c.alterViewSchema(ctx, table.View); err != nil {
			return err
		}

		if err := alterTableName(ctx, c.database.connPool, tablename, table.Snapshot); err != nil {
			return err
		}
	}

	return nil
}

func (c *cloneDdl) close(ctx context.Context) error {
	return dropSchemaCascade(ctx, c.database.connPool, c.namespace)
}

func (c *cloneDdl) createClonedTable(ctx context.Context, snapshot *table) (*clonedTable, error) {
	c.mu.Lock()
	plus, minus, view, err := c.createPlusMinusTableAndView(ctx, snapshot, c.counter)
	if err != nil {
		return nil, fmt.Errorf("failed to create +/- tables or view: %w", err)
	}
	clonedTable := &clonedTable{
		Snapshot: snapshot,
		Plus:     plus,
		Minus:    minus,
		View:     view,
		Counter:  c.counter,
	}

	originalName := snapshot.Name
	c.clonedTables[originalName] = clonedTable
	c.mu.Unlock()

	// For now, do not apply index
	// err = c.applyIndexes(ctx, snapshot, plus, minus)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to apply index, %s", err)
	// }

	err = c.applyRules(ctx, snapshot, view)
	if err != nil {
		return nil, fmt.Errorf("failed to apply rules: %w", err)
	}

	// at the end, rename original snapshot to tablesnapshot and view as the original snapshot name
	err = alterTableName(ctx, c.database.connPool, snapshot.Name+snapshotSuffix, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to alter table names: %w", err)
	}

	err = alterViewName(ctx, c.database.connPool, originalName, view)
	if err != nil {
		return nil, fmt.Errorf("failed to alter view names: %w", err)
	}

	return clonedTable, nil
}

func (c *cloneDdl) alterViewSchema(ctx context.Context, view *view) error {
	query := fmt.Sprintf("ALTER VIEW %s SET SCHEMA %s;", view.Name, c.namespace)

	if _, err := c.database.connPool.Exec(ctx, query); err != nil {
		return err
	}

	view.Name = c.namespace + "." + view.Name
	return nil
}

func (c *cloneDdl) createSchema(ctx context.Context, namespace string) error {
	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", namespace)

	_, err := c.database.connPool.Exec(ctx, query)
	return err
}

func (c *cloneDdl) getPrimaryKeyCols(table *table) []string {
	for _, idx := range table.Indexes {
		if idx.IsUnique && strings.Contains(idx.Name, "pkey") {
			return idx.ColumnNames
		}
	}

	return nil
}

// TODO: Pick name for views and plus, minus which does not exist in database
func (c *cloneDdl) createPlusMinusTableAndView(ctx context.Context, prodTable *table, counter *counter) (*table, *table, *view, error) {
	plus := &table{
		Name: fmt.Sprintf("%s.%splus", c.namespace, prodTable.Name),
		Cols: map[string]column{},
	}

	minus := &table{
		Name: fmt.Sprintf("%s.%sminus", c.namespace, prodTable.Name),
		Cols: map[string]column{},
	}

	var columnslst []string
	for name, col := range prodTable.Cols {
		plus.Cols[name] = col
		minus.Cols[name] = col
		var column strings.Builder

		column.WriteString(name + " " + col.DataType)
		if col.CharacterMaximumLength > 0 {
			fmt.Fprintf(&column, "(%d)", col.CharacterMaximumLength)
		}
		if col.Nullable == "NO" {
			column.WriteString(" NOT NULL")
		}
		columnslst = append(columnslst, column.String())
	}

	//TODO: make sure counter col name does not exist in table and each branch have the same col name

	columnslst = append(columnslst, fmt.Sprintf("%s bigint", counter.Colname))
	columns := strings.Join(columnslst, ",\n")

	// create R+
	plusQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n %s \n);", plus.Name, columns)
	_, err := c.database.connPool.Exec(ctx, plusQuery)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create R+, %w", err)
	}
	// create R-
	minusQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n %s \n);", minus.Name, columns)
	_, err = c.database.connPool.Exec(ctx, minusQuery)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create R-, %w", err)
	}

	view := &view{
		Name: prodTable.Name + "view",
		Cols: map[string]column{},
	}

	// for views, column is always nullable. No constraint is enforced on the view itself, but on the underlying tables.
	var colnames []string
	for name, col := range prodTable.Cols {
		col.Nullable = "YES"
		view.Cols[name] = col
		colnames = append(colnames, name)
	}
	sort.Strings(colnames)

	pk_cols := c.getPrimaryKeyCols(prodTable)
	viewQuery := ""

	// Create a view prod table union all plus except all minus
	if len(pk_cols) == 0 {
		dCols := make([]string, len(colnames))
		cCols := make([]string, len(colnames))
		for i, col := range colnames {
			dCols[i] = "d." + col
			cCols[i] = "c." + col
		}

		viewQuery = fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS(
		WITH numbered_data AS (
			SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS rn 
			FROM (
				SELECT %s FROM %s
				UNION ALL
				SELECT %s FROM %s
			) AS data
		),
		numbered_c AS (
			SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS rn 
			FROM %s
		)
		SELECT %s
		FROM numbered_data d
		LEFT JOIN numbered_c c ON (%s) = (%s) AND d.rn = c.rn
		WHERE (%s) IS NULL
		);
`, view.Name, strings.Join(colnames, ", "), strings.Join(colnames, ", "), strings.Join(colnames, ", "), strings.Join(colnames, ", "), prodTable.Name, strings.Join(colnames, ", "), plus.Name, strings.Join(colnames, ", "), strings.Join(colnames, ", "), strings.Join(colnames, ", "), minus.Name, strings.Join(dCols, ","), strings.Join(dCols, ","), strings.Join(cCols, ","), strings.Join(cCols, ","))

	} else {
		unionCols := make([]string, len(colnames))
		minusCols := make([]string, len(colnames))
		for i, col := range colnames {
			unionCols[i] = "tmp." + col
			minusCols[i] = minus.Name + "." + col
		}

		viewQuery = fmt.Sprintf(`
		CREATE VIEW %s AS
		SELECT %s FROM
		( SELECT %s FROM %s
		UNION ALL
		SELECT %s FROM %s) AS tmp
		WHERE NOT EXISTS(
		SELECT 1 FROM %s
		WHERE (%s) = (%s)
		) ;
		`, view.Name, strings.Join(colnames, ", "), strings.Join(colnames, ", "), prodTable.Name, strings.Join(colnames, ", "), plus.Name, minus.Name, strings.Join(unionCols, ","), strings.Join(minusCols, ","))
	}

	_, err = c.database.connPool.Exec(ctx, viewQuery)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create R view, %w", err)
	}

	return plus, minus, view, nil
}

func (c *cloneDdl) applyIndexes(ctx context.Context, prodTable *table, plus *table, minus *table) error {
	// rename index
	// remove UNIQUE if there is any
	// apply index on both plus and minus tables
	for _, idx := range prodTable.Indexes {
		var plusIndex, minusIndex index
		indexDef := strings.ReplaceAll(strings.ToLower(idx.IndexDef), " unique ", " ")

		plusIndexDef := strings.ReplaceAll(strings.ToLower(indexDef), prodTable.Name, plus.Name)
		plusIndex.IndexDef = plusIndexDef
		plusIndex.Name = strings.ReplaceAll(strings.ToLower(idx.Name), prodTable.Name, plus.Name)
		plusIndex.IsUnique = idx.IsUnique
		plus.Indexes = append(plus.Indexes, plusIndex)

		_, err := c.database.connPool.Exec(ctx, plusIndexDef)
		if err != nil {
			return err
		}

		minusIndexDef := strings.ReplaceAll(strings.ToLower(indexDef), prodTable.Name, minus.Name)
		minusIndex.IndexDef = minusIndexDef
		minusIndex.Name = strings.ReplaceAll(strings.ToLower(idx.Name), prodTable.Name, minus.Name)
		minusIndex.IsUnique = idx.IsUnique
		minus.Indexes = append(minus.Indexes, minusIndex)
		_, err = c.database.connPool.Exec(ctx, minusIndexDef)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cloneDdl) applyRules(ctx context.Context, prodTable *table, view *view) error {
	for _, r := range prodTable.Rules {
		var viewRule rule
		viewRule.Name = "view_" + r.Name
		ruleDef := strings.ReplaceAll(strings.ToLower(r.Definition), r.Name, viewRule.Name)
		ruleDef = strings.ReplaceAll(strings.ToLower(ruleDef), prodTable.Name, view.Name)
		viewRule.Definition = ruleDef
		view.Rules = append(view.Rules, viewRule)

		_, err := c.database.connPool.Exec(ctx, ruleDef)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: check counter name does not exist
func (c *cloneDdl) createCounter(ctx context.Context) error {
	c.counter = &counter{Name: fmt.Sprintf("%s.%s", c.namespace, counterName), Colname: counterColName}

	_, err := c.database.connPool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT);", c.counter.Name))
	if err != nil {
		return err
	}

	_, err = c.database.connPool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES(0)", c.counter.Name))
	return err
}

func (c *cloneDdl) incrementCounter(ctx context.Context) error {
	_, err := c.database.connPool.Exec(ctx, fmt.Sprintf("UPDATE %s SET id=id+1", c.counter.Name))
	return err
}
