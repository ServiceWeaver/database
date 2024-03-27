// This file creates R+/R-/R' clone database based on database metadata

package dbclone

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type clonedTable struct {
	Snapshot *table
	Plus     *table
	Minus    *table
	View     *view
}

type cloneDdl struct {
	clonedTables map[string]*clonedTable
	database     *database
	namespace    string
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

	for tablename, table := range c.database.Tables {
		clonedTable, err := c.createClonedTable(ctx, table)
		if err != nil {
			return err
		}
		c.clonedTables[tablename] = clonedTable
	}

	return nil
}

func (c *cloneDdl) reset(ctx context.Context) error {
	for tablename, table := range c.clonedTables {
		err := c.alterViewSchema(ctx, table.View)
		if err != nil {
			return err
		}
		err = alterTableName(ctx, c.database.connPool, tablename, table.Snapshot)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cloneDdl) close(ctx context.Context) error {
	return dropSchemaCascade(ctx, c.database.connPool, c.namespace)
}

func (c *cloneDdl) createClonedTable(ctx context.Context, snapshot *table) (*clonedTable, error) {
	plus, minus, view, err := c.createPlusMinusTableAndView(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to create +/- tables or view: %w", err)
	}

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
	originalName := snapshot.Name
	err = alterTableName(ctx, c.database.connPool, snapshot.Name+"snapshot", snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to alter table names: %w", err)
	}

	err = alterViewName(ctx, c.database.connPool, originalName, view)
	if err != nil {
		return nil, fmt.Errorf("failed to alter view names: %w", err)
	}

	clonedTable := &clonedTable{
		Snapshot: snapshot,
		Plus:     plus,
		Minus:    minus,
		View:     view,
	}

	c.clonedTables[originalName] = clonedTable
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

// TODO: Pick name for views and plus, minus which does not exist in database
func (c *cloneDdl) createPlusMinusTableAndView(ctx context.Context, prodTable *table) (*table, *table, *view, error) {
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

	viewQuery := fmt.Sprintf(`
	CREATE VIEW %s AS
	SELECT %s FROM %s
	UNION ALL
	SELECT %s FROM %s
	EXCEPT ALL
	SELECT %s FROM %s
	ORDER BY %s;
	`, view.Name, strings.Join(colnames, ", "), prodTable.Name, strings.Join(colnames, ", "), plus.Name, strings.Join(colnames, ", "), minus.Name, strings.Join(colnames, ", "))

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
