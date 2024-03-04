// This file creates R+/R-/R' clone database based on database metadata

package clonedatabase

import (
	"context"
	"fmt"
	"strings"
)

type ClonedTable struct {
	Snapshot *Table
	Plus     *Table
	Minus    *Table
	View     *View
}

type CloneDdl struct {
	ClonedTables map[string]*ClonedTable
	Database     *Database
}

func NewCloneDdl(ctx context.Context, Database *Database) (*CloneDdl, error) {
	clonedTables := make(map[string]*ClonedTable)
	database := &CloneDdl{
		ClonedTables: clonedTables,
		Database:     Database,
	}

	return database, nil
}

func (c *CloneDdl) CreateClonedTables(ctx context.Context) error {
	for tablename, table := range c.Database.Tables {
		clonedTable, err := c.createClonedTable(ctx, table)
		if err != nil {
			return err
		}
		c.ClonedTables[tablename] = clonedTable
	}

	return nil
}

func (c *CloneDdl) Close(ctx context.Context) error {
	// drop all tables, rename the snapshot back
	for tablename, table := range c.ClonedTables {
		err := c.dropView(ctx, table.View.Name)
		if err != nil {
			return err
		}

		err = c.dropTable(ctx, table.Plus.Name)
		if err != nil {
			return err
		}
		err = c.dropTable(ctx, table.Minus.Name)
		if err != nil {
			return err
		}
		err = c.alterTableName(ctx, tablename, table.Snapshot)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CloneDdl) createClonedTable(ctx context.Context, snapshot *Table) (*ClonedTable, error) {
	plus, minus, err := c.createPlusMinusTable(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to create +/- tables, %s", err)
	}
	view, err := c.createView(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to create views, %s", err)
	}

	err = c.applyIndexes(ctx, snapshot, plus, minus)
	if err != nil {
		return nil, fmt.Errorf("failed to apply index, %s", err)
	}

	err = c.applyRules(ctx, snapshot, view)
	if err != nil {
		return nil, fmt.Errorf("failed to apply rules, %s", err)
	}

	// at the end, rename original snapshot to tablesnapshot and view as the original snapshot name
	originalName := snapshot.Name
	err = c.alterTableName(ctx, snapshot.Name+"snapshot", snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to alter table names, %s", err)
	}

	err = c.alterViewName(ctx, originalName, view)
	if err != nil {
		return nil, fmt.Errorf("failed to alter view names, %s", err)
	}

	clonedTable := &ClonedTable{
		Snapshot: snapshot,
		Plus:     plus,
		Minus:    minus,
		View:     view,
	}

	c.ClonedTables[originalName] = clonedTable
	return clonedTable, nil
}

func (c *CloneDdl) alterTableName(ctx context.Context, newName string, table *Table) error {
	query := fmt.Sprintf("ALTER TABLE %s RENAME to %s;", table.Name, newName)
	_, err := c.Database.connPool.Exec(ctx, query)
	if err != nil {
		return err
	}
	table.Name = newName

	return nil
}

func (c *CloneDdl) alterViewName(ctx context.Context, newName string, view *View) error {
	query := fmt.Sprintf("ALTER VIEW %s RENAME to %s;", view.Name, newName)

	_, err := c.Database.connPool.Exec(ctx, query)
	if err != nil {
		return err
	}
	view.Name = newName

	return nil
}

func (c *CloneDdl) dropTable(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", name)

	_, err := c.Database.connPool.Exec(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (c *CloneDdl) dropView(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP VIEW IF EXISTS %s;", name)

	_, err := c.Database.connPool.Exec(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (c *CloneDdl) createPlusMinusTable(ctx context.Context, prodTable *Table) (*Table, *Table, error) {
	plus := &Table{
		Name: prodTable.Name + "plus",
	}
	plus.Cols = make(map[string]Column)

	minus := &Table{
		Name: prodTable.Name + "minus",
	}
	minus.Cols = make(map[string]Column)
	columns := ""
	for name, col := range prodTable.Cols {
		plus.Cols[name] = col
		minus.Cols[name] = col
		columns += name + " "
		columns += col.DataType
		if col.CharacterMaximumLength > 0 {
			columns += fmt.Sprintf("(%d)", col.CharacterMaximumLength)
		}
		if col.Nullable == "NO" {
			columns += " NOT NULL"
		}
		columns += ",\n"
	}
	// remove the last comma
	columns = columns[:len(columns)-2]

	// create R+
	plusQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n %s \n);", prodTable.Name+"plus", columns)
	_, err := c.Database.connPool.Exec(ctx, plusQuery)
	if err != nil {
		return nil, nil, err
	}
	// create R-
	minusQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n %s \n);", prodTable.Name+"minus", columns)
	_, err = c.Database.connPool.Exec(ctx, minusQuery)
	if err != nil {
		return nil, nil, err
	}

	return plus, minus, nil
}

func (c *CloneDdl) createView(ctx context.Context, prodTable *Table) (*View, error) {
	view := &View{
		Name: prodTable.Name + "view",
	}
	view.Cols = make(map[string]Column)

	// for views, column is always nullable. No constraint is enforced on the view itself, but on the underlying tables.
	var colnames []string
	for name, col := range prodTable.Cols {
		col.Nullable = "YES"
		view.Cols[name] = col
		colnames = append(colnames, name)
	}

	viewQuery := fmt.Sprintf(`
	CREATE  VIEW %s AS
	SELECT %s FROM %s
	UNION ALL
	SELECT %s FROM %s
	EXCEPT ALL
	SELECT %s FROM %s;
	`, view.Name, strings.Join(colnames, ", "), prodTable.Name, strings.Join(colnames, ", "), prodTable.Name+"plus", strings.Join(colnames, ", "), prodTable.Name+"minus")

	_, err := c.Database.connPool.Exec(ctx, viewQuery)
	if err != nil {
		return nil, err
	}

	return view, nil
}

func (c *CloneDdl) applyIndexes(ctx context.Context, prodTable *Table, plus *Table, minus *Table) error {
	// rename index
	// remove UNIQUE if there is any
	// apply index on both plus and minus tables
	for _, index := range prodTable.Indexs {
		var plusIndex, minusIndex Index
		indexDef := strings.ReplaceAll(strings.ToLower(index.IndexDef), " unique ", " ")

		plusIndexDef := strings.ReplaceAll(strings.ToLower(indexDef), prodTable.Name, plus.Name)
		plusIndex.IndexDef = plusIndexDef
		plusIndex.Name = strings.ReplaceAll(strings.ToLower(index.Name), prodTable.Name, plus.Name)
		plusIndex.isUnique = index.isUnique
		plus.Indexs = append(plus.Indexs, plusIndex)

		_, err := c.Database.connPool.Exec(ctx, plusIndexDef)
		if err != nil {
			return err
		}

		minusIndexDef := strings.ReplaceAll(strings.ToLower(indexDef), prodTable.Name, minus.Name)
		minusIndex.IndexDef = minusIndexDef
		minusIndex.Name = strings.ReplaceAll(strings.ToLower(index.Name), prodTable.Name, minus.Name)
		minusIndex.isUnique = index.isUnique
		minus.Indexs = append(minus.Indexs, minusIndex)
		_, err = c.Database.connPool.Exec(ctx, minusIndexDef)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CloneDdl) applyRules(ctx context.Context, prodTable *Table, view *View) error {
	for _, rule := range prodTable.Rules {
		var viewRule Rule
		viewRule.Name = "view_" + rule.Name
		ruleDef := strings.ReplaceAll(strings.ToLower(rule.Definition), rule.Name, viewRule.Name)
		ruleDef = strings.ReplaceAll(strings.ToLower(ruleDef), prodTable.Name, view.Name)
		viewRule.Definition = ruleDef
		view.Rules = append(view.Rules, viewRule)
		_, err := c.Database.connPool.Exec(ctx, ruleDef)
		if err != nil {
			return err
		}
	}
	return nil
}
