// This file defines database struct and get database matadata
package dbclone

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

type idGenerator struct {
	IdentityGeneration string
	IdentityStart      int64
	IndentityIncrement int64
	IdentityMaximum    int64
	IdentityMinimum    int64
}

type column struct {
	Name                   string
	DataType               string
	CharacterMaximumLength int64 // default is 0 if not sepcify
	Nullable               string
	IdGenerator            *idGenerator
}

type index struct {
	Name        string
	IndexDef    string
	IsUnique    bool
	ColumnNames []string
}

// a BeRefedTableName(BeRefedColumnName) is referenced by ForeignKeyTableName(ForeignKeyColumnName)
type reference struct {
	ConstraintName        string
	BeRefedTableName      string
	BeRefedColumnNames    []string
	ForeignKeyTableName   string
	ForeignKeyColumnNames []string
	Action                string // TODO: Get actions from table metadata.Set default to no action
}

// a TableName(ColumnName) has a foreign key constraint which refers another RefTableName(RefColumnName)
type foreignKeyConstraint struct {
	ConstraintName string
	TableName      string
	ColumnNames    []string
	RefTableName   string
	RefColumnNames []string
	Action         string // TODO: Get actions from table metadata. set default to no action
}

type rule struct {
	Name       string
	Definition string
}

type procedure struct {
	Name   string
	ProSrc string
}

type trigger struct {
	Name              string
	EventManipulation string // INSERT, DELETE, UPDATE
	ActionStatement   string
	ActionOrientation string // ROW
	ActionTiming      string // BEFORE, AFTER, INSTEAD OF

	Procedure *procedure // stored_procedure definition
}

type view struct {
	Name     string
	Cols     map[string]column
	Rules    []rule
	Triggers []trigger
}

type table struct {
	Name                  string
	Cols                  map[string]column
	Indexes               []index
	Rules                 []rule
	References            []reference
	ForeignKeyConstraints []foreignKeyConstraint
}

type database struct {
	Tables   map[string]*table
	connPool *pgxpool.Pool
}

func newDatabase(ctx context.Context, connPool *pgxpool.Pool) (*database, error) {
	Tables := map[string]*table{}
	database := &database{
		connPool: connPool,
		Tables:   Tables,
	}
	err := database.getDatabaseMetadata(ctx)

	return database, err
}

func (d *database) getDatabaseMetadata(ctx context.Context) error {
	tables, err := d.listTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	for _, tablename := range tables {
		table, err := d.getTable(ctx, tablename)
		if err != nil {
			return fmt.Errorf("failed to get table %s: %w", tablename, err)
		}
		d.Tables[tablename] = table
	}

	constraints, err := d.getForeignKeyConstraints(ctx)
	if err != nil {
		return fmt.Errorf("failed to get foreign key constraints: %w", err)
	}
	for _, constraint := range constraints {
		constraintTable := d.Tables[constraint.TableName]
		constraintTable.ForeignKeyConstraints = append(constraintTable.ForeignKeyConstraints, constraint)

		refTable := d.Tables[constraint.RefTableName]
		refTable.References = append(refTable.References, reference{
			ConstraintName:        constraint.ConstraintName,
			BeRefedTableName:      constraint.RefTableName,
			BeRefedColumnNames:    constraint.RefColumnNames,
			ForeignKeyTableName:   constraint.TableName,
			ForeignKeyColumnNames: constraint.ColumnNames})
	}

	return nil
}

func (d *database) listTables(ctx context.Context) ([]string, error) {
	var tables []string
	query := `SELECT tablename 
	FROM pg_catalog.pg_tables 
	WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'`

	rows, err := d.connPool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tablename string
	for rows.Next() {
		if err := rows.Scan(&tablename); err != nil {
			return nil, err
		}
		tables = append(tables, tablename)
	}

	return tables, rows.Err()
}

func (d *database) getTable(ctx context.Context, tablename string) (*table, error) {
	var table table
	table.Name = tablename
	var err error
	if table.Cols, err = d.getTableCols(ctx, tablename); err != nil {
		return nil, fmt.Errorf("failed to get table cols: %w", err)
	}

	if table.Indexes, err = d.getTableIndexes(ctx, tablename); err != nil {
		return nil, fmt.Errorf("failed to get table indexes: %w", err)
	}

	if table.Rules, err = d.getTableRules(ctx, tablename); err != nil {
		return nil, fmt.Errorf("failed to get table rules: %w", err)
	}

	return &table, nil
}

// TODO: Now we assume all table schema is public, revisit this
func (d *database) getTableCols(ctx context.Context, tablename string) (map[string]column, error) {
	rows, err := d.connPool.Query(ctx, `
		SELECT column_name,  is_nullable, data_type, character_maximum_length, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum 
		FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1
	`, tablename)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}
	defer rows.Close()

	cols := map[string]column{}
	for rows.Next() {
		var col column
		var CharacterMaximumLength *int64
		var identityGeneration *string
		var identityStart *string
		var indentityIncrement *string
		var identityMaximum *string
		var identityMinimum *string
		if err := rows.Scan(&col.Name, &col.Nullable, &col.DataType, &CharacterMaximumLength, &identityGeneration, &identityStart, &indentityIncrement, &identityMaximum, &identityMinimum); err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}
		if CharacterMaximumLength != nil {
			col.CharacterMaximumLength = *CharacterMaximumLength
		}
		if identityGeneration != nil {
			start, err := strconv.ParseInt(*identityStart, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse identity start to int64: %w", err)
			}
			increment, err := strconv.ParseInt(*indentityIncrement, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse identity increment to int64: %w", err)
			}
			maximum, err := strconv.ParseInt(*identityMaximum, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse identity maximum to int64: %w", err)
			}
			minimum, err := strconv.ParseInt(*identityMinimum, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse identity minimum to int64: %w", err)
			}
			idGenerator := &idGenerator{
				IdentityGeneration: *identityGeneration,
				IdentityStart:      start,
				IndentityIncrement: increment,
				IdentityMaximum:    maximum,
				IdentityMinimum:    minimum,
			}
			col.IdGenerator = idGenerator
		}
		cols[col.Name] = col
	}

	return cols, rows.Err()
}

// TODO: Handle more exotic indexes, e.g. indexes on expression
func (d *database) getTableIndexes(ctx context.Context, tablename string) ([]index, error) {
	var indexes []index
	rows, err := d.connPool.Query(
		ctx,
		`SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1`,
		tablename,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var index index
		if err := rows.Scan(&index.Name, &index.IndexDef); err != nil {
			return nil, err
		}
		if strings.Contains(strings.ToLower(index.IndexDef), "unique") {
			index.IsUnique = true
		}

		colFrom := strings.Index(index.IndexDef, "(")
		colEnd := strings.Index(index.IndexDef, ")")
		cols := strings.ToLower(index.IndexDef[colFrom+1 : colEnd])

		index.ColumnNames = append(index.ColumnNames, cols)
		indexes = append(indexes, index)
	}

	return indexes, rows.Err()
}

func (d *database) getTableRules(ctx context.Context, tablename string) ([]rule, error) {
	var rules []rule
	rows, err := d.connPool.Query(
		ctx,
		`SELECT rulename,definition FROM pg_rules WHERE schemaname = 'public' AND tablename = $1`,
		tablename,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var rule rule
		if err := rows.Scan(&rule.Name, &rule.Definition); err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	return rules, rows.Err()
}

func (d *database) getForeignKeyConstraints(ctx context.Context) ([]foreignKeyConstraint, error) {
	constraintsMap := make(map[string]foreignKeyConstraint)
	rows, err := d.connPool.Query(
		ctx, `
	SELECT
		c.constraint_name, 
		x.table_name,
		x.column_name,
		y.table_name as referenced_table_name,
		y.column_name as referenced_column_name
	FROM information_schema.referential_constraints c
	JOIN information_schema.key_column_usage x
		on x.constraint_name = c.constraint_name
	JOIN information_schema.key_column_usage y
		on y.ordinal_position = x.position_in_unique_constraint
		and y.constraint_name = c.unique_constraint_name
	ORDER BY c.constraint_name, x.ordinal_position;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var constraintName, tableName, columnName, refTableName, refColumnName string
		if err := rows.Scan(&constraintName, &tableName, &columnName, &refTableName, &refColumnName); err != nil {
			return nil, err
		}
		if constraint, ok := constraintsMap[constraintName]; ok {
			constraint.ColumnNames = append(constraint.ColumnNames, columnName)
			constraint.RefColumnNames = append(constraint.RefColumnNames, refColumnName)
			if refTableName != constraint.RefTableName || tableName != constraint.TableName {
				return nil, fmt.Errorf("same constraint name %s contains different table/ref tables name", constraintName)
			}
			constraintsMap[constraintName] = constraint
		} else {
			constraintsMap[constraintName] = foreignKeyConstraint{ConstraintName: constraintName, TableName: tableName, ColumnNames: []string{columnName}, RefTableName: refTableName, RefColumnNames: []string{refColumnName}}
		}

	}

	var constraints []foreignKeyConstraint
	for _, constraint := range constraintsMap {
		constraints = append(constraints, constraint)
	}

	return constraints, rows.Err()
}

func (d *database) getTableTriggers(ctx context.Context, tablename string) (map[string]trigger, error) {
	triggers := map[string]trigger{}
	rows, err := d.connPool.Query(
		ctx,
		`select trigger_name, event_manipulation, action_statement, action_orientation, action_timing from information_schema.triggers
		WHERE event_object_table=$1`,
		tablename,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var trigger trigger
		if err := rows.Scan(&trigger.Name, &trigger.EventManipulation, &trigger.ActionStatement, &trigger.ActionOrientation, &trigger.ActionTiming); err != nil {
			return nil, err
		}
		funcName := strings.ReplaceAll(trigger.ActionStatement, "EXECUTE FUNCTION ", "")
		funcName = funcName[:len(funcName)-2] // remove () at the end
		proSrc, err := d.getStoredProcedure(ctx, funcName)
		if err != nil {
			return nil, err
		}
		trigger.Procedure = &procedure{Name: funcName, ProSrc: proSrc}
		triggers[trigger.Name] = trigger
	}

	return triggers, rows.Err()
}

func (d *database) getStoredProcedure(ctx context.Context, funcName string) (string, error) {
	var prosrc string
	d.connPool.QueryRow(ctx, `
	SELECT prosrc FROM pg_proc WHERE proname = $1;
	`, funcName).Scan(&prosrc)
	return prosrc, nil
}
