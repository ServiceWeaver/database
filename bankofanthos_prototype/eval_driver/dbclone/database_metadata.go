// This file defines database struct and get database matadata
package dbclone

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

type IdGenerator struct {
	IdentityGeneration string
	IdentityStart      int64
	IndentityIncrement int64
	IdentityMaximum    int64
	IdentityMinimum    int64
}
type Column struct {
	Name                   string
	DataType               string
	CharacterMaximumLength int64 // default is 0 if not sepcify
	Nullable               string
	IdGenerator            *IdGenerator
}

type Index struct {
	Name        string
	IndexDef    string
	IsUnique    bool
	ColumnNames []string
}

// a BeRefedTableName(BeRefedColumnName) is referenced by ForeignKeyTableName(ForeignKeyColumnName)
type Reference struct {
	ConstraintName       string
	BeRefedTableName     string
	BeRefedColumnName    string
	ForeignKeyTableName  string
	ForeignKeyColumnName string
	Action               string // TODO: Get actions from table metadata.Set default to no action
}

// a TableName(ColumnName) has a foreign key constraint which refers another RefTableName(RefColumnName)
type ForeignKeyConstraint struct {
	ConstraintName string
	TableName      string
	ColumnName     string
	RefTableName   string
	RefColumnName  string
	Action         string // TODO: Get actions from table metadata. set default to no action
}

type Rule struct {
	Name       string
	Definition string
}

type Procedure struct {
	Name   string
	ProSrc string
}
type Trigger struct {
	Name              string
	EventManipulation string // INSERT, DELETE, UPDATE
	ActionStatement   string
	ActionOrientation string // ROW
	ActionTiming      string // BEFORE, AFTER, INSTEAD OF

	Procedure *Procedure // stored_procedure definition
}

type View struct {
	Name     string
	Cols     map[string]Column
	Rules    []Rule
	Triggers []Trigger
}

type Table struct {
	Name                  string
	Cols                  map[string]Column
	Indexes               []Index
	Rules                 []Rule
	References            []Reference
	ForeignKeyConstraints []ForeignKeyConstraint
}

type Database struct {
	Tables   map[string]*Table
	connPool *pgxpool.Pool
}

func NewDatabase(
	ctx context.Context,
	connPool *pgxpool.Pool,
) (*Database, error) {
	Tables := map[string]*Table{}
	database := &Database{
		connPool: connPool,
		Tables:   Tables,
	}
	err := database.getDatabaseMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return database, nil
}

func (d *Database) getDatabaseMetadata(ctx context.Context) error {
	tables, err := d.listTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	for _, tablename := range tables {
		table, err := d.GetTable(ctx, tablename)
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
		refTable.References = append(refTable.References, Reference{
			ConstraintName:       constraint.ConstraintName,
			BeRefedTableName:     constraint.RefTableName,
			BeRefedColumnName:    constraint.RefColumnName,
			ForeignKeyTableName:  constraint.TableName,
			ForeignKeyColumnName: constraint.ColumnName})
	}

	return nil
}

func (d *Database) listTables(ctx context.Context) ([]string, error) {
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

func (d *Database) GetTable(ctx context.Context, tablename string) (*Table, error) {
	var table Table
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
func (d *Database) getTableCols(ctx context.Context, tablename string) (map[string]Column, error) {
	rows, err := d.connPool.Query(ctx, `
		SELECT column_name,  is_nullable, data_type, character_maximum_length, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum 
		FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1
	`, tablename)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}
	defer rows.Close()

	cols := map[string]Column{}
	for rows.Next() {
		var col Column
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
			idGenerator := &IdGenerator{
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
func (d *Database) getTableIndexes(ctx context.Context, tablename string) ([]Index, error) {
	var indexes []Index
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
		var index Index
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

func (d *Database) getTableRules(ctx context.Context, tablename string) ([]Rule, error) {
	var rules []Rule
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
		var rule Rule
		if err := rows.Scan(&rule.Name, &rule.Definition); err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	return rules, rows.Err()
}

func (d *Database) getForeignKeyConstraints(ctx context.Context) ([]ForeignKeyConstraint, error) {
	var constraints []ForeignKeyConstraint
	rows, err := d.connPool.Query(
		ctx, `
		SELECT                                                                                           
		tc.constraint_name, 
		tc.table_name, 
		kcu.column_name, 
		ccu.table_name AS referenced_table_name,
		ccu.column_name AS referenced_column_name 
	FROM 
		information_schema.table_constraints AS tc 
		JOIN information_schema.key_column_usage AS kcu
		ON tc.constraint_name = kcu.constraint_name
		JOIN information_schema.constraint_column_usage AS ccu
		ON ccu.constraint_name = tc.constraint_name
	WHERE constraint_type = 'FOREIGN KEY'; `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var constraint ForeignKeyConstraint
		if err := rows.Scan(&constraint.ConstraintName, &constraint.TableName, &constraint.ColumnName, &constraint.RefTableName, &constraint.RefColumnName); err != nil {
			return nil, err
		}
		constraints = append(constraints, constraint)
	}

	return constraints, rows.Err()
}

func (d *Database) getTableTriggers(ctx context.Context, tablename string) (map[string]Trigger, error) {
	triggers := map[string]Trigger{}
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
		var trigger Trigger
		if err := rows.Scan(&trigger.Name, &trigger.EventManipulation, &trigger.ActionStatement, &trigger.ActionOrientation, &trigger.ActionTiming); err != nil {
			return nil, err
		}
		funcName := strings.ReplaceAll(trigger.ActionStatement, "EXECUTE FUNCTION ", "")
		funcName = funcName[:len(funcName)-2] // remove () at the end
		proSrc, err := d.getStoredProcedure(ctx, funcName)
		if err != nil {
			return nil, err
		}
		trigger.Procedure = &Procedure{Name: funcName, ProSrc: proSrc}
		triggers[trigger.Name] = trigger
	}

	return triggers, rows.Err()
}

func (d *Database) getStoredProcedure(ctx context.Context, funcName string) (string, error) {
	var prosrc string
	d.connPool.QueryRow(ctx, `
	SELECT prosrc FROM pg_proc WHERE proname = $1;
	`, funcName).Scan(&prosrc)
	return prosrc, nil
}
