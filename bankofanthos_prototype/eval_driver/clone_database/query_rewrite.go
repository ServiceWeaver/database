package clonedatabase

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

type QueryRewriter struct {
	table    ClonedTable
	connPool *pgxpool.Pool
}

func NewQueryRewriter(connPool *pgxpool.Pool, clonedTable *ClonedTable) (*QueryRewriter, error) {
	return &QueryRewriter{
		connPool: connPool,
		table:    *clonedTable,
	}, nil
}

func (q *QueryRewriter) CreateTriggers(ctx context.Context) error {
	if err := q.createInsertTriggers(ctx); err != nil {
		return fmt.Errorf("failed to create insert triggers, err=%s", err)
	}
	if err := q.createUpdateTriggers(ctx); err != nil {
		return fmt.Errorf("failed to create update triggers, err=%s", err)
	}
	if err := q.createDeleteTriggers(ctx); err != nil {
		return fmt.Errorf("failed to create delete triggers, err=%s", err)
	}

	return nil
}

func (q *QueryRewriter) createInsertTriggers(ctx context.Context) error {
	var cols []string
	var newCols []string
	idGeneratorQuery := ""
	for colname, col := range q.table.Plus.Cols {
		cols = append(cols, colname)
		newCols = append(newCols, "NEW."+colname)
		if col.IdGenerator != nil {
			idGeneratorQuery += fmt.Sprintf(`
	IF NEW.%s IS NULL THEN
		NEW.%s := (SELECT COALESCE(MAX(%s), %d) FROM %s) + %d;
	END IF;
	`, colname, colname, colname, col.IdGenerator.IdentityMinimum, q.table.View.Name, col.IdGenerator.IndentityIncrement)
		}
	}
	sort.Strings(cols)
	sort.Strings(newCols)

	storedProcedureQuery := fmt.Sprintf(`
	CREATE OR REPLACE FUNCTION %s_redirect_insert()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN
	`, q.table.View.Name)

	// check unique columns
	for _, index := range q.table.Snapshot.Indexs {
		if index.isUnique {
			storedProcedureQuery += fmt.Sprintf(`
		IF EXISTS (SELECT * FROM %s WHERE %s = NEW.%s) THEN
			RAISE EXCEPTION 'column %% already exists', NEW.%s;
		END IF;`, q.table.View.Name, index.ColumnName, index.ColumnName, index.ColumnName)
		}
	}

	// if it has foreign key, check if the key exists in reference table
	for _, constraint := range q.table.Snapshot.ForeignKeyConstraints {
		storedProcedureQuery += fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM %s WHERE %s = NEW.%s) THEN
		RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in %s table';
		END IF;`, constraint.RefTableName, constraint.RefColumnName, constraint.RefColumnName, constraint.RefTableName)
	}

	// TODO: make it more generic way for auto-generate id
	// if it has id generator.
	if idGeneratorQuery != "" {
		storedProcedureQuery += idGeneratorQuery
	}

	storedProcedureQuery += fmt.Sprintf(`
	INSERT INTO %s (%s) 
	VALUES (%s);
	RETURN NEW;
	END;
	$$;
	`, q.table.Plus.Name, strings.Join(cols, ", "), strings.Join(newCols, ", "))

	triggerQuery := fmt.Sprintf(`
	CREATE OR REPLACE TRIGGER %s_redirect_insert_trigger
	INSTEAD OF INSERT ON %s
	FOR EACH ROW
	EXECUTE PROCEDURE %s_redirect_insert();
`, q.table.View.Name, q.table.View.Name, q.table.View.Name)

	_, err := q.connPool.Exec(ctx, storedProcedureQuery)
	if err != nil {
		return err
	}

	_, err = q.connPool.Exec(ctx, triggerQuery)
	if err != nil {
		return err
	}

	return nil
}

func (q *QueryRewriter) createUpdateTriggers(ctx context.Context) error {
	var cols []string
	var newCols []string
	var oldCols []string
	for colname, _ := range q.table.Plus.Cols {
		cols = append(cols, colname)
		oldCols = append(oldCols, "OLD."+colname)
		newCols = append(newCols, "NEW."+colname)
	}
	sort.Strings(cols)
	sort.Strings(oldCols)
	sort.Strings(newCols)

	storedProcedureQuery := fmt.Sprintf(`
	CREATE OR REPLACE FUNCTION %s_redirect_update()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN
	`, q.table.View.Name)

	// if it has foreign key, check if the key exists in reference table
	for _, constraint := range q.table.Snapshot.ForeignKeyConstraints {
		storedProcedureQuery += fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM %s WHERE %s = NEW.%s) THEN
		RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in %s table';
	END IF;`, constraint.RefTableName, constraint.RefColumnName, constraint.RefColumnName, constraint.RefTableName)

	}

	// TODO: Add other foreign key actions
	// check if the key is referenced by other table
	for _, ref := range q.table.Snapshot.References {
		if ref.Action == "" {
			storedProcedureQuery += fmt.Sprintf(`
	IF EXISTS (SELECT * FROM %s WHERE %s = OLD.%s) AND NEW.%s != old.%s THEN
		RAISE EXCEPTION 'violates foreign key constraint';
	END IF;`, ref.ForeignKeyTableName, ref.ForeignKeyColumnName, ref.BeRefedColumnName, ref.BeRefedColumnName, ref.BeRefedColumnName)
		}
	}

	storedProcedureQuery += fmt.Sprintf(`
	INSERT INTO %s (%s) VALUES (%s);
	INSERT INTO %s (%s) VALUES (%s);
	RETURN NEW;
	END;
	$$;
	`, q.table.Minus.Name, strings.Join(cols, ", "), strings.Join(oldCols, ", "), q.table.Plus.Name, strings.Join(cols, ", "), strings.Join(newCols, ", "))

	triggerQuery := fmt.Sprintf(`
	CREATE OR REPLACE TRIGGER %s_redirect_update_trigger
	INSTEAD OF UPDATE ON %s
	FOR EACH ROW
	EXECUTE PROCEDURE %s_redirect_update();
`, q.table.View.Name, q.table.View.Name, q.table.View.Name)

	_, err := q.connPool.Exec(ctx, storedProcedureQuery)
	if err != nil {
		return err
	}

	_, err = q.connPool.Exec(ctx, triggerQuery)
	if err != nil {
		return err
	}

	return nil
}

func (q *QueryRewriter) createDeleteTriggers(ctx context.Context) error {
	var cols []string
	var oldCols []string
	for colname, _ := range q.table.Plus.Cols {
		cols = append(cols, colname)
		oldCols = append(oldCols, "OLD."+colname)
	}
	sort.Strings(cols)
	sort.Strings(oldCols)

	storedProcedureQuery := fmt.Sprintf(`
	CREATE OR REPLACE FUNCTION %s_redirect_delete()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN`, q.table.View.Name)

	// TODO: Add other foreign key actions
	// check if the key is referenced by other table
	for _, ref := range q.table.Snapshot.References {
		if ref.Action == "" {
			storedProcedureQuery += fmt.Sprintf(`
	IF EXISTS (SELECT * FROM %s WHERE %s = OLD.%s) AND NEW.%s != old.%s THEN
		RAISE EXCEPTION 'violates foreign key constraint';
	END IF;`, ref.ForeignKeyTableName, ref.ForeignKeyColumnName, ref.BeRefedColumnName, ref.BeRefedColumnName, ref.BeRefedColumnName)
		}
	}

	storedProcedureQuery += fmt.Sprintf(`
	INSERT INTO %s (%s) VALUES (%s);
	RETURN OLD;
	END;
	$$;
	`, q.table.Minus.Name, strings.Join(cols, ", "), strings.Join(oldCols, ", "))

	triggerQuery := fmt.Sprintf(`
	CREATE OR REPLACE TRIGGER %s_redirect_delete_trigger
	INSTEAD OF DELETE ON %s
	FOR EACH ROW
	EXECUTE PROCEDURE %s_redirect_delete();
`, q.table.View.Name, q.table.View.Name, q.table.View.Name)

	_, err := q.connPool.Exec(ctx, storedProcedureQuery)
	if err != nil {
		return err
	}

	_, err = q.connPool.Exec(ctx, triggerQuery)
	if err != nil {
		return err
	}

	return nil
}
