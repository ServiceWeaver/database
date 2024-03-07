// This file creates triggers for a cloned table to redirect queries
package dbclone

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

func createTriggers(ctx context.Context, connPool *pgxpool.Pool, clonedTable *ClonedTable) error {
	if err := createInsertTriggers(ctx, connPool, clonedTable); err != nil {
		return fmt.Errorf("failed to create insert triggers: %w", err)
	}
	if err := createUpdateTriggers(ctx, connPool, clonedTable); err != nil {
		return fmt.Errorf("failed to create update triggers: %w", err)
	}
	if err := createDeleteTriggers(ctx, connPool, clonedTable); err != nil {
		return fmt.Errorf("failed to create delete triggers: %w", err)
	}

	return nil
}

func createInsertTriggers(ctx context.Context, connPool *pgxpool.Pool, clonedTable *ClonedTable) error {
	var cols []string
	var newCols []string
	idGeneratorQuery := ""
	for colname, col := range clonedTable.Plus.Cols {
		cols = append(cols, colname)
		newCols = append(newCols, "NEW."+colname)
		if col.IdGenerator != nil {
			idGeneratorQuery += fmt.Sprintf(`
	IF NEW.%s IS NULL THEN
	NEW.%s := (SELECT COALESCE(MAX(%s)+%d, %d) FROM %s);
	END IF;`, colname, colname, colname, col.IdGenerator.IndentityIncrement, col.IdGenerator.IdentityMinimum, clonedTable.View.Name)
		}
	}
	sort.Strings(cols)
	sort.Strings(newCols)

	storedProcedureQuery := fmt.Sprintf(`
	CREATE OR REPLACE FUNCTION %s_redirect_insert()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN`, clonedTable.View.Name)

	// TODO: make it more generic way for auto-generate id
	if idGeneratorQuery != "" {
		storedProcedureQuery += idGeneratorQuery
	}

	// TODO: NULL can also be duplicate when we insert into unique columns
	// check unique columns
	for _, index := range clonedTable.Snapshot.Indexes {
		if index.IsUnique {
			storedProcedureQuery += fmt.Sprintf(`
	IF EXISTS (SELECT * FROM %s WHERE `, clonedTable.View.Name)
			colLen := len(index.ColumnNames)
			for i, col := range index.ColumnNames {
				if i < colLen-1 {
					storedProcedureQuery += fmt.Sprintf("%s = New.%s AND", col, col)
				} else if i == colLen-1 {
					storedProcedureQuery += fmt.Sprintf(`%s = New.%s) THEN
	RAISE EXCEPTION 'column %% already exists', NEW.%s;
	END IF;`, col, col, strings.Join(index.ColumnNames, ","))
				}
			}
		}
	}

	// if it has foreign key, check if the key exists in reference table
	for _, constraint := range clonedTable.Snapshot.ForeignKeyConstraints {
		if constraint.RefTableName != constraint.TableName {
			storedProcedureQuery += fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM %s WHERE %s = NEW.%s) THEN
	RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in %s table';
	END IF;`, constraint.RefTableName, constraint.RefColumnName, constraint.RefColumnName, constraint.RefTableName)
		}
	}

	storedProcedureQuery += fmt.Sprintf(`
	INSERT INTO %s (%s) 
	VALUES (%s);
	RETURN NEW;
	END;
	$$;
	`, clonedTable.Plus.Name, strings.Join(cols, ", "), strings.Join(newCols, ", "))

	triggerQuery := fmt.Sprintf(`
	CREATE OR REPLACE TRIGGER %s_redirect_insert_trigger
	INSTEAD OF INSERT ON %s
	FOR EACH ROW
	EXECUTE PROCEDURE %s_redirect_insert();
`, clonedTable.View.Name, clonedTable.View.Name, clonedTable.View.Name)

	_, err := connPool.Exec(ctx, storedProcedureQuery)
	if err != nil {
		return err
	}

	_, err = connPool.Exec(ctx, triggerQuery)
	if err != nil {
		return err
	}

	return nil
}

func createUpdateTriggers(ctx context.Context, connPool *pgxpool.Pool, clonedTable *ClonedTable) error {
	var cols []string
	var newCols []string
	var oldCols []string
	for colname := range clonedTable.Plus.Cols {
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
	BEGIN`, clonedTable.View.Name)

	for _, index := range clonedTable.Snapshot.Indexes {
		if index.IsUnique {
			storedProcedureQuery += fmt.Sprintf(`
	IF EXISTS (SELECT * FROM %s WHERE `, clonedTable.View.Name)

			compareStr := ""
			colLen := len(index.ColumnNames)
			for i, col := range index.ColumnNames {
				if i < colLen-1 {
					storedProcedureQuery += fmt.Sprintf("%s = New.%s AND", col, col)
					compareStr += fmt.Sprintf(" AND NEW.%s != OLD.%s", col, col)
				} else {
					storedProcedureQuery += fmt.Sprintf(`%s = New.%s)%s AND NEW.%s != OLD.%s THEN
	RAISE EXCEPTION 'column %% already exists', NEW.%s;
	END IF;`, col, col, compareStr, col, col, strings.Join(index.ColumnNames, ","))
				}
			}

		}
	}

	// if it has foreign key, check if the key exists in reference table
	for _, constraint := range clonedTable.Snapshot.ForeignKeyConstraints {
		if constraint.RefTableName != constraint.TableName {
			storedProcedureQuery += fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM %s WHERE %s = NEW.%s) THEN
		RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in %s table';
	END IF;`, constraint.RefTableName, constraint.RefColumnName, constraint.RefColumnName, constraint.RefTableName)
		}

	}

	// TODO: Add other foreign key actions
	// check if the key is referenced by other table
	for _, ref := range clonedTable.Snapshot.References {
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
	`, clonedTable.Minus.Name, strings.Join(cols, ", "), strings.Join(oldCols, ", "), clonedTable.Plus.Name, strings.Join(cols, ", "), strings.Join(newCols, ", "))

	triggerQuery := fmt.Sprintf(`
	CREATE OR REPLACE TRIGGER %s_redirect_update_trigger
	INSTEAD OF UPDATE ON %s
	FOR EACH ROW
	EXECUTE PROCEDURE %s_redirect_update();
`, clonedTable.View.Name, clonedTable.View.Name, clonedTable.View.Name)

	_, err := connPool.Exec(ctx, storedProcedureQuery)
	if err != nil {
		return err
	}

	_, err = connPool.Exec(ctx, triggerQuery)
	if err != nil {
		return err
	}

	return nil
}

func createDeleteTriggers(ctx context.Context, connPool *pgxpool.Pool, clonedTable *ClonedTable) error {
	var cols []string
	var oldCols []string
	for colname := range clonedTable.Plus.Cols {
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
	BEGIN`, clonedTable.View.Name)

	// TODO: Add other foreign key actions
	// check if the key is referenced by other table
	for _, ref := range clonedTable.Snapshot.References {
		if ref.Action == "" {
			storedProcedureQuery += fmt.Sprintf(`
	IF EXISTS (SELECT * FROM %s WHERE %s = OLD.%s) THEN
		RAISE EXCEPTION 'violates foreign key constraint';
	END IF;`, ref.ForeignKeyTableName, ref.ForeignKeyColumnName, ref.BeRefedColumnName)
		}
	}

	storedProcedureQuery += fmt.Sprintf(`
	INSERT INTO %s (%s) VALUES (%s);
	RETURN OLD;
	END;
	$$;
	`, clonedTable.Minus.Name, strings.Join(cols, ", "), strings.Join(oldCols, ", "))

	triggerQuery := fmt.Sprintf(`
	CREATE OR REPLACE TRIGGER %s_redirect_delete_trigger
	INSTEAD OF DELETE ON %s
	FOR EACH ROW
	EXECUTE PROCEDURE %s_redirect_delete();
`, clonedTable.View.Name, clonedTable.View.Name, clonedTable.View.Name)

	_, err := connPool.Exec(ctx, storedProcedureQuery)
	if err != nil {
		return err
	}

	_, err = connPool.Exec(ctx, triggerQuery)
	if err != nil {
		return err
	}

	return nil
}
