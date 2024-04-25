package dbbranch

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupTestDatabase(ctx context.Context) (testcontainers.Container, *pgxpool.Pool, string, error) {
	dbContainer, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("docker.io/postgres:16-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithConfigFile("../postgresql.conf"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return nil, nil, "", err
	}

	dbURL, err := dbContainer.ConnectionString(ctx)
	if err != nil {
		return nil, nil, "", err
	}

	connPool, err := pgxpool.Connect(ctx, dbURL)
	if err != nil {
		return nil, nil, "", err
	}

	return dbContainer, connPool, dbURL, nil
}

var sortStringSlice = cmp.Transformer("Sort", func(table []string) []string {
	out := append([]string(nil), table...)
	sort.Strings(out)
	return out
})
var idxOpt = cmp.Comparer(func(x, y index) bool {
	return x.Name == y.Name && reflect.DeepEqual(x.ColumnNames, y.ColumnNames) && reflect.DeepEqual(strings.Fields(strings.ToLower(x.IndexDef)), strings.Fields(strings.ToLower(y.IndexDef))) && x.IsUnique == y.IsUnique
})

var ruleOpt = cmp.Comparer(func(x, y rule) bool {
	return x.Name == y.Name && reflect.DeepEqual(strings.Fields(strings.ToLower(x.Definition)), strings.Fields(strings.ToLower(y.Definition)))
})

var procOpt = cmp.Comparer(func(x, y procedure) bool {
	return x.Name == y.Name && reflect.DeepEqual(strings.Fields(strings.ToLower(x.ProSrc)), strings.Fields(strings.ToLower(y.ProSrc)))
})

func createTables(ctx context.Context, connPool *pgxpool.Pool) error {
	_, err := connPool.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS users (
		accountid CHAR(12)    PRIMARY KEY,
		username  VARCHAR(64) UNIQUE NOT NULL,
		passhash  BYTEA       NOT NULL,
		birthday  DATE       
	);

	CREATE TABLE IF NOT EXISTS contacts (
		username    VARCHAR(64)  NOT NULL,
		account_num CHAR(12)     NOT NULL,
		is_external BOOLEAN      NOT NULL,
		FOREIGN KEY (username) REFERENCES users(username)
	  );		  

	CREATE RULE PREVENT_UPDATE AS ON UPDATE TO users DO INSTEAD NOTHING;
	`)
	return err
}

func dropTables(ctx context.Context, connPool *pgxpool.Pool) error {
	_, err := connPool.Exec(ctx, `
	DROP TABLE contacts;
	DROP TABLE users;
	`)
	return err
}

func TestListTableMetadata(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, _, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dbContainer.Terminate(ctx)

	database, err := newDatabase(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	err = createTables(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ListTables", func(t *testing.T) {
		tables, err := database.listTables(ctx)
		if err != nil {
			t.Fatal(err)
		}
		expectedTables := []string{"users", "contacts"}
		if diff := cmp.Diff(expectedTables, tables, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("GetTableColumns", func(t *testing.T) {
		cols, err := database.getTableCols(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}

		want := map[string]column{
			"accountid": {
				Name:                   "accountid",
				DataType:               "character",
				CharacterMaximumLength: 12,
				Nullable:               "NO",
			},
			"username": {
				Name:                   "username",
				DataType:               "character varying",
				CharacterMaximumLength: 64,
				Nullable:               "NO",
			},
			"passhash": {
				Name:     "passhash",
				DataType: "bytea",
				Nullable: "NO",
			},
			"birthday": {
				Name:                   "birthday",
				DataType:               "date",
				CharacterMaximumLength: 0,
				Nullable:               "YES",
			},
		}
		if diff := cmp.Diff(cols, want); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("GetTableIndexes", func(t *testing.T) {
		indexes, err := database.getTableIndexes(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}

		expectedIndexes := []index{
			{Name: "users_pkey", ColumnNames: []string{"accountid"}, IndexDef: "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (accountid)", IsUnique: true},
			{Name: "users_username_key", ColumnNames: []string{"username"}, IndexDef: "CREATE UNIQUE INDEX users_username_key ON public.users USING btree (username)", IsUnique: true}}
		if diff := cmp.Diff(expectedIndexes, indexes, idxOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("GetTableRules", func(t *testing.T) {
		rules, err := database.getTableRules(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}

		expectedRules := []rule{{Name: "prevent_update", Definition: "CREATE RULE prevent_update AS ON UPDATE TO public.users DO INSTEAD NOTHING;"}}
		if diff := cmp.Diff(expectedRules, rules, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("GetForeignConstraint", func(t *testing.T) {
		contraints, err := database.getForeignKeyConstraints(ctx)
		if err != nil {
			t.Fatal(err)
		}

		expectedConstraints := []foreignKeyConstraint{
			{
				ConstraintName: "contacts_username_fkey",
				TableName:      "contacts",
				ColumnNames:    []string{"username"},
				RefTableName:   "users",
				RefColumnNames: []string{"username"},
			},
		}

		if diff := cmp.Diff(expectedConstraints, contraints, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("GetTableTriggers", func(t *testing.T) {
		_, err = connPool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION users_redirect_delete()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE NOTICE 'Trigger users_redirect_delete executed for username %', OLD.username; 
			INSERT INTO users (accountid, username, passhash, birthday)    
			VALUES (OLD.accountid, OLD.username,OLD.passhash,OLD.birthday);
			RETURN OLD;
		END;
		$$;
	  
		CREATE OR REPLACE TRIGGER users_redirect_delete_trigger
			BEFORE DELETE ON users
			FOR EACH ROW
			EXECUTE PROCEDURE users_redirect_delete();
		`)
		if err != nil {
			t.Fatal(err)
		}

		triggers, err := database.getTableTriggers(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := map[string]trigger{"users_redirect_delete_trigger": {
			Name:              "users_redirect_delete_trigger",
			EventManipulation: "DELETE",
			ActionStatement:   "EXECUTE FUNCTION users_redirect_delete()",
			ActionOrientation: "ROW",
			ActionTiming:      "BEFORE",
			Procedure: &procedure{
				Name: "users_redirect_delete",
				ProSrc: `
				BEGIN
				RAISE NOTICE 'Trigger users_redirect_delete executed for username %', OLD.username; 
				INSERT INTO users (accountid, username, passhash, birthday)    
				VALUES (OLD.accountid, OLD.username,OLD.passhash,OLD.birthday);
				RETURN OLD;
				END;
				`,
			},
		},
		}
		if diff := cmp.Diff(expectedTriggers, triggers, procOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

	})

	t.Run("GetDatabaseMetadata", func(t *testing.T) {
		database, err := newDatabase(ctx, connPool)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := len(database.Tables), 2; got != want {
			t.Errorf("Listed table count: got %d, want %d", got, want)
		}

		expectedUserTable := &table{
			Name: "users",
			Cols: map[string]column{
				"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
				"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
				"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
				"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
			},
			Indexes: []index{
				{Name: "users_pkey", ColumnNames: []string{"accountid"}, IndexDef: "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (accountid)", IsUnique: true},
				{Name: "users_username_key", ColumnNames: []string{"username"}, IndexDef: "CREATE UNIQUE INDEX users_username_key ON public.users USING btree (username)", IsUnique: true}},
			Rules: []rule{{Name: "prevent_update", Definition: "CREATE RULE prevent_update AS ON UPDATE TO public.users DO INSTEAD NOTHING;"}},
			References: []reference{
				{ConstraintName: "contacts_username_fkey", BeRefedTableName: "users", BeRefedColumnNames: []string{"username"}, ForeignKeyTableName: "contacts", ForeignKeyColumnNames: []string{"username"}},
			},
		}
		if diff := cmp.Diff(expectedUserTable, database.Tables["users"], idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedContactTable := &table{
			Name: "contacts",
			Cols: map[string]column{
				"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
				"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
				"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
			},
			ForeignKeyConstraints: []foreignKeyConstraint{
				{
					ConstraintName: "contacts_username_fkey", TableName: "contacts", ColumnNames: []string{"username"}, RefTableName: "users", RefColumnNames: []string{"username"}},
			},
		}
		if diff := cmp.Diff(expectedContactTable, database.Tables["contacts"], idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	err = dropTables(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("GetCompositeForeignConstraint", func(t *testing.T) {
		_, err = connPool.Exec(ctx, `
		CREATE TABLE testa (
			a INT,
			b CHAR
		  );
		  
		ALTER TABLE testa ADD PRIMARY KEY (a,b);
		
		CREATE TABLE testb (
			c INT,
			d CHAR,
		FOREIGN KEY (c,d) REFERENCES testa(a,b)
		);
		  `)
		if err != nil {
			t.Fatal(err)
		}

		contraints, err := database.getForeignKeyConstraints(ctx)
		if err != nil {
			t.Fatal(err)
		}

		expectedConstraints := []foreignKeyConstraint{
			{
				ConstraintName: "testb_c_d_fkey",
				TableName:      "testb",
				ColumnNames:    []string{"c", "d"},
				RefTableName:   "testa",
				RefColumnNames: []string{"a", "b"},
			},
		}

		if diff := cmp.Diff(expectedConstraints, contraints, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		_, err = connPool.Exec(ctx, `
		DROP TABLE testb;
		DROP TABLE testa;`)
		if err != nil {
			t.Fatal(err)
		}
	})
}
