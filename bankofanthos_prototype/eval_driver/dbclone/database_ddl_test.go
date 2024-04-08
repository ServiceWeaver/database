package dbclone

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCreateCloneDatabase(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, _, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dbContainer.Terminate(ctx)

	err = createTables(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}
	database, err := newDatabase(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	cloneDdl, err := newCloneDdl(ctx, database, "test")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("CreateClonedTable", func(t *testing.T) {
		if got, want := len(cloneDdl.clonedTables), 2; got != want {
			t.Errorf("Cloned table count: got %d, want %d", got, want)
		}

		expectedContactTable := &clonedTable{
			Snapshot: &table{
				Name: "contactssnapshot",
				Cols: map[string]column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
				},
				ForeignKeyConstraints: []foreignKeyConstraint{
					{
						ConstraintName: "contacts_username_fkey",
						TableName:      "contacts",
						ColumnNames:    []string{"username"},
						RefTableName:   "users",
						RefColumnNames: []string{"username"}},
				},
			},
			Plus: &table{
				Name: "test.contactsplus",
				Cols: map[string]column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
				}},
			Minus: &table{Name: "test.contactsminus",
				Cols: map[string]column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
				}},
			View: &view{
				Name: "contacts",
				Cols: map[string]column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "YES"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "YES"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "YES"},
				}},
			Counter: &counter{Name: "test.rid", Colname: "rid"},
		}

		if diff := cmp.Diff(expectedContactTable, cloneDdl.clonedTables["contacts"], idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedUserTable := &clonedTable{
			Snapshot: &table{
				Name: "userssnapshot",
				Cols: map[string]column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
				Indexes: []index{
					{Name: "users_pkey", IndexDef: "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (accountid)", IsUnique: true, ColumnNames: []string{"accountid"}},
					{Name: "users_username_key", IndexDef: "CREATE UNIQUE INDEX users_username_key ON public.users USING btree (username)", IsUnique: true, ColumnNames: []string{"username"}}},
				Rules: []rule{{Name: "prevent_update", Definition: "CREATE RULE prevent_update AS ON UPDATE TO public.users DO INSTEAD NOTHING;"}},
				References: []reference{
					{ConstraintName: "contacts_username_fkey", BeRefedTableName: "users", BeRefedColumnNames: []string{"username"}, ForeignKeyTableName: "contacts", ForeignKeyColumnNames: []string{"username"}},
				},
			},
			Plus: &table{
				Name: "test.usersplus",
				Cols: map[string]column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
			},
			Minus: &table{Name: "test.usersminus",
				Cols: map[string]column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
			},
			View: &view{
				Name: "users",
				Cols: map[string]column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "YES"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "YES"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "YES"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
				Rules: []rule{{Name: "view_prevent_update", Definition: "CREATE RULE view_prevent_update AS ON UPDATE TO public.usersview DO INSTEAD NOTHING;"}},
			},
			Counter: &counter{Name: "test.rid", Colname: "rid"},
		}

		if diff := cmp.Diff(expectedUserTable, cloneDdl.clonedTables["users"], idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = cloneDdl.reset(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = cloneDdl.close(ctx)
		if err != nil {
			t.Fatal(err)
		}

	})
}
