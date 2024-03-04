package clonedatabase

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCreateCloneDatabase(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, _, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Error(err)
	}
	defer dbContainer.Terminate(ctx)

	sortStringSlice := cmp.Transformer("Sort", func(table []string) []string {
		out := append([]string(nil), table...)
		sort.Strings(out)
		return out
	})

	colOpt := cmp.Comparer(func(x, y Column) bool {
		if x.Name != y.Name || x.DataType != y.DataType ||
			x.Nullable != y.Nullable ||
			x.CharacterMaximumLength != y.CharacterMaximumLength {
			return false
		}

		if x.IdGenerator == nil && y.IdGenerator == nil {
			return true
		} else if x.IdGenerator != nil && y.IdGenerator != nil {
			return x.IdGenerator.IdentityGeneration == y.IdGenerator.IdentityGeneration && x.IdGenerator.IdentityMaximum == y.IdGenerator.IdentityMaximum &&
				x.IdGenerator.IdentityMinimum == y.IdGenerator.IdentityMinimum && x.IdGenerator.IdentityStart == y.IdGenerator.IdentityStart &&
				x.IdGenerator.IndentityIncrement == y.IdGenerator.IndentityIncrement
		}

		return false
	})

	idxOpt := cmp.Comparer(func(x, y Index) bool {
		return x.Name == y.Name && reflect.DeepEqual(strings.Fields(strings.ToLower(x.IndexDef)), strings.Fields(strings.ToLower(y.IndexDef))) && x.isUnique == y.isUnique
	})

	ruleOpt := cmp.Comparer(func(x, y Rule) bool {
		return x.Name == y.Name && reflect.DeepEqual(strings.Fields(strings.ToLower(x.Definition)), strings.Fields(strings.ToLower(y.Definition)))
	})

	_, err = connPool.Exec(ctx, `
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
	if err != nil {
		t.Error(err)
	}
	database, err := NewDatabase(ctx, connPool)
	if err != nil {
		t.Error(err)
	}

	CloneDdl, err := NewCloneDdl(ctx, database)
	if err != nil {
		t.Error(err)
	}

	t.Run("CreateTablePlusAndMinus", func(t *testing.T) {
		plus, minus, err := CloneDdl.createPlusMinusTable(ctx, CloneDdl.Database.Tables["users"])
		if err != nil {
			t.Error(err)
		}
		plusTable, err := CloneDdl.Database.GetTable(ctx, "usersplus")
		if err != nil {
			t.Error(err)
		}

		if diff := cmp.Diff(plusTable, plus, colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedPlus := &Table{
			Name: "usersplus",
			Cols: map[string]Column{
				"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
				"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
				"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
				"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
			},
		}
		if diff := cmp.Diff(expectedPlus, plusTable, colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		minusTable, err := CloneDdl.Database.GetTable(ctx, "usersminus")
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(minusTable, minus, colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedMinus := &Table{
			Name: "usersminus",
			Cols: map[string]Column{
				"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
				"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
				"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
				"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
			},
		}
		if diff := cmp.Diff(expectedMinus, minusTable, colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = CloneDdl.dropTable(ctx, "usersplus")
		if err != nil {
			t.Error(err)
		}

		err = CloneDdl.dropTable(ctx, "usersminus")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("CreateView", func(t *testing.T) {
		_, _, err := CloneDdl.createPlusMinusTable(ctx, CloneDdl.Database.Tables["users"])
		if err != nil {
			t.Error(err)
		}

		view, err := CloneDdl.createView(ctx, CloneDdl.Database.Tables["users"])
		if err != nil {
			t.Error(err)
		}

		cols, err := CloneDdl.Database.getTableCols(ctx, "usersview")
		if err != nil {
			t.Error(err)
		}

		gotView := &View{
			Name: "usersview",
			Cols: cols,
		}
		if diff := cmp.Diff(gotView, view, colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedView := &View{
			Name: "usersview",
			Cols: map[string]Column{
				"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "YES"},
				"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "YES"},
				"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "YES"},
				"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
			},
		}
		if diff := cmp.Diff(expectedView, gotView, colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = CloneDdl.dropView(ctx, view.Name)
		if err != nil {
			t.Error(err)
		}
		err = CloneDdl.dropTable(ctx, "usersplus")
		if err != nil {
			t.Error(err)
		}

		err = CloneDdl.dropTable(ctx, "usersminus")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("CreateClonedTable", func(t *testing.T) {

		CloneDdl, err = NewCloneDdl(ctx, database)
		if err != nil {
			t.Error(err)
		}
		err := CloneDdl.CreateClonedTables(ctx)
		if err != nil {
			t.Error(err)
		}

		if got, want := len(CloneDdl.ClonedTables), 2; got != want {
			t.Errorf("Cloned table count: got %d, want %d", got, want)
		}

		expectedContactTable := &ClonedTable{
			Snapshot: &Table{
				Name: "contactssnapshot",
				Cols: map[string]Column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
				},
				ForeignKeyConstraints: []ForeignKeyConstraint{
					{
						ConstraintName: "contacts_username_fkey",
						TableName:      "contacts",
						ColumnName:     "username",
						RefTableName:   "users",
						RefColumnName:  "username"},
				},
			},
			Plus: &Table{
				Name: "contactsplus",
				Cols: map[string]Column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
				}},
			Minus: &Table{Name: "contactsminus",
				Cols: map[string]Column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "NO"},
				}},
			View: &View{
				Name: "contacts",
				Cols: map[string]Column{
					"username":    {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "YES"},
					"account_num": {Name: "account_num", DataType: "character", CharacterMaximumLength: 12, Nullable: "YES"},
					"is_external": {Name: "is_external", DataType: "boolean", Nullable: "YES"},
				}},
		}

		if diff := cmp.Diff(expectedContactTable, CloneDdl.ClonedTables["contacts"], colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedUserTable := &ClonedTable{
			Snapshot: &Table{
				Name: "userssnapshot",
				Cols: map[string]Column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
				Indexs: []Index{
					{Name: "users_pkey", IndexDef: "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (accountid)", isUnique: true},
					{Name: "users_username_key", IndexDef: "CREATE UNIQUE INDEX users_username_key ON public.users USING btree (username)", isUnique: true}},
				Rules: []Rule{{Name: "prevent_update", Definition: "CREATE RULE prevent_update AS ON UPDATE TO public.users DO INSTEAD NOTHING;"}},
				References: []Reference{
					{ConstraintName: "contacts_username_fkey", BeRefedTableName: "users", BeRefedColumnName: "username", ForeignKeyTableName: "contacts", ForeignKeyColumnName: "username"},
				},
			},
			Plus: &Table{
				Name: "usersplus",
				Cols: map[string]Column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
				Indexs: []Index{
					{Name: "usersplus_pkey", IndexDef: "CREATE INDEX usersplus_pkey ON public.usersplus USING btree (accountid)", isUnique: true},
					{Name: "usersplus_username_key", IndexDef: "CREATE INDEX usersplus_username_key ON public.usersplus USING btree (username)", isUnique: true}},
			},
			Minus: &Table{Name: "usersminus",
				Cols: map[string]Column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "NO"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "NO"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "NO"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
				Indexs: []Index{
					{Name: "usersminus_pkey", IndexDef: "CREATE INDEX usersminus_pkey ON public.usersminus USING btree (accountid)", isUnique: true},
					{Name: "usersminus_username_key", IndexDef: "CREATE INDEX usersminus_username_key ON public.usersminus USING btree (username)", isUnique: true}},
			},
			View: &View{
				Name: "users",
				Cols: map[string]Column{
					"accountid": {Name: "accountid", DataType: "character", CharacterMaximumLength: 12, Nullable: "YES"},
					"username":  {Name: "username", DataType: "character varying", CharacterMaximumLength: 64, Nullable: "YES"},
					"passhash":  {Name: "passhash", DataType: "bytea", Nullable: "YES"},
					"birthday":  {Name: "birthday", DataType: "date", Nullable: "YES"},
				},
				Rules: []Rule{{Name: "view_prevent_update", Definition: "CREATE RULE view_prevent_update AS ON UPDATE TO public.usersview DO INSTEAD NOTHING;"}},
			},
		}

		if diff := cmp.Diff(expectedUserTable, CloneDdl.ClonedTables["users"], colOpt, idxOpt, ruleOpt, sortStringSlice); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		err = CloneDdl.Close(ctx)
		if err != nil {
			t.Error(err)
		}

	})
}
