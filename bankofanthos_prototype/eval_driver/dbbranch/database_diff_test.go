package dbbranch

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestCloneDatabaseDiffs(t *testing.T) {
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

	dbDiff := newDbDiff(connPool, "rid", nil, "generated_id")

	t.Run("DumpView", func(t *testing.T) {
		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["users"])
		if err != nil {
			t.Fatal(err)
		}

		_, err = connPool.Exec(ctx,
			`
		INSERT INTO users(accountid, username, passhash, birthday) VALUES
		('101122611122', 'testuser', '1234', '2000-01-01'),
		('103362343333', 'alice', '2345', '2001-01-01'),
		('107744137744', 'eve', '3456', '2002-01-01');
		`)
		if err != nil {
			t.Fatal(err)
		}

		rows, colNames, err := dbDiff.dumpView(ctx, cloneDdl.clonedTables["users"].View)
		if err != nil {
			t.Fatal(err)
		}

		expectedColNames := []string{"accountid", "birthday", "passhash", "username"}
		if diff := cmp.Diff(expectedColNames, colNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedRows := []*Row{
			{"101122611122", time.Date(2000, time.Month(1), 1, 0, 0, 0, 0, time.UTC), []byte("1234"), "testuser"},
			{"103362343333", time.Date(2001, time.Month(1), 1, 0, 0, 0, 0, time.UTC), []byte("2345"), "alice"},
			{"107744137744", time.Date(2002, time.Month(1), 1, 0, 0, 0, 0, time.UTC), []byte("3456"), "eve"},
		}
		if diff := cmp.Diff(expectedRows, rows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	err = cloneDdl.reset(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = cloneDdl.close(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("TableMinus", func(t *testing.T) {
		_, err = connPool.Exec(ctx,
			`
		CREATE TABLE TableA (id INTEGER, name VARCHAR, rid BIGINT);
		CREATE TABLE TableB (id INTEGER, name VARCHAR, rid BIGINT);

		INSERT INTO TableA VALUES(1,'a', 1);
		INSERT INTO TableA VALUES(2,'b', 2);

		INSERT INTO tableB VALUES(2,'b', 2);
		`)
		if err != nil {
			t.Fatal(err)
		}
		tableA := &table{
			Name: "tableA",
			Cols: map[string]column{
				"name": {Name: "name", DataType: "character varying", Nullable: "YES"},
				"id":   {Name: "id", DataType: "INTEGER", Nullable: "YES"},
			},
		}

		tableB := &table{
			Name: "tableB",
			Cols: map[string]column{
				"name": {Name: "name", DataType: "character varying", Nullable: "YES"},
				"id":   {Name: "id", DataType: "INTEGER", Nullable: "YES"},
			},
		}

		AminusBView, err := dbDiff.minusTables(ctx, tableA, tableB, "AMinusB")
		if err != nil {
			t.Fatal(err)
		}
		AminusBViewRows, AminusBColNames, err := dbDiff.dumpView(ctx, AminusBView)
		if err != nil {
			t.Fatal(err)
		}

		err = dropView(ctx, connPool, AminusBView.Name)
		if err != nil {
			t.Fatal(err)
		}

		expectedAminusBRows := []*Row{{int32(1), "a"}}
		if diff := cmp.Diff([]string{"id", "name"}, AminusBColNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		if diff := cmp.Diff(expectedAminusBRows, AminusBViewRows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		BminusAsView, err := dbDiff.minusTables(ctx, tableB, tableA, "BMinusA")
		if err != nil {
			t.Fatal(err)
		}

		BminusAViewRows, BminusAColNames, err := dbDiff.dumpView(ctx, BminusAsView)
		if err != nil {
			t.Fatal(err)
		}
		err = dropView(ctx, connPool, BminusAsView.Name)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([]string{"id", "name"}, BminusAColNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if diff := cmp.Diff([]*Row(nil), BminusAViewRows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("TableIntersect", func(t *testing.T) {
		tableA := &table{
			Name: "tableA",
			Cols: map[string]column{
				"name": {Name: "name", DataType: "character varying", Nullable: "YES"},
				"id":   {Name: "id", DataType: "INTEGER", Nullable: "YES"},
			},
		}

		tableB := &table{
			Name: "tableB",
			Cols: map[string]column{
				"name": {Name: "name", DataType: "character varying", Nullable: "YES"},
				"id":   {Name: "id", DataType: "INTEGER", Nullable: "YES"},
			},
		}

		AintersectB, err := dbDiff.intersectTables(ctx, tableA, tableB, "AintersectB")
		if err != nil {
			t.Fatal(err)
		}

		AintersectBRows, AintersectBColNames, err := dbDiff.dumpView(ctx, AintersectB)
		if err != nil {
			t.Fatal(err)
		}

		err = dropView(ctx, connPool, AintersectB.Name)
		if err != nil {
			t.Fatal(err)
		}

		BintersectA, err := dbDiff.intersectTables(ctx, tableB, tableA, "BintersectA")
		if err != nil {
			t.Fatal(err)
		}
		BintersectARows, BintersectAColNames, err := dbDiff.dumpView(ctx, BintersectA)
		if err != nil {
			t.Fatal(err)
		}

		err = dropView(ctx, connPool, BintersectA.Name)
		if err != nil {
			t.Fatal(err)
		}

		expectedRows := []*Row{{int32(2), "b"}}
		if diff := cmp.Diff([]string{"id", "name"}, AintersectBColNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if diff := cmp.Diff(expectedRows, AintersectBRows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if diff := cmp.Diff([]string{"id", "name"}, BintersectAColNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if diff := cmp.Diff(expectedRows, BintersectARows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = dropTable(ctx, connPool, "tableA")
		if err != nil {
			t.Fatal(err)
		}

		err = dropTable(ctx, connPool, "tableB")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("getPrimaryKeyCols", func(t *testing.T) {
		usersCols := dbDiff.getPrimaryKeyCols(cloneDdl.database.Tables["users"])

		usersExpectedCols := []string{"accountid"}
		if diff := cmp.Diff(usersExpectedCols, usersCols); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		contactsCols := dbDiff.getPrimaryKeyCols(cloneDdl.database.Tables["contacts"])

		if got, want := len(contactsCols), 0; got != want {
			t.Errorf("(-want,+got):\n(%d,%d)", want, got)
		}
	})

	t.Run("trimClonedTable", func(t *testing.T) {

		cloneDdl, err := newCloneDdl(ctx, database, "trimtest")
		if err != nil {
			t.Fatal(err)
		}

		dbDiff := newDbDiff(connPool, "rid", nil, "generated_id")

		_, err = connPool.Exec(ctx,
			`
		INSERT INTO trimtest.usersplus(accountid, username, passhash, birthday, rid) VALUES
		('101122611122', 'testuser', '1234', '2000-01-01', 0),
		('103362343333', 'alice', '2345', '2001-01-01', 0), 
		('107744137744', 'eve', '3456', '2002-01-01', 0);

		INSERT INTO trimtest.usersminus(accountid, username, passhash, birthday, rid) VALUES
		('101122611122', 'testuser', '1234', '2000-01-01', 0),
		('107744137744', 'eve', '3456', '2003-01-01', 0);
		`)
		if err != nil {
			t.Fatal(err)
		}

		updatedA, _, err := dbDiff.getclonedTablesAtNReqs(ctx, cloneDdl.clonedTables["users"], cloneDdl.clonedTables["users"], 0)
		if err != nil {
			t.Fatal(err)
		}

		trimPlus, trimMinus, err := dbDiff.trimClonedTable(ctx, updatedA.Plus, updatedA.Minus)
		if err != nil {
			t.Fatal(err)
		}

		trimPlusRows, trimPlusColNames, err := dbDiff.dumpView(ctx, trimPlus)
		if err != nil {
			t.Fatal(err)
		}

		expectedColNames := []string{"accountid", "birthday", "passhash", "username"}
		if diff := cmp.Diff(expectedColNames, trimPlusColNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedTrimPlusRows := []*Row{
			{"103362343333", time.Date(2001, time.Month(1), 1, 0, 0, 0, 0, time.UTC), []byte("2345"), "alice"},
			{"107744137744", time.Date(2002, time.Month(1), 1, 0, 0, 0, 0, time.UTC), []byte("3456"), "eve"},
		}
		if diff := cmp.Diff(expectedTrimPlusRows, trimPlusRows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		trimMinusRows, trimMinusColNames, err := dbDiff.dumpView(ctx, trimMinus)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(expectedColNames, trimMinusColNames); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		expectedTrimMinusRows := []*Row{
			{"107744137744", time.Date(2003, time.Month(1), 1, 0, 0, 0, 0, time.UTC), []byte("3456"), "eve"},
		}
		if diff := cmp.Diff(expectedTrimMinusRows, trimMinusRows); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = dropView(ctx, connPool, trimPlus.Name)
		if err != nil {
			t.Fatal(err)
		}

		err = dropView(ctx, connPool, trimMinus.Name)
		if err != nil {
			t.Fatal(err)
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

	err = dropTables(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("getNonPrimaryKeyRowDiff", func(t *testing.T) {
		_, err = connPool.Exec(ctx,
			`
		CREATE TABLE a (id INTEGER, name VARCHAR);
		CREATE TABLE b (id INTEGER, name VARCHAR);

		INSERT INTO a(id, name) VALUES(0,'O');
		INSERT INTO a(id, name) VALUES(1,'A');
		INSERT INTO a(id, name) VALUES(2,'B');
		INSERT INTO a(id, name) VALUES(4,'D');

		INSERT INTO b(id, name) VALUES(0,'O');
		INSERT INTO b(id, name) VALUES(1,'A');
		INSERT INTO b(id, name) VALUES(2,'B');
		INSERT INTO b(id, name) VALUES(4,'D');
		`)
		if err != nil {
			t.Fatal(err)
		}
		nonPrimaryDb, err := newDatabase(ctx, connPool)
		if err != nil {
			t.Fatal(err)
		}

		cloneDdl, err := newCloneDdl(ctx, nonPrimaryDb, "test")
		if err != nil {
			t.Fatal(err)
		}

		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["a"])
		if err != nil {
			t.Fatal(err)
		}

		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["b"])
		if err != nil {
			t.Fatal(err)
		}

		_, err = connPool.Exec(ctx,
			`
		INSERT INTO A(id, name) VALUES(3,'C');
		INSERT INTO A(id, name) VALUES(2,'B');
		DELETE FROM A WHERE (id, name) = (0,'O');
		DELETE FROM A WHERE (id, name) = (4,'D');
		DELETE FROM A WHERE (id, name) = (1,'A');
		INSERT INTO a(id, name) VALUES(1,'A');
		INSERT INTO B(id, name) VALUES(3,'C');
		UPDATE B SET (id, name) = (1,'D') where (id, name) = (1,'A');
		DELETE FROM B WHERE (id, name) = (0,'O');
		`)
		if err != nil {
			t.Fatal(err)
		}

		dbDiff := newDbDiff(connPool, "rid", nil, "generated_id")
		updatedA, updatedB, err := dbDiff.getclonedTablesAtNReqs(ctx, cloneDdl.clonedTables["a"], cloneDdl.clonedTables["b"], 1)
		if err != nil {
			t.Fatal(err)
		}

		rowDiffs, err := dbDiff.getNonPrimaryKeyRowDiff(ctx, updatedA, updatedB)
		if err != nil {
			t.Fatal(err)
		}
		// order is {APlusOnly, BPlusOnly, APlusBPlus, AMinusOnly, BMinusOnly, AMinusBMinus}
		expectedRowDiffs := &Diff{
			Control: []*Row{
				{int32(2), "B"}, // A+
				{nil, nil},      // B+
				{int32(3), "C"}, //A+B+
				{nil, nil},      // A-
				{int32(1), "A"}, //B-
				{nil, nil},      // A-B-
			},
			Baseline: []*Row{
				{nil, nil},      // A+
				{nil, nil},      // B+
				{nil, nil},      //A+B+
				{int32(4), "D"}, // A-
				{int32(1), "A"}, //B-
				{int32(0), "O"}, // A-B-
			},
			Experimental: []*Row{
				{nil, nil},      // A+
				{int32(1), "D"}, // B+
				{int32(3), "C"}, //A+B+
				{int32(4), "D"}, // A-
				{nil, nil},      //B-
				{nil, nil},      // A-B-
			},
			ColNames: []string{"id", "name"},
		}
		if diff := cmp.Diff(expectedRowDiffs, rowDiffs); diff != "" {
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

		err = dropTable(ctx, connPool, "a")
		if err != nil {
			t.Fatal(err)
		}

		err = dropTable(ctx, connPool, "b")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("getPrimaryKeyRowDiff", func(t *testing.T) {
		_, err = connPool.Exec(ctx,
			`
		CREATE TABLE a (id INTEGER PRIMARY KEY, name VARCHAR);
		CREATE TABLE b (id INTEGER PRIMARY KEY, name VARCHAR);

		INSERT INTO a(id, name) VALUES(0,'O');
		INSERT INTO a(id, name) VALUES(1,'A');
		INSERT INTO a(id, name) VALUES(2,'B');
		INSERT INTO a(id, name) VALUES(3,'C');
		INSERT INTO a(id, name) VALUES(4,'D');
		INSERT INTO a(id, name) VALUES(5,'E');
		INSERT INTO a(id, name) VALUES(6,'F');
		INSERT INTO a(id, name) VALUES(7,'G');

		INSERT INTO b(id, name) VALUES(0,'O');
		INSERT INTO b(id, name) VALUES(1,'A');
		INSERT INTO b(id, name) VALUES(2,'B');
		INSERT INTO b(id, name) VALUES(3,'C');
		INSERT INTO b(id, name) VALUES(4,'D');
		INSERT INTO b(id, name) VALUES(5,'E');
		INSERT INTO b(id, name) VALUES(6,'F');
		INSERT INTO b(id, name) VALUES(7,'G');
		`)
		if err != nil {
			t.Fatal(err)
		}
		nonPrimaryDb, err := newDatabase(ctx, connPool)
		if err != nil {
			t.Fatal(err)
		}

		cloneDdl, err := newCloneDdl(ctx, nonPrimaryDb, "test")
		if err != nil {
			t.Fatal(err)
		}

		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["a"])
		if err != nil {
			t.Fatal(err)
		}

		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["b"])
		if err != nil {
			t.Fatal(err)
		}

		_, err = connPool.Exec(ctx,
			`
		DELETE FROM B WHERE (id, name) = (0,'O');
		UPDATE B SET (id, name) = (1,'AA') where (id, name) = (1, 'A');
		DELETE FROM A WHERE (id, name) = (2,'B');
		UPDATE A SET (id, name) = (3,'CC') where (id, name) = (3, 'C');
		DELETE FROM A WHERE (id, name) = (4,'D');
		DELETE FROM B WHERE (id, name) = (4,'D');
		DELETE FROM A WHERE (id, name) = (5,'E');
		UPDATE B SET (id, name) = (5,'EE') where (id, name) = (5, 'E');
		DELETE FROM B WHERE (id, name) = (6,'F');
		UPDATE A SET (id, name) = (6,'FF') where (id, name) = (6, 'F');
		UPDATE A SET (id, name) = (7,'GG') where (id, name) = (7, 'G');
		UPDATE B SET (id, name) = (7,'GGG') where (id, name) = (7, 'G');
		INSERT INTO B(id, name) VALUES(8, 'H');
		INSERT INTO A(id, name) VALUES(9, 'I');
		INSERT INTO A(id, name) VALUES(10, 'J');
		INSERT INTO B(id, name) VALUES(10, 'J');
		`)
		if err != nil {
			t.Fatal(err)
		}

		dbDiff := newDbDiff(connPool, "rid", nil, "generated_id")

		updatedA, updatedB, err := dbDiff.getclonedTablesAtNReqs(ctx, cloneDdl.clonedTables["a"], cloneDdl.clonedTables["b"], 1)
		if err != nil {
			t.Fatal(err)
		}
		rowDiffs, err := dbDiff.getPrimaryKeyRowDiff(ctx, updatedA, updatedB)
		if err != nil {
			t.Fatal(err)
		}

		expectedRowDiffs := &Diff{
			Control: []*Row{
				{int32(0), "O"},
				{int32(1), "A"},
				{nil, nil},
				{int32(3), "CC"},
				{nil, nil},
				{nil, nil},
				{int32(6), "FF"},
				{int32(7), "GG"},
				{nil, nil},
				{int32(9), "I"},
				{int32(10), "J"},
			},
			Baseline: []*Row{
				{int32(0), "O"},
				{int32(1), "A"},
				{int32(2), "B"},
				{int32(3), "C"},
				{int32(4), "D"},
				{int32(5), "E"},
				{int32(6), "F"},
				{int32(7), "G"},
				{nil, nil},
				{nil, nil},
				{nil, nil},
			},
			Experimental: []*Row{
				{nil, nil},
				{int32(1), "AA"},
				{int32(2), "B"},
				{int32(3), "C"},
				{nil, nil},
				{int32(5), "EE"},
				{nil, nil},
				{int32(7), "GGG"},
				{int32(8), "H"},
				{nil, nil},
				{int32(10), "J"},
			},
			ColNames: []string{"id", "name"},
		}

		if diff := cmp.Diff(expectedRowDiffs, rowDiffs); diff != "" {
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

		err = dropTable(ctx, connPool, "a")
		if err != nil {
			t.Fatal(err)
		}

		err = dropTable(ctx, connPool, "b")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("getPrimaryKeyRowDiffAt3", func(t *testing.T) {
		_, err = connPool.Exec(ctx,
			`
		CREATE TABLE a (id INTEGER PRIMARY KEY, name VARCHAR);
		CREATE TABLE b (id INTEGER PRIMARY KEY, name VARCHAR);

		INSERT INTO a(id, name) VALUES(0,'O');
		INSERT INTO a(id, name) VALUES(1,'A');
		INSERT INTO a(id, name) VALUES(2,'B');
		INSERT INTO a(id, name) VALUES(3,'C');
		INSERT INTO a(id, name) VALUES(4,'D');
		INSERT INTO a(id, name) VALUES(5,'E');

		INSERT INTO b(id, name) VALUES(0,'O');
		INSERT INTO b(id, name) VALUES(1,'A');
		INSERT INTO b(id, name) VALUES(2,'B');
		INSERT INTO b(id, name) VALUES(3,'C');
		INSERT INTO b(id, name) VALUES(4,'D');
		INSERT INTO b(id, name) VALUES(5,'E');
		`)
		if err != nil {
			t.Fatal(err)
		}
		nonPrimaryDb, err := newDatabase(ctx, connPool)
		if err != nil {
			t.Fatal(err)
		}

		cloneDdl, err := newCloneDdl(ctx, nonPrimaryDb, "test")
		if err != nil {
			t.Fatal(err)
		}

		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["a"])
		if err != nil {
			t.Fatal(err)
		}
		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["b"])
		if err != nil {
			t.Fatal(err)
		}

		_, err = connPool.Exec(ctx,
			`
		DELETE FROM B WHERE (id, name) = (0,'O');
		UPDATE test.rid SET id = id+1;

		INSERT INTO B (id, name) VALUES (0, 'O');
		UPDATE B SET (id, name) = (1,'AA') where (id, name) = (1, 'A');
		UPDATE test.rid SET id = id+1;

		DELETE FROM A WHERE (id, name) = (2,'B');
		UPDATE test.rid SET id = id+1;

		UPDATE A SET (id, name) = (3,'CC') where (id, name) = (3, 'C');
		UPDATE test.rid SET id = id+1;

		DELETE FROM A WHERE (id, name) = (4,'D');
		DELETE FROM B WHERE (id, name) = (4,'D');
		UPDATE test.rid SET id = id+1;

		DELETE FROM A WHERE (id, name) = (5,'E');
		UPDATE B SET (id, name) = (5,'EE') where (id, name) = (5, 'E');
		UPDATE test.rid SET id = id+1;
		`)
		if err != nil {
			t.Fatal(err)
		}

		dbDiff := newDbDiff(connPool, "rid", nil, "generated_id")
		rowDiffs, err := dbDiff.getClonedTableRowDiffAtNReqs(ctx, cloneDdl.clonedTables["a"], cloneDdl.clonedTables["b"], 3)
		if err != nil {
			t.Fatal(err)
		}

		expectedRowDiffs := &Diff{
			Control: []*Row{
				{int32(1), "A"},
				{nil, nil},
				{int32(3), "CC"},
			},
			Baseline: []*Row{
				{int32(1), "A"},
				{int32(2), "B"},
				{int32(3), "C"},
			},
			Experimental: []*Row{
				{int32(1), "AA"},
				{int32(2), "B"},
				{int32(3), string("C")},
			},
			ColNames: []string{"id", "name"},
		}

		if diff := cmp.Diff(expectedRowDiffs, rowDiffs); diff != "" {
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

		err = dropTable(ctx, connPool, "a")
		if err != nil {
			t.Fatal(err)
		}

		err = dropTable(ctx, connPool, "b")
		if err != nil {
			t.Fatal(err)
		}
	})
}
