package dbbranch

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestRaceCondition(t *testing.T) {
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

	// t1: tx1 begin
	// t2: tx1 exec insert unique acctid
	// t3: tx2 begin
	// t4: tx2 exec insert same acctid
	// t5: tx2 commit
	// t6: tx1 commit (tx1 succeed commit if isolation level is read committed and fail if isolation level is serializable)
	// This test tests that if two concurrent query insert the same unique value,
	// it should fail one instead of insert two rows for unique column.
	t.Run("raceCondition", func(t *testing.T) {
		err = createTriggers(ctx, connPool, cloneDdl.clonedTables["users"])
		if err != nil {
			t.Fatal(err)
		}

		tx1, err := connPool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx1.Rollback(ctx)

		acctId := "111111111111"
		insertSql := fmt.Sprintf(`INSERT INTO users(accountid, username, passhash, birthday) VALUES
		('%s', 'eve', '1234', '2002-01-01');`, acctId)
		_, err = tx1.Exec(ctx, insertSql)
		if err != nil {
			t.Fatal(err)
		}

		tx2, err := connPool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx2.Rollback(ctx)

		_, err = tx2.Exec(ctx, insertSql)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			t.Fatal(err)
		}

		tx2.Commit(ctx)
		tx1.Commit(ctx)

		rows, err := connPool.Query(ctx, "SELECT accountid,username FROM users")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatal(err)
			}
			if id == acctId {
				count += 1
			}
		}

		if got, want := count, 1; got != want {
			t.Errorf("(-want,+got):\n(%d,%d)", want, got)
		}
	})
}
