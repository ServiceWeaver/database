package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupTestDatabase(ctx context.Context) (testcontainers.Container, *pgxpool.Pool, error) {
	dbContainer, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("docker.io/postgres:16-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return nil, nil, err
	}

	dbURL, err := dbContainer.ConnectionString(ctx)
	if err != nil {
		return nil, nil, err
	}

	connPool, err := pgxpool.Connect(ctx, dbURL)
	if err != nil {
		return nil, nil, err
	}

	return dbContainer, connPool, err
}

func TestUsersInsert(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Error(err)
	}
	defer dbContainer.Terminate(ctx)

	db := NewDatabase(ctx, connPool)
	db.CreateTriggers(ctx)

	_, err = connPool.Exec(ctx, `
		INSERT INTO users VALUES (1,'user1');
		INSERT INTO users VALUES (2,'user2');
		INSERT INTO users VALUES (3,'user3');
		INSERT INTO users VALUES (4,'user4');
		INSERT INTO users VALUES (5,'user5');
	`)
	if err != nil {
		t.Error(err)
	}

	userOpt := cmp.Comparer(func(x, y User) bool {
		return x.id == y.id && x.name == y.name
	})

	checkExists := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		got, err := db.FindUser(ctx, table, user)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if diff := cmp.Diff(user, got, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindUser(ctx, table, user)
		if err == nil {
			t.Fatalf("expected to have no rows error")
		}
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	t.Run("UsersprimeAndUsersSame", func(t *testing.T) {
		users, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}

		usersprime, err := db.Dump(ctx, Usersprime)
		if err != nil {
			t.Error(err)
		}

		if diff := cmp.Diff(users, usersprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	// Insert new id
	// Expect users remain the same
	// Expect new id exists in usersprime, usersplus and not exist in usersminus
	t.Run("InsertNewId", func(t *testing.T) {
		origUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}

		newUser := &User{6, "user6"}

		err = db.Insert(ctx, newUser)
		if err != nil {
			t.Error(err)
		}

		// exist in usersprime
		checkExists(t, ctx, newUser, Usersprime)

		// exist in usersplus
		checkExists(t, ctx, newUser, Usersplus)

		// not exist in usersminus
		checkMissing(t, ctx, newUser, Usersminus)

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(origUsers, updateUsers, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		db.Reset(ctx)
	})

	// Insert existing id
	// Expect already exists error
	t.Run("InsertExistingId", func(t *testing.T) {
		existUser := &User{1, "user2"}

		err = db.Insert(ctx, existUser)
		if !strings.Contains(err.Error(), "id already exists") {
			t.Errorf("InsertExistingID: got %q, want %q", err, "id already exists")
		}
	})

	// Insert id just got deleted
	// Expect new id exists in usersprime, usersplus and not exist in usersminus
	t.Run("InsertDeletedId", func(t *testing.T) {
		existUser := &User{1, "user1"}
		err = db.Delete(ctx, existUser)
		if err != nil {
			t.Error(err)
		}

		err = db.Insert(ctx, existUser)
		if err != nil {
			t.Error(err)
		}

		// exist in usersprime
		checkExists(t, ctx, existUser, Usersprime)

		// exist in usersplus
		checkExists(t, ctx, existUser, Usersplus)

		// exist in usersminus
		checkExists(t, ctx, existUser, Usersminus)

		db.Reset(ctx)
	})

	// Insert id is current maxId+1
	t.Run("InsertMaxId", func(t *testing.T) {
		users, err := db.Dump(ctx, Usersprime)
		if err != nil {
			t.Error(err)
		}
		newUser := &User{len(users) + 1, "test"}

		_, err = connPool.Exec(ctx, `
		INSERT INTO USERSPRIME (id, name)
		VALUES (
    		(SELECT MAX(id) + 1 FROM USERSPRIME), 
    		'test'
		);
		`)
		if err != nil {
			t.Error(err)
		}

		// exist in usersprime
		checkExists(t, ctx, newUser, Usersprime)

		// exist in usersplus
		checkExists(t, ctx, newUser, Usersplus)

		// not exist in usersminus
		checkMissing(t, ctx, newUser, Usersminus)

		db.Reset(ctx)
	})
}

func TestUsersDelete(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Error(err)
	}
	defer dbContainer.Terminate(ctx)

	db := NewDatabase(ctx, connPool)
	db.CreateTriggers(ctx)

	_, err = connPool.Exec(ctx, `
		INSERT INTO users VALUES (1,'user1');
		INSERT INTO users VALUES (2,'user2');
		INSERT INTO users VALUES (3,'user3');
		INSERT INTO users VALUES (4,'user4');
		INSERT INTO users VALUES (5,'user5');
	`)
	if err != nil {
		t.Error(err)
	}

	userOpt := cmp.Comparer(func(x, y User) bool {
		return x.id == y.id && x.name == y.name
	})

	checkExists := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		got, err := db.FindUser(ctx, table, user)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if diff := cmp.Diff(user, got, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindUser(ctx, table, user)
		if err == nil {
			t.Fatalf("expected to have no rows error")
		}
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	// Delete existing id
	// Expect users unchanged, id not exist in usersprime, usersplus
	// id exist in usersminus
	t.Run("DeleteExistId", func(t *testing.T) {
		existUser := &User{5, "user5"}
		origUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}

		err = db.Delete(ctx, existUser)
		if err != nil {
			t.Error(err)
		}

		// not exist in usersprime
		checkMissing(t, ctx, existUser, Usersprime)

		// not exist in usersplus
		checkMissing(t, ctx, existUser, Usersplus)

		// exist in usersminus
		checkExists(t, ctx, existUser, Usersminus)

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(origUsers, updateUsers, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = db.Insert(ctx, existUser)
		if err != nil {
			t.Error(err)
		}

		db.Reset(ctx)
	})

	// Delete existing id
	// Expect users unchanged, id not exist in usersprime, usersplus
	// id exist in usersminus
	t.Run("DeleteInsertedId", func(t *testing.T) {
		newUser := &User{6, "user6"}

		err = db.Insert(ctx, newUser)
		if err != nil {
			t.Error(err)
		}

		usersprime, err := db.FindUser(ctx, Usersprime, newUser)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(newUser, usersprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = db.Delete(ctx, newUser)
		if err != nil {
			t.Error(err)
		}

		// not exist in usersprime
		checkMissing(t, ctx, newUser, Usersprime)

		// exist in usersplus
		checkExists(t, ctx, newUser, Usersplus)

		// exist in usersminus
		checkExists(t, ctx, newUser, Usersminus)

		db.Reset(ctx)
	})

	// Delete non-exist Id
	// Expect no-op
	t.Run("DeleteNonExistId", func(t *testing.T) {
		nonexistUser := &User{10, "nonuser"}
		err := db.Delete(ctx, nonexistUser)
		if err != nil {
			t.Error(err)
		}

		// not exist in usersprime
		checkMissing(t, ctx, nonexistUser, Usersprime)

		// not exist in usersplus
		checkMissing(t, ctx, nonexistUser, Usersplus)

		// not exist in usersminus
		checkMissing(t, ctx, nonexistUser, Usersminus)

		db.Reset(ctx)
	})

	// Make sure users and usersprime is the same as prereq
	// Get rows r from users for the expression for delete query later
	// Expect rows r are deleted from usersprime
	t.Run("NestedDelete", func(t *testing.T) {
		userUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}
		primeUsers, err := db.Dump(ctx, Usersprime)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(userUsers, primeUsers, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		_, err = connPool.Exec(ctx, `
		DELETE FROM USERSPRIME
		WHERE id >= (SELECT AVG(id) FROM USERSPRIME as S WHERE S.id >= USERSPRIME.id);
		`)
		if err != nil {
			t.Error(err)
		}

		var users []*User
		rows, err := db.connPool.Query(ctx, `
		SELECT * FROM USERS
		WHERE id >= (SELECT AVG(id) FROM USERS as S WHERE S.id >= USERS.id);`)
		if err != nil {
			t.Error(err)
		}

		defer rows.Close()

		for rows.Next() {
			var u User
			if err := rows.Scan(&u.id, &u.name); err != nil {
				t.Error(err)
			}
			users = append(users, &u)
		}

		for _, user := range users {
			// not exist in usersprime
			checkMissing(t, ctx, user, Usersprime)

			// not exist in usersplus
			checkMissing(t, ctx, user, Usersplus)

			// exist in usersminus
			checkExists(t, ctx, user, Usersminus)
		}
		updatedPrimeUsers, err := db.Dump(ctx, Usersprime)
		if err != nil {
			t.Error(err)
		}

		// deleted rows + current rows still equal to original prime rows
		updatedPrimeUsers = append(updatedPrimeUsers, users...)
		if diff := cmp.Diff(primeUsers, updatedPrimeUsers, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		db.Reset(ctx)
	})
}

func TestUsersUpdate(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Error(err)
	}
	defer dbContainer.Terminate(ctx)

	db := NewDatabase(ctx, connPool)
	db.CreateTriggers(ctx)

	_, err = connPool.Exec(ctx, `
		INSERT INTO users VALUES (1,'user1');
		INSERT INTO users VALUES (2,'user2');
		INSERT INTO users VALUES (3,'user3');
		INSERT INTO users VALUES (4,'user4');
		INSERT INTO users VALUES (5,'user5');
	`)
	if err != nil {
		t.Error(err)
	}

	userOpt := cmp.Comparer(func(x, y User) bool {
		return x.id == y.id && x.name == y.name
	})

	checkExists := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		got, err := db.FindUser(ctx, table, user)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if diff := cmp.Diff(user, got, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindUser(ctx, table, user)
		if err == nil {
			t.Fatalf("expected to have no rows error")
		}
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	// Update existing id
	// Expect users unchanged, row exist in usersprime, usersplus
	// id not exist in usersminus
	t.Run("UpdateExistId", func(t *testing.T) {
		origUser := &User{3, "user3"}
		newUser := &User{3, "test3"}
		origUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}

		err = db.Update(ctx, newUser)
		if err != nil {
			t.Error(err)
		}

		// newUser exist in usersprime
		checkExists(t, ctx, newUser, Usersprime)

		// origUser not exist in usersprime
		checkMissing(t, ctx, origUser, Usersprime)

		// newUser exist in usersplus
		checkExists(t, ctx, newUser, Usersplus)

		// origUser exist in usersminus
		checkExists(t, ctx, origUser, Usersminus)

		// newUser not exist in usersminus
		checkMissing(t, ctx, newUser, Usersminus)

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(origUsers, updateUsers, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		db.Reset(ctx)
	})

	// Insert into usersprime first, and updated the new record
	// Expect new record exist in usersprime, usersplus, and not exist in usersminus
	t.Run("UpdateNewInsert", func(t *testing.T) {
		newUser := &User{6, "user6"}
		err = db.Insert(ctx, newUser)
		// newUser exists in usersprime
		insertUserprime, err := db.FindUser(ctx, Usersprime, newUser)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(newUser, insertUserprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		updatedUser := &User{newUser.id, "test6"}

		err = db.Update(ctx, updatedUser)
		if err != nil {
			t.Error(err)
		}

		// updated user exist in usersprime
		checkExists(t, ctx, updatedUser, Usersprime)

		// inserted user not exist in usersprime
		checkMissing(t, ctx, newUser, Usersprime)

		// updated user exist in usersplus
		checkExists(t, ctx, updatedUser, Usersplus)

		// inserted user exist in usersplus
		checkExists(t, ctx, newUser, Usersplus)

		// inserted user exist in usersminus
		checkExists(t, ctx, newUser, Usersminus)

		// updatedUser not exist in usersminus
		checkMissing(t, ctx, updatedUser, Usersminus)

		db.Reset(ctx)
	})

	// Update non-exist Id
	// Expect no-op
	t.Run("UpdateNonExist", func(t *testing.T) {
		nonexistUser := &User{10, "nonuser"}
		err := db.Update(ctx, nonexistUser)
		if err != nil {
			t.Error(err)
		}
		// not exist in usersprime
		checkMissing(t, ctx, nonexistUser, Usersprime)

		// not exist in usersplus
		checkMissing(t, ctx, nonexistUser, Usersplus)

		// not exist in usersminus
		checkMissing(t, ctx, nonexistUser, Usersminus)

		db.Reset(ctx)
	})
}

func TestNoPrimaryKeyOperations(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Error(err)
	}
	defer dbContainer.Terminate(ctx)

	db := NewDatabase(ctx, connPool)
	db.createNonUniqueInsertTrigger(ctx)
	db.createDeleteTrigger(ctx)
	db.createUpdateTrigger(ctx)

	// drop primary key from users table
	_, err = connPool.Exec(ctx, `
		ALTER TABLE users DROP CONSTRAINT pk_id;

		INSERT INTO users VALUES (1,'user1');
		INSERT INTO users VALUES (1,'user1');
		INSERT INTO users VALUES (1,'user1');
		INSERT INTO users VALUES (2,'user2');
		INSERT INTO users VALUES (2,'user3');
	`)
	if err != nil {
		t.Error(err)
	}

	userOpt := cmp.Comparer(func(x, y User) bool {
		return x.id == y.id && x.name == y.name
	})

	checkExists := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		got, err := db.FindUser(ctx, table, user)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if diff := cmp.Diff(user, got, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindUser(ctx, table, user)
		if err == nil {
			t.Fatalf("expected to have no rows error")
		}
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	t.Run("InsertDuplicateId", func(t *testing.T) {
		origUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}

		newUser := &User{1, "user5"}

		err = db.Insert(ctx, newUser)
		if err != nil {
			t.Error(err)
		}

		// exist in usersprime
		checkExists(t, ctx, newUser, Usersprime)

		// exist in usersplus
		checkExists(t, ctx, newUser, Usersplus)

		// not exist in usersminus
		checkMissing(t, ctx, newUser, Usersminus)

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(origUsers, updateUsers, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		db.Reset(ctx)
	})

	t.Run("UpdateIdToIdPlus", func(t *testing.T) {
		origUsers, err := db.Dump(ctx, Usersprime)
		if err != nil {
			t.Error(err)
		}

		_, err = connPool.Exec(ctx, `
			UPDATE usersprime set id=id+1;
		`)
		if err != nil {
			t.Error(err)
		}

		for _,user := range origUsers {
			updatedUser := &User{user.id+1, user.name}
			
			// updated user exist in usersprime
			checkExists(t, ctx, updatedUser, Usersprime)

			// updated user exist in usersprime
			checkExists(t, ctx, updatedUser, Usersprime)

			// exist in usersplus
			checkExists(t, ctx, updatedUser, Usersplus)

			// orig user not exist in usersprime
			checkMissing(t, ctx, user, Usersprime)

			// orig user exist in usersminus
			checkExists(t, ctx, user, Usersminus)
		}
		db.Reset(ctx)
	})

	t.Run("UpdateIdToIdMinus", func(t *testing.T) {
		origUsers, err := db.Dump(ctx, Usersprime)
		if err != nil {
			t.Error(err)
		}

		_, err = connPool.Exec(ctx, `
			UPDATE usersprime set id=id-1;
		`)
		if err != nil {
			t.Error(err)
		}

		for _,user := range origUsers {
			updatedUser := &User{user.id-1, user.name}
			
			// updatedUser exist in usersprime
			checkExists(t, ctx, updatedUser, Usersprime)

			// updatedUser exist in usersplus
			checkExists(t, ctx, updatedUser, Usersplus)

			// orig user not exist in usersprime
			checkMissing(t, ctx, user, Usersprime)

			// orig user exist in usersminus
			checkExists(t, ctx, user, Usersminus)
		}
		db.Reset(ctx)
	})

	t.Run("DeleteDuplicateIds", func(t *testing.T) {
		deletedUser := &User{1,"user1"}

		err = db.Delete(ctx,deletedUser)
		if err != nil {
			t.Error(err)
		}
			
		// deletedUser not exist in usersprime
		checkMissing(t, ctx, deletedUser, Usersprime)

		// deletedUser not exist in usersplus
		checkMissing(t, ctx, deletedUser, Usersplus)

		// deleted user exist in usersminus
		checkExists(t, ctx, deletedUser, Usersminus)

		db.Reset(ctx)
	})

	t.Run("DeleteInsertDeleteSameRows", func(t *testing.T) {
		deletedUser := &User{1,"user1"}
		deletedUserCnt := 0
		origUsersPrime,err := db.Dump(ctx,Usersprime)
		if err != nil {
			t.Error(err)
		}
		for _,user := range origUsersPrime{
			if user.id == deletedUser.id && user.name == deletedUser.name{
				deletedUserCnt+=1
			}
		}
		if diff := cmp.Diff(3, deletedUserCnt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		err = db.Delete(ctx,deletedUser)
		if err != nil {
			t.Error(err)
		}
			
		// deletedUser not exist in usersprime
		checkMissing(t, ctx, deletedUser, Usersprime)

		// deletedUser not exist in usersplus
		checkMissing(t, ctx, deletedUser, Usersplus)

		// deleted user exist in usersminus
		checkExists(t, ctx, deletedUser, Usersminus)

		// insert twice
		err = db.Insert(ctx,deletedUser)
		if err != nil {
			t.Error(err)
		}
		err = db.Insert(ctx,deletedUser)
		if err != nil {
			t.Error(err)
		}

		insertedUserCnt := 0
		updatedUsersPrime,err := db.Dump(ctx,Usersprime)
		if err != nil {
			t.Error(err)
		}
		for _,user := range updatedUsersPrime{
			if user.id == deletedUser.id && user.name == deletedUser.name{
				insertedUserCnt+=1
			}
		}

		if diff := cmp.Diff(2, insertedUserCnt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
			
		// deletedUser exist in usersprime
		checkExists(t, ctx, deletedUser, Usersprime)

		// deletedUser exist in usersplus
		checkExists(t, ctx, deletedUser, Usersplus)

		// deleted user exist in usersminus
		checkExists(t, ctx, deletedUser, Usersminus)

		err = db.Delete(ctx,deletedUser)
		if err != nil {
			t.Error(err)
		}

		// deletedUser not exist in usersprime
		checkMissing(t, ctx, deletedUser, Usersprime)

		// deletedUser exist in usersplus
		checkExists(t, ctx, deletedUser, Usersplus)

		// deleted user exist in usersminus
		checkExists(t, ctx, deletedUser, Usersminus)

		db.Reset(ctx)
	})
}
