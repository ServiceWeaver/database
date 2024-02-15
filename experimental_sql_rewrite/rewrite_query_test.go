package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupTestDatabase() (testcontainers.Container, *pgxpool.Pool, error) {
	containerReq := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_DB":       "testdb",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
		},
	}
	dbContainer, err := testcontainers.GenericContainer(
		context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: containerReq,
			Started:          true,
		})
	if err != nil {
		return nil, nil, err
	}
	port, err := dbContainer.MappedPort(context.Background(), "5432")
	if err != nil {
		return nil, nil, err
	}
	host, err := dbContainer.Host(context.Background())
	if err != nil {
		return nil, nil, err
	}

	dbURI := fmt.Sprintf("postgres://postgres:postgres@%v:%v/testdb", host, port.Port())

	connPool, err := pgxpool.Connect(context.Background(), dbURI)
	if err != nil {
		return nil, nil, err
	}

	return dbContainer, connPool, err
}

func TestUsersInsert(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase()
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
		got, err := db.FindById(ctx, table, user.id)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if table != Usersminus {
			if diff := cmp.Diff(user, got, userOpt); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		} else {
			if diff := cmp.Diff(user.id, got.id); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindById(ctx, table, user.id)
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
	})

	// Insert existing id
	// Expect already exists error
	t.Run("InsertExistingId", func(t *testing.T) {
		existUser := &User{1, "user1"}

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

		// not exist in usersminus
		checkMissing(t, ctx, existUser, Usersminus)
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
	})
}

func TestUsersDelete(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase()
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
		got, err := db.FindById(ctx, table, user.id)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if table != Usersminus {
			if diff := cmp.Diff(user, got, userOpt); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		} else {
			if diff := cmp.Diff(user.id, got.id); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindById(ctx, table, user.id)
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

		usersprime, err := db.FindById(ctx, Usersprime, newUser.id)
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

		// not exist in usersplus
		checkMissing(t, ctx, newUser, Usersplus)

		// exist in usersminus
		checkExists(t, ctx, newUser, Usersminus)
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
	})
}

func TestUsersUpdate(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase()
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
		got, err := db.FindById(ctx, table, user.id)
		if err != nil {
			t.Fatalf("user %d does not exist: %v", user.id, err)
		}
		if table != Usersminus {
			if diff := cmp.Diff(user, got, userOpt); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		} else {
			if diff := cmp.Diff(user.id, got.id); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		}
	}

	checkMissing := func(t *testing.T, ctx context.Context, user *User, table Table) {
		t.Helper()
		_, err := db.FindById(ctx, table, user.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	}

	// Update existing id
	// Expect users unchanged, row exist in usersprime, usersplus
	// id not exist in usersminus
	t.Run("UpdateExistId", func(t *testing.T) {
		newUser := &User{3, "test3"}
		origUsers, err := db.Dump(ctx, Users)
		if err != nil {
			t.Error(err)
		}

		err = db.Update(ctx, newUser)
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
	})

	// Insert into usersprime first, and updated the new record
	// Expect new record exist in usersprime, usersplus, and not exist in usersminus
	t.Run("UpdateNewInsert", func(t *testing.T) {
		newUser := &User{6, "user6"}
		err = db.Insert(ctx, newUser)
		// newUser exists in usersprime
		insertUserprime, err := db.FindById(ctx, Usersprime, newUser.id)
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

		// exist in usersprime
		checkExists(t, ctx, updatedUser, Usersprime)

		// exist in usersplus
		checkExists(t, ctx, updatedUser, Usersplus)

		// not exist in usersminus
		checkMissing(t, ctx, updatedUser, Usersminus)
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
	})

}
