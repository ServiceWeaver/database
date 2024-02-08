package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/gookit/color"
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
			"POSTGRES_DB":       "postgres",
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

	dbURI := fmt.Sprintf("postgres://postgres:postgres@%v:%v/postgres", host, port.Port())

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
	defer dbContainer.Terminate(context.Background())

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

	t.Run("UsersPrimeAndUsersSame", func(t *testing.T) {
		users, err := db.Dump(ctx, Table(Users))
		if err != nil {
			t.Error(err)
		}

		usersprime, err := db.Dump(ctx, Table(UsersPrime))
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
		origUsers, err := db.Dump(ctx, Table(Users))
		if err != nil {
			t.Error(err)
		}

		newUser := &User{6, "user6"}

		err = db.Insert(ctx, newUser)
		if err != nil {
			t.Error(err)
		}

		// check usersprime
		usersprime, err := db.FindById(ctx, Table(UsersPrime), newUser.id)
		if err != nil {
			t.Error(err)
		}

		if diff := cmp.Diff(newUser, usersprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		usersplus, err := db.FindById(ctx, Table(UsersPlus), newUser.id)
		if diff := cmp.Diff(newUser, usersplus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if err != nil {
			t.Error(err)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), newUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Table(Users))
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

	// Insert a id just got deleted
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

		// check usersprime
		usersprime, err := db.FindById(ctx, Table(UsersPrime), existUser.id)
		if err != nil {
			t.Error(err)
		}

		if diff := cmp.Diff(existUser, usersprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		usersplus, err := db.FindById(ctx, Table(UsersPlus), existUser.id)
		if diff := cmp.Diff(existUser, usersplus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if err != nil {
			t.Error(err)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), existUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	// Insert id is current maxId+1
	t.Run("InsertMaxId", func(t *testing.T) {
		users, err := db.Dump(ctx, UsersPrime)
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

		// check usersprime
		usersprime, err := db.FindById(ctx, Table(UsersPrime), newUser.id)
		if err != nil {
			t.Error(err)
		}

		if diff := cmp.Diff(newUser, usersprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		usersplus, err := db.FindById(ctx, Table(UsersPlus), newUser.id)
		if diff := cmp.Diff(newUser, usersplus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
		if err != nil {
			t.Error(err)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), newUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})
}

func TestUsersDelete(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, err := SetupTestDatabase()
	if err != nil {
		t.Error(err)
	}
	defer dbContainer.Terminate(context.Background())

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

	// Delete existing id
	// Expect users unchanged, id not exist in usersprime, usersplus
	// id exist in usersminus
	t.Run("DeleteExistId", func(t *testing.T) {
		existUser := &User{5, "user5"}
		origUsers, err := db.Dump(ctx, Table(Users))
		if err != nil {
			t.Error(err)
		}

		err = db.Delete(ctx, existUser)
		if err != nil {
			t.Error(err)
		}

		// check usersprime
		_, err = db.FindById(ctx, Table(UsersPrime), existUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		_, err = db.FindById(ctx, Table(UsersPlus), existUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersminus
		userminus, err := db.FindById(ctx, Table(UsersMinus), existUser.id)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(existUser, userminus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Table(Users))
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

		usersprime, err := db.FindById(ctx, Table(UsersPrime), newUser.id)
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

		// check usersprime
		_, err = db.FindById(ctx, Table(UsersPrime), newUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		_, err = db.FindById(ctx, Table(UsersPlus), newUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersminus
		userminus, err := db.FindById(ctx, Table(UsersMinus), newUser.id)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(newUser, userminus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

	})

	// TODO: Delete a non-exist id should raise error, but since there is no row to trigger the
	// trigger function, error will not be raised.
	t.Run("DeleteNonExistId", func(t *testing.T) {
		nonexistUser := &User{10, "nonuser"}
		err := db.Delete(ctx, nonexistUser)
		if err != nil {
			t.Error(err)
		}

		// check usersprime
		_, err = db.FindById(ctx, Table(UsersPrime), nonexistUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		_, err = db.FindById(ctx, Table(UsersPlus), nonexistUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), nonexistUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

	})

	// Make sure users and usersprime is the same as prereq
	// Get rows r from users for the expression for delete query later
	// Expect rows r are deleted from usersprime
	t.Run("NestedDelete", func(t *testing.T) {
		userUsers, err := db.Dump(ctx, Table(Users))
		if err != nil {
			t.Error(err)
		}
		primeUsers, err := db.Dump(ctx, Table(UsersPrime))
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
				color.Redf("Error reading user row: %v", err)
			}
			users = append(users, &u)
		}

		for _, user := range users {
			// check usersprime
			_, err = db.FindById(ctx, Table(UsersPrime), user.id)
			if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}

			// check usersplus
			_, err = db.FindById(ctx, Table(UsersPlus), user.id)
			if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}

			// check usersminus
			userminus, err := db.FindById(ctx, Table(UsersMinus), user.id)
			if err != nil {
				t.Error(err)
			}
			if diff := cmp.Diff(user, userminus, userOpt); diff != "" {
				t.Errorf("(-want,+got):\n%s", diff)
			}
		}
		updatedPrimeUsers, err := db.Dump(ctx, Table(UsersPrime))
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
	defer dbContainer.Terminate(context.Background())

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

	// Update existing id
	// Expect users unchanged, row exist in usersprime, usersplus
	// id not exist in usersminus
	t.Run("UpdateExistId", func(t *testing.T) {
		newUser := &User{3, "test3"}
		origUsers, err := db.Dump(ctx, Table(Users))
		if err != nil {
			t.Error(err)
		}

		err = db.Update(ctx, newUser)

		// check usersprime
		userprime, err := db.FindById(ctx, Table(UsersPrime), newUser.id)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(newUser, userprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		userplus, err := db.FindById(ctx, Table(UsersPrime), newUser.id)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(newUser, userplus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), newUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check users table are unchanged
		updateUsers, err := db.Dump(ctx, Table(Users))
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
		// check usersprime
		insertUserprime, err := db.FindById(ctx, Table(UsersPrime), newUser.id)
		if err != nil {
			t.Error(err)
		}
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(newUser, insertUserprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		updatedUser := &User{newUser.id, "test6"}

		err = db.Update(ctx, updatedUser)

		// check usersprime
		userprime, err := db.FindById(ctx, Table(UsersPrime), updatedUser.id)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(updatedUser, userprime, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		userplus, err := db.FindById(ctx, Table(UsersPrime), updatedUser.id)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(updatedUser, userplus, userOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), updatedUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

	})

	// TODO: Update a non-exist id should return error, however, the trigger will not
	// be triggered if there is no row
	t.Run("UpdateNonExist", func(t *testing.T) {
		nonexistUser := &User{10, "nonuser"}
		err := db.Update(ctx, nonexistUser)
		if err != nil {
			t.Error(err)
		}

		// check usersprime
		_, err = db.FindById(ctx, Table(UsersPrime), nonexistUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersplus
		_, err = db.FindById(ctx, Table(UsersPlus), nonexistUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}

		// check usersminus
		_, err = db.FindById(ctx, Table(UsersMinus), nonexistUser.id)
		if diff := cmp.Diff(err.Error(), pgx.ErrNoRows.Error()); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

}
