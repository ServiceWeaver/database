package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Table int

const (
	Users Table = iota + 1
	Usersplus
	Usersminus
	Usersprime
)

func (t Table) String() string {
	return [...]string{"Users", "Usersplus", "Usersminus", "Usersprime"}[t-1]
}

type User struct {
	id   int
	name string
}

type Database struct {
	connPool *pgxpool.Pool
}

func NewDatabase(ctx context.Context, connPool *pgxpool.Pool) *Database {
	// Create tables
	if _, err := connPool.Exec(ctx, `
		CREATE TABLE Users(id INT PRIMARY KEY, name VARCHAR(80));
		CREATE TABLE Usersplus(id INT PRIMARY KEY, name VARCHAR(80));
		CREATE TABLE Usersminus(id INT PRIMARY KEY);

		CREATE VIEW Usersprime AS
		SELECT *
		FROM Users
		WHERE id NOT IN (SELECT id FROM Usersplus)
		AND id NOT IN (SELECT id FROM Usersminus)
		UNION ALL
		SELECT * FROM Usersplus;
		`); err != nil {
		log.Fatalf("failed to exec query, err=%v\n", err)
	}

	return &Database{
		connPool: connPool,
	}
}

func (d *Database) createInsertTrigger(ctx context.Context) error {
	_, err := d.connPool.Exec(ctx, `
	CREATE OR REPLACE FUNCTION redirect_insert()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN
	RAISE NOTICE 'Trigger redirect_insert executed for ID %', NEW.id;
	IF EXISTS (SELECT * FROM USERSPRIME WHERE id = NEW.id) THEN
		RAISE EXCEPTION 'id already exists %', OLD.id;
	ELSE
		DELETE FROM usersminus WHERE id=NEW.id;
		INSERT INTO usersplus (name, id)
		VALUES (NEW.name, NEW.id);
		RETURN NEW;
	END IF;
	END;
	$$;

	CREATE OR REPLACE TRIGGER redirect_insert_trigger
		INSTEAD OF INSERT ON USERSPRIME
		FOR EACH ROW
		EXECUTE PROCEDURE redirect_insert();
	`)

	return err
}

func (d *Database) createDeleteTrigger(ctx context.Context) error {
	_, err := d.connPool.Exec(ctx, `
	CREATE OR REPLACE FUNCTION redirect_delete()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN
	RAISE NOTICE 'Trigger redirect_delete executed for ID %', OLD.id;
	IF EXISTS (SELECT * FROM usersplus WHERE ID = OLD.id) THEN
		DELETE FROM usersplus WHERE id = OLD.id;
	END IF;
	INSERT INTO usersminus (id)
	VALUES (OLD.id);
	RETURN OLD;
	END;
	$$;

	CREATE OR REPLACE TRIGGER redirect_delete_trigger
	INSTEAD OF DELETE ON usersprime
	FOR EACH ROW
	EXECUTE PROCEDURE redirect_delete();
	`)

	return err
}

func (d *Database) createUpdateTrigger(ctx context.Context) error {
	_, err := d.connPool.Exec(ctx, `
	CREATE OR REPLACE FUNCTION redirect_update()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN
	RAISE NOTICE 'Trigger redirect_update executed for ID %', NEW.id;
	IF NOT EXISTS (SELECT * FROM usersplus WHERE ID = OLD.id) THEN
		INSERT INTO usersplus SELECT * FROM USERSPRIME where id=OLD.id;
	END IF;
	UPDATE usersplus SET name = NEW.name WHERE id = NEW.id;
	RETURN NEW;
	END;
	$$;

	CREATE OR REPLACE TRIGGER redirect_update_trigger
	INSTEAD OF UPDATE ON USERSPRIME
	FOR EACH ROW
	EXECUTE PROCEDURE redirect_update();
  `)

	return err
}

func (d *Database) CreateTriggers(ctx context.Context) {
	if err := d.createInsertTrigger(ctx); err != nil {
		log.Fatal("failed to create insert trigger", err)
	}

	if err := d.createDeleteTrigger(ctx); err != nil {
		log.Fatal("failed to create delete trigger", err)
	}

	if err := d.createUpdateTrigger(ctx); err != nil {
		log.Fatal("failed to create update trigger", err)
	}
}

func (d *Database) FindById(ctx context.Context, table Table, uid int) (*User, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = %d", table, uid)

	row := d.connPool.QueryRow(ctx, query)
	var (
		id   int
		name string
		user *User
	)

	if table == Usersminus {
		err := row.Scan(&id)
		if err != nil {
			return nil, err
		}

		user = &User{
			id: id,
		}
	} else {
		err := row.Scan(&id, &name)
		if err != nil {
			return nil, err
		}

		user = &User{
			id:   id,
			name: name,
		}
	}

	return user, nil
}

func (d *Database) Dump(ctx context.Context, table Table) ([]*User, error) {
	var users []*User

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id", table)
	rows, err := d.connPool.Query(ctx, query)
	if err != nil {
		return users, err
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		if err := rows.Scan(&u.id, &u.name); err != nil {
			return nil, err
		}
		users = append(users, &u)
	}
	return users, nil
}

func (d *Database) Insert(ctx context.Context, user *User) error {
	_, err := d.connPool.Exec(ctx, `Insert into usersprime Values($1,$2);`, user.id, user.name)
	return err
}

func (d *Database) Delete(ctx context.Context, user *User) error {
	_, err := d.connPool.Exec(ctx, `Delete from usersprime where id=$1 AND name=$2;`, user.id, user.name)
	return err
}

func (d *Database) Update(ctx context.Context, user *User) error {
	_, err := d.connPool.Exec(ctx, `Update usersprime SET name=$1 where id=$2;`, user.name, user.id)
	return err
}

// print out all tables
func (d *Database) Print(ctx context.Context) error {
	for _, table := range []string{
		"Users",
		"Usersprime",
		"Usersplus",
		"Usersminus",
	} {
		fmt.Println(table)
		q := fmt.Sprintf("SELECT * FROM %s;", table)
		rows, err := d.connPool.Query(ctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if table == Usersminus.String() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				fmt.Println(id)
			} else {
				var id int
				var name string
				if err := rows.Scan(&id, &name); err != nil {
					return err
				}
				fmt.Printf("%d: %q\n", id, name)
			}

		}
		if err := rows.Err(); err != nil {
			return err
		}

		fmt.Println()
	}

	return nil
}
