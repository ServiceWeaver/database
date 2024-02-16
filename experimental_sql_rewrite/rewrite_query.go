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
		CREATE TABLE IF NOT EXISTS users (
			id        INT,
			name varchar(80),
			CONSTRAINT pk_id
			PRIMARY KEY (id)
		);
		
		CREATE TABLE IF NOT EXISTS usersplus (
			id        INT,
			name varchar(80)
		);
		
		CREATE TABLE IF NOT EXISTS usersminus (
			id        INT,
			name varchar(80)
		);

		CREATE OR REPLACE VIEW USERSPRIME AS
		SELECT * FROM users
		UNION ALL
		SELECT * FROM usersplus
		EXCEPT ALL
		SELECT * FROM usersminus;
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
	RAISE NOTICE 'Trigger redirect_insert executed for ID % NAME %', NEW.id, NEW.name; 
	IF EXISTS (SELECT * FROM USERSPRIME WHERE id = NEW.id) THEN
		RAISE EXCEPTION 'id already exists %', OLD.id;
	ELSE
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

func (d *Database) createNonUniqueInsertTrigger(ctx context.Context) error {
	_, err := d.connPool.Exec(ctx, `
	CREATE OR REPLACE FUNCTION redirect_insert()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	AS $$
	BEGIN
	RAISE NOTICE 'Trigger redirect_insert executed for ID %', NEW.id; 
	INSERT INTO usersplus VALUES (NEW.id,NEW.name);
	RETURN NEW;
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
	RAISE NOTICE 'Trigger redirect_delete executed for ID % NAME %', OLD.id, OLD.name; 
	INSERT INTO usersminus (id,name) 
	VALUES (OLD.id, OLD.name);
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
	RAISE NOTICE 'Trigger redirect_update executed for NEW ID % New NAME % OLD ID % OLD name %', NEW.id,NEW.name,OLD.id,OLD.name;
	INSERT INTO usersminus (id,name) VALUES (OLD.id,OLD.name);
	INSERT INTO usersplus (id,name) VALUES (NEW.id,NEW.name);
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

func (d *Database) FindUser(ctx context.Context, table Table, user *User) (*User, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=%d AND name='%s'", table, user.id, user.name)

	row := d.connPool.QueryRow(ctx, query)
	var (
		id   int
		name string
	)

	err := row.Scan(&id, &name)
	if err != nil {
		return nil, err
	}

	got := &User{
		id:   id,
		name: name,
	}

	return got, nil
}

// drop all rows in usersplus and usersminus
func (d *Database) Reset(ctx context.Context) {
	_, err := d.connPool.Exec(ctx, `
	DELETE FROM usersplus;
	DELETE FROM usersminus;
	`)
	if err != nil {
		log.Fatal("failed to drop rows from usersplus/usersminus", err)
	}
}

func (d *Database) Dump(ctx context.Context, table Table) ([]*User, error) {
	var users []*User

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id, name", table)
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
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return err
			}
			fmt.Printf("%d: %q\n", id, name)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		fmt.Println()
	}

	return nil
}
