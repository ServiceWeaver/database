package service

import (
	"bankofanthos_prototype/eval_driver/dbclone"
	"bankofanthos_prototype/eval_driver/utility"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type Database struct {
	Name string
	Url  string
}

// This function will be deprecated soon after integrating with database three way diff
func dumpDb(dbDumpPath string) error {
	outfile, err := os.Create(dbDumpPath)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	// dump postgresdb transactions table
	transactionsColumns := []string{"transaction_id", "from_acct", "to_acct", "from_route", "to_route", "amount", "timestamp"}
	transactionsQuery := fmt.Sprintf("SELECT %s FROM transactions ORDER BY %s;", strings.Join(transactionsColumns, ","), strings.Join(transactionsColumns, ","))

	url := "postgresql://admin:admin@localhost:5432/postgresdb?sslmode=disable"
	dumpPostgresdbCmd := exec.Command("psql", url, "-c", transactionsQuery)
	dumpPostgresdbCmd.Stdout = outfile
	dumpPostgresdbCmd.Stderr = outfile
	err = dumpPostgresdbCmd.Run()
	if err != nil {
		return fmt.Errorf("failed to dump postgresdb: %v", err)
	}

	// dump accountsdb users and contacts table
	usersColumns := []string{"accountid", "username", "passhash", "firstname", "lastname", "birthday", "timezone", "address", "state", "zip", "ssn"}
	usersQuery := fmt.Sprintf("SELECT %s FROM Users ORDER BY %s;", strings.Join(usersColumns, ","), strings.Join(usersColumns, ","))

	contactsColumns := []string{"username", "label", "account_num", "routing_num", "is_external"}
	contactsQuery := fmt.Sprintf("SELECT %s FROM Contacts ORDER BY %s;", strings.Join(contactsColumns, ","), strings.Join(contactsColumns, ","))
	url = "postgresql://admin:admin@localhost:5432/accountsdb?sslmode=disable"
	dumpAccountsdbCmd := exec.Command("psql", url, "-c", usersQuery, "-c", contactsQuery)
	dumpAccountsdbCmd.Stdout = outfile
	dumpPostgresdbCmd.Stderr = outfile

	err = dumpAccountsdbCmd.Run()
	if err != nil {
		return fmt.Errorf("failed to dump accountsdb: %v", err)
	}
	return nil
}

func TakeSnapshot(db *Database, snapshotPath string) error {
	cmd := exec.Command("pg_dump", db.Url, "-f", snapshotPath)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to take snapshot, %w", err)
	}

	return nil
}

func dbExists(name string, prodDb *Database) (bool, error) {
	cmd := exec.Command("psql", prodDb.Url, "-c", fmt.Sprintf(`\l %s`, name))
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("failed check database exist, %w", err)
	}
	exists := strings.Contains(string(out), name)
	return exists, nil
}

// RestoreSnapshot creates a separate snapshot db if not exist, and restore prod db to snapshot db
func RestoreSnapshot(snapshotPath string, prodDb *Database) (*Database, error) {
	dbName := utility.GetSnapshotDbNameByProd(prodDb.Name)
	exists, err := dbExists(dbName, prodDb)
	if err != nil {
		return nil, err
	}
	if !exists {
		createQuery := fmt.Sprintf("CREATE DATABASE %s;", dbName)
		createCmd := exec.Command("psql", prodDb.Url, "-c", createQuery)
		err := createCmd.Run()
		if err != nil {
			return nil, fmt.Errorf("failed to create snapshot database, %w", err)
		}
	}

	snapshotUrl := strings.ReplaceAll(prodDb.Url, prodDb.Name, dbName)
	snapshot := &Database{Name: dbName, Url: snapshotUrl}
	restoreCmd := exec.Command("psql", snapshot.Url, "-f", snapshotPath)
	err = restoreCmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to restore snapshot, %w", err)
	}
	return snapshot, nil
}

func CloneDB(ctx context.Context, snapshot *Database, namespace string) (*dbclone.ClonedDb, error) {
	return dbclone.Clone(ctx, snapshot.Url, namespace)
}

func CloseSnapshotDB(prod *Database, snapshotName string) error {
	query := fmt.Sprintf("DROP DATABASE %s;", snapshotName)
	cmd := exec.Command("psql", prod.Url, "-c", query)
	return cmd.Run()
}
