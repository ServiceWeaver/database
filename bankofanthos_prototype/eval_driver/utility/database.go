package utility

import (
	"fmt"
	"os/exec"
	"strings"
)

func GetSnapshotDbNameByProd(name string) string {
	return name + "snapshot"
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
	dbName := GetSnapshotDbNameByProd(prodDb.Name)
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

func CloseSnapshotDB(prod *Database, snapshotName string) error {
	query := fmt.Sprintf("DROP DATABASE %s;", snapshotName)
	cmd := exec.Command("psql", prod.Url, "-c", query)
	return cmd.Run()
}
