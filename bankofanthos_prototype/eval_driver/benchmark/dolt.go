package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
)

type dolt struct {
	binPath         string
	snapshotPath    string
	mysqlPath       string
	port            string
	convertToolPath string
	mysqlConn       []string
	table           string

	client  *sql.DB
	service *exec.Cmd
}

func newDoltClient(port string, table string) (*dolt, error) {
	snapshotPath := filepath.Join(dumpDir, "postgres.sql")
	mysqlPath := filepath.Join(dumpDir, "mysql.sql")

	if _, err := os.Stat(mysqlPath); err == nil {
		if err := os.Remove(mysqlPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing file, %s", err)
		}
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home dir: %s", err)
	}
	convertToolPath := filepath.Join(homeDir, "pg2mysql-main", "pg2mysql.pl")
	binPath := filepath.Join(homeDir, "dolt")
	mysqlConn := []string{"mysql", "--host", "127.0.0.1", "--port", port, "-u", "root", "public"}
	return &dolt{snapshotPath: snapshotPath, mysqlPath: mysqlPath, convertToolPath: convertToolPath, binPath: binPath, mysqlConn: mysqlConn, table: table, port: port}, nil
}

// run in a separate goroutine since the call is blocking
func (d *dolt) start(ctx context.Context) {
	startCmd := exec.CommandContext(ctx, "dolt", "sql-server")
	startCmd.Dir = d.binPath

	if err := startCmd.Start(); err != nil {
		log.Panicf("failed to start dolt server, %s", err)
	}

	d.service = startCmd
}

func (d *dolt) stop(ctx context.Context, cancel context.CancelFunc) error {
	if err := d.dropDatabase(); err != nil {
		return err
	}

	cancel()

	err := d.service.Wait()
	if err != nil && ctx.Err() != context.Canceled {
		return fmt.Errorf("failed to running dolt service, %s", err)
	}

	return d.client.Close()
}

func (d *dolt) convertPostgres() error {
	outputFile, err := os.Create(d.mysqlPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %s", err)
	}
	defer outputFile.Close()

	convertCmd := exec.Command(d.convertToolPath, d.snapshotPath)
	convertCmd.Stdout = outputFile
	err = convertCmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func (d *dolt) loadData() error {
	inputFile, err := os.Open(d.mysqlPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	loadCmd := exec.Command("dolt", "sql")
	loadCmd.Stdin = inputFile
	loadCmd.Dir = d.binPath
	err = loadCmd.Run()
	return err
}

func (d *dolt) commit() error {
	_, err := d.client.Exec(`CALL DOLT_COMMIT('-Am', 'commit');`)
	return err
}

func (d *dolt) createNewBranch(branch string) error {
	_, err := d.client.Exec(fmt.Sprintf("CALL DOLT_CHECKOUT('-b', '%s');", branch))
	return err
}

func (d *dolt) dropDatabase() error {
	_, err := d.client.Exec("DROP DATABASE PUBLIC;")
	return err
}

func (d *dolt) diffBranch() error {
	var args []string
	args = append(args, d.mysqlConn...)
	args = append(args, "-e", fmt.Sprintf("SELECT * FROM dolt_diff('main', 'n', '%s');", d.table))
	diffCmd := exec.Command(args[0], args[1:]...)
	out, err := diffCmd.CombinedOutput()
	if err != nil {
		fmt.Printf("diff out: %s\n", out)
		return fmt.Errorf("failed to diff branches, %s", err)
	}
	return nil
}

func (d *dolt) connect() error {
	// Connect to the server.
	uri := fmt.Sprintf("root:@tcp(localhost:%s)/public", d.port)
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return fmt.Errorf("sql.Open(%q, %q): %s", "mysql", uri, err)
	}
	if err := db.Ping(); err != nil {
		return fmt.Errorf("db.Ping(): %s", err)
	}
	d.client = db
	return nil
}
