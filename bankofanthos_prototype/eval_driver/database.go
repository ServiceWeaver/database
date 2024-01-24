package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type database struct {
	branch string
	port   string
}

func getDatabaseByBranchName(branchName string) (database, error) {
	database := database{}

	// list all endpoints
	listCmd := exec.Command("cargo", "neon", "endpoint", "list")

	listOuput, err := listCmd.Output()
	if err != nil {
		err = fmt.Errorf("Failed to create a new branch: %v", err)
	}
	fmt.Printf("Endpoint list output:\n %s\n", listOuput)

	// parse output for all neon database endpoints
	var address string
	lines := strings.Split(string(listOuput), "\n")

	// find the address of given branch name
	for _, line := range lines[1:] {
		newLine := strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		for j, word := range strings.Split(newLine, " ") {
			if j == 0 && word != branchName {
				break
			}
			if j == 1 {
				address = word
				break
			}
		}
	}

	// fail to find the branch
	if address == "" {
		fmt.Printf("Failed to find branch %s\n", branchName)
		return database, nil
	}

	database.port = strings.Split(address, ":")[1]
	database.branch = branchName

	return database, nil
}

// cloneDatabase clones a database from ancestorBranchName if it does not exist.
func cloneDatabase(branchName, ancestorBranchName string) (database, error) {
	clonedDatabase := database{}

	// running database fork command under neon directory
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		return clonedDatabase, err
	}

	home, _ := os.UserHomeDir()
	err = os.Chdir(filepath.Join(home, "neon"))

	if err != nil {
		fmt.Printf("Error changing directory: %v\n", err)
		return clonedDatabase, err
	}
	defer func() {
		err = os.Chdir(currentDir)
		if err != nil {
			fmt.Printf("Error changing back to original directory: %v\n", err)
			return
		}
	}()

	existingDb, err := getDatabaseByBranchName(branchName)
	if err != nil {
		return clonedDatabase, nil
	}
	if existingDb.branch == branchName && existingDb.port != "" {
		return existingDb, nil
	}

	// create a new branch
	cloneCmd := exec.Command("cargo", "neon", "timeline", "branch", "--ancestor-branch-name", ancestorBranchName, "--branch-name", branchName)

	err = cloneCmd.Run()
	if err != nil {
		return clonedDatabase, fmt.Errorf("Failed to create a new branch: %v", err)
	}

	// create progressql on that branch
	createPostgresCmd := exec.Command("cargo", "neon", "endpoint", "create", branchName, "--branch-name", branchName)
	err = createPostgresCmd.Run()
	if err != nil {
		return clonedDatabase, fmt.Errorf("Failed to create a postgres on the branch: %v", err)
	}

	// start postgresql on that branch
	startCmd := exec.Command("cargo", "neon", "endpoint", "start", branchName)
	err = startCmd.Run()
	if err != nil {
		return clonedDatabase, fmt.Errorf("Failed to start postgres on the branch: %v", err)
	}

	clonedDatabase, err = getDatabaseByBranchName(branchName)
	if err != nil {
		return clonedDatabase, nil
	}

	return clonedDatabase, err
}

func dumpDb(dbPort, dbDumpPath string) error {
	outfile, err := os.Create(dbDumpPath)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	// dump postgresdb
	dumpPostgresdbCmd := exec.Command("pg_dump", "-p", dbPort, "-h", "127.0.0.1", "-U", "admin", "postgresdb")

	dumpPostgresdbCmd.Stdout = outfile
	dumpPostgresdbCmd.Stderr = outfile

	err = dumpPostgresdbCmd.Run()
	if err != nil {
		return fmt.Errorf("Failed to dump postgresdb: %v", err)
	}

	// dump accountsdb
	dumpAccountsdbCmd := exec.Command("pg_dump", "-p", dbPort, "-h", "127.0.0.1", "-U", "admin", "accountsdb")
	dumpAccountsdbCmd.Stdout = outfile
	dumpPostgresdbCmd.Stdout = outfile

	err = dumpAccountsdbCmd.Run()
	if err != nil {
		return fmt.Errorf("Failed to dump accountsdb: %v", err)
	}

	fmt.Printf("Successfully dump port %s to %s\n", dbPort, dbDumpPath)
	return nil
}
