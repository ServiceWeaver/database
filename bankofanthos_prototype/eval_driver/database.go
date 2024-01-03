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
		err = fmt.Errorf("failed to create a new branch, err:=%+v", err)
	}
	fmt.Printf("endpoint list output:\n %s\n", listOuput)
	// parse the output
	var address string
	lines := strings.Split(string(listOuput), "\n")

	// Hardcoding
	for _, line := range lines[1:] {
		newLine := strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		for j, word := range strings.Split(newLine, " ") {
			if j == 0 && word != branchName {
				break
			}
			if j == 1 {
				address = word
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

func cloneDatabase(branchName, ancestorBranchName string) (database, error) {
	clonedDatabase := database{}

	// running database fork command under neon directory
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return clonedDatabase, err
	}

	home, _ := os.UserHomeDir()
	err = os.Chdir(filepath.Join(home, "neon"))

	if err != nil {
		fmt.Println("Error changing directory:", err)
		return clonedDatabase, err
	}
	defer func() {
		err = os.Chdir(currentDir)
		if err != nil {
			fmt.Println("Error changing back to original directory:", err)
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

	//create a new branch
	cloneCmd := exec.Command("cargo", "neon", "timeline", "branch", "--ancestor-branch-name", ancestorBranchName, "--branch-name", branchName)

	err = cloneCmd.Run()
	if err != nil {
		err = fmt.Errorf("failed to create a new branch, err:=%+v", err)
	}

	// start progress at that branch
	createPostgresCmd := exec.Command("cargo", "neon", "endpoint", "create", branchName, "--branch-name", branchName)
	err = createPostgresCmd.Run()
	if err != nil && strings.Contains(err.Error(), "exists already") {
		return clonedDatabase, err
	} else if err != nil {
		err = fmt.Errorf("failed to create a postgres on the branch, err:=%+v", err)
	}

	// cargo neon endpoint start clone
	startCmd := exec.Command("cargo", "neon", "endpoint", "start", branchName)
	err = startCmd.Run()
	if err != nil {
		err = fmt.Errorf("failed to start postgres on the branch, err:=%+v", err)
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

	dumpPostgresdbCmd := exec.Command("pg_dump", "-p", dbPort, "-h", "127.0.0.1", "-U", "admin", "postgresdb")

	dumpPostgresdbCmd.Stdout = outfile
	dumpPostgresdbCmd.Stderr = outfile

	err = dumpPostgresdbCmd.Run()
	if err != nil {
		err = fmt.Errorf("failed to dump postgresdb, err:=%+v", err)
	}

	dumpAccountsdbCmd := exec.Command("pg_dump", "-p", dbPort, "-h", "127.0.0.1", "-U", "admin", "accountsdb")
	dumpAccountsdbCmd.Stdout = outfile
	dumpPostgresdbCmd.Stdout = outfile

	err = dumpAccountsdbCmd.Run()
	if err != nil {
		err = fmt.Errorf("failed to dump accountsdb, err:=%+v", err)
	}

	fmt.Printf("Successfully dump port %s to %s\n", dbPort, dbDumpPath)
	return nil
}
