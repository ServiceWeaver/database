package diff

import (
	"fmt"
	"os"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
)

var (
	fromFile = "control"
	toFile   = "experimental"
)

// outputEq compares two files content, print out the diff and return
// a equal bool.
func OutputEq(path1 string, path2 string) (bool, error) {
	output1, err := os.ReadFile(path1)
	if err != nil {
		return false, err
	}

	output2, err := os.ReadFile(path2)
	if err != nil {
		return false, err
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(output1)),
		B:        difflib.SplitLines(string(output2)),
		FromFile: fromFile,
		ToFile:   toFile,
		Context:  0,
		Eol:      "\n",
	}
	result, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return false, err
	}
	if result == "" {
		return true, nil
	}

	fmt.Println(strings.Replace(result, "\t", " ", -1))
	return false, nil
}
