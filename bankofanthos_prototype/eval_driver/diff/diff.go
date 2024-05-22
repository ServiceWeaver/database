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

func printDiff(diff string) string {
	var b strings.Builder
	diffLines := strings.Split(diff, "\n")
	for _, line := range diffLines {
		if strings.HasPrefix(line, "@@") || strings.HasPrefix(line, "---") || strings.HasPrefix(line, "+++") {
			b.WriteString(line)
			b.WriteString("\n")
			continue
		}
		b.WriteString(string(Bold))
		b.WriteString(line)
		b.WriteString("\n")
		b.WriteString(string("\x1b[0m"))
	}
	return b.String()
}

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

	// hack the response
	output1Str := strings.ReplaceAll(string(output1), "400 Bad Request", "")
	output2Str := strings.ReplaceAll(string(output2), "400 Bad Request", "")

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(output1Str),
		B:        difflib.SplitLines(output2Str),
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

	result = strings.ReplaceAll(result, "-", "<")
	result = strings.ReplaceAll(result, "+", ">")

	fmt.Println(printDiff(result))
	return false, nil
}
