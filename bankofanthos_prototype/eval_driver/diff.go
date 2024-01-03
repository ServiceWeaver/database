package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
)

func outputEq(path1 string, path2 string) (bool, error) {
	output1, err := os.ReadFile(path1)
	if err != nil {
		return false, err
	}

	output2, err := os.ReadFile(path2)
	if err != nil {
		return false, err
	}

	diff := difflib.ContextDiff{
		A:        difflib.SplitLines(string(output1)),
		B:        difflib.SplitLines(string(output2)),
		FromFile: "original",
		ToFile:   "current",
		Context:  3,
		Eol:      "\n",
	}
	result, err := difflib.GetContextDiffString(diff)
	if err != nil {
		return false, err
	}
	if result == "" {
		return true, nil
	}
	fmt.Printf(strings.Replace(result, "\t", " ", -1))
	return false, nil
}
