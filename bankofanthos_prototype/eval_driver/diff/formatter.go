package diff

import (
	"bankofanthos_prototype/eval_driver/dbbranch"
	"fmt"
	"strings"
)

type Code string

const (
	Reset Code = "\x1b[0m"        // The ANSI escape code that resets formatting.
	Bold  Code = "\x1b[1m"        // The ANSI escape code for bold text.
	Dim   Code = "\x1b[38;5;245m" // light grey
)

type atom struct {
	S     string
	Bold  bool
	Color Code
}

func (a atom) String() string {
	var b strings.Builder
	b.WriteString(string(a.Color))
	if a.Bold {
		b.WriteString(string(Bold))
	}
	b.WriteString(a.S)
	b.WriteString(string(Reset))
	return b.String()
}

func boldUnequalColumns(baseline, control, experimental []atom) {
	var rows [][]atom
	for _, row := range [][]atom{baseline, control, experimental} {
		if len(row) > 0 {
			rows = append(rows, row)
		}
	}

	for col := range rows[0] {
		allEqual := true
		for _, row := range rows {
			if row[col] != rows[0][col] {
				allEqual = false
				break
			}
		}
		if !allEqual {
			for _, row := range rows {
				row[col].Bold = true
				row[col].Color = Reset
			}
		}
	}
}

func stringifyRow(row *dbbranch.Row) ([]string, error) {
	allNil := true
	for _, val := range *row {
		if val != nil {
			allNil = false
			break
		}
	}

	if allNil {
		return nil, nil
	}

	var rowSlice []string
	for _, col := range *row {
		rowSlice = append(rowSlice, fmt.Sprintf("%v", col))
	}

	return rowSlice, nil
}

func stringifyRows(left []*dbbranch.Row, middle []*dbbranch.Row, right []*dbbranch.Row) ([][]string, [][]string, [][]string, error) {
	if len(left) != len(right) || len(left) != len(middle) {
		return nil, nil, nil, fmt.Errorf("different length for 3 way diffs, left %d, right: %d, middle: %d", len(left), len(right), len(middle))
	}
	var baseline, control, experimental [][]string
	for c := 0; c < len(left); c++ {
		leftVal, err := stringifyRow(left[c])
		if err != nil {
			return nil, nil, nil, err
		}
		control = append(control, leftVal)

		middleVal, err := stringifyRow(middle[c])
		if err != nil {
			return nil, nil, nil, err
		}
		baseline = append(baseline, middleVal)

		rightVal, err := stringifyRow(right[c])
		if err != nil {
			return nil, nil, nil, err
		}
		experimental = append(experimental, rightVal)
	}

	return control, baseline, experimental, nil
}

func DisplayDiff(branchDiffs map[string]*dbbranch.Diff, displayInlineDiff bool) (string, error) {
	var b strings.Builder
	for tableName, tableDiff := range branchDiffs {
		if len(tableDiff.Control) == 0 && len(tableDiff.Experimental) == 0 && len(tableDiff.Baseline) == 0 {
			continue
		}
		if displayInlineDiff {
			formatter := newInlineFormatter(&b, tableDiff, tableName)
			err := formatter.flush()
			if err != nil {
				return "", err
			}
		} else {
			formatter := newSideBySideDiffFormatter(&b, tableDiff, tableName)
			err := formatter.flush()
			if err != nil {
				return "", err
			}
		}
	}

	return b.String(), nil
}
