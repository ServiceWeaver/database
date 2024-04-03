package diff

import (
	"bankofanthos_prototype/eval_driver/dbclone"
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

func boldUnequalColumns(baseline []atom, control []atom, experimental []atom) error {
	boldCol := func(a1 *atom, a2 *atom) {
		a1.Bold = true
		a1.Color = Reset

		a2.Bold = true
		a2.Color = Reset
	}

	var colorBoldRows [][]atom
	if len(baseline) != 0 {
		colorBoldRows = append(colorBoldRows, baseline)
	}
	if len(control) != 0 {
		colorBoldRows = append(colorBoldRows, control)
	}
	if len(experimental) != 0 {
		colorBoldRows = append(colorBoldRows, experimental)
	}

	if len(colorBoldRows) < 2 {
		return nil
	}

	for m := 0; m < len(colorBoldRows[0]); m++ {
		if colorBoldRows[0][m] != colorBoldRows[1][m] {
			boldCol(&colorBoldRows[0][m], &colorBoldRows[1][m])
		}
		if len(colorBoldRows) > 2 {
			if colorBoldRows[0][m] != colorBoldRows[2][m] {
				boldCol(&colorBoldRows[0][m], &colorBoldRows[2][m])
			}
			if colorBoldRows[1][m] != colorBoldRows[2][m] {
				boldCol(&colorBoldRows[1][m], &colorBoldRows[2][m])
			}
		}
	}

	return nil
}

func stringifyRow(row *dbclone.Row) ([]string, error) {
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

func stringifyRows(left []*dbclone.Row, middle []*dbclone.Row, right []*dbclone.Row) ([][]string, [][]string, [][]string, error) {
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

	return baseline, control, experimental, nil
}

func DisplayDiff(branchDiffs map[string]*dbclone.Diff, displayInlineDiff bool) (string, error) {
	var b strings.Builder
	for tableName, tableDiff := range branchDiffs {
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
