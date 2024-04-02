package diff

import (
	"bankofanthos_prototype/eval_driver/dbclone"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type Code string

const (
	Reset Code = "\x1b[0m" // The ANSI escape code that resets formatting.
	Bold  Code = "\x1b[1m" // The ANSI escape code for bold text.

	Red   Code = "\x1b[31m"
	Green Code = "\x1b[32m"
	Blue  Code = "\x1b[34m"
)

type atom struct {
	S     string
	Bold  bool
	Color Code
}

type text struct {
	Color  string
	Row    []atom
	Prefix string
}

type formatter interface {
	parseCol(oneWay [][]string) []text
	calculateWidths()
	format() error
	parseDiff() error
	flush() error
}

func (s1 atom) Equal(s2 atom) bool {
	return s1.S == s2.S
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

func (a atom) len() int {
	var b strings.Builder
	b.WriteString(a.String())
	return len(b.String())
}

func colorBold(rows ...[]atom) error {
	if len(rows) < 2 {
		return nil
	}

	baseLength := len(rows[0])
	for _, s := range rows {
		if len(s) != baseLength {
			return fmt.Errorf("rows have different length, baselength: %d, compared rows: %d", baseLength, len(s))
		}
	}

	for m := 0; m < baseLength; m++ {
		baseValue := rows[0][m]
		for _, s := range rows[1:] {
			if !s[m].Equal(baseValue) {
				s[m].Bold = true
				rows[0][m].Bold = true
			}
		}
	}

	return nil
}

func colorRow(baseline *text, control *text, experimental *text) error {
	var colorBoldRows [][]atom
	if len(baseline.Row) != 0 {
		colorBoldRows = append(colorBoldRows, baseline.Row)
	}
	if len(control.Row) != 0 {
		colorBoldRows = append(colorBoldRows, control.Row)
	}
	if len(experimental.Row) != 0 {
		colorBoldRows = append(colorBoldRows, experimental.Row)
	}

	// if baseline is empty
	if len(baseline.Row) == 0 && len(control.Row) > 0 {
		control.Color = string(Green)
	}
	if len(baseline.Row) == 0 && len(experimental.Row) > 0 {
		experimental.Color = string(Green)
	}
	// if baseline has values while compared row is deleted
	if len(baseline.Row) > 0 && len(control.Row) == 0 {
		control.Color = string(Red)
	}
	if len(baseline.Row) > 0 && len(experimental.Row) == 0 {
		experimental.Color = string(Red)
	}
	// if value is updated
	if len(baseline.Row) > 0 && len(control.Row) > 0 && !slices.EqualFunc(baseline.Row, control.Row, func(s1, s2 atom) bool {
		return s1.S == s2.S
	}) {
		control.Color = string(Blue)
	}
	if len(baseline.Row) > 0 && len(experimental.Row) > 0 && !slices.EqualFunc(baseline.Row, experimental.Row, func(s1, s2 atom) bool {
		return s1.S == s2.S
	}) {
		experimental.Color = string(Blue)
	}

	baseline.Prefix = baselinePrefix
	control.Prefix = controlPrefix
	experimental.Prefix = experimentalPrefix
	return colorBold(colorBoldRows...)
}

func isAllNil(row *dbclone.Row) (bool, error) {
	for _, innerElement := range *row {
		innerSlice, ok := innerElement.([]any)
		if !ok {
			return false, fmt.Errorf("unexpected type within the outer slice")
		}

		for _, value := range innerSlice {
			if value != nil {
				return false, nil
			}
		}
	}
	return true, nil
}

func getRowVal(row *dbclone.Row) ([]string, error) {
	rowNil, err := isAllNil(row)
	if err != nil {
		return nil, err
	}
	if rowNil {
		return nil, nil
	}
	jsonString, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	rowSlice := strings.Split(string(jsonString)[2:len(jsonString)-2], ",")

	return rowSlice, nil
}

func getRowVals(left []*dbclone.Row, middle []*dbclone.Row, right []*dbclone.Row) ([][]string, [][]string, [][]string, error) {
	if len(left) != len(right) || len(left) != len(middle) {
		return nil, nil, nil, fmt.Errorf("different length for 3 way diffs, left %d, right: %d, middle: %d", len(left), len(right), len(middle))
	}
	var baseline, control, experimental [][]string
	for c := 0; c < len(left); c++ {
		leftVal, err := getRowVal(left[c])
		if err != nil {
			return nil, nil, nil, err
		}
		control = append(control, leftVal)

		middleVal, err := getRowVal(middle[c])
		if err != nil {
			return nil, nil, nil, err
		}
		baseline = append(baseline, middleVal)

		rightVal, err := getRowVal(right[c])
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
		var formatter formatter
		if displayInlineDiff {
			formatter = newInlineFormatter(&b, tableDiff, tableName)
		} else {
			formatter = newSideBySideDiffFormatter(&b, tableDiff, tableName)
		}

		err := formatter.flush()
		if err != nil {
			return "", err
		}
	}

	return b.String(), nil
}
