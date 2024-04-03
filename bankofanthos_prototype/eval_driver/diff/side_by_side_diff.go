package diff

import (
	"bankofanthos_prototype/eval_driver/dbclone"
	"fmt"
	"io"
	"strings"
)

// TODO: boxify side by side diffs
type sideBySideDiffFormatter struct {
	tableDiff *dbclone.Diff
	tableName string

	widths       []int
	baseline     [][]atom
	control      [][]atom
	experimental [][]atom
	w            io.Writer
}

func newSideBySideDiffFormatter(w io.Writer, tableDiff *dbclone.Diff, tableName string) *sideBySideDiffFormatter {
	return &sideBySideDiffFormatter{
		tableDiff: tableDiff,
		tableName: tableName,
		widths:    make([]int, len(tableDiff.ColNames)),
		w:         w,
	}
}

func (s *sideBySideDiffFormatter) parseRows(rows [][]string) [][]atom {
	var textRows [][]atom
	for r := 0; r < len(rows); r++ {
		var row []atom
		for c := 0; c < len(rows[r]); c++ {
			a := atom{S: rows[r][c], Color: Dim}
			s.widths[c] = max(len(a.S), s.widths[c])
			row = append(row, a)
		}
		textRows = append(textRows, row)
	}
	return textRows
}

func (s *sideBySideDiffFormatter) format() error {
	writeRow := func(end string, col func(j, width int) string) {
		for j, width := range s.widths {
			fmt.Fprint(s.w, col(j, width))
		}
		fmt.Fprint(s.w, end)
	}
	// table name
	fmt.Fprintln(s.w, strings.ToUpper(s.tableName))

	// col names
	for i := 0; i < 3; i++ {
		end := "|"
		if i == 2 {
			end = "\n"
		}
		writeRow(end, func(j, w int) string {
			colName := s.tableDiff.ColNames[j]
			return fmt.Sprintf(" %-*s ", w, strings.ToUpper(colName))
		})
	}

	// for each row
	for r := 0; r < len(s.baseline); r++ {
		err := boldUnequalColumns(s.baseline[r], s.control[r], s.experimental[r])
		if err != nil {
			return err
		}
		texts := [][]atom{s.control[r], s.baseline[r], s.experimental[r]}
		for i, text := range texts {
			end := "|"
			if i == 2 {
				end = "\n"
			}
			writeRow(end, func(j, w int) string {
				a := atom{}
				if len(text) > 0 {
					a = text[j]
				}
				s := a.String()
				return fmt.Sprintf(" %-*s ", w-len(a.S)+len(s), a)
			})
		}
	}
	return nil
}

func (s *sideBySideDiffFormatter) parseDiff() error {
	baseline, control, experimental, err := stringifyRows(s.tableDiff.Left, s.tableDiff.Middle, s.tableDiff.Right)
	if err != nil {
		return err
	}
	s.baseline = s.parseRows(baseline)
	s.control = s.parseRows(control)
	s.experimental = s.parseRows(experimental)
	s.parseRows([][]string{s.tableDiff.ColNames})

	return nil
}

func (s *sideBySideDiffFormatter) flush() error {
	err := s.parseDiff()
	if err != nil {
		return err
	}

	return s.format()
}
