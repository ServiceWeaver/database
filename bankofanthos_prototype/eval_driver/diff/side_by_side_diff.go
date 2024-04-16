package diff

import (
	"bankofanthos_prototype/eval_driver/dbbranch"
	"fmt"
	"io"
	"strings"
)

// TODO: boxify side by side diffs
type sideBySideDiffFormatter struct {
	tableDiff *dbbranch.Diff
	tableName string

	widths       []int
	baseline     [][]atom
	control      [][]atom
	experimental [][]atom
	w            io.Writer
}

func newSideBySideDiffFormatter(w io.Writer, tableDiff *dbbranch.Diff, tableName string) *sideBySideDiffFormatter {
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
	writeRow("│", func(j, w int) string {
		s := strings.ToUpper(controlPrefix)
		if j != 0 {
			s = ""
		}
		return fmt.Sprintf(" %-*s ", w, s)
	})
	writeRow("│", func(j, w int) string {
		s := strings.ToUpper(baselinePrefix)
		if j != 0 {
			s = ""
		}
		return fmt.Sprintf(" %-*s ", w, s)
	})
	writeRow("\n", func(j, w int) string {
		s := strings.ToUpper(experimentalPrefix)
		if j != 0 {
			s = ""
		}
		return fmt.Sprintf(" %-*s ", w, s)
	})

	for i := 0; i < 3; i++ {
		end := "│"
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
		boldUnequalColumns(s.baseline[r], s.control[r], s.experimental[r])

		texts := [][]atom{s.control[r], s.baseline[r], s.experimental[r]}
		for i, text := range texts {
			end := "│"
			if i == 2 {
				end = "\n"
			}
			writeRow(end, func(j, w int) string {
				var a atom
				if len(text) > 0 {
					a = text[j]
				} else {
					a = atom{S: "-", Color: "", Bold: true}
				}
				s := a.String()
				return fmt.Sprintf(" %-*s ", w-len(a.S)+len(s), a)
			})
		}
	}
	return nil
}

func (s *sideBySideDiffFormatter) parseDiff() error {
	control, baseline, experimental, err := stringifyRows(s.tableDiff.Control, s.tableDiff.Baseline, s.tableDiff.Experimental)
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
