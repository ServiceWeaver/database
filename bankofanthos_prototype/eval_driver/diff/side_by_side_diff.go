package diff

import (
	"bankofanthos_prototype/eval_driver/dbclone"
	"fmt"
	"io"
	"strings"
)

type sideBySideDiffFormatter struct {
	tableDiff *dbclone.Diff
	tableName string

	width        int
	widths       []int
	baseline     []text
	control      []text
	experimental []text
	w            io.Writer
}

func newSideBySideDiffFormatter(w io.Writer, tableDiff *dbclone.Diff, tableName string) *sideBySideDiffFormatter {
	return &sideBySideDiffFormatter{
		tableDiff: tableDiff,
		tableName: tableName,
		width:     0,
		widths:    make([]int, len(tableDiff.ColNames)),
		w:         w,
	}
}

func (s *sideBySideDiffFormatter) parseCol(oneWay [][]string) []text {
	var rows []text
	for r := 0; r < len(oneWay); r++ {
		var row []atom
		for c := 0; c < len(oneWay[r]); c++ {
			a := atom{S: oneWay[r][c], Color: Dim}
			s.widths[c] = max(len(a.S), s.widths[c])
			row = append(row, a)
		}
		rows = append(rows, text{Row: row})
	}
	return rows
}

func (s *sideBySideDiffFormatter) calculateWidths() {
	colNums := len(s.tableDiff.ColNames)
	s.width = 0
	for w := 0; w < colNums; w++ {
		s.width += s.widths[w] + 2
	}
	s.width = max(s.width, len(s.tableName))
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
		err := colorRow(&s.baseline[r], &s.control[r], &s.experimental[r])
		if err != nil {
			return err
		}
		texts := []text{s.control[r], s.baseline[r], s.experimental[r]}
		for i, text := range texts {
			end := "|"
			if i == 2 {
				end = "\n"
			}
			writeRow(end, func(j, w int) string {
				a := atom{}
				if len(text.Row) > 0 {
					a = text.Row[j]
				}
				return fmt.Sprintf(" %-*s ", w-len(a.S)+a.len(), a)
			})
		}
	}
	return nil
}

func (s *sideBySideDiffFormatter) parseDiff() error {
	baseline, control, experimental, err := getRowVals(s.tableDiff.Left, s.tableDiff.Middle, s.tableDiff.Right)
	if err != nil {
		return err
	}
	s.baseline = s.parseCol(baseline)
	s.control = s.parseCol(control)
	s.experimental = s.parseCol(experimental)
	s.parseCol([][]string{s.tableDiff.ColNames})

	s.calculateWidths()
	return nil
}

func (s *sideBySideDiffFormatter) flush() error {
	err := s.parseDiff()
	if err != nil {
		return err
	}

	return s.format()
}
