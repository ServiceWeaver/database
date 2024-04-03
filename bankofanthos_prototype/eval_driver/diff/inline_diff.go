package diff

import (
	"bankofanthos_prototype/eval_driver/dbclone"
	"fmt"
	"io"
	"strings"
)

const (
	baselinePrefix     = "="
	controlPrefix      = "<"
	experimentalPrefix = ">"
)

type inlineFormatter struct {
	tableDiff *dbclone.Diff
	tableName string

	width        int
	widths       []int
	baseline     [][]atom
	control      [][]atom
	experimental [][]atom
	w            io.Writer
}

func newInlineFormatter(w io.Writer, tableDiff *dbclone.Diff, tableName string) *inlineFormatter {
	return &inlineFormatter{
		tableDiff: tableDiff,
		tableName: tableName,
		width:     0,
		widths:    make([]int, len(tableDiff.ColNames)+1), // first length will be 3way inline prefix, table columns are followed
		w:         w,
	}
}

func (i *inlineFormatter) calculateWidths() {
	i.widths[0] = 1
	colNums := len(i.tableDiff.ColNames)
	for w := 0; w <= colNums; w++ {
		i.width += i.widths[w] + 3 // space + | + space
	}
	i.width += 1
	i.width = max(i.width, len(i.tableName))
}

func (i *inlineFormatter) format() error {
	// writeRow writes a row with (l)eft, (m)iddle, and (r)ight separators. The
	// content between the separators is provided by the col function.
	writeRow := func(l, m, r string, col func(j, width int) string) {
		fmt.Fprint(i.w, l)
		for j, width := range i.widths {
			fmt.Fprint(i.w, col(j, width))
			if j != len(i.widths)-1 {
				fmt.Fprint(i.w, m)
			}

		}
		fmt.Fprintln(i.w, r)
	}

	writeRow("╭", "─", "╮", func(j, w int) string {
		return strings.Repeat("─", w+2)
	})

	// table name
	fmt.Fprintf(i.w, "│ %-*s │\n", i.width-4, strings.ToUpper(i.tableName))

	writeRow("├", "┬", "┤", func(j, w int) string {
		return strings.Repeat("─", w+2)
	})

	// col names
	writeRow("│", "│", "│", func(j, w int) string {
		if j > 0 {
			colName := i.tableDiff.ColNames[j-1]
			return fmt.Sprintf(" %-*s ", w, strings.ToUpper(colName))
		}
		return fmt.Sprintf(" %-*s ", w, " ")
	})
	if len(i.baseline) == 0 {
		writeRow("╰", "┴", "╯", func(j, w int) string {
			return strings.Repeat("─", w+2)
		})
		return nil
	}
	writeRow("├", "┼", "┤", func(j, w int) string {
		return strings.Repeat("─", w+2)
	})

	// for each row
	prefix := []string{baselinePrefix, controlPrefix, experimentalPrefix}
	for r := 0; r < len(i.baseline); r++ {
		err := boldUnequalColumns(i.baseline[r], i.control[r], i.experimental[r])
		if err != nil {
			return err
		}

		texts := [][]atom{i.baseline[r], i.control[r], i.experimental[r]}
		for p, text := range texts {
			writeRow("│", "│", "│", func(j, w int) string {
				a := atom{}
				if j == 0 {
					a.S = prefix[p]
				} else if len(text) > 0 {
					a = text[j-1]
				}
				s := a.String()
				return fmt.Sprintf(" %-*s ", w-len(a.S)+len(s), a)
			})
		}

		if r == len(i.baseline)-1 {
			writeRow("╰", "┴", "╯", func(j, w int) string {
				return strings.Repeat("─", w+2)
			})
		} else {
			writeRow("├", "┼", "┤", func(j, w int) string {
				return strings.Repeat("─", w+2)
			})
		}
	}

	return nil
}

func (i *inlineFormatter) parseRows(rows [][]string) [][]atom {
	var textRows [][]atom
	for r := 0; r < len(rows); r++ {
		var row []atom
		for c := 0; c < len(rows[r]); c++ {
			a := atom{S: rows[r][c], Color: Dim}
			i.widths[c+1] = max(len(a.S), i.widths[c+1])
			row = append(row, a)
		}
		textRows = append(textRows, row)
	}
	return textRows
}

func (i *inlineFormatter) parseDiff() error {
	baseline, control, experimental, err := stringifyRows(i.tableDiff.Left, i.tableDiff.Middle, i.tableDiff.Right)
	if err != nil {
		return err
	}

	i.baseline = i.parseRows(baseline)
	i.control = i.parseRows(control)
	i.experimental = i.parseRows(experimental)
	i.parseRows([][]string{i.tableDiff.ColNames})

	i.calculateWidths()
	return nil
}

func (i *inlineFormatter) flush() error {
	err := i.parseDiff()
	if err != nil {
		return err
	}

	return i.format()
}
