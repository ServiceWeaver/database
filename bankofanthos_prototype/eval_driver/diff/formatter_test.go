package diff

import (
	"bankofanthos_prototype/eval_driver/dbclone"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func removeColorCodes(str string) string {
	ansi := regexp.MustCompile("\033\\[(?:[0-9;]*m)")
	return ansi.ReplaceAllString(str, "")
}

// Generate random string, length between 1 and 20
func randomGenerate(seed int64) string {
	r := rand.New(rand.NewSource(seed))
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	strLength := r.Intn(20) + 1

	var builder strings.Builder
	for i := 0; i < strLength; i++ {
		builder.WriteByte(chars[r.Intn(len(chars))])
	}

	return builder.String()
}

var tableDiff = &dbclone.Diff{
	Left: []*dbclone.Row{
		{[]any{int32(0), randomGenerate(0), "A"}},
		{[]any{int32(1), randomGenerate(1), "B"}},
		{[]any{nil, nil, nil}},
		{[]any{nil, nil, nil}},
		{[]any{int32(4), randomGenerate(4), "E"}},
		{[]any{int32(5), randomGenerate(5), "FFFF"}},
	},
	Middle: []*dbclone.Row{
		{[]any{int32(0), randomGenerate(0), "A"}},
		{[]any{int32(1), randomGenerate(1), "BB"}},
		{[]any{int32(2), randomGenerate(2), "C"}},
		{[]any{int32(3), randomGenerate(3), "D"}},
		{[]any{nil, nil, nil}},
		{[]any{int32(5), randomGenerate(5), "F"}},
	},
	Right: []*dbclone.Row{
		{[]any{nil, nil, nil}},
		{[]any{int32(1), randomGenerate(1), "B"}},
		{[]any{int32(2), randomGenerate(2), "C"}},
		{[]any{nil, nil, nil}},
		{[]any{int32(4), randomGenerate(4), "E"}},
		{[]any{nil, nil, nil}},
	},
	ColNames: []string{"id", "password", "name"},
}

func TestInlineDiffFormat(t *testing.T) {
	output, err := DisplayDiff(map[string]*dbclone.Diff{"user": tableDiff}, true)
	if err != nil {
		t.Fatal(err)
	}

	plainText := removeColorCodes(output)
	expectedString := `
╭───-────-───────────────────-────────╮
│ USER                                │
├───┬────┬───────────────────┬────────┤
│   │ ID │ PASSWORD          │ NAME   │
├───┼────┼───────────────────┼────────┤
│ = │ 0  │ "UNERA9rI2cvTK4U" │ "A"    │
│ < │ 0  │ "UNERA9rI2cvTK4U" │ "A"    │
│ > │    │                   │        │
├───┼────┼───────────────────┼────────┤
│ = │ 1  │ "pL"              │ "BB"   │
│ < │ 1  │ "pL"              │ "B"    │
│ > │ 1  │ "pL"              │ "B"    │
├───┼────┼───────────────────┼────────┤
│ = │ 2  │ "SiOW4eQ"         │ "C"    │
│ < │    │                   │        │
│ > │ 2  │ "SiOW4eQ"         │ "C"    │
├───┼────┼───────────────────┼────────┤
│ = │ 3  │ "jKsRdMxCv"       │ "D"    │
│ < │    │                   │        │
│ > │    │                   │        │
├───┼────┼───────────────────┼────────┤
│ = │    │                   │        │
│ < │ 4  │ "gltBHYVJQV"      │ "E"    │
│ > │ 4  │ "gltBHYVJQV"      │ "E"    │
├───┼────┼───────────────────┼────────┤
│ = │ 5  │ "gvMTIQB"         │ "F"    │
│ < │ 5  │ "gvMTIQB"         │ "FFFF" │
│ > │    │                   │        │
╰───┴────┴───────────────────┴────────╯
`
	if diff := cmp.Diff(expectedString[1:], plainText); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
	fmt.Println(output)
}

func TestSideBySideDiffFormat(t *testing.T) {
	output, err := DisplayDiff(map[string]*dbclone.Diff{"user": tableDiff}, false)
	if err != nil {
		t.Fatal(err)
	}

	expectedString := `
USER
 ID  PASSWORD           NAME   | ID  PASSWORD           NAME   | ID  PASSWORD           NAME   
 0   "UNERA9rI2cvTK4U"  "A"    | 0   "UNERA9rI2cvTK4U"  "A"    |                               
 1   "pL"               "B"    | 1   "pL"               "BB"   | 1   "pL"               "B"    
                               | 2   "SiOW4eQ"          "C"    | 2   "SiOW4eQ"          "C"    
                               | 3   "jKsRdMxCv"        "D"    |                               
 4   "gltBHYVJQV"       "E"    |                               | 4   "gltBHYVJQV"       "E"    
 5   "gvMTIQB"          "FFFF" | 5   "gvMTIQB"          "F"    |                               
`

	plainText := removeColorCodes(output)
	if diff := cmp.Diff(expectedString[1:], plainText); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
	fmt.Println(output)
}

func TestInlineDiffColor(t *testing.T) {
	var b strings.Builder
	formatter := newInlineFormatter(&b, tableDiff, "user")
	formatter.flush()

	expectedColorsControl := []Code{"", Blue, Red, Red, Green, Blue}
	var controlColors []Code
	for _, c := range formatter.control {
		controlColors = append(controlColors, Code(c.Color))
	}
	if diff := cmp.Diff(expectedColorsControl, controlColors); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}

	expectedColorsExperimental := []Code{Red, Blue, "", Red, Green, Red}
	var experimentalColors []Code
	for _, c := range formatter.experimental {
		experimentalColors = append(experimentalColors, Code(c.Color))
	}
	if diff := cmp.Diff(expectedColorsExperimental, experimentalColors); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

func TestSideBySideDiffColor(t *testing.T) {
	var b strings.Builder
	formatter := newSideBySideDiffFormatter(&b, tableDiff, "user")
	formatter.flush()

	expectedColorsControl := []Code{"", Blue, Red, Red, Green, Blue}
	var controlColors []Code
	for _, c := range formatter.control {
		controlColors = append(controlColors, Code(c.Color))
	}
	if diff := cmp.Diff(expectedColorsControl, controlColors); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}

	expectedColorsExperimental := []Code{Red, Blue, "", Red, Green, Red}
	var experimentalColors []Code
	for _, c := range formatter.experimental {
		experimentalColors = append(experimentalColors, Code(c.Color))
	}
	if diff := cmp.Diff(expectedColorsExperimental, experimentalColors); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}
