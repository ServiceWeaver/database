package diff

import (
	"bankofanthos_prototype/eval_driver/dbbranch"
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

var tableDiff = &dbbranch.Diff{
	Control: []*dbbranch.Row{
		{int32(0), randomGenerate(0), "A"},
		{int32(1), randomGenerate(1), "BB"},
		{nil, nil, nil},
		{nil, nil, nil},
		{int32(4), randomGenerate(4), "E"},
		{int32(5), randomGenerate(5), "FFFF"},
	},
	Baseline: []*dbbranch.Row{
		{int32(0), randomGenerate(0), "A"},
		{int32(1), randomGenerate(1), "BB"},
		{int32(2), randomGenerate(2), "C"},
		{int32(3), randomGenerate(3), "D"},
		{nil, nil, nil},
		{int32(5), randomGenerate(5), "F"},
	},
	Experimental: []*dbbranch.Row{
		{nil, nil, nil},
		{int32(1), randomGenerate(1), "B"},
		{int32(2), randomGenerate(2), "C"},
		{nil, nil, nil},
		{int32(4), randomGenerate(8), "E"},
		{nil, nil, nil},
	},
	ColNames: []string{"id", "password", "name"},
}

func TestInlineDiffFormat(t *testing.T) {
	output, err := DisplayDiff(map[string]*dbbranch.Diff{"user": tableDiff}, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	plainText := removeColorCodes(output)
	expectedString := `
╭─────────────────────────────────╮
│ USER                            │
├───┬────┬─────────────────┬──────┤
│   │ ID │ PASSWORD        │ NAME │
├───┼────┼─────────────────┼──────┤
│ = │ 0  │ UNERA9rI2cvTK4U │ A    │
│ < │ 0  │ UNERA9rI2cvTK4U │ A    │
│ > │ -  │ -               │ -    │
├───┼────┼─────────────────┼──────┤
│ = │ 1  │ pL              │ BB   │
│ < │ 1  │ pL              │ BB   │
│ > │ 1  │ pL              │ B    │
├───┼────┼─────────────────┼──────┤
│ = │ 2  │ SiOW4eQ         │ C    │
│ < │ -  │ -               │ -    │
│ > │ 2  │ SiOW4eQ         │ C    │
├───┼────┼─────────────────┼──────┤
│ = │ 3  │ jKsRdMxCv       │ D    │
│ < │ -  │ -               │ -    │
│ > │ -  │ -               │ -    │
├───┼────┼─────────────────┼──────┤
│ = │ -  │ -               │ -    │
│ < │ 4  │ gltBHYVJQV      │ E    │
│ > │ 4  │ orCMYJxL8       │ E    │
├───┼────┼─────────────────┼──────┤
│ = │ 5  │ gvMTIQB         │ F    │
│ < │ 5  │ gvMTIQB         │ FFFF │
│ > │ -  │ -               │ -    │
╰───┴────┴─────────────────┴──────╯
`
	if diff := cmp.Diff(expectedString[1:], plainText); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
	fmt.Println(output)
}

func TestSideBySideDiffFormat(t *testing.T) {
	output, err := DisplayDiff(map[string]*dbbranch.Diff{"user": tableDiff}, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	expectedString := `
USER
 <                         │ =                         │ >                         
 ID  PASSWORD         NAME │ ID  PASSWORD         NAME │ ID  PASSWORD         NAME 
 0   UNERA9rI2cvTK4U  A    │ 0   UNERA9rI2cvTK4U  A    │ -   -                -    
 1   pL               BB   │ 1   pL               BB   │ 1   pL               B    
 -   -                -    │ 2   SiOW4eQ          C    │ 2   SiOW4eQ          C    
 -   -                -    │ 3   jKsRdMxCv        D    │ -   -                -    
 4   gltBHYVJQV       E    │ -   -                -    │ 4   orCMYJxL8        E    
 5   gvMTIQB          FFFF │ 5   gvMTIQB          F    │ -   -                -    
`

	plainText := removeColorCodes(output)
	if diff := cmp.Diff(expectedString[1:], plainText); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
	fmt.Println(output)
}
