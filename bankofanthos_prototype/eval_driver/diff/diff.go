package diff

import (
	"bankofanthos_prototype/eval_driver/pb"
	"bankofanthos_prototype/eval_driver/service"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"google.golang.org/protobuf/proto"
)

var (
	fromFile              = "control"
	toFile                = "experimental"
	nonDeterministicField = "nondeterministic/"
	responseType          = "response"
)

// checkLine checks each row to find non-deterministic column
func checkLine(control, experimental []string, idx int) ([]*pb.RowInfo, error) {
	var rowInfos []*pb.RowInfo
	if len(control) != len(experimental) {
		return rowInfos, nil
	}
	// two versions have different row numbers,no need to check each column
	for i := 0; i < idx; i++ {
		// get rid of the first char which is used for diff display
		controlCols := strings.Fields(control[i][1:])
		experimentalCols := strings.Fields(experimental[i][1:])
		if len(controlCols) != len(experimentalCols) {
			continue
		}

		rowInfo := &pb.RowInfo{DiffLineIdx: int64(i)}

		for j := 0; j < len(controlCols); j++ {
			if controlCols[j] != experimentalCols[j] {
				rowInfo.ColNumber = append(rowInfo.ColNumber, int64(j))
			}
		}
		rowInfos = append(rowInfos, rowInfo)
	}

	return rowInfos, nil
}

// getDiffHelper parses diff result in protobuf format
func getDiffHelper(result string) (*pb.DiffInfos, error) {
	diffInfos := &pb.DiffInfos{}
	diffLines := strings.Split(result, "\n")
	for i := 0; i < len(diffLines); i++ {
		line := diffLines[i]
		elem := strings.Fields(line)
		if strings.HasPrefix(line, "@@") && strings.HasSuffix(line, "@@") {
			if len(elem) != 4 {
				return diffInfos, fmt.Errorf("diff result is not expected, line number %d, line %s", i, line)
			}
			fromLineChanges := strings.Split(elem[1], ",")
			fromLineChangeCnt := 1
			fromLineChangeS, err := strconv.Atoi(fromLineChanges[0])
			if err != nil {
				return diffInfos, err
			}
			if len(fromLineChanges) == 2 {
				fromLineChangeCnt, err = strconv.Atoi(fromLineChanges[1])
				if err != nil {
					return diffInfos, err
				}
			}

			toLineChanges := strings.Split(elem[2], ",")
			toLineChangeCnt := 1
			toLineChangeS, err := strconv.Atoi(toLineChanges[0])
			if err != nil {
				return diffInfos, err
			}
			if len(toLineChanges) == 2 {
				toLineChangeCnt, err = strconv.Atoi(toLineChanges[1])
				if err != nil {
					return diffInfos, err
				}
			}
			diffInfo := &pb.DiffInfo{FromLineNumber: int64(fromLineChangeS), FromLineCnt: int64(fromLineChangeCnt), ToLineNumber: int64(toLineChangeS), ToLineCnt: int64(toLineChangeCnt)}

			if diffInfo.FromLineCnt == diffInfo.ToLineCnt {
				control := diffLines[i+1 : 1+i+fromLineChangeCnt]
				experimental := diffLines[1+i+fromLineChangeCnt : 1+i+fromLineChangeCnt*2]
				rowInfos, err := checkLine(control, experimental, int(diffInfo.FromLineCnt))
				if err != nil {
					return diffInfos, err
				}
				diffInfo.RowInfo = rowInfos
			}
			diffInfos.DiffInfo = append(diffInfos.DiffInfo, diffInfo)
			i = i + fromLineChangeCnt + toLineChangeCnt
		}
	}

	return diffInfos, nil
}

// getNonDeterministicInfo uses diff libary to get diff string and then convert string into diff protobuf format
func getNonDeterministicInfo(path1, path2 string, compareType string) error {
	output1, err := os.ReadFile(path1)
	if err != nil {
		return err
	}

	output2, err := os.ReadFile(path2)
	if err != nil {
		return err
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(output1)),
		B:        difflib.SplitLines(string(output2)),
		FromFile: fromFile,
		ToFile:   toFile,
		Context:  0,
		Eol:      "\n",
	}
	result, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return err
	}

	// parse the result, get the lines and columns for the non-deterministic field
	diffInfos, err := getDiffHelper(result)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(diffInfos)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return err
	}

	// store control diffs protobuf in a file
	file, err := os.Create(nonDeterministicField + compareType)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(string(data))
	if err != nil {
		return err
	}
	return nil
}

func GetNonDeterministic(controlService1, controlService2 *service.Service) error {
	// get response diff
	err := getNonDeterministicInfo(controlService1.OutputPath, controlService2.OutputPath, responseType)
	if err != nil {
		return err
	}

	return nil
}

func printDiff(diff string) string {
	var b strings.Builder
	diffLines := strings.Split(diff, "\n")
	for _, line := range diffLines {
		if strings.HasPrefix(line, "@@") || strings.HasPrefix(line, "---") || strings.HasPrefix(line, "+++") {
			b.WriteString(line)
			b.WriteString("\n")
			continue
		}
		b.WriteString(string(Bold))
		b.WriteString(line)
		b.WriteString("\n")
		b.WriteString(string("\x1b[0m"))
	}
	return b.String()
}

// outputEq compares two files content, print out the diff and return
// a equal bool.
func OutputEq(path1 string, path2 string, compareType string) (bool, error) {
	output1, err := os.ReadFile(path1)
	if err != nil {
		return false, err
	}

	output2, err := os.ReadFile(path2)
	if err != nil {
		return false, err
	}

	// hack the response
	output1Str := strings.ReplaceAll(string(output1), "400 Bad Request", "")
	output2Str := strings.ReplaceAll(string(output2), "400 Bad Request", "")

	controlDiffStr, err := os.ReadFile(nonDeterministicField + compareType)
	if err != nil {
		return false, err
	}

	controlDiff := &pb.DiffInfos{}
	err = proto.Unmarshal(controlDiffStr, controlDiff)
	if err != nil {
		return false, err
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(output1Str),
		B:        difflib.SplitLines(output2Str),
		FromFile: fromFile,
		ToFile:   toFile,
		Context:  0,
		Eol:      "\n",
	}
	result, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return false, err
	}
	if result == "" {
		return true, nil
	}

	experimentalDiff, err := getDiffHelper(result)
	if err != nil {
		return false, err
	}

	if proto.Equal(controlDiff, experimentalDiff) {
		return true, nil
	}

	result = strings.ReplaceAll(result, "-", "<")
	result = strings.ReplaceAll(result, "+", ">")

	fmt.Println(printDiff(result))
	return false, nil
}
