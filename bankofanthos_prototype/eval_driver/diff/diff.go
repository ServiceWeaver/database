package diff

import (
	"bankofanthos_prototype/eval_driver/pb"
	"bankofanthos_prototype/eval_driver/service"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gookit/color"
	"github.com/pmezard/go-difflib/difflib"
	"google.golang.org/protobuf/proto"
)

var (
	fromFile              = "baseline"
	toFile                = "comparison"
	nonDeterministicField = "nondeterministic/"
	databaseType          = "database"
	responseType          = "response"
)

// checkLine checks each row to find non-deterministic column
func checkLine(baseline, experimental []string, idx int) ([]*pb.RowInfo, error) {
	var rowInfos []*pb.RowInfo
	if len(baseline) != len(experimental) {
		return rowInfos, nil
	}
	// two versions have different row numbers,no need to check each column
	for i := 0; i < idx; i++ {
		// get rid of the first char which is used for diff display
		baselineCols := strings.Fields(baseline[i][1:])
		experimentalCols := strings.Fields(experimental[i][1:])
		if len(baselineCols) != len(experimentalCols) {
			fmt.Println("Warning, failed to compare two columns with different fields number")
			continue
		}

		rowInfo := &pb.RowInfo{DiffLineIdx: int64(i)}

		for j := 0; j < len(baselineCols); j++ {
			if baselineCols[j] != experimentalCols[j] {
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
				baseline := diffLines[i+1 : 1+i+fromLineChangeCnt]
				experimental := diffLines[1+i+fromLineChangeCnt : 1+i+fromLineChangeCnt*2]
				rowInfos, err := checkLine(baseline, experimental, int(diffInfo.FromLineCnt))
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

	// store baseline diffs protobuf in a file
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

func GetNonDeterministic(baselineService1, baselineService2 service.Service) error {
	// get database diff
	err := getNonDeterministicInfo(baselineService1.DumpDbPath, baselineService2.DumpDbPath, databaseType)
	if err != nil {
		return err
	}

	// get response diff
	err = getNonDeterministicInfo(baselineService1.OutputPath, baselineService2.OutputPath, responseType)
	if err != nil {
		return err
	}

	return nil
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

	baselineDiffStr, err := os.ReadFile(nonDeterministicField + compareType)
	if err != nil {
		return false, err
	}

	baselineDiff := &pb.DiffInfos{}
	err = proto.Unmarshal(baselineDiffStr, baselineDiff)
	if err != nil {
		return false, err
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
		return false, err
	}
	if result == "" {
		return true, nil
	}

	experimentalDiff, err := getDiffHelper(result)
	if err != nil {
		return false, err
	}

	if proto.Equal(baselineDiff, experimentalDiff) {
		return true, nil
	}

	color.Yellowf(strings.Replace(result, "\t", " ", -1))
	return false, nil
}
