package main

import (
	"bankofanthos_prototype/eval_driver/pb"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gookit/color"
	"github.com/pmezard/go-difflib/difflib"
	"google.golang.org/protobuf/proto"
)

var (
	fromFile = "baseline"
	toFile   = "comparison"
)

// checkLine checks each row to find non-deterministic column
func checkLine(diffLines []string, idx int, diffInfo *pb.DiffInfo) error {
	// two versions have different row numbers,no need to check each column
	if diffInfo.FromLineCnt != diffInfo.ToLineCnt {
		return nil
	}
	baselineIdx := idx
	experimentalIdx := idx + int(diffInfo.FromLineCnt)
	for i := baselineIdx; i < experimentalIdx; i++ {
		// get rid of the first char which is used for diff display
		baselineCols := strings.Split(diffLines[i][1:], "\t")
		experimentalCols := strings.Split(diffLines[i+int(diffInfo.FromLineCnt)][1:], "\t")
		if len(baselineCols) != len(experimentalCols) {
			return fmt.Errorf("different columns number for baseline %v and experiemntal %v", baselineCols, experimentalCols)
		}

		rowInfo := &pb.RowInfo{DiffLineIdx: int64(i)}

		for j := 0; j < len(baselineCols); j++ {
			if baselineCols[j] != experimentalCols[j] {
				rowInfo.ColNumber = append(rowInfo.ColNumber, int64(j))
			}
		}
		diffInfo.RowInfo = append(diffInfo.RowInfo, rowInfo)
	}

	return nil
}

// getDiffHelper parses diff result in protobuf format
func getDiffHelper(result string, diffInfos *pb.DiffInfos, compareType string) error {
	diffLines := strings.Split(result, "\n")
	for i := 0; i < len(diffLines); i++ {
		line := diffLines[i]
		elem := strings.Fields(line)
		if strings.HasPrefix(line, "@@") && strings.HasSuffix(line, "@@") {
			if len(elem) != 4 {
				return fmt.Errorf("diff result is not expected, line number %d, line %s", i, line)
			}
			fromLineChanges := strings.Split(elem[1], ",")
			fromLineChangeCnt := 1
			fromLineChangeS, err := strconv.Atoi(fromLineChanges[0])
			if err != nil {
				return err
			}
			if len(fromLineChanges) == 2 {
				fromLineChangeCnt, err = strconv.Atoi(fromLineChanges[1])
				if err != nil {
					return err
				}
			}

			toLineChanges := strings.Split(elem[2], ",")
			toLineChangeCnt := 1
			toLineChangeS, err := strconv.Atoi(toLineChanges[0])
			if err != nil {
				return err
			}
			if len(toLineChanges) == 2 {
				toLineChangeCnt, err = strconv.Atoi(toLineChanges[1])
				if err != nil {
					return err
				}
			}
			diffInfo := &pb.DiffInfo{FromLineNumber: int64(fromLineChangeS), FromLineCnt: int64(fromLineChangeCnt), ToLineNumber: int64(toLineChangeS), ToLineCnt: int64(toLineChangeCnt)}
			diffInfos.DiffInfo = append(diffInfos.DiffInfo, diffInfo)

			err = checkLine(diffLines, i+1, diffInfo)
			if err != nil {
				return err
			}

			i = i + fromLineChangeCnt + toLineChangeCnt
		}
	}

	return nil
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
	if result == "" {
		return nil
	}

	// get column and line info for nondeterministic field
	diffInfos := &pb.DiffInfos{}

	// parse the result, get the lines and columns for the non-deterministic field
	err = getDiffHelper(result, diffInfos, compareType)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(diffInfos)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return nil
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

func getNonDeterministic(baselineService1, baselineService2 Service) error {
	// get database diff
	err := getNonDeterministicInfo(baselineService1.dumpDbPath, baselineService2.dumpDbPath, databaseType)
	if err != nil {
		return err
	}

	// get response diff
	err = getNonDeterministicInfo(baselineService1.outputPath, baselineService2.outputPath, responseType)
	if err != nil {
		return err
	}

	return nil
}

// outputEq compares two files content, print out the diff and return
// a equal bool.
func outputEq(path1 string, path2 string, compareType string) (bool, error) {
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

	experimentalDiff := &pb.DiffInfos{}
	err = getDiffHelper(result, experimentalDiff, compareType)
	if err != nil {
		return false, err
	}

	if proto.Equal(baselineDiff, experimentalDiff) {
		return true, nil
	}

	color.Yellowf(strings.Replace(result, "\t", " ", -1))
	return false, nil
}
