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

// dbCheckLine checks each row to find non-deterministic column
func dbCheckLine(diffLines []string, idx int, dbInfo *pb.DbInfo) error {
	// two versions have different row numbers,no need to check each column
	if dbInfo.FromLineCnt != dbInfo.ToLineCnt {
		return nil
	}
	baselineIdx := idx
	experimentalIdx := idx + int(dbInfo.FromLineCnt)
	for i := baselineIdx; i < experimentalIdx; i++ {
		// get rid of the first char which is used for diff display
		baselineCols := strings.Split(diffLines[i][1:], "\t")
		experimentalCols := strings.Split(diffLines[i+int(dbInfo.FromLineCnt)][1:], "\t")
		if len(baselineCols) != len(experimentalCols) {
			return fmt.Errorf("different columns number for baseline %v and experiemntal %v", baselineCols, experimentalCols)
		}

		rowInfo := &pb.RowInfo{DiffLineIdx: int64(i)}

		for j := 0; j < len(baselineCols); j++ {
			if baselineCols[j] != experimentalCols[j] {
				rowInfo.ColNumber = append(rowInfo.ColNumber, int64(j))
			}
		}
		dbInfo.RowInfo = append(dbInfo.RowInfo, rowInfo)
	}

	return nil
}

func getDbDiffHelper(result string, dbDiff *pb.DiffDbTable) error {
	dbInfo := &pb.DbInfo{}
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
			dbInfo = &pb.DbInfo{FromLineNumber: int64(fromLineChangeS), FromLineCnt: int64(fromLineChangeCnt), ToLineNumber: int64(toLineChangeS), ToLineCnt: int64(toLineChangeCnt)}
			dbDiff.DiffDbInfo = append(dbDiff.DiffDbInfo, dbInfo)
			err = dbCheckLine(diffLines, i+1, dbInfo)
			if err != nil {
				return err
			}
			i = i + fromLineChangeCnt + toLineChangeCnt
		}
	}

	return nil
}

func getNonDeterministicDbInfo(dumpPath1, dumpPath2 string) error {
	output1, err := os.ReadFile(dumpPath1)
	if err != nil {
		return err
	}

	output2, err := os.ReadFile(dumpPath2)
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
		color.Greenf("Output %s and %s are equal.\n", dumpPath1, dumpPath2)
		return nil
	}

	// get column and line for nondeterministic field
	db := &pb.DiffDbTable{}

	// parse the result, get the lines and columns for the non-deterministic field
	err = getDbDiffHelper(result, db)
	if err != nil {
		return err
	}

	// Marshal the message into binary format
	data, err := proto.Marshal(db)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return nil
	}

	// For DB diff, check each column to find the non-deterministic field
	file, err := os.Create(nonDeterministicField + "database")
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

func getRespDiffHelper(result string, respDiff *pb.DiffResp) error {
	for i, line := range strings.Split(result, "\n") {
		elem := strings.Fields(line)
		if strings.HasPrefix(line, "@@") && strings.HasSuffix(line, "@@") {
			if len(elem) != 4 {
				return fmt.Errorf("diff result is not expected, line number %d, line %s, len should be 4, len %d, elem %+v", i, line, len(elem), elem)
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
			respInfo := &pb.RespInfo{FromLineNumber: int64(fromLineChangeS), FromLineCnt: int64(fromLineChangeCnt), ToLineNumber: int64(toLineChangeS), ToLineCnt: int64(toLineChangeCnt)}
			respDiff.DiffRespInfo = append(respDiff.DiffRespInfo, respInfo)
		}
	}
	return nil
}

func getNonDeterministicRespInfo(respPath1, respPath2 string) error {
	output1, err := os.ReadFile(respPath1)
	if err != nil {
		return err
	}

	output2, err := os.ReadFile(respPath2)
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
		color.Greenf("Output %s and %s are equal.\n", respPath1, respPath2)
		return nil
	}

	respDiff := &pb.DiffResp{}
	err = getRespDiffHelper(result, respDiff)
	if err != nil {
		return err
	}
	// Marshal the message into binary format
	data, err := proto.Marshal(respDiff)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return nil
	}

	// For DB diff, check each column to find the non-deterministic field
	file, err := os.Create(nonDeterministicField + "resp")
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
	err := getNonDeterministicDbInfo(baselineService1.dumpDbPath, baselineService2.dumpDbPath)
	if err != nil {
		return err
	}
	// For response, check the whole line for now
	err = getNonDeterministicRespInfo(baselineService1.outputPath, baselineService2.outputPath)
	if err != nil {
		return err
	}

	return nil
}

// outputEq compares two files content, print out the diff and return
// a equal bool.
func outputDbEq(path1 string, path2 string) (bool, string, error) {
	output1, err := os.ReadFile(path1)
	if err != nil {
		return false, "", err
	}

	output2, err := os.ReadFile(path2)
	if err != nil {
		return false, "", err
	}

	baselineDiffStr, err := os.ReadFile(nonDeterministicField + "database")
	if err != nil {
		return false, "", err
	}

	// Marshal the message into binary format
	baselineDiff := &pb.DiffDbTable{}
	err = proto.Unmarshal(baselineDiffStr, baselineDiff)
	if err != nil {
		return false, "", err
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
		return false, "", err
	}
	if result == "" {
		return true, "", nil
	}

	experimentalDiff := &pb.DiffDbTable{}
	err = getDbDiffHelper(result, experimentalDiff)
	if err != nil {
		return false, "", err
	}

	if proto.Equal(baselineDiff, experimentalDiff) {
		return true, "", nil
	}

	result = strings.Replace(result, "\t", " ", -1)
	color.Yellowf(result)
	return false, result, nil
}

func outputRespEq(path1 string, path2 string) (bool, string, error) {
	output1, err := os.ReadFile(path1)
	if err != nil {
		return false, "", err
	}

	output2, err := os.ReadFile(path2)
	if err != nil {
		return false, "", err
	}

	baselineDiffStr, err := os.ReadFile(nonDeterministicField + "resp")
	if err != nil {
		return false, "", err
	}

	// Marshal the message into binary format
	baselineDiff := &pb.DiffResp{}
	err = proto.Unmarshal(baselineDiffStr, baselineDiff)
	if err != nil {
		return false, "", err
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
		return false, "", err
	}
	if result == "" {
		return true, "", nil
	}

	experimentalDiff := &pb.DiffResp{}

	err = getRespDiffHelper(result, experimentalDiff)
	if err != nil {
		return false, "", err
	}
	if proto.Equal(baselineDiff, experimentalDiff) {
		return true, "", nil
	}

	result = strings.Replace(result, "\t", " ", -1)
	color.Bluef(result)

	return false, result, nil
}
