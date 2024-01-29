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

func getDbDiffHelper(result string, dbDiff *pb.DiffDbTable) error {
	for i, line := range strings.Split(result, "\n") {
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
			dbInfo := &pb.DbInfo{FromLineNumber: int64(fromLineChangeS), FromLineCnt: int64(fromLineChangeCnt), ToLineNumber: int64(toLineChangeS), ToLineCnt: int64(toLineChangeCnt)}
			dbDiff.DiffDbInfo = append(dbDiff.DiffDbInfo, dbInfo)
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

	//  --- original
	// 	+++ current
	// @@ -125,3 +125,3 @@
	// -67	1234567890  	1489489523  	123456789	883745000	180000	2024-01-29 09:26:35.967826
	// -68	1489489523  	9876543210  	883745000	883745000	100000	2024-01-29 09:26:36.28292
	// -69	1489489523  	9876543210  	883745000	883745000	80000	2024-01-29 09:26:36.550685
	// +67	1234567890  	6621133561  	123456789	883745000	180000	2024-01-29 09:26:43.560705
	// +68	6621133561  	9876543210  	883745000	883745000	100000	2024-01-29 09:26:43.866854
	// +69	6621133561  	9876543210  	883745000	883745000	80000	2024-01-29 09:26:44.135197
	// var fromFileChar char
	// var toFileChar char

	// Marshal the message into binary format
	data, err := proto.Marshal(db)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return nil
	}

	color.Yellowf("Print out diff for the same query\n%s\n", result)
	color.Greenf("Print out pb:\n%+v\n", db)
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

	color.Yellowf("Print out diff for the same query%s\n", result)
	color.Greenf("Print out pb:\n%+v\n", respDiff)

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

	color.Greenf("baselineDiff %+v\n", baselineDiff)
	color.Yellowf("experimentalDiff %+v\n", experimentalDiff)

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
	color.Yellowf("baselineDiff %+v\n", baselineDiff)
	color.Bluef("experimentalDiff %+v\n", experimentalDiff)
	result = strings.Replace(result, "\t", " ", -1)
	color.Bluef(result)

	return false, result, nil
}
