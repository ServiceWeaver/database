package main

import (
	"bankofanthos_prototype/eval_driver/pb"
	"fmt"
	"os"
	"strings"

	"github.com/gookit/color"
	"github.com/pmezard/go-difflib/difflib"
	"google.golang.org/protobuf/proto"
)

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
		FromFile: "original",
		ToFile:   "current",
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

	sm := difflib.NewMatcherWithJunk(difflib.SplitLines(string(output1)), difflib.SplitLines(string(output2)), true, checkJunk)
	fmt.Printf("get baseline ratio %f\n", sm.Ratio())

	db := &pb.DiffDbTable{
		Ratio: float32(sm.Ratio()),
	}

	// Marshal the message into binary format
	data, err := proto.Marshal(db)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return nil
	}

	color.Yellowf("Print out diff for the same query%s\n", result)

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
		FromFile: "original",
		ToFile:   "current",
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

	sm := difflib.NewMatcherWithJunk(difflib.SplitLines(string(output1)), difflib.SplitLines(string(output2)), true, checkJunk)
	fmt.Printf("get baseline ratio %f\n", sm.Ratio())

	respDiff := &pb.DiffResp{
		Ratio: float32(sm.Ratio()),
	}

	// Marshal the message into binary format
	data, err := proto.Marshal(respDiff)
	if err != nil {
		fmt.Println("Failed to marshal:", err)
		return nil
	}

	color.Yellowf("Print out diff for the same query%s\n", result)

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

func checkJunk(x string) bool {
	return false
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

	baselineDiff, err := os.ReadFile(nonDeterministicField + "database")
	if err != nil {
		return false, "", err
	}

	// Marshal the message into binary format
	dbField := &pb.DiffDbTable{}
	err = proto.Unmarshal(baselineDiff, dbField)
	if err != nil {
		return false, "", err
	}

	diff := difflib.ContextDiff{
		A:        difflib.SplitLines(string(output1)),
		B:        difflib.SplitLines(string(output2)),
		FromFile: "original",
		ToFile:   "current",
		Context:  0,
		Eol:      "\n",
	}
	result, err := difflib.GetContextDiffString(diff)
	if err != nil {
		return false, "", err
	}
	if result == "" {
		return true, "", nil
	}

	sm := difflib.NewMatcherWithJunk(difflib.SplitLines(string(output1)), difflib.SplitLines(string(output2)), true, checkJunk)

	fmt.Printf("Compare db ratio %f with baseline ratio %f.\n", float32(sm.Ratio()), dbField.GetRatio())

	// if float32(sm.Ratio()) == dbField.GetRatio() {
	// 	return true, "", nil
	// }

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

	baselineDiff, err := os.ReadFile(nonDeterministicField + "resp")
	if err != nil {
		return false, "", err
	}

	// Marshal the message into binary format
	respField := &pb.DiffResp{}
	err = proto.Unmarshal(baselineDiff, respField)
	if err != nil {
		return false, "", err
	}

	diff := difflib.ContextDiff{
		A:        difflib.SplitLines(string(output1)),
		B:        difflib.SplitLines(string(output2)),
		FromFile: "original",
		ToFile:   "current",
		Context:  0,
		Eol:      "\n",
	}
	result, err := difflib.GetContextDiffString(diff)
	if err != nil {
		return false, "", err
	}
	if result == "" {
		return true, "", nil
	}

	sm := difflib.NewMatcherWithJunk(difflib.SplitLines(string(output1)), difflib.SplitLines(string(output2)), true, checkJunk)
	fmt.Printf("get response diff ratio %f\n", sm.Ratio())
	// if float32(sm.Ratio()) == respField.GetRatio() {
	// 	return true, "", nil
	// }

	result = strings.Replace(result, "\t", " ", -1)
	color.Bluef(result)
	return false, result, nil
}
