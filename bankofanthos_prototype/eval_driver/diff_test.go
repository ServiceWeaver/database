package main

import (
	"bankofanthos_prototype/eval_driver/pb"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckLines(t *testing.T) {
	diff := `--- baseline
+++ comparison
@@ -125,3 +125,3 @@
-67 1234567890   5983650298   123456789 883745000 120000 2024-02-01 13:23:52.152196
-68 5983650298   9876543210   883745000 883745000 40000 2024-02-01 13:23:52.449237
-69 5983650298   9876543210   883745000 883745000 80000 2024-02-01 13:23:52.710463
+67 001234567890 002289066048 123456789 883745000 120000 2024-02-01 13:24:15.745736
+68 002289066048 009876543210 883745000 883745000 40000 2024-02-01 13:24:16.063933
+69 002289066048 009876543210 883745000 883745000 80000 2024-02-01 13:24:16.346361`

	diffLines := strings.Split(diff, "\n")

	diffInfo := &pb.DiffInfo{FromLineNumber: -125, FromLineCnt: 3, ToLineNumber: 125, ToLineCnt: 3}
	err := checkLine(diffLines, 3, diffInfo)

	expectedRowInfo1 := &pb.RowInfo{DiffLineIdx: 3, ColNumber: []int64{1, 2, 7}}
	expectedRowInfo2 := &pb.RowInfo{DiffLineIdx: 4, ColNumber: []int64{1, 2, 7}}
	expectedRowInfo3 := &pb.RowInfo{DiffLineIdx: 5, ColNumber: []int64{1, 2, 7}}
	expectedDiffInfo := &pb.DiffInfo{FromLineNumber: -125, FromLineCnt: 3, ToLineNumber: 125, ToLineCnt: 3}
	expectedDiffInfo.RowInfo = append(expectedDiffInfo.RowInfo, expectedRowInfo1, expectedRowInfo2, expectedRowInfo3)

	assert.NoError(t, err)
	assert.Equal(t, expectedDiffInfo, diffInfo)
}

func TestGetDiffHelper(t *testing.T) {
	diff := `--- baseline
+++ comparison
@@ -125,3 +125,3 @@
-67 1234567890   5983650298   123456789 883745000 120000 2024-02-01 13:23:52.152196
-68 5983650298   9876543210   883745000 883745000 40000 2024-02-01 13:23:52.449237
-69 5983650298   9876543210   883745000 883745000 80000 2024-02-01 13:23:52.710463
+67 001234567890 002289066048 123456789 883745000 120000 2024-02-01 13:24:15.74573
+68 002289066048 009876543210 883745000 883745000 40000 2024-02-01 13:24:16.063933
+69 002289066048 009876543210 883745000 883745000 80000 2024-02-01 13:24:16.346361`

	diffInfos, err := getDiffHelper(diff, "database")

	expectedRowInfo1 := &pb.RowInfo{DiffLineIdx: 3, ColNumber: []int64{1, 2, 7}}
	expectedRowInfo2 := &pb.RowInfo{DiffLineIdx: 4, ColNumber: []int64{1, 2, 7}}
	expectedRowInfo3 := &pb.RowInfo{DiffLineIdx: 5, ColNumber: []int64{1, 2, 7}}
	expectedDiffInfo := &pb.DiffInfo{FromLineNumber: -125, FromLineCnt: 3, ToLineNumber: 125, ToLineCnt: 3}
	expectedDiffInfo.RowInfo = append(expectedDiffInfo.RowInfo, expectedRowInfo1, expectedRowInfo2, expectedRowInfo3)
	expectedDiffInfos := &pb.DiffInfos{DiffInfo: []*pb.DiffInfo{expectedDiffInfo}}

	assert.NoError(t, err)
	assert.Equal(t, expectedDiffInfos, diffInfos)
}
