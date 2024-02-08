package main

import (
	"bankofanthos_prototype/eval_driver/pb"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
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

	expectedRowInfos := []*pb.RowInfo{
		{DiffLineIdx: 0, ColNumber: []int64{1, 2, 7}},
		{DiffLineIdx: 1, ColNumber: []int64{1, 2, 7}},
		{DiffLineIdx: 2, ColNumber: []int64{1, 2, 7}},
	}

	rows, err := checkLine(diffLines[3:6], diffLines[6:9], 3)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(expectedRowInfos, rows, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want,+got):\n%s", diff)
	}
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

	expectedDiffInfos := &pb.DiffInfos{
		DiffInfo: []*pb.DiffInfo{
			{FromLineNumber: -125,
				FromLineCnt:  3,
				ToLineNumber: 125,
				ToLineCnt:    3,
				RowInfo: []*pb.RowInfo{
					{DiffLineIdx: 0, ColNumber: []int64{1, 2, 7}},
					{DiffLineIdx: 1, ColNumber: []int64{1, 2, 7}},
					{DiffLineIdx: 2, ColNumber: []int64{1, 2, 7}},
				},
			},
		},
	}

	diffInfos, err := getDiffHelper(diff, "database")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(expectedDiffInfos, diffInfos, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want,+got):\n%s", diff)
	}
}
