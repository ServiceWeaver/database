package main

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"time"

	"golang.org/x/exp/maps"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

type FloatString struct {
	Str   string
	Value float64
}

type FloatStringSlice []FloatString

func (fs FloatStringSlice) Len() int           { return len(fs) }
func (fs FloatStringSlice) Swap(i, j int)      { fs[i], fs[j] = fs[j], fs[i] }
func (fs FloatStringSlice) Less(i, j int) bool { return fs[i].Value < fs[j].Value }

func extractFloat(str string) float64 {
	re := regexp.MustCompile(`-?\d+\.?\d*`)
	match := re.FindString(str)
	value, _ := strconv.ParseFloat(match, 64)
	return value
}

func convertMs(t time.Duration) float64 {
	return float64(t.Milliseconds())
}

func plotMetrics(metricsStats map[string]map[string]*metrics) error {
	// plot branch, read/write/delete and diff operation on R+/R-, Dolt and postgres
	branchTimeStats := make(map[string]map[string]time.Duration) // {tableSize: {types(postgres/dolt/r+/r-):ts}}
	writeTimeStats := make(map[string]map[string]time.Duration)
	deleteTimeStats := make(map[string]map[string]time.Duration)
	readTimeStats := make(map[string]map[string]time.Duration)
	diffTimeStats := make(map[string]map[string]time.Duration)
	for _, tableMetrics := range metricsStats {
		for table, metrics := range tableMetrics {
			if table == "users_pk" {
				metrics.TableSize += "(pk)"
			}
			branchTimeStats[metrics.TableSize] = metrics.Branch.Time
			writeTimeStats[metrics.TableSize] = metrics.Writes[2].Time   // write 1000 rows
			deleteTimeStats[metrics.TableSize] = metrics.Deletes[0].Time // 1 delete query
			readTimeStats[metrics.TableSize] = metrics.Reads[0].Time     // 1000 read query
			diffTimeStats[metrics.TableSize] = metrics.Diffs[2].Time     // diff 1000 rows
		}
		if err := plotTimeWithTableSize(branchTimeStats, "branch", false); err != nil {
			return err
		}
		if err := plotTimeWithTableSize(writeTimeStats, "write100Rows", false); err != nil {
			return err
		}
		if err := plotTimeWithTableSize(deleteTimeStats, "delete1Query", false); err != nil {
			return err
		}
		if err := plotTimeWithTableSize(readTimeStats, "read1000Query", false); err != nil {
			return err
		}
		if err := plotTimeWithTableSize(diffTimeStats, "diff1000Rows", false); err != nil {
			return err
		}
	}

	// plot write and diff operation different rows for R+/R-
	writeRowsStats := make(map[string]map[string]time.Duration) // {tableSize: {ModifiedRows:ts}}
	diffRowsStats := make(map[string]map[string]time.Duration)
	for _, tableMetrics := range metricsStats {
		for _, metrics := range tableMetrics {
			for _, w := range metrics.Writes {
				if len(writeRowsStats[metrics.TableSize]) == 0 {
					writeRowsStats[metrics.TableSize] = make(map[string]time.Duration)
				}
				writeRowsStats[metrics.TableSize][fmt.Sprintf("%d row(s)", len(w.queries))] = w.Time[RPlusRMinus.String()]
			}
			for _, w := range metrics.Diffs {
				if len(diffRowsStats[metrics.TableSize]) == 0 {
					diffRowsStats[metrics.TableSize] = make(map[string]time.Duration)
				}
				diffRowsStats[metrics.TableSize][fmt.Sprintf("%d row(s)", w.ModifiedRows)] = w.Time[RPlusRMinus.String()]
			}
		}
	}

	if err := plotTimeWithQueriesSize(writeRowsStats, "WriteStats"); err != nil {
		return err
	}
	if err := plotTimeWithQueriesSize(diffRowsStats, "DiffStats"); err != nil {
		return err
	}

	return nil
}

func plotTimeWithQueriesSize(timeStats map[string]map[string]time.Duration, chartName string) error {
	pts := make([]plotter.Values, 3)

	tableSizes := maps.Keys(timeStats)
	tableSizeStr := make(FloatStringSlice, len(tableSizes))
	for i, str := range tableSizes {
		tableSizeStr[i] = FloatString{str, extractFloat(str)}
	}
	sort.Sort(tableSizeStr)

	var barNames []string
	for _, s := range tableSizeStr {
		metricsPerSize := timeStats[s.Str]
		barNames = maps.Keys(metricsPerSize)
		sort.Strings(barNames)
		for i, name := range barNames {
			pts[i] = append(pts[i], convertMs(metricsPerSize[name]))
		}
	}

	p := plot.New()

	p.Title.Text = "R+/R-" + chartName + " chart"
	p.Y.Label.Text = chartName + " latency (milliseconds)"

	w := vg.Points(10)

	barsA, err := plotter.NewBarChart(pts[0], w)
	if err != nil {
		return err
	}
	barsA.LineStyle.Width = vg.Length(0)
	barsA.Color = plotutil.Color(0)
	barsA.Offset = -w

	barsB, err := plotter.NewBarChart(pts[1], w)
	if err != nil {
		return err
	}
	barsB.LineStyle.Width = vg.Length(0)
	barsB.Color = plotutil.Color(1)

	barsC, err := plotter.NewBarChart(pts[2], w)
	if err != nil {
		return err
	}
	barsC.LineStyle.Width = vg.Length(0)
	barsC.Color = plotutil.Color(2)
	barsC.Offset = w

	p.Add(barsA, barsB, barsC)

	p.Legend.Add(barNames[0], barsA)
	p.Legend.Add(barNames[1], barsB)
	p.Legend.Add(barNames[2], barsC)
	p.Legend.Top = true

	x := make([]string, len(tableSizeStr))
	for i, s := range tableSizeStr {
		x[i] = s.Str
	}
	p.NominalX(x...)

	if err := p.Save(8*vg.Inch, 8*vg.Inch, fmt.Sprintf("dump/%s.png", chartName)); err != nil {
		return err
	}
	return nil
}

func plotTimeWithTableSize(timeStats map[string]map[string]time.Duration, chartName string, useLogLatency bool) error {
	var plusMinusPoints plotter.Values
	var doltPoints plotter.Values
	var postgresPoints plotter.Values

	tableSizes := maps.Keys(timeStats)
	tableSizeStr := make(FloatStringSlice, len(tableSizes))
	for i, str := range tableSizes {
		tableSizeStr[i] = FloatString{str, extractFloat(str)}
	}
	sort.Sort(tableSizeStr)

	for _, s := range tableSizeStr {
		metricsPerSize := timeStats[s.Str]
		if useLogLatency {
			plusMinusPoints = append(plusMinusPoints, math.Log2(convertMs(metricsPerSize[RPlusRMinus.String()])))
			doltPoints = append(doltPoints, math.Log2(convertMs(metricsPerSize[Dolt.String()])))
			postgresPoints = append(postgresPoints, math.Log2(convertMs(metricsPerSize[Postgres.String()])))
		} else {
			plusMinusPoints = append(plusMinusPoints, convertMs(metricsPerSize[RPlusRMinus.String()]))
			doltPoints = append(doltPoints, convertMs(metricsPerSize[Dolt.String()]))
			postgresPoints = append(postgresPoints, convertMs(metricsPerSize[Postgres.String()]))
		}
	}

	p := plot.New()

	p.Title.Text = chartName + " chart"

	p.Y.Label.Text = chartName + " latency (milliseconds)"
	if useLogLatency {
		p.Y.Label.Text = chartName + " latency log2(milliseconds)"
	}

	w := vg.Points(10)

	barsA, err := plotter.NewBarChart(plusMinusPoints, w)
	if err != nil {
		return err
	}
	barsA.LineStyle.Width = vg.Length(0)
	barsA.Color = plotutil.Color(0)
	barsA.Offset = -w

	barsB, err := plotter.NewBarChart(doltPoints, w)
	if err != nil {
		return err
	}
	barsB.LineStyle.Width = vg.Length(0)
	barsB.Color = plotutil.Color(1)

	barsC, err := plotter.NewBarChart(postgresPoints, w)
	if err != nil {
		return err
	}
	barsC.LineStyle.Width = vg.Length(0)
	barsC.Color = plotutil.Color(2)
	barsC.Offset = w

	p.Add(barsA, barsB, barsC)
	p.Legend.Add("R+/R-", barsA)
	p.Legend.Add("Dolt", barsB)
	p.Legend.Add("Postgres", barsC)
	p.Legend.Top = true

	x := make([]string, len(tableSizeStr))
	for i, s := range tableSizeStr {
		x[i] = s.Str
	}
	p.NominalX(x...)

	if err := p.Save(8*vg.Inch, 8*vg.Inch, fmt.Sprintf("dump/%s.png", chartName)); err != nil {
		return err
	}

	return nil
}
