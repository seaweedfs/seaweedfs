package dash

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"
)

// Chart palette, matching the muted colors in static/css/admin.css.
const (
	ChartPrimary = "#6b8caf"
	ChartSuccess = "#5a8a72"
	ChartInfo    = "#6a9aaa"
	ChartWarning = "#b8995e"
	ChartDanger  = "#a5615c"
)

// Y-axis value formats.
const (
	UnitCount   = ""
	UnitBytes   = "bytes"
	UnitBytesPS = "bps"
	UnitSeconds = "seconds"
	UnitPercent = "pct"
)

// Point is one sample, keeping its scrape timestamp so series recorded at
// different times are never paired by index.
type Point struct {
	T time.Time
	V float64
}

type ChartSeries struct {
	Name  string
	Color string
	Data  []Point
	Area  bool
}

type ChartOptions struct {
	Unit string
	// Threshold draws a dashed reference line, e.g. a disk-usage limit.
	Threshold *float64
	// Labels are x-axis tick labels, oldest first. Falls back to relative
	// sample offsets when empty.
	Labels []string
}

// RenderChart draws a multi-series line chart as a self-contained inline SVG,
// with no JavaScript. Series are positioned on a shared time axis, so series
// with gaps or differing sample times stay correctly aligned.
func RenderChart(series []ChartSeries, opts ChartOptions) string {
	const w, h = 560.0, 190.0
	const padL, padR, padT, padB = 46.0, 8.0, 10.0, 22.0
	plotH := h - padT - padB

	times := unionTimes(series)
	if len(times) < 2 {
		return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="xMidYMid meet" style="width:100%%;height:auto"><line x1="%g" y1="%g" x2="%g" y2="%g" stroke="#e3e6f0" stroke-width="1"/><text x="%g" y="%g" text-anchor="middle" font-size="10" fill="#858796">collecting data…</text></svg>`,
			w, h, padL, padT+plotH/2, w-padR, padT+plotH/2, w/2, padT+plotH/2-8)
	}
	slot := make(map[time.Time]int, len(times))
	for i, t := range times {
		slot[t] = i
	}
	n := len(times)

	min, max := chartRange(series, opts.Threshold)
	span := max - min

	px := func(i int) float64 { return padL + float64(i)*(w-padL-padR)/float64(n-1) }
	py := func(v float64) float64 { return padT + plotH*(1-(v-min)/span) }

	var s strings.Builder
	for g := 0; g <= 4; g++ {
		v := min + span*float64(g)/4
		y := py(v)
		fmt.Fprintf(&s, `<line x1="%g" y1="%.1f" x2="%g" y2="%.1f" stroke="#e3e6f0" stroke-width="1"/>`, padL, y, w-padR, y)
		fmt.Fprintf(&s, `<text x="%g" y="%.1f" text-anchor="end" font-size="9" fill="#858796">%s</text>`, padL-6, y+3, html.EscapeString(FormatChartValue(v, opts.Unit)))
	}
	for _, i := range []int{0, n / 2, n - 1} {
		fmt.Fprintf(&s, `<text x="%.1f" y="%g" text-anchor="middle" font-size="9" fill="#858796">%s</text>`, px(i), h-6, html.EscapeString(xLabel(opts, times, i)))
	}
	if opts.Threshold != nil {
		fmt.Fprintf(&s, `<line x1="%g" y1="%.1f" x2="%g" y2="%.1f" stroke="%s" stroke-width="1" stroke-dasharray="4 3"/>`, padL, py(*opts.Threshold), w-padR, py(*opts.Threshold), ChartDanger)
	}
	for _, se := range series {
		color := se.Color
		if color == "" {
			color = ChartPrimary
		}
		// Break the path wherever the series has no sample, so gaps are not
		// drawn as straight lines through missing time.
		for _, run := range contiguousRuns(se.Data, slot) {
			var d strings.Builder
			for k, p := range run {
				x, y := px(slot[p.T]), py(p.V)
				if k == 0 {
					fmt.Fprintf(&d, "M%.1f %.1f", x, y)
				} else {
					fmt.Fprintf(&d, " L%.1f %.1f", x, y)
				}
			}
			if len(run) == 1 {
				fmt.Fprintf(&s, `<circle cx="%.1f" cy="%.1f" r="2" fill="%s"/>`, px(slot[run[0].T]), py(run[0].V), color)
				continue
			}
			if se.Area {
				base := py(min)
				fmt.Fprintf(&s, `<path d="%s L%.1f %.1f L%.1f %.1f Z" fill="%s" opacity="0.12"/>`,
					d.String(), px(slot[run[len(run)-1].T]), base, px(slot[run[0].T]), base, color)
			}
			fmt.Fprintf(&s, `<path d="%s" fill="none" stroke="%s" stroke-width="1.8" stroke-linejoin="round"/>`, d.String(), color)
		}
	}
	return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="xMidYMid meet" style="width:100%%;height:auto">%s</svg>`, w, h, s.String())
}

// unionTimes returns every timestamp present in any series, sorted.
func unionTimes(series []ChartSeries) []time.Time {
	seen := map[time.Time]bool{}
	var out []time.Time
	for _, se := range series {
		for _, p := range se.Data {
			if !seen[p.T] {
				seen[p.T] = true
				out = append(out, p.T)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// contiguousRuns splits a series into runs of samples that occupy adjacent
// slots on the shared time axis.
func contiguousRuns(data []Point, slot map[time.Time]int) [][]Point {
	sorted := make([]Point, 0, len(data))
	for _, p := range data {
		if _, ok := slot[p.T]; ok {
			sorted = append(sorted, p)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return slot[sorted[i].T] < slot[sorted[j].T] })

	var runs [][]Point
	var cur []Point
	for i, p := range sorted {
		if i > 0 && slot[p.T] != slot[sorted[i-1].T]+1 {
			runs = append(runs, cur)
			cur = nil
		}
		cur = append(cur, p)
	}
	if len(cur) > 0 {
		runs = append(runs, cur)
	}
	return runs
}

// RenderLegend renders series names and colors as Bootstrap-friendly markup.
func RenderLegend(series []ChartSeries) string {
	var b strings.Builder
	b.WriteString(`<div class="d-flex flex-wrap gap-3 mt-2">`)
	for _, se := range series {
		color := se.Color
		if color == "" {
			color = ChartPrimary
		}
		fmt.Fprintf(&b, `<span class="text-xs text-muted"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:%s;margin-right:4px"></span>%s</span>`,
			html.EscapeString(color), html.EscapeString(se.Name))
	}
	b.WriteString(`</div>`)
	return b.String()
}

// chartRange picks the y-axis bounds. It anchors at zero so magnitudes stay
// comparable, and never returns a zero span.
func chartRange(series []ChartSeries, threshold *float64) (float64, float64) {
	min, max := 0.0, 0.0
	for _, se := range series {
		for _, p := range se.Data {
			if p.V < min {
				min = p.V
			}
			if p.V > max {
				max = p.V
			}
		}
	}
	if threshold != nil && *threshold > max {
		max = *threshold * 1.15
	}
	if max == min {
		max = min + 1
	}
	return min, max
}

func xLabel(opts ChartOptions, times []time.Time, i int) string {
	if i < len(opts.Labels) {
		return opts.Labels[i]
	}
	if i == len(times)-1 {
		return "now"
	}
	return times[i].Format("15:04")
}

// FormatChartValue renders an axis value in the given unit.
func FormatChartValue(v float64, unit string) string {
	switch unit {
	case UnitBytes:
		return formatChartBytes(v)
	case UnitBytesPS:
		return formatChartBytes(v) + "/s"
	case UnitSeconds:
		// Latency quantiles are often sub-millisecond, so step down the unit
		// rather than rounding everything to "0 ms".
		switch {
		case v == 0:
			return "0"
		case v < 0.001:
			return fmt.Sprintf("%.0f µs", v*1e6)
		case v < 0.01:
			return fmt.Sprintf("%.2f ms", v*1000)
		case v < 1:
			return fmt.Sprintf("%.1f ms", v*1000)
		default:
			return fmt.Sprintf("%.2f s", v)
		}
	case UnitPercent:
		return fmt.Sprintf("%.0f%%", v)
	default:
		switch {
		case v >= 1e6:
			return fmt.Sprintf("%.1fM", v/1e6)
		case v >= 1000:
			return fmt.Sprintf("%.1fk", v/1000)
		case v == float64(int64(v)):
			return fmt.Sprintf("%d", int64(v))
		default:
			return fmt.Sprintf("%.2f", v)
		}
	}
}

func formatChartBytes(v float64) string {
	const unit = 1024.0
	if v < unit {
		return fmt.Sprintf("%.0f B", v)
	}
	div, exp := unit, 0
	for v/div >= unit && exp < 5 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", v/div, "KMGTPE"[exp])
}

// LatestValue returns the newest sample's value, or 0 when there is no data.
func LatestValue(data []Point) float64 {
	if len(data) == 0 {
		return 0
	}
	newest := data[0]
	for _, p := range data[1:] {
		if p.T.After(newest.T) {
			newest = p
		}
	}
	return newest.V
}
