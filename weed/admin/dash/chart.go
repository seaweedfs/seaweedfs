package dash

import (
	"fmt"
	"html"
	"strings"
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

type ChartSeries struct {
	Name  string
	Color string
	Data  []float64
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
// with no JavaScript. Series must be equal length; shorter ones are left-padded
// so all series share the newest sample.
func RenderChart(series []ChartSeries, opts ChartOptions) string {
	const w, h = 560.0, 190.0
	const padL, padR, padT, padB = 46.0, 8.0, 10.0, 22.0
	plotH := h - padT - padB

	series = alignSeries(series)
	n := seriesLen(series)
	if n < 2 {
		return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="xMidYMid meet" style="width:100%%;height:auto"><line x1="%g" y1="%g" x2="%g" y2="%g" stroke="#e3e6f0" stroke-width="1"/><text x="%g" y="%g" text-anchor="middle" font-size="10" fill="#858796">collecting data…</text></svg>`,
			w, h, padL, padT+plotH/2, w-padR, padT+plotH/2, w/2, padT+plotH/2-8)
	}

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
		fmt.Fprintf(&s, `<text x="%.1f" y="%g" text-anchor="middle" font-size="9" fill="#858796">%s</text>`, px(i), h-6, html.EscapeString(xLabel(opts, i, n)))
	}
	if opts.Threshold != nil {
		fmt.Fprintf(&s, `<line x1="%g" y1="%.1f" x2="%g" y2="%.1f" stroke="%s" stroke-width="1" stroke-dasharray="4 3"/>`, padL, py(*opts.Threshold), w-padR, py(*opts.Threshold), ChartDanger)
	}
	for _, se := range series {
		var d strings.Builder
		for i, v := range se.Data {
			if i == 0 {
				fmt.Fprintf(&d, "M%.1f %.1f", px(i), py(v))
			} else {
				fmt.Fprintf(&d, " L%.1f %.1f", px(i), py(v))
			}
		}
		color := se.Color
		if color == "" {
			color = ChartPrimary
		}
		if se.Area {
			base := py(min)
			fmt.Fprintf(&s, `<path d="%s L%.1f %.1f L%.1f %.1f Z" fill="%s" opacity="0.12"/>`, d.String(), px(n-1), base, px(0), base, color)
		}
		fmt.Fprintf(&s, `<path d="%s" fill="none" stroke="%s" stroke-width="1.8" stroke-linejoin="round"/>`, d.String(), color)
	}
	return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="xMidYMid meet" style="width:100%%;height:auto">%s</svg>`, w, h, s.String())
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

// seriesLen returns the sample count shared by all series.
func seriesLen(series []ChartSeries) int {
	n := 0
	for _, se := range series {
		if len(se.Data) > n {
			n = len(se.Data)
		}
	}
	return n
}

// alignSeries left-pads shorter series so every series ends on the newest
// sample. Series with no data are dropped.
func alignSeries(series []ChartSeries) []ChartSeries {
	n := seriesLen(series)
	if n == 0 {
		return nil
	}
	out := make([]ChartSeries, 0, len(series))
	for _, se := range series {
		if len(se.Data) == 0 {
			continue
		}
		if len(se.Data) < n {
			padded := make([]float64, n)
			copy(padded[n-len(se.Data):], se.Data)
			se.Data = padded
		}
		out = append(out, se)
	}
	return out
}

// chartRange picks the y-axis bounds. It anchors at zero so magnitudes stay
// comparable, and never returns a zero span.
func chartRange(series []ChartSeries, threshold *float64) (float64, float64) {
	min, max := 0.0, 0.0
	for _, se := range series {
		for _, v := range se.Data {
			if v < min {
				min = v
			}
			if v > max {
				max = v
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

func xLabel(opts ChartOptions, i, n int) string {
	if i < len(opts.Labels) {
		return opts.Labels[i]
	}
	if i == n-1 {
		return "now"
	}
	return fmt.Sprintf("-%d", n-1-i)
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

// LatestValue returns the newest sample, or 0 when there is no data.
func LatestValue(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	return data[len(data)-1]
}
