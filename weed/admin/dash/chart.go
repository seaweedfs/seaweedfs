package dash

import (
	"fmt"
	"strings"
)

type chartSeries struct {
	name  string
	color string
	data  []float64
	area  bool
}

type chartOptions struct {
	unit      string
	threshold *float64
	labels    []string
}

func renderChartSVG(series []chartSeries, opts chartOptions) string {
	const w, h = 560.0, 190.0
	const l, r, t, b = 46.0, 8.0, 10.0, 22.0

	if len(series) == 0 || len(series[0].data) < 2 {
		return flatChart(w, h, l, r, t, b)
	}
	n := len(series[0].data)
	min, max := computeRange(series, opts.threshold)
	if min > 0 {
		min = 0
	}
	if max == min {
		max = min + 1
	}
	span := max - min

	px := func(i int) float64 { return l + float64(i)*(w-l-r)/float64(n-1) }
	py := func(v float64) float64 { return t + (h-t-b)*(1-(v-min)/span) }

	var s strings.Builder
	for g := 0; g <= 4; g++ {
		v := min + span*float64(g)/4
		y := py(v)
		fmt.Fprintf(&s, `<line x1="%g" y1="%g" x2="%g" y2="%g" stroke="#e3e6f0" stroke-width="1"/>`, l, y, w-r, y)
		fmt.Fprintf(&s, `<text x="%g" y="%g" text-anchor="end" font-size="9" fill="#858796">%s</text>`, l-6, y+3, formatChartValue(v, opts.unit))
	}
	for _, i := range []int{0, n / 2, n - 1} {
		fmt.Fprintf(&s, `<text x="%g" y="%g" text-anchor="middle" font-size="9" fill="#858796">%s</text>`, px(i), h-6, xLabel(opts, i, n))
	}
	if opts.threshold != nil {
		y := py(*opts.threshold)
		fmt.Fprintf(&s, `<line x1="%g" y1="%g" x2="%g" y2="%g" stroke="#a5615c" stroke-width="1" stroke-dasharray="4 3"/>`, l, y, w-r, y)
	}
	for _, se := range series {
		var d strings.Builder
		for i, v := range se.data {
			if i == 0 {
				fmt.Fprintf(&d, "M%.1f %.1f", px(i), py(v))
			} else {
				fmt.Fprintf(&d, " L%.1f %.1f", px(i), py(v))
			}
		}
		if se.area {
			fmt.Fprintf(&s, `<path d="%s L%.1f %.1f L%.1f %.1f Z" fill="%s" opacity="0.12"/>`, d.String(), px(n-1), py(0), px(0), py(0), se.color)
		}
		fmt.Fprintf(&s, `<path d="%s" fill="none" stroke="%s" stroke-width="1.8" stroke-linejoin="round"/>`, d.String(), se.color)
	}
	return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="xMidYMid meet" style="width:100%%;height:auto">%s</svg>`, w, h, s.String())
}

func flatChart(w, h, l, r, t, b float64) string {
	return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="xMidYMid meet" style="width:100%%;height:auto"><line x1="%g" y1="%g" x2="%g" y2="%g" stroke="#e3e6f0" stroke-width="1"/></svg>`, w, h, l, (t + (h-t-b)/2), w-r, (t + (h-t-b)/2))
}

func computeRange(series []chartSeries, threshold *float64) (float64, float64) {
	min, max := 1e9, -1e9
	for _, se := range series {
		for _, v := range se.data {
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
	return min, max
}

func xLabel(opts chartOptions, i, n int) string {
	if i < len(opts.labels) {
		return opts.labels[i]
	}
	return fmt.Sprintf("-%dm", n-1-i)
}

func formatChartValue(v float64, unit string) string {
	switch unit {
	case "bytes":
		return chartFormatBytes(int64(v))
	case "bps":
		return chartFormatBytes(int64(v)) + "/s"
	case "ms":
		return fmt.Sprintf("%.0f ms", v)
	case "pct":
		return fmt.Sprintf("%.0f%%", v)
	default:
		if v >= 1000 {
			return fmt.Sprintf("%.1fk", v/1000)
		}
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%.1f", v)
	}
}

func chartFormatBytes(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func renderLegend(series []chartSeries) string {
	var b strings.Builder
	b.WriteString(`<div class="legend">`)
	for _, se := range series {
		fmt.Fprintf(&b, `<span class="key"><span class="dot" style="background:%s"></span>%s</span>`, se.color, se.name)
	}
	b.WriteString(`</div>`)
	return b.String()
}

func sampleTimes(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		m := -(n - 1 - i)
		if m == 0 {
			out[i] = "now"
		} else {
			out[i] = fmt.Sprintf("%dm", m)
		}
	}
	return out
}

func seriesFromSamples(samples []metricsSample) []float64 {
	out := make([]float64, len(samples))
	for i, s := range samples {
		if v, ok := s.values[""]; ok {
			out[i] = v
		}
	}
	return out
}

func timeLabels(samples []metricsSample) []string {
	out := make([]string, len(samples))
	for i, s := range samples {
		out[i] = s.t.Format("15:04")
	}
	return out
}
