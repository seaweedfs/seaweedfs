package dash

import (
	"fmt"
	"strings"
	"time"
)

// dashMaxSamples bounds the in-memory trend ring buffer. At the 15s sample
// cadence (piggy-backed on publishMaintenanceMetrics) this is ~15 minutes.
const dashMaxSamples = 60

// dashSample is one point-in-time snapshot of a few headline cluster numbers,
// derived from data the admin already holds (cluster topology + the in-process
// maintenance queue) — no Prometheus scrape required.
type dashSample struct {
	t        time.Time
	volumes  float64
	ecShards float64
	diskUsed float64
	files    float64
	tasks    float64 // pending/assigned/in-progress maintenance tasks
	workers  float64
}

// DashboardTrends carries inline-SVG sparklines of recent cluster history,
// keyed to the dashboard's existing summary cards so each card shows a value
// plus its trend (rather than a separate, duplicate row). Maintenance metrics
// that have no existing card carry their current value too, and fill the
// previously-empty columns of the EC row.
type DashboardTrends struct {
	Samples int `json:"samples"`

	// Sparklines (raw <svg>) for the existing summary cards.
	Volumes  string `json:"-"`
	Files    string `json:"-"`
	DiskUsed string `json:"-"`
	EcShards string `json:"-"`

	// Maintenance cards: value + sparkline.
	Tasks        string `json:"-"`
	TasksValue   string `json:"tasks"`
	Workers      string `json:"-"`
	WorkersValue string `json:"workers"`
}

// recordDashboardSample snapshots headline cluster numbers into the ring
// buffer. Cheap: topology is already cached, and the maintenance stats are
// in-memory. Called on the existing maintenance-metrics ticker.
func (s *AdminServer) recordDashboardSample() {
	topology, err := s.GetClusterTopology()
	if err != nil || topology == nil {
		return
	}
	ecShards := 0
	for _, vs := range topology.VolumeServers {
		ecShards += vs.EcShards
	}
	sample := dashSample{
		t:        time.Now(),
		volumes:  float64(topology.TotalVolumes),
		ecShards: float64(ecShards),
		diskUsed: float64(topology.TotalSize),
		files:    float64(topology.TotalFiles),
	}
	if s.maintenanceManager != nil {
		if stats := s.maintenanceManager.GetStats(); stats != nil {
			active := 0
			for status, n := range stats.TasksByStatus {
				switch string(status) {
				case "pending", "assigned", "in_progress":
					active += n
				}
			}
			sample.tasks = float64(active)
			sample.workers = float64(stats.ActiveWorkers)
		}
	}

	s.dashSamplesMu.Lock()
	s.dashSamples = append(s.dashSamples, sample)
	if len(s.dashSamples) > dashMaxSamples {
		s.dashSamples = s.dashSamples[len(s.dashSamples)-dashMaxSamples:]
	}
	s.dashSamplesMu.Unlock()
}

// GetDashboardTrends builds the trend cards from the current ring buffer.
func (s *AdminServer) GetDashboardTrends() DashboardTrends {
	s.dashSamplesMu.Lock()
	samples := make([]dashSample, len(s.dashSamples))
	copy(samples, s.dashSamples)
	s.dashSamplesMu.Unlock()

	series := func(pick func(dashSample) float64) []float64 {
		out := make([]float64, len(samples))
		for i, smp := range samples {
			out[i] = pick(smp)
		}
		return out
	}
	tasks := series(func(s dashSample) float64 { return s.tasks })
	workers := series(func(s dashSample) float64 { return s.workers })

	// Sparkline colors match the existing cards' border colors.
	return DashboardTrends{
		Samples:      len(samples),
		Volumes:      sparklineSVG(series(func(s dashSample) float64 { return s.volumes }), "#1cc88a"),  // success
		Files:        sparklineSVG(series(func(s dashSample) float64 { return s.files }), "#36b9cc"),    // info
		DiskUsed:     sparklineSVG(series(func(s dashSample) float64 { return s.diskUsed }), "#f6c23e"), // warning
		EcShards:     sparklineSVG(series(func(s dashSample) float64 { return s.ecShards }), "#5a5c69"), // dark
		Tasks:        sparklineSVG(tasks, "#36b9cc"),
		TasksValue:   trendCount(last(tasks)),
		Workers:      sparklineSVG(workers, "#4e73df"),
		WorkersValue: trendCount(last(workers)),
	}
}

func last(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1]
}

// sparklineSVG renders a fixed-viewBox, width-responsive inline SVG line of the
// given points. Self-contained (no JS/chart lib); safe to inline in the page.
func sparklineSVG(pts []float64, color string) string {
	const w, h = 240.0, 48.0
	if len(pts) < 2 {
		// Not enough history yet — draw a flat baseline so the card isn't empty.
		return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="none" style="width:100%%;height:48px"><line x1="0" y1="%g" x2="%g" y2="%g" stroke="%s" stroke-width="2" opacity="0.4"/></svg>`, w, h, h/2, w, h/2, color)
	}
	minV, maxV := pts[0], pts[0]
	for _, v := range pts {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	span := maxV - minV
	if span == 0 {
		span = 1
	}
	dx := w / float64(len(pts)-1)
	var line strings.Builder
	for i, v := range pts {
		x := float64(i) * dx
		y := h - 3 - (v-minV)/span*(h-6) // 3px padding top/bottom; SVG y grows down
		if i == 0 {
			fmt.Fprintf(&line, "M%.1f %.1f", x, y)
		} else {
			fmt.Fprintf(&line, " L%.1f %.1f", x, y)
		}
	}
	// Area path closes back along the baseline for a subtle fill.
	area := fmt.Sprintf("%s L%.1f %.1f L0 %.1f Z", line.String(), w, h, h)
	return fmt.Sprintf(`<svg viewBox="0 0 %g %g" preserveAspectRatio="none" style="width:100%%;height:48px"><path d="%s" fill="%s" opacity="0.12"/><path d="%s" fill="none" stroke="%s" stroke-width="2"/></svg>`,
		w, h, area, color, line.String(), color)
}

// trendCount formats a count with thousands separators.
func trendCount(v float64) string {
	n := int64(v)
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}
