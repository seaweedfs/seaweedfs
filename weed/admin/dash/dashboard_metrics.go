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

// TrendCard is one at-a-glance metric rendered on the dashboard: a current
// value plus an inline SVG sparkline of its recent history.
type TrendCard struct {
	Title    string
	Value    string
	Icon     string // FontAwesome icon class, e.g. "fa-database"
	Color    string // Bootstrap contextual color, e.g. "primary"
	SparkSVG string // self-contained inline <svg>…</svg>
}

// DashboardTrends is the set of trend cards shown on the dashboard.
type DashboardTrends struct {
	Cards   []TrendCard `json:"-"`
	Samples int         `json:"samples"`
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
	vol := series(func(s dashSample) float64 { return s.volumes })
	ec := series(func(s dashSample) float64 { return s.ecShards })
	disk := series(func(s dashSample) float64 { return s.diskUsed })
	files := series(func(s dashSample) float64 { return s.files })
	tasks := series(func(s dashSample) float64 { return s.tasks })
	workers := series(func(s dashSample) float64 { return s.workers })

	cards := []TrendCard{
		{Title: "Volumes", Value: trendCount(last(vol)), Icon: "fa-database", Color: "primary", SparkSVG: sparklineSVG(vol, "#4e73df")},
		{Title: "EC Shards", Value: trendCount(last(ec)), Icon: "fa-th-large", Color: "info", SparkSVG: sparklineSVG(ec, "#36b9cc")},
		{Title: "Disk Used", Value: trendBytes(last(disk)), Icon: "fa-hdd", Color: "success", SparkSVG: sparklineSVG(disk, "#1cc88a")},
		{Title: "Files", Value: trendCount(last(files)), Icon: "fa-file", Color: "warning", SparkSVG: sparklineSVG(files, "#f6c23e")},
		{Title: "Active Tasks", Value: trendCount(last(tasks)), Icon: "fa-tasks", Color: "secondary", SparkSVG: sparklineSVG(tasks, "#858796")},
		{Title: "Workers", Value: trendCount(last(workers)), Icon: "fa-users-cog", Color: "dark", SparkSVG: sparklineSVG(workers, "#5a5c69")},
	}
	return DashboardTrends{Cards: cards, Samples: len(samples)}
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

// trendBytes formats a byte count as a human-readable size.
func trendBytes(v float64) string {
	const unit = 1024.0
	if v < unit {
		return fmt.Sprintf("%.0f B", v)
	}
	div, exp := unit, 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", v/div, "KMGTPE"[exp])
}
