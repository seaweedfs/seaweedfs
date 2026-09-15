package dash

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

type scrapedMetric struct {
	name   string
	labels map[string]string
	value  float64
}

func scrapeMetrics(ctx context.Context, target string) ([]scrapedMetric, error) {
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}
	target = strings.TrimRight(target, "/") + "/metrics"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", string(expfmt.TextVersion))

	client := util_http.GetGlobalHttpClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: status %d", target, resp.StatusCode)
	}

	return parsePrometheusText(resp.Body)
}

func parsePrometheusText(r io.Reader) ([]scrapedMetric, error) {
	dec := expfmt.NewDecoder(r, expfmt.NewFormat(expfmt.TypeTextPlain))
	var out []scrapedMetric
	for {
		var fam dto.MetricFamily
		if err := dec.Decode(&fam); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for _, m := range fam.Metric {
			labels := map[string]string{}
			for _, l := range m.Label {
				labels[l.GetName()] = l.GetValue()
			}
			out = append(out, scrapedMetric{name: fam.GetName(), labels: labels, value: metricValue(m)})
		}
	}
	return out, nil
}

func metricValue(m *dto.Metric) float64 {
	switch {
	case m.Gauge != nil:
		return m.Gauge.GetValue()
	case m.Counter != nil:
		return m.Counter.GetValue()
	case m.Untyped != nil:
		return m.Untyped.GetValue()
	case m.Histogram != nil:
		return m.Histogram.GetSampleSum()
	case m.Summary != nil:
		return m.Summary.GetSampleSum()
	}
	return 0
}

// gatherLocalMetrics records the admin's own registry (maintenance tasks,
// worker slots) without a network round trip.
func (s *AdminServer) gatherLocalMetrics(now time.Time) {
	families, err := stats_collect.Gather.Gather()
	if err != nil {
		glog.V(1).Infof("gather admin metrics: %v", err)
		return
	}
	for _, fam := range families {
		for _, m := range fam.Metric {
			labels := map[string]string{}
			for _, l := range m.Label {
				labels[l.GetName()] = l.GetValue()
			}
			s.metricsStore.recordLabeled("admin/local", fam.GetName(), labels, metricValue(m), now)
		}
	}
}

func (s *AdminServer) scrapeAllServers(ctx context.Context) {
	now := time.Now()
	s.gatherLocalMetrics(now)

	targets := s.scrapeTargets()
	if len(targets) == 0 {
		return
	}
	type result struct {
		source  string
		metrics []scrapedMetric
		err     error
	}
	results := make(chan result, len(targets))
	scrapeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, t := range targets {
		go func(t scrapeTarget) {
			ms, err := scrapeMetrics(scrapeCtx, t.address)
			results <- result{source: t.source, metrics: ms, err: err}
		}(t)
	}
	for i := 0; i < len(targets); i++ {
		r := <-results
		if r.err != nil {
			glog.V(1).Infof("metrics scrape %s: %v", r.source, r.err)
			continue
		}
		for _, m := range r.metrics {
			s.metricsStore.recordLabeled(r.source, m.name, m.labels, m.value, now)
		}
	}
}

type scrapeTarget struct {
	source  string
	address string
}

func (s *AdminServer) scrapeTargets() []scrapeTarget {
	var out []scrapeTarget
	if topo, err := s.GetClusterTopology(); err == nil && topo != nil {
		for _, m := range topo.Masters {
			out = append(out, scrapeTarget{source: "master/" + m.Address, address: m.Address})
		}
		for _, vs := range topo.VolumeServers {
			out = append(out, scrapeTarget{source: "volume/" + vs.Address, address: vs.Address})
		}
	}
	for _, f := range s.getFilerNodesStatus() {
		out = append(out, scrapeTarget{source: "filer/" + f.Address, address: f.Address})
	}
	for _, n := range s.getS3NodesStatus() {
		out = append(out, scrapeTarget{source: "s3/" + n.Address, address: n.Address})
	}
	return out
}

func (s *AdminServer) startMetricsScraper(ctx context.Context) {
	const interval = 15 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.scrapeAllServers(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scrapeAllServers(ctx)
		}
	}
}
