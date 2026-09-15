package dash

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

type metricKind int

const (
	kindGauge metricKind = iota
	kindCounter
	kindHistogram
)

type histogramBucket struct {
	upperBound float64
	count      float64
}

type scrapedMetric struct {
	name    string
	labels  map[string]string
	kind    metricKind
	value   float64
	buckets []histogramBucket
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
			out = append(out, toScrapedMetric(fam.GetName(), m))
		}
	}
	return out, nil
}

func toScrapedMetric(name string, m *dto.Metric) scrapedMetric {
	labels := map[string]string{}
	for _, l := range m.Label {
		labels[l.GetName()] = l.GetValue()
	}
	sm := scrapedMetric{name: name, labels: labels}
	switch {
	case m.Counter != nil:
		sm.kind, sm.value = kindCounter, m.Counter.GetValue()
	case m.Histogram != nil:
		sm.kind = kindHistogram
		for _, b := range m.Histogram.Bucket {
			sm.buckets = append(sm.buckets, histogramBucket{upperBound: b.GetUpperBound(), count: float64(b.GetCumulativeCount())})
		}
	case m.Summary != nil:
		sm.kind, sm.value = kindCounter, m.Summary.GetSampleSum()
	case m.Gauge != nil:
		sm.value = m.Gauge.GetValue()
	case m.Untyped != nil:
		sm.value = m.Untyped.GetValue()
	}
	return sm
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
			s.metricsDeriver.record(s.metricsStore, "admin/local", toScrapedMetric(fam.GetName(), m), now)
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
			s.metricsDeriver.record(s.metricsStore, r.source, m, now)
		}
	}
}

// scrapeTarget is one Prometheus endpoint. source is the endpoint address, not
// a component name: a combined "weed server" advertises one listener for
// master, volume, filer and S3 alike, and metric names already identify the
// component. nodes records which cluster members advertised this endpoint, so
// the UI can label it.
type scrapeTarget struct {
	source  string
	address string
	nodes   []string
}

// scrapeTargets lists the distinct metrics endpoints advertised by the cluster.
// Nodes started without -metricsPort advertise 0 and are skipped, so nothing is
// scraped from a client-facing service port.
func (s *AdminServer) scrapeTargets() []scrapeTarget {
	byAddress := map[string][]string{}
	add := func(nodeAddress string, metricsPort uint32) {
		endpoint := metricsEndpoint(nodeAddress, metricsPort)
		if endpoint == "" {
			return
		}
		byAddress[endpoint] = append(byAddress[endpoint], nodeAddress)
	}

	for _, m := range s.mastersWithMetricsPort() {
		add(m.address, m.metricsPort)
	}
	if topo, err := s.GetClusterTopology(); err == nil && topo != nil {
		for _, vs := range topo.VolumeServers {
			add(vs.Address, vs.MetricsPort)
		}
	}
	for _, f := range s.getFilerNodesStatus() {
		add(f.Address, f.MetricsPort)
	}
	for _, n := range s.getS3NodesStatus() {
		add(n.Address, n.MetricsPort)
	}

	out := make([]scrapeTarget, 0, len(byAddress))
	for endpoint, nodes := range byAddress {
		sort.Strings(nodes)
		out = append(out, scrapeTarget{source: endpoint, address: endpoint, nodes: nodes})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].source < out[j].source })
	return out
}

// metricsEndpoint combines a node's host with its advertised metrics port.
// Returns "" when the node does not run a metrics listener.
func metricsEndpoint(nodeAddress string, metricsPort uint32) string {
	if metricsPort == 0 {
		return ""
	}
	host, _, err := net.SplitHostPort(nodeAddress)
	if err != nil {
		host = nodeAddress
	}
	return net.JoinHostPort(host, strconv.Itoa(int(metricsPort)))
}

type masterMetricsTarget struct {
	address     string
	metricsPort uint32
}

// mastersWithMetricsPort asks each master for its own metrics port.
// GetMasterConfiguration reports the configuration of the master that answers,
// so it is called per address rather than once via the leader.
func (s *AdminServer) mastersWithMetricsPort() []masterMetricsTarget {
	md, err := s.GetClusterMasters()
	if err != nil || md == nil {
		return nil
	}
	var out []masterMetricsTarget
	for _, m := range md.Masters {
		address := m.Address
		err := pb.WithMasterClient(context.Background(), false, pb.ServerAddress(address), s.grpcDialOption, false,
			func(client master_pb.SeaweedClient) error {
				resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return err
				}
				out = append(out, masterMetricsTarget{address: address, metricsPort: resp.MetricsPort})
				return nil
			})
		if err != nil {
			glog.V(1).Infof("master %s configuration: %v", address, err)
		}
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
