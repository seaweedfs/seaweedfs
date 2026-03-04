package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// metricsAdapter wraps a BlockDevice and feeds Prometheus counters/histograms.
// It sits in the adapter stack: metricsAdapter -> instrumentedAdapter -> BlockVolAdapter.
// Counters count all attempts (including errors) per Prometheus conventions.
type metricsAdapter struct {
	inner iscsi.BlockDevice

	writeOps     prometheus.Counter
	readOps      prometheus.Counter
	trimOps      prometheus.Counter
	syncOps      prometheus.Counter
	writeBytes   prometheus.Counter
	readBytes    prometheus.Counter
	writeLatency prometheus.Observer
	readLatency  prometheus.Observer
	syncLatency  prometheus.Observer
}

// gaugeSource provides gauge data from the BlockVol engine.
type gaugeSource struct {
	vol *blockvol.BlockVol
}

// newMetricsAdapter creates a metricsAdapter wrapping inner, registers all
// metrics on the given registry, and wires GaugeFunc callbacks via vol.
func newMetricsAdapter(inner iscsi.BlockDevice, vol *blockvol.BlockVol, reg prometheus.Registerer) *metricsAdapter {
	writeOps := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "write_ops_total", Help: "Total write operations",
	})
	readOps := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "read_ops_total", Help: "Total read operations",
	})
	trimOps := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "trim_ops_total", Help: "Total trim operations",
	})
	syncOps := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "sync_ops_total", Help: "Total sync operations",
	})
	writeBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "write_bytes_total", Help: "Total bytes written",
	})
	readBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "read_bytes_total", Help: "Total bytes read",
	})

	latencyBuckets := []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1}
	writeLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "write_latency_seconds", Help: "Write latency distribution",
		Buckets: latencyBuckets,
	})
	readLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "read_latency_seconds", Help: "Read latency distribution",
		Buckets: latencyBuckets,
	})
	syncLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "sync_latency_seconds", Help: "Sync latency distribution",
		Buckets: latencyBuckets,
	})

	reg.MustRegister(writeOps, readOps, trimOps, syncOps, writeBytes, readBytes,
		writeLatency, readLatency, syncLatency)

	// Gauge callbacks from the engine.
	gs := &gaugeSource{vol: vol}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "wal_used_fraction", Help: "WAL space usage (0.0 - 1.0)",
	}, gs.walUsedFraction))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "dirty_map_entries", Help: "Number of dirty map entries",
	}, gs.dirtyMapEntries))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "epoch", Help: "Current fencing epoch",
	}, gs.epoch))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "role", Help: "Current role (0=None, 1=Primary, 2=Replica, ...)",
	}, gs.role))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "seaweedfs", Subsystem: "blockvol",
		Name: "snapshot_count", Help: "Number of active snapshots",
	}, gs.snapshotCount))

	return &metricsAdapter{
		inner:        inner,
		writeOps:     writeOps,
		readOps:      readOps,
		trimOps:      trimOps,
		syncOps:      syncOps,
		writeBytes:   writeBytes,
		readBytes:    readBytes,
		writeLatency: writeLatency,
		readLatency:  readLatency,
		syncLatency:  syncLatency,
	}
}

func (m *metricsAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	start := time.Now()
	data, err := m.inner.ReadAt(lba, length)
	m.readLatency.Observe(time.Since(start).Seconds())
	m.readOps.Inc()
	m.readBytes.Add(float64(length))
	return data, err
}

func (m *metricsAdapter) WriteAt(lba uint64, data []byte) error {
	start := time.Now()
	err := m.inner.WriteAt(lba, data)
	m.writeLatency.Observe(time.Since(start).Seconds())
	m.writeOps.Inc()
	m.writeBytes.Add(float64(len(data)))
	return err
}

func (m *metricsAdapter) Trim(lba uint64, length uint32) error {
	err := m.inner.Trim(lba, length)
	m.trimOps.Inc()
	return err
}

func (m *metricsAdapter) SyncCache() error {
	start := time.Now()
	err := m.inner.SyncCache()
	m.syncLatency.Observe(time.Since(start).Seconds())
	m.syncOps.Inc()
	return err
}

func (m *metricsAdapter) BlockSize() uint32  { return m.inner.BlockSize() }
func (m *metricsAdapter) VolumeSize() uint64 { return m.inner.VolumeSize() }
func (m *metricsAdapter) IsHealthy() bool    { return m.inner.IsHealthy() }

// ALUAProvider proxy: delegate to inner device if it implements ALUAProvider.
func (m *metricsAdapter) ALUAState() uint8 {
	if p, ok := m.inner.(iscsi.ALUAProvider); ok {
		return p.ALUAState()
	}
	return iscsi.ALUAStandby
}
func (m *metricsAdapter) TPGroupID() uint16 {
	if p, ok := m.inner.(iscsi.ALUAProvider); ok {
		return p.TPGroupID()
	}
	return 1
}
func (m *metricsAdapter) DeviceNAA() [8]byte {
	if p, ok := m.inner.(iscsi.ALUAProvider); ok {
		return p.DeviceNAA()
	}
	return [8]byte{}
}

// --- gaugeSource callbacks ---

func (gs *gaugeSource) walUsedFraction() float64 {
	return gs.vol.WALUsedFraction()
}

func (gs *gaugeSource) dirtyMapEntries() float64 {
	return float64(gs.vol.DirtyMapLen())
}

func (gs *gaugeSource) epoch() float64 {
	return float64(gs.vol.Status().Epoch)
}

func (gs *gaugeSource) role() float64 {
	return float64(gs.vol.Role())
}

func (gs *gaugeSource) snapshotCount() float64 {
	return float64(len(gs.vol.ListSnapshots()))
}
