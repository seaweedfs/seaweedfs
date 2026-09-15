package dash

import (
	"sort"
	"strings"
	"time"
)

// Metric names scraped from each component, as registered in weed/stats.
const (
	mMasterUnderReplicated = "SeaweedFS_master_under_replicated_volumes"
	mMasterWritable        = "SeaweedFS_master_volume_layout_writable"
	mMasterCrowded         = "SeaweedFS_master_volume_layout_crowded"
	mMasterHeartbeats      = "SeaweedFS_master_received_heartbeats"
	mMasterVolumeCreation  = "SeaweedFS_master_volume_creation_total"
	mMasterLeaderChanges   = "SeaweedFS_master_leader_changes"
	mMasterIsLeader        = "SeaweedFS_master_is_leader"
	mMasterPlacementMiss   = "SeaweedFS_master_replica_placement_mismatch"

	mVolumeRequests   = "SeaweedFS_volumeServer_request_total"
	mVolumeLatency    = "SeaweedFS_volumeServer_request_seconds"
	mVolumeResource   = "SeaweedFS_volumeServer_resource"
	mVolumeDiskError  = "SeaweedFS_volumeServer_disk_error_status"
	mVolumeIOError    = "SeaweedFS_volumeServer_storage_io_error_total"
	mVolumeQuarantine = "SeaweedFS_volumeServer_io_quarantine"

	mFilerRequests = "SeaweedFS_filer_request_total"
	mFilerLatency  = "SeaweedFS_filer_request_seconds"
	mFilerInFlight = "SeaweedFS_filer_in_flight_requests"
	mFilerStoreLat = "SeaweedFS_filerStore_request_seconds"
	mFilerSyncLag  = "SeaweedFS_filerSync_lag_seconds"

	mS3Requests = "SeaweedFS_s3_request_total"
	mS3Latency  = "SeaweedFS_s3_request_seconds"

	mAdminTasksByStatus = "SeaweedFS_admin_maintenance_tasks_by_status"
	mAdminTasksDone     = "SeaweedFS_admin_maintenance_tasks_completed_total"
	mAdminWorkerSlots   = "SeaweedFS_admin_worker_slots"
	mAdminWorkers       = "SeaweedFS_admin_workers_connected"
)

// Scrape source components.
const (
	srcMaster = "master"
	srcVolume = "volume"
	srcFiler  = "filer"
	srcS3     = "s3"
	srcAdmin  = "admin/local"
)

type MonitoringData struct {
	LastUpdated   time.Time
	Overview      MonitoringOverview
	VolumeServers []MonitoringVolumeServer
	Filers        []MonitoringFiler
	S3            []MonitoringS3
	Masters       []MonitoringMaster
	Workers       MonitoringWorkers
}

type MonitoringOverview struct {
	UnderReplicatedVolumes []float64
	WritableVolumes        []float64
	CrowdedVolumes         []float64
	DiskUsagePct           []float64

	VolumeReadRate   []float64
	VolumeWriteRate  []float64
	FilerRequestRate []float64
	S3RequestRate    []float64

	VolumeP50 []float64
	VolumeP95 []float64
	VolumeP99 []float64
	FilerP50  []float64
	FilerP95  []float64
	FilerP99  []float64

	VolumeErrorRate []float64
	FilerErrorRate  []float64
	S3ErrorRate     []float64
	DiskErrors      []float64
	Quarantined     []float64

	QueueDepth []float64
	SlotsUsed  []float64
	SlotsMax   []float64
}

type MonitoringVolumeServer struct {
	Address      string
	RequestRate  []float64
	P99          []float64
	DiskUsagePct []float64
	ErrorRate    []float64
	HasData      bool
}

type MonitoringFiler struct {
	Address     string
	RequestRate []float64
	P99         []float64
	StoreP99    []float64
	InFlight    []float64
	SyncLag     []float64
	HasData     bool
}

type MonitoringS3 struct {
	Address     string
	RequestRate []float64
	Errors4xx   []float64
	Errors5xx   []float64
	P99         []float64
	HasData     bool
}

type MonitoringMaster struct {
	Address        string
	IsLeader       bool
	HeartbeatRate  []float64
	VolumeCreation []float64
	LeaderChanges  []float64
	PlacementMiss  []float64
	HasData        bool
}

type MonitoringWorkers struct {
	QueueDepth []float64
	SlotsUsed  []float64
	SlotsMax   []float64
	TaskRate   []float64
	Connected  int
	Workers    []MonitoringWorker
}

type MonitoringWorker struct {
	ID string
}

func (s *AdminServer) GetMonitoringData() *MonitoringData {
	d := &MonitoringData{LastUpdated: time.Now()}
	s.fillOverview(d)
	s.fillVolumeServers(d)
	s.fillFilers(d)
	s.fillS3(d)
	s.fillMasters(d)
	s.fillWorkers(d)
	return d
}

// isErrorCode reports whether a request_total series carries a 4xx/5xx code.
func isErrorCode(labels map[string]string) bool {
	code := labels["code"]
	return strings.HasPrefix(code, "4") || strings.HasPrefix(code, "5")
}

func hasLabel(key, value string) func(map[string]string) bool {
	return func(labels map[string]string) bool { return labels[key] == value }
}

func (s *AdminServer) fillOverview(d *MonitoringData) {
	o := &d.Overview
	o.UnderReplicatedVolumes = s.sum(srcMaster, mMasterUnderReplicated)
	o.WritableVolumes = s.sum(srcMaster, mMasterWritable)
	o.CrowdedVolumes = s.sum(srcMaster, mMasterCrowded)
	o.DiskUsagePct = s.diskUsagePct(srcVolume)

	o.VolumeReadRate = s.sumFiltered(srcVolume, mVolumeRequests+suffixRate, hasLabel("type", "read"))
	o.VolumeWriteRate = s.sumFiltered(srcVolume, mVolumeRequests+suffixRate, hasLabel("type", "write"))
	o.FilerRequestRate = s.sum(srcFiler, mFilerRequests+suffixRate)
	o.S3RequestRate = s.sum(srcS3, mS3Requests+suffixRate)

	o.VolumeP50 = s.max(srcVolume, mVolumeLatency+suffixP50)
	o.VolumeP95 = s.max(srcVolume, mVolumeLatency+suffixP95)
	o.VolumeP99 = s.max(srcVolume, mVolumeLatency+suffixP99)
	o.FilerP50 = s.max(srcFiler, mFilerLatency+suffixP50)
	o.FilerP95 = s.max(srcFiler, mFilerLatency+suffixP95)
	o.FilerP99 = s.max(srcFiler, mFilerLatency+suffixP99)

	o.VolumeErrorRate = s.sumFiltered(srcVolume, mVolumeRequests+suffixRate, isErrorCode)
	o.FilerErrorRate = s.sumFiltered(srcFiler, mFilerRequests+suffixRate, isErrorCode)
	o.S3ErrorRate = s.sumFiltered(srcS3, mS3Requests+suffixRate, isErrorCode)
	o.DiskErrors = s.sum(srcVolume, mVolumeDiskError)
	o.Quarantined = s.sum(srcVolume, mVolumeQuarantine)

	o.QueueDepth = s.sumFiltered(srcAdmin, mAdminTasksByStatus, func(l map[string]string) bool {
		return l["status"] == "pending" || l["status"] == "assigned" || l["status"] == "in_progress"
	})
	o.SlotsUsed = s.sumFiltered(srcAdmin, mAdminWorkerSlots, hasLabel("state", "used"))
	o.SlotsMax = s.sumFiltered(srcAdmin, mAdminWorkerSlots, hasLabel("state", "max"))
}

func (s *AdminServer) fillVolumeServers(d *MonitoringData) {
	for _, addr := range s.sourceAddresses(srcVolume) {
		src := srcVolume + "/" + addr
		vs := MonitoringVolumeServer{
			Address:      addr,
			RequestRate:  s.sum(src, mVolumeRequests+suffixRate),
			P99:          s.max(src, mVolumeLatency+suffixP99),
			DiskUsagePct: s.diskUsagePct(src),
			ErrorRate:    s.sumFiltered(src, mVolumeRequests+suffixRate, isErrorCode),
		}
		vs.HasData = len(vs.RequestRate) > 0 || len(vs.DiskUsagePct) > 0
		d.VolumeServers = append(d.VolumeServers, vs)
	}
}

func (s *AdminServer) fillFilers(d *MonitoringData) {
	for _, addr := range s.sourceAddresses(srcFiler) {
		src := srcFiler + "/" + addr
		f := MonitoringFiler{
			Address:     addr,
			RequestRate: s.sum(src, mFilerRequests+suffixRate),
			P99:         s.max(src, mFilerLatency+suffixP99),
			StoreP99:    s.max(src, mFilerStoreLat+suffixP99),
			InFlight:    s.sum(src, mFilerInFlight),
			SyncLag:     s.max(src, mFilerSyncLag),
		}
		f.HasData = len(f.RequestRate) > 0 || len(f.InFlight) > 0
		d.Filers = append(d.Filers, f)
	}
}

func (s *AdminServer) fillS3(d *MonitoringData) {
	for _, addr := range s.sourceAddresses(srcS3) {
		src := srcS3 + "/" + addr
		n := MonitoringS3{
			Address:     addr,
			RequestRate: s.sum(src, mS3Requests+suffixRate),
			Errors4xx: s.sumFiltered(src, mS3Requests+suffixRate, func(l map[string]string) bool {
				return strings.HasPrefix(l["code"], "4")
			}),
			Errors5xx: s.sumFiltered(src, mS3Requests+suffixRate, func(l map[string]string) bool {
				return strings.HasPrefix(l["code"], "5")
			}),
			P99: s.max(src, mS3Latency+suffixP99),
		}
		n.HasData = len(n.RequestRate) > 0
		d.S3 = append(d.S3, n)
	}
}

func (s *AdminServer) fillMasters(d *MonitoringData) {
	leaders := map[string]bool{}
	if topo, err := s.GetClusterTopology(); err == nil && topo != nil {
		for _, m := range topo.Masters {
			leaders[m.Address] = m.IsLeader
		}
	}
	for _, addr := range s.sourceAddresses(srcMaster) {
		src := srcMaster + "/" + addr
		m := MonitoringMaster{
			Address:        addr,
			IsLeader:       leaders[addr],
			HeartbeatRate:  s.sum(src, mMasterHeartbeats+suffixRate),
			VolumeCreation: s.sum(src, mMasterVolumeCreation+suffixRate),
			LeaderChanges:  s.sum(src, mMasterLeaderChanges+suffixRate),
			PlacementMiss:  s.sum(src, mMasterPlacementMiss),
		}
		m.HasData = len(m.HeartbeatRate) > 0 || len(m.PlacementMiss) > 0
		d.Masters = append(d.Masters, m)
	}
}

func (s *AdminServer) fillWorkers(d *MonitoringData) {
	w := &d.Workers
	w.QueueDepth = d.Overview.QueueDepth
	w.SlotsUsed = d.Overview.SlotsUsed
	w.SlotsMax = d.Overview.SlotsMax
	w.TaskRate = s.sum(srcAdmin, mAdminTasksDone+suffixRate)
	if s.workerGrpcServer == nil {
		return
	}
	ids := s.workerGrpcServer.GetConnectedWorkers()
	sort.Strings(ids)
	w.Connected = len(ids)
	for _, id := range ids {
		w.Workers = append(w.Workers, MonitoringWorker{ID: id})
	}
}

// sourceAddresses lists the scraped server addresses for a component, sorted.
func (s *AdminServer) sourceAddresses(component string) []string {
	seen := map[string]bool{}
	for _, t := range s.scrapeTargets() {
		if strings.HasPrefix(t.source, component+"/") {
			seen[strings.TrimPrefix(t.source, component+"/")] = true
		}
	}
	out := make([]string, 0, len(seen))
	for addr := range seen {
		out = append(out, addr)
	}
	sort.Strings(out)
	return out
}

func (s *AdminServer) sum(source, metric string) []float64 {
	return s.sumFiltered(source, metric, nil)
}

// sumFiltered adds every matching series together per sample timestamp. Use it
// for counts and rates, which are additive across servers and labels.
func (s *AdminServer) sumFiltered(source, metric string, keep func(map[string]string) bool) []float64 {
	return s.reduce(source, metric, keep, func(acc, v float64) float64 { return acc + v })
}

// max takes the worst value per timestamp. Use it for latency quantiles, which
// cannot be summed meaningfully across servers.
func (s *AdminServer) max(source, metric string) []float64 {
	return s.reduce(source, metric, nil, func(acc, v float64) float64 {
		if v > acc {
			return v
		}
		return acc
	})
}

func (s *AdminServer) reduce(source, metric string, keep func(map[string]string) bool, combine func(acc, v float64) float64) []float64 {
	matches := s.metricsStore.matchFiltered(source, metric, keep)
	if len(matches) == 0 {
		return nil
	}
	byTime := map[time.Time]float64{}
	var times []time.Time
	for _, ser := range matches {
		for _, sm := range ser.snapshot() {
			if _, ok := byTime[sm.t]; !ok {
				times = append(times, sm.t)
			}
			byTime[sm.t] = combine(byTime[sm.t], sm.values[""])
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	out := make([]float64, len(times))
	for i, t := range times {
		out[i] = byTime[t]
	}
	return out
}

// diskUsagePct derives used/total disk as a percentage from the volume server
// resource gauge, which reports bytes per mount under type=used and type=all.
func (s *AdminServer) diskUsagePct(source string) []float64 {
	used := s.sumFiltered(source, mVolumeResource, hasLabel("type", "used"))
	all := s.sumFiltered(source, mVolumeResource, hasLabel("type", "all"))
	n := len(used)
	if len(all) < n {
		n = len(all)
	}
	if n == 0 {
		return nil
	}
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		if all[i] > 0 {
			out[i] = used[i] / all[i] * 100
		}
	}
	return out
}
