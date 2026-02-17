package balance

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Detector scans for volume distribution imbalance across servers
type Detector struct {
	masterAddr string
}

// ServerVolumeInfo represents volume count on a server
type ServerVolumeInfo struct {
	ServerID       string
	Rack           string
	VolumeCount    int32
	TotalVolumeGB  int64
	AvailableGB    int64
	WriteableCount int32
}

// VolumeInfo represents a volume that can be migrated
type VolumeInfo struct {
	ID             string
	Collection     string
	SizeGB         int64
	SourceServer   string
	TargetServer   string
	Replicas       int32
	IsWriteable    bool
	LastModifiedAt time.Time
}

// NewDetector creates a new balance detector
func NewDetector(masterAddr string) *Detector {
	return &Detector{
		masterAddr: masterAddr,
	}
}

// DetectJobs identifies volume distribution imbalance and returns migration jobs
// Returns DetectedJob items with priority based on imbalance severity
func (d *Detector) DetectJobs(ctx context.Context, config *plugin_pb.JobTypeConfig) ([]*plugin_pb.DetectedJob, error) {
	var detectedJobs []*plugin_pb.DetectedJob

	// Extract configuration parameters
	imbalanceThreshold := float64(20) // default 20%
	minServers := int32(2)            // default 2
	preferRackDiversity := false      // default false

	for _, cfv := range config.AdminConfig {
		if cfv.FieldName == "imbalanceThreshold" {
			imbalanceThreshold = float64(cfv.IntValue)
		} else if cfv.FieldName == "minServers" {
			minServers = int32(cfv.IntValue)
		} else if cfv.FieldName == "preferRackDiversity" {
			preferRackDiversity = cfv.BoolValue
		}
	}

	glog.Infof("balance detector: scanning volume distribution (threshold=%.1f%%, minServers=%d, rackDiversity=%v)",
		imbalanceThreshold, minServers, preferRackDiversity)

	// Get server and volume information
	serverInfo, volumes := d.scanServerVolumes(ctx)

	if int32(len(serverInfo)) < minServers {
		glog.Infof("balance detector: insufficient servers (%d < %d), skipping detection",
			len(serverInfo), minServers)
		return detectedJobs, nil
	}

	// Calculate volume distribution imbalance
	imbalance, maxServer, minServer := d.calculateImbalance(serverInfo)

	glog.Infof("balance detector: current imbalance=%.1f%%, max=%d volumes on %s, min=%d volumes on %s",
		imbalance, maxServer.VolumeCount, maxServer.ServerID, minServer.VolumeCount, minServer.ServerID)

	// Check if imbalance exceeds threshold
	if imbalance <= imbalanceThreshold {
		glog.Infof("balance detector: imbalance %.1f%% is within threshold %.1f%%, no rebalancing needed",
			imbalance, imbalanceThreshold)
		return detectedJobs, nil
	}

	glog.Infof("balance detector: imbalance %.1f%% exceeds threshold %.1f%%, triggering rebalancing",
		imbalance, imbalanceThreshold)

	// Generate migration jobs to reduce imbalance
	migrations := d.generateMigrations(serverInfo, volumes, preferRackDiversity)

	now := time.Now()
	for i, migration := range migrations {
		priority := int64((imbalance * 1000)) - int64(i*10) // Higher imbalance = higher priority

		jobKey := fmt.Sprintf("balance_%s_to_%s_%s", migration.SourceServer, migration.TargetServer, now.Format("20060102150405"))

		job := &plugin_pb.DetectedJob{
			JobKey:  jobKey,
			JobType: "balance",
			Description: fmt.Sprintf("Migrate volume %s from %s to %s (imbalance reduction: %.1f%%)",
				migration.ID, migration.SourceServer, migration.TargetServer, imbalance),
			Priority:          priority,
			EstimatedDuration: durationpb.New(time.Duration(migration.SizeGB) * time.Second),
			Metadata: map[string]string{
				"volume_id":      migration.ID,
				"collection":     migration.Collection,
				"source_server":  migration.SourceServer,
				"target_server":  migration.TargetServer,
				"volume_size_gb": fmt.Sprintf("%d", migration.SizeGB),
				"imbalance":      fmt.Sprintf("%.2f", imbalance),
				"rack_diversity": fmt.Sprintf("%v", preferRackDiversity),
			},
			SuggestedConfig: []*plugin_pb.ConfigFieldValue{},
		}

		detectedJobs = append(detectedJobs, job)
		glog.Infof("balance detector: detected migration job %s (priority=%d)", migration.ID, priority)
	}

	glog.Infof("balance detector: found %d migration jobs to reduce imbalance", len(detectedJobs))
	return detectedJobs, nil
}

// calculateImbalance calculates volume distribution imbalance
// imbalance = (max_volumes - min_volumes) / avg_volumes * 100
func (d *Detector) calculateImbalance(servers []ServerVolumeInfo) (imbalance float64, maxServer, minServer ServerVolumeInfo) {
	if len(servers) == 0 {
		return 0, ServerVolumeInfo{}, ServerVolumeInfo{}
	}

	// Sort by volume count to find min and max
	sorted := make([]ServerVolumeInfo, len(servers))
	copy(sorted, servers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].VolumeCount < sorted[j].VolumeCount
	})

	minServer = sorted[0]
	maxServer = sorted[len(sorted)-1]

	// Calculate average
	totalVolumes := int32(0)
	for _, s := range servers {
		totalVolumes += s.VolumeCount
	}

	avgVolumes := float64(totalVolumes) / float64(len(servers))

	// Avoid division by zero
	if avgVolumes == 0 {
		return 0, maxServer, minServer
	}

	// Calculate imbalance percentage
	imbalance = float64(maxServer.VolumeCount-minServer.VolumeCount) / avgVolumes * 100

	// Ensure it's not negative
	if imbalance < 0 {
		imbalance = 0
	}

	return imbalance, maxServer, minServer
}

// generateMigrations generates a list of volume migrations to reduce imbalance
func (d *Detector) generateMigrations(servers []ServerVolumeInfo, volumes []VolumeInfo, preferRackDiversity bool) []VolumeInfo {
	var migrations []VolumeInfo

	// Sort servers by volume count (descending)
	serversCopy := make([]ServerVolumeInfo, len(servers))
	copy(serversCopy, servers)
	sort.Slice(serversCopy, func(i, j int) bool {
		return serversCopy[i].VolumeCount > serversCopy[j].VolumeCount
	})

	// Calculate target volume count (average)
	totalVolumes := int32(0)
	for _, s := range serversCopy {
		totalVolumes += s.VolumeCount
	}
	targetPerServer := totalVolumes / int32(len(serversCopy))

	// For each overloaded server, select volumes to migrate
	for _, sourceServer := range serversCopy {
		if sourceServer.VolumeCount <= targetPerServer {
			break // Rest are balanced
		}

		volumesToMove := sourceServer.VolumeCount - targetPerServer

		// Find candidate volumes on this server
		for _, vol := range volumes {
			if volumesToMove <= 0 {
				break
			}

			if vol.SourceServer != sourceServer.ServerID {
				continue
			}

			// Skip read-only volumes
			if !vol.IsWriteable {
				continue
			}

			// Find a target server (underloaded, different if rack diversity preferred)
			targetServer := d.selectTargetServer(serversCopy, sourceServer, preferRackDiversity)
			if targetServer == nil {
				continue
			}

			migration := VolumeInfo{
				ID:           vol.ID,
				Collection:   vol.Collection,
				SizeGB:       vol.SizeGB,
				SourceServer: sourceServer.ServerID,
				TargetServer: targetServer.ServerID,
				Replicas:     vol.Replicas,
				IsWriteable:  vol.IsWriteable,
			}

			migrations = append(migrations, migration)
			volumesToMove--

			// Update server counts
			sourceServer.VolumeCount--
			targetServer.VolumeCount++
		}
	}

	// Sort by volume size (largest first for priority)
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].SizeGB > migrations[j].SizeGB
	})

	return migrations
}

// selectTargetServer finds a suitable target server for migration
func (d *Detector) selectTargetServer(servers []ServerVolumeInfo, sourceServer ServerVolumeInfo, preferRackDiversity bool) *ServerVolumeInfo {
	// Sort by volume count (ascending) - pick least loaded
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].VolumeCount < servers[j].VolumeCount
	})

	for i := range servers {
		server := &servers[i]

		// Skip source server
		if server.ServerID == sourceServer.ServerID {
			continue
		}

		// If rack diversity preferred, try to pick different rack
		if preferRackDiversity && server.Rack == sourceServer.Rack {
			continue
		}

		return server
	}

	// If rack diversity is preferred but all servers are in same rack, fallback to least loaded
	if preferRackDiversity {
		for i := range servers {
			server := &servers[i]
			if server.ServerID != sourceServer.ServerID {
				return server
			}
		}
	}

	return nil
}

// scanServerVolumes performs a scan of servers and volumes from the master
// This is a placeholder that would connect to master in production
func (d *Detector) scanServerVolumes(ctx context.Context) ([]ServerVolumeInfo, []VolumeInfo) {
	// TODO: Connect to master server at d.masterAddr and get:
	// 1. List of data nodes with their rack information
	// 2. Volume distribution across nodes
	// 3. Volume metadata (size, collection, writeable status)
	//
	// For now, return empty lists as a framework
	var servers []ServerVolumeInfo
	var volumes []VolumeInfo

	return servers, volumes
}
