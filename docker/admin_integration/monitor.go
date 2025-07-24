package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type Monitor struct {
	masterAddr string
	adminAddr  string
	filerAddr  string
	interval   time.Duration
	startTime  time.Time
	stats      MonitorStats
}

type MonitorStats struct {
	TotalChecks     int64
	MasterHealthy   int64
	AdminHealthy    int64
	FilerHealthy    int64
	VolumeCount     int64
	LastVolumeCheck time.Time
	ECTasksDetected int64
	WorkersActive   int64
	LastWorkerCheck time.Time
}

type ClusterStatus struct {
	IsLeader bool     `json:"IsLeader"`
	Leader   string   `json:"Leader"`
	Peers    []string `json:"Peers"`
}

type VolumeStatus struct {
	Volumes []VolumeInfo `json:"Volumes"`
}

type VolumeInfo struct {
	Id               uint32 `json:"Id"`
	Size             uint64 `json:"Size"`
	Collection       string `json:"Collection"`
	FileCount        int64  `json:"FileCount"`
	DeleteCount      int64  `json:"DeleteCount"`
	DeletedByteCount uint64 `json:"DeletedByteCount"`
	ReadOnly         bool   `json:"ReadOnly"`
	CompactRevision  uint32 `json:"CompactRevision"`
	Version          uint32 `json:"Version"`
}

type AdminStatus struct {
	Status  string `json:"status"`
	Uptime  string `json:"uptime"`
	Tasks   int    `json:"tasks"`
	Workers int    `json:"workers"`
}

// checkMasterHealth checks the master server health
func (m *Monitor) checkMasterHealth() bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/cluster/status", m.masterAddr))
	if err != nil {
		log.Printf("ERROR: Cannot reach master %s: %v", m.masterAddr, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: Master returned status %d", resp.StatusCode)
		return false
	}

	var status ClusterStatus
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("ERROR: Cannot read master response: %v", err)
		return false
	}

	err = json.Unmarshal(body, &status)
	if err != nil {
		log.Printf("WARNING: Cannot parse master status: %v", err)
		// Still consider it healthy if we got a response
		return true
	}

	log.Printf("Master status: Leader=%s, IsLeader=%t, Peers=%d",
		status.Leader, status.IsLeader, len(status.Peers))

	m.stats.MasterHealthy++
	return true
}

// checkAdminHealth checks the admin server health
func (m *Monitor) checkAdminHealth() bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/health", m.adminAddr))
	if err != nil {
		log.Printf("ERROR: Cannot reach admin %s: %v", m.adminAddr, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: Admin returned status %d", resp.StatusCode)
		return false
	}

	var status AdminStatus
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("ERROR: Cannot read admin response: %v", err)
		return false
	}

	err = json.Unmarshal(body, &status)
	if err != nil {
		log.Printf("WARNING: Cannot parse admin status: %v", err)
		return true
	}

	log.Printf("Admin status: %s, Uptime=%s, Tasks=%d, Workers=%d",
		status.Status, status.Uptime, status.Tasks, status.Workers)

	m.stats.AdminHealthy++
	m.stats.ECTasksDetected += int64(status.Tasks)
	m.stats.WorkersActive = int64(status.Workers)
	m.stats.LastWorkerCheck = time.Now()

	return true
}

// checkFilerHealth checks the filer health
func (m *Monitor) checkFilerHealth() bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/", m.filerAddr))
	if err != nil {
		log.Printf("ERROR: Cannot reach filer %s: %v", m.filerAddr, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: Filer returned status %d", resp.StatusCode)
		return false
	}

	m.stats.FilerHealthy++
	return true
}

// checkVolumeStatus checks volume information from master
func (m *Monitor) checkVolumeStatus() {
	resp, err := http.Get(fmt.Sprintf("http://%s/vol/status", m.masterAddr))
	if err != nil {
		log.Printf("ERROR: Cannot get volume status: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: Volume status returned status %d", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("ERROR: Cannot read volume status: %v", err)
		return
	}

	var volumeStatus VolumeStatus
	err = json.Unmarshal(body, &volumeStatus)
	if err != nil {
		log.Printf("WARNING: Cannot parse volume status: %v", err)
		return
	}

	m.stats.VolumeCount = int64(len(volumeStatus.Volumes))
	m.stats.LastVolumeCheck = time.Now()

	// Analyze volumes
	var readOnlyCount, fullVolumeCount, ecCandidates int
	var totalSize, totalFiles uint64

	for _, vol := range volumeStatus.Volumes {
		totalSize += vol.Size
		totalFiles += uint64(vol.FileCount)

		if vol.ReadOnly {
			readOnlyCount++
		}

		// Volume is close to full (>40MB for 50MB limit)
		if vol.Size > 40*1024*1024 {
			fullVolumeCount++
			if !vol.ReadOnly {
				ecCandidates++
			}
		}
	}

	log.Printf("Volume analysis: Total=%d, ReadOnly=%d, Full=%d, EC_Candidates=%d",
		len(volumeStatus.Volumes), readOnlyCount, fullVolumeCount, ecCandidates)
	log.Printf("Storage stats: Total_Size=%.2fMB, Total_Files=%d",
		float64(totalSize)/(1024*1024), totalFiles)

	if ecCandidates > 0 {
		log.Printf("⚠️  DETECTED %d volumes that should be EC'd!", ecCandidates)
	}
}

// healthHandler provides a health endpoint for the monitor itself
func (m *Monitor) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "healthy",
		"uptime":     time.Since(m.startTime).String(),
		"checks":     m.stats.TotalChecks,
		"last_check": m.stats.LastVolumeCheck.Format(time.RFC3339),
	})
}

// statusHandler provides detailed monitoring status
func (m *Monitor) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"monitor": map[string]interface{}{
			"uptime":      time.Since(m.startTime).String(),
			"master_addr": m.masterAddr,
			"admin_addr":  m.adminAddr,
			"filer_addr":  m.filerAddr,
			"interval":    m.interval.String(),
		},
		"stats": m.stats,
		"health": map[string]interface{}{
			"master_healthy": m.stats.MasterHealthy > 0 && time.Since(m.stats.LastVolumeCheck) < 2*m.interval,
			"admin_healthy":  m.stats.AdminHealthy > 0 && time.Since(m.stats.LastWorkerCheck) < 2*m.interval,
			"filer_healthy":  m.stats.FilerHealthy > 0,
		},
	})
}

// runMonitoring runs the main monitoring loop
func (m *Monitor) runMonitoring() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	log.Printf("Starting monitoring loop every %v", m.interval)

	for {
		m.stats.TotalChecks++

		log.Printf("=== Monitoring Check #%d ===", m.stats.TotalChecks)

		// Check master health
		if m.checkMasterHealth() {
			// If master is healthy, check volumes
			m.checkVolumeStatus()
		}

		// Check admin health
		m.checkAdminHealth()

		// Check filer health
		m.checkFilerHealth()

		// Print summary
		log.Printf("Health Summary: Master=%t, Admin=%t, Filer=%t, Volumes=%d, Workers=%d",
			m.stats.MasterHealthy > 0,
			m.stats.AdminHealthy > 0,
			m.stats.FilerHealthy > 0,
			m.stats.VolumeCount,
			m.stats.WorkersActive)

		log.Printf("=== End Check #%d ===", m.stats.TotalChecks)

		<-ticker.C
	}
}

func main() {
	masterAddr := os.Getenv("MASTER_ADDRESS")
	if masterAddr == "" {
		masterAddr = "master:9333"
	}

	adminAddr := os.Getenv("ADMIN_ADDRESS")
	if adminAddr == "" {
		adminAddr = "admin:9900"
	}

	filerAddr := os.Getenv("FILER_ADDRESS")
	if filerAddr == "" {
		filerAddr = "filer:8888"
	}

	intervalStr := os.Getenv("MONITOR_INTERVAL")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		interval = 10 * time.Second
	}

	monitor := &Monitor{
		masterAddr: masterAddr,
		adminAddr:  adminAddr,
		filerAddr:  filerAddr,
		interval:   interval,
		startTime:  time.Now(),
		stats:      MonitorStats{},
	}

	log.Printf("Starting SeaweedFS Cluster Monitor")
	log.Printf("Master: %s", masterAddr)
	log.Printf("Admin: %s", adminAddr)
	log.Printf("Filer: %s", filerAddr)
	log.Printf("Interval: %v", interval)

	// Setup HTTP endpoints
	http.HandleFunc("/health", monitor.healthHandler)
	http.HandleFunc("/status", monitor.statusHandler)

	// Start HTTP server in background
	go func() {
		log.Println("Monitor HTTP server starting on :9999")
		if err := http.ListenAndServe(":9999", nil); err != nil {
			log.Printf("Monitor HTTP server error: %v", err)
		}
	}()

	// Wait for services to be ready
	log.Println("Waiting for services to be ready...")
	for {
		masterOK := false
		adminOK := false
		filerOK := false

		if resp, err := http.Get(fmt.Sprintf("http://%s/cluster/status", masterAddr)); err == nil && resp.StatusCode == http.StatusOK {
			masterOK = true
			resp.Body.Close()
		}

		if resp, err := http.Get(fmt.Sprintf("http://%s/health", adminAddr)); err == nil && resp.StatusCode == http.StatusOK {
			adminOK = true
			resp.Body.Close()
		}

		if resp, err := http.Get(fmt.Sprintf("http://%s/", filerAddr)); err == nil && resp.StatusCode == http.StatusOK {
			filerOK = true
			resp.Body.Close()
		}

		if masterOK && adminOK && filerOK {
			log.Println("All services are ready!")
			break
		}

		log.Printf("Services ready: Master=%t, Admin=%t, Filer=%t", masterOK, adminOK, filerOK)
		time.Sleep(5 * time.Second)
	}

	// Start monitoring
	monitor.runMonitoring()
}
