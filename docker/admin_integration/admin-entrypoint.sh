#!/bin/sh

set -e

echo "Starting SeaweedFS Admin Server..."
echo "Master Address: $MASTER_ADDRESS"
echo "Admin Port: $ADMIN_PORT"
echo "Scan Interval: $SCAN_INTERVAL"

# Wait for master to be ready
echo "Waiting for master to be ready..."
until curl -f http://$MASTER_ADDRESS/cluster/status > /dev/null 2>&1; do
    echo "Master not ready, waiting..."
    sleep 5
done
echo "Master is ready!"

# For now, use a simple HTTP server to simulate admin functionality
# In a real implementation, this would start the actual admin server
cat > /tmp/admin_server.go << 'EOF'
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "strconv"
    "time"
)

type AdminServer struct {
    masterAddr string
    port       string
    startTime  time.Time
    tasks      []Task
    workers    []Worker
}

type Task struct {
    ID       string    `json:"id"`
    Type     string    `json:"type"`
    VolumeID int       `json:"volume_id"`
    Status   string    `json:"status"`
    Progress float64   `json:"progress"`
    Created  time.Time `json:"created"`
}

type Worker struct {
    ID           string    `json:"id"`
    Address      string    `json:"address"`
    Capabilities []string  `json:"capabilities"`
    Status       string    `json:"status"`
    LastSeen     time.Time `json:"last_seen"`
}

func (s *AdminServer) healthHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status": "healthy",
        "uptime": time.Since(s.startTime).String(),
        "tasks":  len(s.tasks),
        "workers": len(s.workers),
    })
}

func (s *AdminServer) statusHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "admin_server": "running",
        "master_addr":  s.masterAddr,
        "tasks":        s.tasks,
        "workers":      s.workers,
        "uptime":       time.Since(s.startTime).String(),
    })
}

func (s *AdminServer) detectVolumesForEC() {
    // Simulate volume detection logic
    // In real implementation, this would query the master for volume status
    ticker := time.NewTicker(30 * time.Second)
    go func() {
        for range ticker.C {
            log.Println("Scanning for volumes requiring EC...")
            
            // Check master for volume status
            resp, err := http.Get(fmt.Sprintf("http://%s/vol/status", s.masterAddr))
            if err != nil {
                log.Printf("Error checking master: %v", err)
                continue
            }
            resp.Body.Close()
            
            // Simulate detecting a volume that needs EC
            if len(s.tasks) < 5 { // Don't create too many tasks
                taskID := fmt.Sprintf("ec-task-%d", len(s.tasks)+1)
                volumeID := 1000 + len(s.tasks)
                
                task := Task{
                    ID:       taskID,
                    Type:     "erasure_coding",
                    VolumeID: volumeID,
                    Status:   "pending",
                    Progress: 0.0,
                    Created:  time.Now(),
                }
                
                s.tasks = append(s.tasks, task)
                log.Printf("Created EC task %s for volume %d", taskID, volumeID)
            }
        }
    }()
}

func main() {
    masterAddr := os.Getenv("MASTER_ADDRESS")
    if masterAddr == "" {
        masterAddr = "master:9333"
    }
    
    port := os.Getenv("ADMIN_PORT")
    if port == "" {
        port = "9900"
    }
    
    server := &AdminServer{
        masterAddr: masterAddr,
        port:       port,
        startTime:  time.Now(),
        tasks:      make([]Task, 0),
        workers:    make([]Worker, 0),
    }
    
    http.HandleFunc("/health", server.healthHandler)
    http.HandleFunc("/status", server.statusHandler)
    
    // Start volume detection
    server.detectVolumesForEC()
    
    log.Printf("Admin server starting on port %s", port)
    log.Printf("Master address: %s", masterAddr)
    
    if err := http.ListenAndServe(":"+port, nil); err != nil {
        log.Fatal("Server failed to start:", err)
    }
}
EOF

# Compile and run the admin server
cd /tmp
go mod init admin-server
go run admin_server.go 