#!/bin/sh

set -e

echo "Starting SeaweedFS EC Worker..."
echo "Worker ID: $WORKER_ID"
echo "Admin Address: $ADMIN_ADDRESS"
echo "Capabilities: $CAPABILITIES"

# Wait for admin server to be ready
echo "Waiting for admin server to be ready..."
until curl -f http://$ADMIN_ADDRESS/health > /dev/null 2>&1; do
    echo "Admin server not ready, waiting..."
    sleep 5
done
echo "Admin server is ready!"

# Create worker simulation
cat > /tmp/worker.go << 'EOF'
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "strings"
    "time"
)

type Worker struct {
    id           string
    adminAddr    string
    address      string
    capabilities []string
    maxConcurrent int
    workDir      string
    startTime    time.Time
    activeTasks  map[string]*Task
}

type Task struct {
    ID       string    `json:"id"`
    Type     string    `json:"type"`
    VolumeID int       `json:"volume_id"`
    Status   string    `json:"status"`
    Progress float64   `json:"progress"`
    Started  time.Time `json:"started"`
}

func (w *Worker) healthHandler(res http.ResponseWriter, req *http.Request) {
    res.Header().Set("Content-Type", "application/json")
    json.NewEncoder(res).Encode(map[string]interface{}{
        "status":       "healthy",
        "worker_id":    w.id,
        "uptime":       time.Since(w.startTime).String(),
        "active_tasks": len(w.activeTasks),
        "capabilities": w.capabilities,
    })
}

func (w *Worker) statusHandler(res http.ResponseWriter, req *http.Request) {
    res.Header().Set("Content-Type", "application/json")
    json.NewEncoder(res).Encode(map[string]interface{}{
        "worker_id":     w.id,
        "admin_addr":    w.adminAddr,
        "capabilities":  w.capabilities,
        "max_concurrent": w.maxConcurrent,
        "active_tasks":  w.activeTasks,
        "uptime":        time.Since(w.startTime).String(),
    })
}

func (w *Worker) simulateECTask(taskID string, volumeID int) {
    log.Printf("Starting EC task %s for volume %d", taskID, volumeID)
    
    task := &Task{
        ID:       taskID,
        Type:     "erasure_coding",
        VolumeID: volumeID,
        Status:   "running",
        Progress: 0.0,
        Started:  time.Now(),
    }
    
    w.activeTasks[taskID] = task
    
    // Simulate EC process phases
    phases := []struct {
        progress float64
        phase    string
        duration time.Duration
    }{
        {5.0, "Copying volume data locally", 10 * time.Second},
        {25.0, "Marking volume read-only", 2 * time.Second},
        {60.0, "Performing local EC encoding", 30 * time.Second},
        {70.0, "Calculating optimal shard placement", 5 * time.Second},
        {90.0, "Distributing shards to servers", 20 * time.Second},
        {100.0, "Verification and cleanup", 3 * time.Second},
    }
    
    go func() {
        for _, phase := range phases {
            if task.Status != "running" {
                break
            }
            
            time.Sleep(phase.duration)
            task.Progress = phase.progress
            log.Printf("Task %s: %.1f%% - %s", taskID, phase.progress, phase.phase)
        }
        
        if task.Status == "running" {
            task.Status = "completed"
            task.Progress = 100.0
            log.Printf("Task %s completed successfully", taskID)
        }
        
        // Remove from active tasks after completion
        time.Sleep(5 * time.Second)
        delete(w.activeTasks, taskID)
    }()
}

func (w *Worker) registerWithAdmin() {
    ticker := time.NewTicker(30 * time.Second)
    go func() {
        for {
            // Register/heartbeat with admin server
            log.Printf("Sending heartbeat to admin server...")
            
            data := map[string]interface{}{
                "worker_id":     w.id,
                "address":       w.address,
                "capabilities":  w.capabilities,
                "max_concurrent": w.maxConcurrent,
                "active_tasks":  len(w.activeTasks),
                "status":        "active",
            }
            
            jsonData, _ := json.Marshal(data)
            
            // In real implementation, this would be a proper gRPC call
            resp, err := http.Post(
                fmt.Sprintf("http://%s/register-worker", w.adminAddr),
                "application/json",
                strings.NewReader(string(jsonData)),
            )
            if err != nil {
                log.Printf("Failed to register with admin: %v", err)
            } else {
                resp.Body.Close()
                log.Printf("Successfully sent heartbeat to admin")
            }
            
            // Simulate requesting new tasks
            if len(w.activeTasks) < w.maxConcurrent {
                // In real implementation, worker would request tasks from admin
                // For simulation, we'll create some tasks periodically
                if len(w.activeTasks) == 0 && time.Since(w.startTime) > 1*time.Minute {
                    taskID := fmt.Sprintf("%s-task-%d", w.id, time.Now().Unix())
                    volumeID := 2000 + int(time.Now().Unix()%1000)
                    w.simulateECTask(taskID, volumeID)
                }
            }
            
            <-ticker.C
        }
    }()
}

func main() {
    workerID := os.Getenv("WORKER_ID")
    if workerID == "" {
        workerID = "worker-1"
    }
    
    adminAddr := os.Getenv("ADMIN_ADDRESS")
    if adminAddr == "" {
        adminAddr = "admin:9900"
    }
    
    address := os.Getenv("WORKER_ADDRESS")
    if address == "" {
        address = "worker:9001"
    }
    
    capabilities := strings.Split(os.Getenv("CAPABILITIES"), ",")
    if len(capabilities) == 0 || capabilities[0] == "" {
        capabilities = []string{"erasure_coding"}
    }
    
    worker := &Worker{
        id:           workerID,
        adminAddr:    adminAddr,
        address:      address,
        capabilities: capabilities,
        maxConcurrent: 2,
        workDir:      "/work",
        startTime:    time.Now(),
        activeTasks:  make(map[string]*Task),
    }
    
    http.HandleFunc("/health", worker.healthHandler)
    http.HandleFunc("/status", worker.statusHandler)
    
    // Start registration and heartbeat
    worker.registerWithAdmin()
    
    log.Printf("Worker %s starting on address %s", workerID, address)
    log.Printf("Admin address: %s", adminAddr)
    log.Printf("Capabilities: %v", capabilities)
    
    port := ":9001"
    if strings.Contains(address, ":") {
        parts := strings.Split(address, ":")
        port = ":" + parts[1]
    }
    
    if err := http.ListenAndServe(port, nil); err != nil {
        log.Fatal("Worker failed to start:", err)
    }
}
EOF

# Compile and run the worker
cd /tmp
go mod init worker
go run worker.go 