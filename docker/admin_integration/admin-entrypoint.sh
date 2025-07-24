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

func (s *AdminServer) registerWorkerHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != "POST" {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    var worker Worker
    if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    worker.LastSeen = time.Now()
    worker.Status = "active"
    
    // Check if worker already exists, update if so
    found := false
    for i, w := range s.workers {
        if w.ID == worker.ID {
            s.workers[i] = worker
            found = true
            break
        }
    }
    
    if !found {
        s.workers = append(s.workers, worker)
        log.Printf("Registered new worker: %s with capabilities: %v", worker.ID, worker.Capabilities)
    } else {
        log.Printf("Updated worker: %s", worker.ID)
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
}

func (s *AdminServer) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != "POST" {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    var heartbeat struct {
        WorkerID string `json:"worker_id"`
        Status   string `json:"status"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    // Update worker last seen time
    for i, w := range s.workers {
        if w.ID == heartbeat.WorkerID {
            s.workers[i].LastSeen = time.Now()
            s.workers[i].Status = heartbeat.Status
            break
        }
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *AdminServer) assignTaskHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != "POST" {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    var request struct {
        WorkerID     string   `json:"worker_id"`
        Capabilities []string `json:"capabilities"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    // Find a pending task that matches worker capabilities
    for i, task := range s.tasks {
        if task.Status == "pending" {
            // Assign task to worker
            s.tasks[i].Status = "assigned"
            
            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(map[string]interface{}{
                "task_id":    task.ID,
                "type":       task.Type,
                "volume_id":  task.VolumeID,
                "parameters": map[string]interface{}{
                    "server": "volume1:8080",  // Simplified assignment
                },
            })
            
            log.Printf("Assigned task %s to worker %s", task.ID, request.WorkerID)
            return
        }
    }
    
    // No tasks available
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"status": "no_tasks"})
}

func (s *AdminServer) taskProgressHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != "POST" {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    var progress struct {
        TaskID   string  `json:"task_id"`
        Progress float64 `json:"progress"`
        Status   string  `json:"status"`
        Message  string  `json:"message"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&progress); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    // Update task progress
    for i, task := range s.tasks {
        if task.ID == progress.TaskID {
            s.tasks[i].Progress = progress.Progress
            s.tasks[i].Status = progress.Status
            
            log.Printf("Task %s: %.1f%% - %s", progress.TaskID, progress.Progress, progress.Message)
            break
        }
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

func (s *AdminServer) webUIHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/html")
    
    html := `<!DOCTYPE html>
<html>
<head>
    <title>SeaweedFS Admin - EC Task Monitor</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0; padding: 20px; background: #f5f5f5;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { 
            background: #2c3e50; color: white; padding: 20px; border-radius: 8px; 
            margin-bottom: 20px; text-align: center;
        }
        .stats { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; margin-bottom: 20px;
        }
        .stat-card { 
            background: white; padding: 20px; border-radius: 8px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center;
        }
        .stat-number { font-size: 2em; font-weight: bold; color: #3498db; }
        .stat-label { color: #7f8c8d; margin-top: 5px; }
        .section { 
            background: white; border-radius: 8px; margin-bottom: 20px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;
        }
        .section-header { 
            background: #34495e; color: white; padding: 15px 20px; 
            font-weight: bold; font-size: 1.1em;
        }
        .section-content { padding: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ecf0f1; }
        th { background: #f8f9fa; font-weight: 600; }
        .status-pending { color: #f39c12; font-weight: bold; }
        .status-assigned { color: #3498db; font-weight: bold; }
        .status-running { color: #e67e22; font-weight: bold; }
        .status-completed { color: #27ae60; font-weight: bold; }
        .status-failed { color: #e74c3c; font-weight: bold; }
        .progress-bar { 
            background: #ecf0f1; height: 20px; border-radius: 10px; 
            overflow: hidden; position: relative;
        }
        .progress-fill { 
            height: 100%; background: linear-gradient(90deg, #3498db, #2ecc71); 
            transition: width 0.3s ease;
        }
        .progress-text { 
            position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
            font-size: 0.8em; font-weight: bold; color: white; text-shadow: 1px 1px 2px rgba(0,0,0,0.5);
        }
        .worker-online { color: #27ae60; }
        .worker-offline { color: #e74c3c; }
        .refresh-btn { 
            background: #3498db; color: white; padding: 10px 20px; 
            border: none; border-radius: 5px; cursor: pointer; margin-bottom: 20px;
        }
        .refresh-btn:hover { background: #2980b9; }
        .last-updated { color: #7f8c8d; font-size: 0.9em; text-align: center; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧪 SeaweedFS EC Task Monitor</h1>
            <p>Real-time Erasure Coding Task Management Dashboard</p>
        </div>
        
        <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>
        
        <div class="stats" id="stats">
            <div class="stat-card">
                <div class="stat-number" id="total-tasks">0</div>
                <div class="stat-label">Total Tasks</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="active-workers">0</div>
                <div class="stat-label">Active Workers</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="completed-tasks">0</div>
                <div class="stat-label">Completed</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="uptime">--</div>
                <div class="stat-label">Uptime</div>
            </div>
        </div>
        
        <div class="section">
            <div class="section-header">📋 EC Tasks</div>
            <div class="section-content">
                <table>
                    <thead>
                        <tr>
                            <th>Task ID</th>
                            <th>Type</th>
                            <th>Volume ID</th>
                            <th>Status</th>
                            <th>Progress</th>
                            <th>Created</th>
                        </tr>
                    </thead>
                    <tbody id="tasks-table">
                        <tr><td colspan="6" style="text-align: center; color: #7f8c8d;">Loading tasks...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="section">
            <div class="section-header">⚙️ Workers</div>
            <div class="section-content">
                <table>
                    <thead>
                        <tr>
                            <th>Worker ID</th>
                            <th>Address</th>
                            <th>Status</th>
                            <th>Capabilities</th>
                            <th>Last Seen</th>
                        </tr>
                    </thead>
                    <tbody id="workers-table">
                        <tr><td colspan="5" style="text-align: center; color: #7f8c8d;">Loading workers...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="last-updated">
            Last updated: <span id="last-updated">--</span> | 
            Auto-refresh every 5 seconds
        </div>
    </div>

    <script>
        function formatTime(timestamp) {
            return new Date(timestamp).toLocaleString();
        }
        
        function formatUptime(uptime) {
            if (!uptime) return '--';
            const matches = uptime.match(/(\d+h)?(\d+m)?(\d+\.?\d*s)?/);
            if (!matches) return uptime;
            
            let parts = [];
            if (matches[1]) parts.push(matches[1]);
            if (matches[2]) parts.push(matches[2]);
            if (matches[3] && !matches[1]) parts.push(matches[3]);
            
            return parts.join(' ') || uptime;
        }
        
        function updateData() {
            fetch('/status')
                .then(response => response.json())
                .then(data => {
                    // Update stats
                    document.getElementById('total-tasks').textContent = data.tasks.length;
                    document.getElementById('active-workers').textContent = data.workers.length;
                                         document.getElementById('completed-tasks').textContent = 
                         data.tasks.filter(function(t) { return t.status === 'completed'; }).length;
                    document.getElementById('uptime').textContent = formatUptime(data.uptime);
                    
                    // Update tasks table
                    const tasksTable = document.getElementById('tasks-table');
                    if (data.tasks.length === 0) {
                        tasksTable.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #7f8c8d;">No tasks available</td></tr>';
                    } else {
                                             tasksTable.innerHTML = data.tasks.map(task => '<tr>' +
                             '<td><code>' + task.id + '</code></td>' +
                             '<td>' + task.type + '</td>' +
                             '<td>' + task.volume_id + '</td>' +
                             '<td><span class="status-' + task.status + '">' + task.status.toUpperCase() + '</span></td>' +
                             '<td><div class="progress-bar">' +
                                 '<div class="progress-fill" style="width: ' + task.progress + '%"></div>' +
                                 '<div class="progress-text">' + task.progress.toFixed(1) + '%</div>' +
                             '</div></td>' +
                             '<td>' + formatTime(task.created) + '</td>' +
                         '</tr>').join('');
                    }
                    
                    // Update workers table
                    const workersTable = document.getElementById('workers-table');
                    if (data.workers.length === 0) {
                        workersTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #7f8c8d;">No workers registered</td></tr>';
                                         } else {
                         workersTable.innerHTML = data.workers.map(worker => '<tr>' +
                             '<td><strong>' + worker.id + '</strong></td>' +
                             '<td>' + (worker.address || 'N/A') + '</td>' +
                             '<td><span class="worker-' + (worker.status === 'active' ? 'online' : 'offline') + '">' + worker.status.toUpperCase() + '</span></td>' +
                             '<td>' + worker.capabilities.join(', ') + '</td>' +
                             '<td>' + formatTime(worker.last_seen) + '</td>' +
                         '</tr>').join('');
                    }
                    
                    document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
                })
                .catch(error => {
                    console.error('Failed to fetch data:', error);
                });
        }
        
        // Initial load
        updateData();
        
        // Auto-refresh every 5 seconds
        setInterval(updateData, 5000);
    </script>
</body>
</html>`
    
    fmt.Fprint(w, html)
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
    http.HandleFunc("/register", server.registerWorkerHandler)
    http.HandleFunc("/register-worker", server.registerWorkerHandler)  // Worker compatibility
    http.HandleFunc("/heartbeat", server.heartbeatHandler)
    http.HandleFunc("/assign-task", server.assignTaskHandler)
    http.HandleFunc("/task-progress", server.taskProgressHandler)
    http.HandleFunc("/", server.webUIHandler)  // Web UI
    
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