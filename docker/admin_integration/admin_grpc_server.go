package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type AdminServer struct {
	pb.UnimplementedWorkerServiceServer

	// Server configuration
	grpcPort   string
	httpPort   string
	masterAddr string
	startTime  time.Time

	// Data storage
	workers   map[string]*WorkerInfo
	tasks     map[string]*TaskInfo
	taskQueue []string

	// Synchronization
	mu sync.RWMutex

	// Active streams for workers
	workerStreams map[string]pb.WorkerService_WorkerStreamServer
	streamMu      sync.RWMutex
}

type WorkerInfo struct {
	ID             string
	Address        string
	Capabilities   []string
	MaxConcurrent  int32
	Status         string
	CurrentLoad    int32
	LastSeen       time.Time
	TasksCompleted int32
	TasksFailed    int32
	UptimeSeconds  int64
	Stream         pb.WorkerService_WorkerStreamServer
}

type TaskInfo struct {
	ID         string
	Type       string
	VolumeID   uint32
	Status     string
	Progress   float32
	AssignedTo string
	Created    time.Time
	Updated    time.Time
	Server     string
	Collection string
	DataCenter string
	Rack       string
	Parameters map[string]string
}

// gRPC service implementation
func (s *AdminServer) WorkerStream(stream pb.WorkerService_WorkerStreamServer) error {
	var workerID string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Worker %s disconnected", workerID)
			s.removeWorkerStream(workerID)
			return nil
		}
		if err != nil {
			log.Printf("Stream error from worker %s: %v", workerID, err)
			s.removeWorkerStream(workerID)
			return err
		}

		// Handle different message types
		switch msg := req.Message.(type) {
		case *pb.WorkerMessage_Registration:
			workerID = msg.Registration.WorkerId
			s.handleWorkerRegistration(workerID, msg.Registration, stream)

		case *pb.WorkerMessage_Heartbeat:
			s.handleWorkerHeartbeat(msg.Heartbeat, stream)

		case *pb.WorkerMessage_TaskRequest:
			s.handleTaskRequest(msg.TaskRequest, stream)

		case *pb.WorkerMessage_TaskUpdate:
			s.handleTaskUpdate(msg.TaskUpdate)

		case *pb.WorkerMessage_TaskComplete:
			s.handleTaskComplete(msg.TaskComplete)

		case *pb.WorkerMessage_Shutdown:
			log.Printf("Worker %s shutting down: %s", msg.Shutdown.WorkerId, msg.Shutdown.Reason)
			s.removeWorkerStream(msg.Shutdown.WorkerId)
			return nil
		}
	}
}

func (s *AdminServer) handleWorkerRegistration(workerID string, reg *pb.WorkerRegistration, stream pb.WorkerService_WorkerStreamServer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker := &WorkerInfo{
		ID:             reg.WorkerId,
		Address:        reg.Address,
		Capabilities:   reg.Capabilities,
		MaxConcurrent:  reg.MaxConcurrent,
		Status:         "active",
		CurrentLoad:    0,
		LastSeen:       time.Now(),
		TasksCompleted: 0,
		TasksFailed:    0,
		UptimeSeconds:  0,
		Stream:         stream,
	}

	s.workers[reg.WorkerId] = worker
	s.addWorkerStream(reg.WorkerId, stream)

	log.Printf("Registered worker %s with capabilities: %v", reg.WorkerId, reg.Capabilities)

	// Send registration response
	response := &pb.AdminMessage{
		AdminId:   "admin-server",
		Timestamp: time.Now().Unix(),
		Message: &pb.AdminMessage_RegistrationResponse{
			RegistrationResponse: &pb.RegistrationResponse{
				Success:          true,
				Message:          "Worker registered successfully",
				AssignedWorkerId: reg.WorkerId,
			},
		},
	}

	if err := stream.Send(response); err != nil {
		log.Printf("Failed to send registration response to worker %s: %v", reg.WorkerId, err)
	}
}

func (s *AdminServer) handleWorkerHeartbeat(heartbeat *pb.WorkerHeartbeat, stream pb.WorkerService_WorkerStreamServer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[heartbeat.WorkerId]; exists {
		worker.Status = heartbeat.Status
		worker.CurrentLoad = heartbeat.CurrentLoad
		worker.LastSeen = time.Now()
		worker.TasksCompleted = heartbeat.TasksCompleted
		worker.TasksFailed = heartbeat.TasksFailed
		worker.UptimeSeconds = heartbeat.UptimeSeconds

		log.Printf("Heartbeat from worker %s: status=%s, load=%d/%d",
			heartbeat.WorkerId, heartbeat.Status, heartbeat.CurrentLoad, heartbeat.MaxConcurrent)
	}

	// Send heartbeat response
	response := &pb.AdminMessage{
		AdminId:   "admin-server",
		Timestamp: time.Now().Unix(),
		Message: &pb.AdminMessage_HeartbeatResponse{
			HeartbeatResponse: &pb.HeartbeatResponse{
				Success: true,
				Message: "Heartbeat received",
			},
		},
	}

	if err := stream.Send(response); err != nil {
		log.Printf("Failed to send heartbeat response to worker %s: %v", heartbeat.WorkerId, err)
	}
}

func (s *AdminServer) handleTaskRequest(taskReq *pb.TaskRequest, stream pb.WorkerService_WorkerStreamServer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find a pending task that matches worker capabilities
	for taskID, task := range s.tasks {
		if task.Status == "pending" {
			// Check if worker has required capability
			hasCapability := false
			for _, capability := range taskReq.Capabilities {
				if capability == task.Type {
					hasCapability = true
					break
				}
			}

			if hasCapability && taskReq.AvailableSlots > 0 {
				// Assign task to worker
				task.Status = "assigned"
				task.AssignedTo = taskReq.WorkerId
				task.Updated = time.Now()

				log.Printf("Assigned task %s (volume %d) to worker %s", taskID, task.VolumeID, taskReq.WorkerId)

				// Send task assignment
				response := &pb.AdminMessage{
					AdminId:   "admin-server",
					Timestamp: time.Now().Unix(),
					Message: &pb.AdminMessage_TaskAssignment{
						TaskAssignment: &pb.TaskAssignment{
							TaskId:      taskID,
							TaskType:    task.Type,
							Priority:    1,
							CreatedTime: task.Created.Unix(),
							Params: &pb.TaskParams{
								VolumeId:   task.VolumeID,
								Server:     task.Server,
								Collection: task.Collection,
								DataCenter: task.DataCenter,
								Rack:       task.Rack,
								Parameters: task.Parameters,
							},
						},
					},
				}

				if err := stream.Send(response); err != nil {
					log.Printf("Failed to send task assignment to worker %s: %v", taskReq.WorkerId, err)
				}
				return
			}
		}
	}

	log.Printf("No suitable tasks available for worker %s", taskReq.WorkerId)
}

func (s *AdminServer) handleTaskUpdate(update *pb.TaskUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if task, exists := s.tasks[update.TaskId]; exists {
		task.Progress = update.Progress
		task.Status = update.Status
		task.Updated = time.Now()

		log.Printf("Task %s progress: %.1f%% - %s", update.TaskId, update.Progress, update.Message)
	}
}

func (s *AdminServer) handleTaskComplete(complete *pb.TaskComplete) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if task, exists := s.tasks[complete.TaskId]; exists {
		if complete.Success {
			task.Status = "completed"
			task.Progress = 100.0
		} else {
			task.Status = "failed"
		}
		task.Updated = time.Now()

		// Update worker stats
		if worker, workerExists := s.workers[complete.WorkerId]; workerExists {
			if complete.Success {
				worker.TasksCompleted++
			} else {
				worker.TasksFailed++
			}
			if worker.CurrentLoad > 0 {
				worker.CurrentLoad--
			}
		}

		log.Printf("Task %s completed by worker %s: success=%v", complete.TaskId, complete.WorkerId, complete.Success)
	}
}

func (s *AdminServer) addWorkerStream(workerID string, stream pb.WorkerService_WorkerStreamServer) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	s.workerStreams[workerID] = stream
}

func (s *AdminServer) removeWorkerStream(workerID string) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	delete(s.workerStreams, workerID)

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, workerID)
}

// HTTP handlers for web UI
func (s *AdminServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert internal data to JSON-friendly format
	taskList := make([]map[string]interface{}, 0, len(s.tasks))
	for _, task := range s.tasks {
		taskList = append(taskList, map[string]interface{}{
			"id":        task.ID,
			"type":      task.Type,
			"volume_id": task.VolumeID,
			"status":    task.Status,
			"progress":  task.Progress,
			"created":   task.Created,
		})
	}

	workerList := make([]map[string]interface{}, 0, len(s.workers))
	for _, worker := range s.workers {
		workerList = append(workerList, map[string]interface{}{
			"id":           worker.ID,
			"address":      worker.Address,
			"capabilities": worker.Capabilities,
			"status":       worker.Status,
			"last_seen":    worker.LastSeen,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"admin_server": "running",
		"master_addr":  s.masterAddr,
		"tasks":        taskList,
		"workers":      workerList,
		"uptime":       time.Since(s.startTime).String(),
	})
}

func (s *AdminServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "healthy",
		"uptime":  time.Since(s.startTime).String(),
		"tasks":   len(s.tasks),
		"workers": len(s.workers),
	})
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
        .grpc-badge { 
            background: #9b59b6; color: white; padding: 4px 8px; 
            border-radius: 12px; font-size: 0.8em; margin-left: 10px;
        }
        .worker-badge { 
            background: #27ae60; color: white; padding: 4px 8px; 
            border-radius: 12px; font-size: 0.8em; margin-left: 10px;
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
        .worker-online { color: #27ae60; }
        .refresh-btn { 
            background: #3498db; color: white; padding: 10px 20px; 
            border: none; border-radius: 5px; cursor: pointer; margin-bottom: 20px;
        }
        .refresh-btn:hover { background: #2980b9; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧪 SeaweedFS EC Task Monitor 
                <span class="grpc-badge">gRPC Streaming</span>
                <span class="worker-badge">worker.proto</span>
            </h1>
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
            <div class="section-header">⚙️ Workers (via gRPC Streaming)</div>
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
                    document.getElementById('total-tasks').textContent = data.tasks.length;
                    document.getElementById('active-workers').textContent = data.workers.length;
                    document.getElementById('completed-tasks').textContent = 
                        data.tasks.filter(function(t) { return t.status === 'completed'; }).length;
                    document.getElementById('uptime').textContent = formatUptime(data.uptime);
                    
                    const tasksTable = document.getElementById('tasks-table');
                    if (data.tasks.length === 0) {
                        tasksTable.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #7f8c8d;">No tasks available</td></tr>';
                    } else {
                        tasksTable.innerHTML = data.tasks.map(task => '<tr>' +
                            '<td><code>' + task.id + '</code></td>' +
                            '<td>' + task.type + '</td>' +
                            '<td>' + task.volume_id + '</td>' +
                            '<td><span class="status-' + task.status + '">' + task.status.toUpperCase() + '</span></td>' +
                            '<td>' + task.progress.toFixed(1) + '%</td>' +
                            '<td>' + formatTime(task.created) + '</td>' +
                        '</tr>').join('');
                    }
                    
                    const workersTable = document.getElementById('workers-table');
                    if (data.workers.length === 0) {
                        workersTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #7f8c8d;">No workers registered</td></tr>';
                    } else {
                        workersTable.innerHTML = data.workers.map(worker => '<tr>' +
                            '<td><strong>' + worker.id + '</strong></td>' +
                            '<td>' + (worker.address || 'N/A') + '</td>' +
                            '<td><span class="worker-online">' + worker.status.toUpperCase() + '</span></td>' +
                            '<td>' + worker.capabilities.join(', ') + '</td>' +
                            '<td>' + formatTime(worker.last_seen) + '</td>' +
                        '</tr>').join('');
                    }
                })
                .catch(error => {
                    console.error('Failed to fetch data:', error);
                });
        }
        
        updateData();
        setInterval(updateData, 5000);
    </script>
</body>
</html>`

	fmt.Fprint(w, html)
}

// Task detection and creation
func (s *AdminServer) detectVolumesForEC() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			s.mu.Lock()

			log.Println("Scanning for volumes requiring EC...")

			// Simulate volume detection - in real implementation, query master
			if len(s.tasks) < 5 { // Don't create too many tasks
				taskID := fmt.Sprintf("ec-task-%d", time.Now().Unix())
				volumeID := uint32(1000 + len(s.tasks))

				task := &TaskInfo{
					ID:         taskID,
					Type:       "erasure_coding",
					VolumeID:   volumeID,
					Status:     "pending",
					Progress:   0.0,
					Created:    time.Now(),
					Updated:    time.Now(),
					Server:     "volume1:8080", // Simplified
					Collection: "",
					DataCenter: "dc1",
					Rack:       "rack1",
					Parameters: map[string]string{
						"data_shards":   "10",
						"parity_shards": "4",
					},
				}

				s.tasks[taskID] = task
				log.Printf("Created EC task %s for volume %d", taskID, volumeID)
			}

			s.mu.Unlock()
		}
	}()
}

func main() {
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9901"
	}

	httpPort := os.Getenv("ADMIN_PORT")
	if httpPort == "" {
		httpPort = "9900"
	}

	masterAddr := os.Getenv("MASTER_ADDRESS")
	if masterAddr == "" {
		masterAddr = "master:9333"
	}

	server := &AdminServer{
		grpcPort:      grpcPort,
		httpPort:      httpPort,
		masterAddr:    masterAddr,
		startTime:     time.Now(),
		workers:       make(map[string]*WorkerInfo),
		tasks:         make(map[string]*TaskInfo),
		taskQueue:     make([]string, 0),
		workerStreams: make(map[string]pb.WorkerService_WorkerStreamServer),
	}

	// Start gRPC server
	go func() {
		lis, err := net.Listen("tcp", ":"+grpcPort)
		if err != nil {
			log.Fatalf("Failed to listen on gRPC port %s: %v", grpcPort, err)
		}

		grpcServer := grpc.NewServer()
		pb.RegisterWorkerServiceServer(grpcServer, server)
		reflection.Register(grpcServer)

		log.Printf("gRPC server starting on port %s", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Start HTTP server for web UI
	go func() {
		http.HandleFunc("/", server.webUIHandler)
		http.HandleFunc("/status", server.statusHandler)
		http.HandleFunc("/health", server.healthHandler)

		log.Printf("HTTP server starting on port %s", httpPort)
		if err := http.ListenAndServe(":"+httpPort, nil); err != nil {
			log.Fatalf("Failed to serve HTTP: %v", err)
		}
	}()

	// Start task detection
	server.detectVolumesForEC()

	log.Printf("Admin server started - gRPC port: %s, HTTP port: %s, Master: %s", grpcPort, httpPort, masterAddr)

	// Keep the main goroutine running
	select {}
}
