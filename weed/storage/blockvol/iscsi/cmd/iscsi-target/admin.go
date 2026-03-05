// admin.go provides an HTTP admin server for the standalone iscsi-target binary.
// Endpoints:
//   POST /assign   -- inject assignment {epoch, role, lease_ttl_ms}
//   GET  /status   -- return JSON status
//   POST /replica  -- set WAL shipping target {data_addr, ctrl_addr}
//   POST /rebuild  -- start/stop rebuild server {action, listen_addr}
//   POST /snapshot -- create/delete/restore/list snapshots
//   POST /resize   -- expand volume {new_size_bytes}
//   GET  /metrics  -- Prometheus metrics (requires X-Admin-Token if auth enabled)
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* handlers on DefaultServeMux
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// adminServer provides HTTP admin control of the BlockVol.
type adminServer struct {
	vol             *blockvol.BlockVol
	token           string // optional auth token; empty = no auth
	logger          *log.Logger
	metricsRegistry *prometheus.Registry // dedicated Prometheus registry (nil = no /metrics)
}

// assignRequest is the JSON body for POST /assign.
type assignRequest struct {
	Epoch      *uint64 `json:"epoch"`
	Role       *uint32 `json:"role"`
	LeaseTTLMs *uint32 `json:"lease_ttl_ms"`
}

// replicaRequest is the JSON body for POST /replica.
type replicaRequest struct {
	DataAddr string `json:"data_addr"`
	CtrlAddr string `json:"ctrl_addr"`
}

// rebuildRequest is the JSON body for POST /rebuild.
type rebuildRequest struct {
	Action      string `json:"action"`
	ListenAddr  string `json:"listen_addr"`  // for "start"
	RebuildAddr string `json:"rebuild_addr"` // for "connect"
	Epoch       uint64 `json:"epoch"`        // for "connect"
}

// snapshotRequest is the JSON body for POST /snapshot.
type snapshotRequest struct {
	Action string `json:"action"` // "create", "delete", "restore", "list"
	ID     uint32 `json:"id"`
}

// resizeRequest is the JSON body for POST /resize.
type resizeRequest struct {
	NewSizeBytes uint64 `json:"new_size_bytes"`
}

// statusResponse is the JSON body for GET /status.
type statusResponse struct {
	Path          string `json:"path"`
	Epoch         uint64 `json:"epoch"`
	Role          string `json:"role"`
	WALHeadLSN    uint64 `json:"wal_head_lsn"`
	CheckpointLSN uint64 `json:"checkpoint_lsn"`
	HasLease      bool   `json:"has_lease"`
	Healthy       bool   `json:"healthy"`
	VolumeSize    uint64 `json:"volume_size"`
}

const maxValidRole = uint32(blockvol.RoleDraining)

func newAdminServer(vol *blockvol.BlockVol, token string, logger *log.Logger) *adminServer {
	return &adminServer{vol: vol, token: token, logger: logger}
}

func (a *adminServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a.token != "" {
		if r.Header.Get("X-Admin-Token") != a.token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
	}

	switch r.URL.Path {
	case "/assign":
		a.handleAssign(w, r)
	case "/status":
		a.handleStatus(w, r)
	case "/replica":
		a.handleReplica(w, r)
	case "/rebuild":
		a.handleRebuild(w, r)
	case "/snapshot":
		a.handleSnapshot(w, r)
	case "/resize":
		a.handleResize(w, r)
	case "/metrics":
		a.handleMetrics(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (a *adminServer) handleAssign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req assignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Epoch == nil || req.Role == nil {
		jsonError(w, "epoch and role are required", http.StatusBadRequest)
		return
	}
	if *req.Role > maxValidRole {
		jsonError(w, fmt.Sprintf("invalid role: %d (max %d)", *req.Role, maxValidRole), http.StatusBadRequest)
		return
	}
	ttl := time.Duration(0)
	if req.LeaseTTLMs != nil {
		ttl = time.Duration(*req.LeaseTTLMs) * time.Millisecond
	}
	role := blockvol.Role(*req.Role)
	err := a.vol.HandleAssignment(*req.Epoch, role, ttl)
	if err != nil {
		jsonError(w, err.Error(), http.StatusConflict)
		return
	}
	a.logger.Printf("admin: assigned epoch=%d role=%s lease=%v", *req.Epoch, role, ttl)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func (a *adminServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	st := a.vol.Status()
	info := a.vol.Info()
	resp := statusResponse{
		Epoch:         st.Epoch,
		Role:          st.Role.String(),
		WALHeadLSN:    st.WALHeadLSN,
		CheckpointLSN: st.CheckpointLSN,
		HasLease:      st.HasLease,
		Healthy:       info.Healthy,
		VolumeSize:    info.VolumeSize,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *adminServer) handleReplica(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req replicaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Both fields must be set or both empty
	if (req.DataAddr == "") != (req.CtrlAddr == "") {
		jsonError(w, "both data_addr and ctrl_addr must be set or both empty", http.StatusBadRequest)
		return
	}
	a.vol.SetReplicaAddr(req.DataAddr, req.CtrlAddr)
	a.logger.Printf("admin: replica target set to data=%s ctrl=%s", req.DataAddr, req.CtrlAddr)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func (a *adminServer) handleRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req rebuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	switch req.Action {
	case "start":
		if req.ListenAddr == "" {
			jsonError(w, "listen_addr required for start", http.StatusBadRequest)
			return
		}
		if err := a.vol.StartRebuildServer(req.ListenAddr); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		a.logger.Printf("admin: rebuild server started on %s", req.ListenAddr)
	case "stop":
		a.vol.StopRebuildServer()
		a.logger.Printf("admin: rebuild server stopped")
	case "connect":
		if req.RebuildAddr == "" {
			jsonError(w, "rebuild_addr required for connect", http.StatusBadRequest)
			return
		}
		fromLSN := a.vol.Status().WALHeadLSN
		go func() {
			if err := blockvol.StartRebuild(a.vol, req.RebuildAddr, fromLSN, req.Epoch); err != nil {
				a.logger.Printf("admin: rebuild connect to %s failed: %v", req.RebuildAddr, err)
			} else {
				a.logger.Printf("admin: rebuild from %s completed", req.RebuildAddr)
			}
		}()
		a.logger.Printf("admin: rebuild connect started (addr=%s epoch=%d fromLSN=%d)", req.RebuildAddr, req.Epoch, fromLSN)
	default:
		jsonError(w, "action must be 'start', 'stop', or 'connect'", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func (a *adminServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req snapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	switch req.Action {
	case "create":
		if err := a.vol.CreateSnapshot(req.ID); err != nil {
			jsonError(w, err.Error(), http.StatusConflict)
			return
		}
		a.logger.Printf("admin: created snapshot %d", req.ID)
	case "delete":
		if err := a.vol.DeleteSnapshot(req.ID); err != nil {
			jsonError(w, err.Error(), http.StatusNotFound)
			return
		}
		a.logger.Printf("admin: deleted snapshot %d", req.ID)
	case "restore":
		if err := a.vol.RestoreSnapshot(req.ID); err != nil {
			jsonError(w, err.Error(), http.StatusConflict)
			return
		}
		a.logger.Printf("admin: restored snapshot %d", req.ID)
	case "list":
		snaps := a.vol.ListSnapshots()
		type snapInfo struct {
			ID        uint32 `json:"id"`
			BaseLSN   uint64 `json:"base_lsn"`
			CreatedAt string `json:"created_at"`
			CoWBlocks uint64 `json:"cow_blocks"`
		}
		result := make([]snapInfo, len(snaps))
		for i, s := range snaps {
			result[i] = snapInfo{
				ID:        s.ID,
				BaseLSN:   s.BaseLSN,
				CreatedAt: s.CreatedAt.Format(time.RFC3339),
				CoWBlocks: s.CoWBlocks,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"snapshots": result})
		return
	default:
		jsonError(w, "action must be 'create', 'delete', 'restore', or 'list'", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func (a *adminServer) handleResize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req resizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.NewSizeBytes == 0 {
		jsonError(w, "new_size_bytes is required", http.StatusBadRequest)
		return
	}
	if err := a.vol.Expand(req.NewSizeBytes); err != nil {
		jsonError(w, err.Error(), http.StatusConflict)
		return
	}
	a.logger.Printf("admin: resized to %d bytes", req.NewSizeBytes)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":          true,
		"volume_size": a.vol.Info().VolumeSize,
	})
}

func (a *adminServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if a.metricsRegistry == nil {
		http.Error(w, `{"error":"metrics not configured"}`, http.StatusNotFound)
		return
	}
	promhttp.HandlerFor(a.metricsRegistry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

// startAdminServer starts the HTTP admin server in a background goroutine.
// Returns the listener so tests can determine the actual bound port.
// Includes /debug/pprof/* endpoints for profiling.
func startAdminServer(addr string, srv *adminServer) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("admin listen %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/assign", srv)
	mux.Handle("/status", srv)
	mux.Handle("/replica", srv)
	mux.Handle("/rebuild", srv)
	mux.Handle("/snapshot", srv)
	mux.Handle("/resize", srv)
	mux.Handle("/metrics", srv)
	// pprof handlers registered on DefaultServeMux by net/http/pprof import.
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	go func() {
		if err := http.Serve(ln, mux); err != nil && !isClosedErr(err) {
			srv.logger.Printf("admin server error: %v", err)
		}
	}()
	srv.logger.Printf("admin server listening on %s", ln.Addr())
	return ln, nil
}

func isClosedErr(err error) bool {
	return err != nil && (err == http.ErrServerClosed ||
		isNetClosedErr(err))
}

func isNetClosedErr(err error) bool {
	// net.ErrClosed is not always directly comparable
	return err != nil && (err.Error() == "use of closed network connection" ||
		err.Error() == "http: Server closed")
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
