// admin.go provides an HTTP admin server for the standalone iscsi-target binary.
// Endpoints:
//   POST /assign   -- inject assignment {epoch, role, lease_ttl_ms}
//   GET  /status   -- return JSON status
//   POST /replica  -- set WAL shipping target {data_addr, ctrl_addr}
//   POST /rebuild  -- start/stop rebuild server {action, listen_addr}
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* handlers on DefaultServeMux
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// adminServer provides HTTP admin control of the BlockVol.
type adminServer struct {
	vol    *blockvol.BlockVol
	token  string // optional auth token; empty = no auth
	logger *log.Logger
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
	Action     string `json:"action"`
	ListenAddr string `json:"listen_addr"`
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
	default:
		jsonError(w, "action must be 'start' or 'stop'", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
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
