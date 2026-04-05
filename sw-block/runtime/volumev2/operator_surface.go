package volumev2

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// OperatorSurfaceHandle controls one bounded HTTP operator surface.
type OperatorSurfaceHandle struct {
	addr     string
	server   *http.Server
	listener net.Listener
}

// Address returns the listen address of the operator surface.
func (h *OperatorSurfaceHandle) Address() string {
	if h == nil {
		return ""
	}
	return h.addr
}

// Close stops the operator surface.
func (h *OperatorSurfaceHandle) Close() error {
	if h == nil {
		return nil
	}
	if h.server != nil {
		_ = h.server.Shutdown(context.Background())
	}
	if h.listener != nil {
		return h.listener.Close()
	}
	return nil
}

// StartOperatorSurface starts one bounded HTTP operator surface over the
// runtime-owned snapshots and projections.
func (m *InProcessRuntimeManager) StartOperatorSurface(listenAddr string) (*OperatorSurfaceHandle, error) {
	if m == nil {
		return nil, fmt.Errorf("volumev2: runtime manager is nil")
	}
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/volumes/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/v1/volumes/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		volumeName, viewName := parts[0], parts[1]
		switch viewName {
		case "rf2":
			value, ok := m.RF2VolumeSurface(volumeName)
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeOperatorJSON(w, value)
		case "failover":
			value, ok := m.FailoverSnapshot(volumeName)
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeOperatorJSON(w, value)
		case "loop2":
			value, ok := m.Loop2Snapshot(volumeName)
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeOperatorJSON(w, value)
		case "continuity":
			value, ok := m.ReplicatedContinuitySnapshot(volumeName)
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeOperatorJSON(w, value)
		case "frontend":
			value, ok := m.ISCSIExport(volumeName)
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeOperatorJSON(w, value)
		default:
			http.NotFound(w, r)
		}
	})

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("volumev2: operator surface listen: %w", err)
	}
	server := &http.Server{Handler: mux}
	handle := &OperatorSurfaceHandle{
		addr:     ln.Addr().String(),
		server:   server,
		listener: ln,
	}
	go func() {
		_ = server.Serve(ln)
	}()
	return handle, nil
}

func writeOperatorJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
