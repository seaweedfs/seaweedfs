package volumev2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// FailoverEvidenceTransport carries failover-time query traffic across a
// transport/session boundary. This is the first transport seam needed for M1:
// promotion evidence and bounded replica summaries.
type FailoverEvidenceTransport interface {
	QueryPromotionEvidence(nodeID string, req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error)
	QueryReplicaSummary(nodeID string, req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error)
}

// ManagedFailoverEvidenceTransport is the runtime-owned transport surface used
// by the runtime manager. It keeps the adapter query shape stable while allowing
// different registration mechanics underneath.
type ManagedFailoverEvidenceTransport interface {
	FailoverEvidenceTransport
	RegisterHandler(nodeID string, handler FailoverEvidenceHandler) error
	UnregisterHandler(nodeID string)
	Close() error
}

// FailoverEvidenceHandler is the server-side evidence surface registered behind
// a transport implementation.
type FailoverEvidenceHandler interface {
	QueryPromotionEvidence(masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error)
	QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error)
}

// NewTransportEvidenceAdapter wraps one stable node id behind the supplied
// failover evidence transport.
func NewTransportEvidenceAdapter(nodeID string, transport FailoverEvidenceTransport) (FailoverEvidenceAdapter, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("volumev2: evidence adapter node id is required")
	}
	if transport == nil {
		return nil, fmt.Errorf("volumev2: evidence transport is nil")
	}
	return transportFailoverEvidenceAdapter{
		nodeID:    nodeID,
		transport: transport,
	}, nil
}

// NewHybridInProcessFailoverTarget builds the first transport-backed failover
// target shape: evidence crosses a transport/session seam, while takeover
// remains primary-local on the selected node.
func NewHybridInProcessFailoverTarget(node *Node, transport FailoverEvidenceTransport) (FailoverTarget, error) {
	if node == nil {
		return FailoverTarget{}, fmt.Errorf("volumev2: node is nil")
	}
	evidence, err := NewTransportEvidenceAdapter(node.NodeID(), transport)
	if err != nil {
		return FailoverTarget{}, err
	}
	return FailoverTarget{
		NodeID:   node.NodeID(),
		Evidence: evidence,
		Takeover: inProcessFailoverTakeoverAdapter{node: node},
	}, nil
}

type transportFailoverEvidenceAdapter struct {
	nodeID    string
	transport FailoverEvidenceTransport
}

func (a transportFailoverEvidenceAdapter) QueryPromotionEvidence(req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	if a.transport == nil {
		return masterv2.PromotionQueryResponse{}, fmt.Errorf("volumev2: transport evidence adapter is nil")
	}
	return a.transport.QueryPromotionEvidence(a.nodeID, req)
}

func (a transportFailoverEvidenceAdapter) QueryReplicaSummary(req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	if a.transport == nil {
		return protocolv2.ReplicaSummaryResponse{}, fmt.Errorf("volumev2: transport evidence adapter is nil")
	}
	return a.transport.QueryReplicaSummary(a.nodeID, req)
}

// InMemoryFailoverEvidenceTransport is the first request/response transport-style
// adapter. It is still in-process, but it eliminates direct orchestration-time
// calls to `*Node` for promotion evidence and replica-summary queries.
type InMemoryFailoverEvidenceTransport struct {
	mu       sync.RWMutex
	handlers map[string]FailoverEvidenceHandler
}

// NewInMemoryFailoverEvidenceTransport creates an in-memory request/response
// transport for failover evidence.
func NewInMemoryFailoverEvidenceTransport() *InMemoryFailoverEvidenceTransport {
	return &InMemoryFailoverEvidenceTransport{
		handlers: make(map[string]FailoverEvidenceHandler),
	}
}

// RegisterHandler binds one stable node id to one evidence handler.
func (t *InMemoryFailoverEvidenceTransport) RegisterHandler(nodeID string, handler FailoverEvidenceHandler) error {
	if t == nil {
		return fmt.Errorf("volumev2: evidence transport is nil")
	}
	if nodeID == "" {
		return fmt.Errorf("volumev2: evidence handler node id is required")
	}
	if handler == nil {
		return fmt.Errorf("volumev2: evidence handler %q is nil", nodeID)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[nodeID] = handler
	return nil
}

// UnregisterHandler removes one evidence handler from the in-memory transport.
func (t *InMemoryFailoverEvidenceTransport) UnregisterHandler(nodeID string) {
	if t == nil || nodeID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, nodeID)
}

// Close releases any resources owned by the transport.
func (t *InMemoryFailoverEvidenceTransport) Close() error {
	return nil
}

func (t *InMemoryFailoverEvidenceTransport) QueryPromotionEvidence(nodeID string, req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	handler, err := t.handler(nodeID)
	if err != nil {
		return masterv2.PromotionQueryResponse{}, err
	}
	return handler.QueryPromotionEvidence(req)
}

func (t *InMemoryFailoverEvidenceTransport) QueryReplicaSummary(nodeID string, req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	handler, err := t.handler(nodeID)
	if err != nil {
		return protocolv2.ReplicaSummaryResponse{}, err
	}
	return handler.QueryReplicaSummary(req)
}

func (t *InMemoryFailoverEvidenceTransport) handler(nodeID string) (FailoverEvidenceHandler, error) {
	if t == nil {
		return nil, fmt.Errorf("volumev2: evidence transport is nil")
	}
	if nodeID == "" {
		return nil, fmt.Errorf("volumev2: evidence node id is required")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	handler, ok := t.handlers[nodeID]
	if !ok {
		return nil, fmt.Errorf("volumev2: unknown evidence handler %q", nodeID)
	}
	return handler, nil
}

type httpEvidenceServer struct {
	baseURL  string
	server   *http.Server
	listener net.Listener
}

// HTTPFailoverEvidenceTransport carries failover-time evidence over a real
// loopback HTTP transport while keeping takeover primary-local.
type HTTPFailoverEvidenceTransport struct {
	mu      sync.RWMutex
	client  *http.Client
	servers map[string]*httpEvidenceServer
}

// NewHTTPFailoverEvidenceTransport creates one managed loopback HTTP transport.
func NewHTTPFailoverEvidenceTransport() *HTTPFailoverEvidenceTransport {
	return &HTTPFailoverEvidenceTransport{
		client:  &http.Client{Timeout: 2 * time.Second},
		servers: make(map[string]*httpEvidenceServer),
	}
}

// RegisterHandler binds one stable node id behind one loopback HTTP server.
func (t *HTTPFailoverEvidenceTransport) RegisterHandler(nodeID string, handler FailoverEvidenceHandler) error {
	if t == nil {
		return fmt.Errorf("volumev2: http evidence transport is nil")
	}
	if nodeID == "" {
		return fmt.Errorf("volumev2: http evidence handler node id is required")
	}
	if handler == nil {
		return fmt.Errorf("volumev2: http evidence handler %q is nil", nodeID)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/promotion-evidence", func(w http.ResponseWriter, r *http.Request) {
		var req masterv2.PromotionQueryRequest
		if err := decodeEvidenceJSON(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := handler.QueryPromotionEvidence(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeEvidenceJSON(w, resp)
	})
	mux.HandleFunc("/replica-summary", func(w http.ResponseWriter, r *http.Request) {
		var req protocolv2.ReplicaSummaryRequest
		if err := decodeEvidenceJSON(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := handler.QueryReplicaSummary(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeEvidenceJSON(w, resp)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("volumev2: http evidence listen %q: %w", nodeID, err)
	}
	server := &http.Server{Handler: mux}
	entry := &httpEvidenceServer{
		baseURL:  "http://" + ln.Addr().String(),
		server:   server,
		listener: ln,
	}

	t.mu.Lock()
	if prev, ok := t.servers[nodeID]; ok {
		t.mu.Unlock()
		_ = prev.server.Shutdown(context.Background())
		_ = prev.listener.Close()
		t.mu.Lock()
	}
	t.servers[nodeID] = entry
	t.mu.Unlock()

	go func() {
		_ = server.Serve(ln)
	}()
	return nil
}

// UnregisterHandler stops one loopback HTTP handler.
func (t *HTTPFailoverEvidenceTransport) UnregisterHandler(nodeID string) {
	if t == nil || nodeID == "" {
		return
	}
	t.mu.Lock()
	entry, ok := t.servers[nodeID]
	if ok {
		delete(t.servers, nodeID)
	}
	t.mu.Unlock()
	if !ok {
		return
	}
	_ = entry.server.Shutdown(context.Background())
	_ = entry.listener.Close()
}

// Close stops all registered loopback HTTP handlers.
func (t *HTTPFailoverEvidenceTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	entries := make([]*httpEvidenceServer, 0, len(t.servers))
	for nodeID, entry := range t.servers {
		entries = append(entries, entry)
		delete(t.servers, nodeID)
	}
	t.mu.Unlock()
	for _, entry := range entries {
		_ = entry.server.Shutdown(context.Background())
		_ = entry.listener.Close()
	}
	return nil
}

func (t *HTTPFailoverEvidenceTransport) QueryPromotionEvidence(nodeID string, req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	endpoint, err := t.endpoint(nodeID, "/promotion-evidence")
	if err != nil {
		return masterv2.PromotionQueryResponse{}, err
	}
	var resp masterv2.PromotionQueryResponse
	if err := t.postJSON(endpoint, req, &resp); err != nil {
		return masterv2.PromotionQueryResponse{}, err
	}
	return resp, nil
}

func (t *HTTPFailoverEvidenceTransport) QueryReplicaSummary(nodeID string, req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	endpoint, err := t.endpoint(nodeID, "/replica-summary")
	if err != nil {
		return protocolv2.ReplicaSummaryResponse{}, err
	}
	var resp protocolv2.ReplicaSummaryResponse
	if err := t.postJSON(endpoint, req, &resp); err != nil {
		return protocolv2.ReplicaSummaryResponse{}, err
	}
	return resp, nil
}

func (t *HTTPFailoverEvidenceTransport) endpoint(nodeID, suffix string) (string, error) {
	if t == nil {
		return "", fmt.Errorf("volumev2: http evidence transport is nil")
	}
	if nodeID == "" {
		return "", fmt.Errorf("volumev2: http evidence node id is required")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	entry, ok := t.servers[nodeID]
	if !ok {
		return "", fmt.Errorf("volumev2: unknown http evidence handler %q", nodeID)
	}
	return entry.baseURL + suffix, nil
}

func (t *HTTPFailoverEvidenceTransport) postJSON(endpoint string, req any, resp any) error {
	if t == nil {
		return fmt.Errorf("volumev2: http evidence transport is nil")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("volumev2: marshal evidence request: %w", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("volumev2: build evidence request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("volumev2: http evidence request: %w", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("volumev2: http evidence status %d", httpResp.StatusCode)
	}
	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return fmt.Errorf("volumev2: decode evidence response: %w", err)
	}
	return nil
}

func decodeEvidenceJSON(r *http.Request, dst any) error {
	if r == nil || r.Body == nil {
		return fmt.Errorf("volumev2: empty evidence request")
	}
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		return fmt.Errorf("volumev2: unsupported evidence method %q", r.Method)
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		return fmt.Errorf("volumev2: decode evidence request: %w", err)
	}
	return nil
}

func writeEvidenceJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
