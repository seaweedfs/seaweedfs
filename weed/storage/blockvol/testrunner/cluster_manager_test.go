package testrunner

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockNode implements NodeRunner for testing.
type mockNode struct {
	commands []string
	mu       sync.Mutex
}

func (m *mockNode) Run(ctx context.Context, cmd string) (string, string, int, error) {
	m.mu.Lock()
	m.commands = append(m.commands, cmd)
	m.mu.Unlock()
	// Simulate curl responses for cluster probing.
	if strings.Contains(cmd, "/cluster/status") {
		return `{"IsLeader":true}`, "", 0, nil
	}
	if strings.Contains(cmd, "/dir/status") {
		return `{"Topology":{"DataCenters":[{"Racks":[{"DataNodes":[{},{}]}]}]}}`, "", 0, nil
	}
	return "", "", 0, nil
}

func (m *mockNode) RunRoot(ctx context.Context, cmd string) (string, string, int, error) {
	m.mu.Lock()
	m.commands = append(m.commands, "ROOT:"+cmd)
	m.mu.Unlock()
	if strings.Contains(cmd, "nohup") && strings.Contains(cmd, "weed master") {
		return "12345", "", 0, nil
	}
	if strings.Contains(cmd, "nohup") && strings.Contains(cmd, "weed volume") {
		return "12346", "", 0, nil
	}
	return "", "", 0, nil
}

func (m *mockNode) Upload(local, remote string) error { return nil }
func (m *mockNode) Close()                            {}

func (m *mockNode) hasCommand(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.commands {
		if strings.Contains(c, substr) {
			return true
		}
	}
	return false
}

func TestClusterManager_NilSpec_Noop(t *testing.T) {
	cm := NewClusterManager(nil, t.Logf)
	actx := &ActionContext{Vars: map[string]string{}}
	if err := cm.Setup(context.Background(), actx); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if cm.State().Mode != ClusterModeNone {
		t.Fatalf("mode: got %s, want none", cm.State().Mode)
	}
	cm.Teardown(context.Background()) // no-op, no panic
}

func TestClusterManager_Fallback_Fail(t *testing.T) {
	spec := &ClusterSpec{
		Require:  ClusterRequire{Servers: 1},
		Fallback: "fail",
	}
	cm := NewClusterManager(spec, t.Logf)
	actx := &ActionContext{
		Scenario: &Scenario{Env: map[string]string{"master_url": "http://127.0.0.1:1"}},
		Vars:     map[string]string{},
		Nodes:    map[string]NodeRunner{},
	}
	err := cm.Setup(context.Background(), actx)
	if err == nil {
		t.Fatal("expected error for fallback=fail with no cluster")
	}
	if !strings.Contains(err.Error(), "fallback=fail") {
		t.Fatalf("error: %v", err)
	}
}

func TestClusterManager_Fallback_Skip(t *testing.T) {
	spec := &ClusterSpec{
		Require:  ClusterRequire{Servers: 1},
		Fallback: "skip",
	}
	cm := NewClusterManager(spec, t.Logf)
	actx := &ActionContext{
		Scenario: &Scenario{Env: map[string]string{"master_url": "http://127.0.0.1:1"}},
		Vars:     map[string]string{},
		Nodes:    map[string]NodeRunner{},
	}
	err := cm.Setup(context.Background(), actx)
	if err != nil {
		t.Fatalf("skip should not error: %v", err)
	}
	if !cm.Skipped() {
		t.Fatal("expected Skipped()=true")
	}
}

func TestClusterManager_SetVars(t *testing.T) {
	cm := &ClusterManager{
		logFunc: t.Logf,
		state: ClusterState{
			Mode:      ClusterModeManaged,
			MasterURL: "http://1.2.3.4:9333",
			Servers:   2,
			BlockCap:  1,
		},
	}
	actx := &ActionContext{Vars: map[string]string{}}
	cm.setVars(actx)
	if actx.Vars["master_url"] != "http://1.2.3.4:9333" {
		t.Fatalf("master_url: got %q", actx.Vars["master_url"])
	}
	if actx.Vars["cluster_mode"] != "managed" {
		t.Fatalf("cluster_mode: got %q", actx.Vars["cluster_mode"])
	}
	if actx.Vars["cluster_servers"] != "2" {
		t.Fatalf("cluster_servers: got %q", actx.Vars["cluster_servers"])
	}
	if actx.Vars["cluster_block_capable"] != "1" {
		t.Fatalf("cluster_block_capable: got %q", actx.Vars["cluster_block_capable"])
	}
}

func TestClusterManager_Teardown_AutoManaged_Kills(t *testing.T) {
	node := &mockNode{}
	cm := &ClusterManager{
		spec:    &ClusterSpec{Cleanup: "auto"},
		logFunc: t.Logf,
		node:    node,
		state: ClusterState{
			Mode: ClusterModeManaged,
			Pids: []string{"111", "222"},
			Dirs: []string{"/tmp/test-master", "/tmp/test-vs"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cm.Teardown(ctx)

	if !node.hasCommand("kill -9 111") {
		t.Fatal("expected kill for PID 111")
	}
	if !node.hasCommand("kill -9 222") {
		t.Fatal("expected kill for PID 222")
	}
	if !node.hasCommand("rm -rf /tmp/test-master") {
		t.Fatal("expected rm for master dir")
	}
	if !node.hasCommand("rm -rf /tmp/test-vs") {
		t.Fatal("expected rm for vs dir")
	}
}

func TestClusterManager_Teardown_AutoAttached_NoKill(t *testing.T) {
	node := &mockNode{}
	cm := &ClusterManager{
		spec:    &ClusterSpec{Cleanup: "auto"},
		logFunc: t.Logf,
		state:   ClusterState{Mode: ClusterModeAttached},
		attachedNodes: []NodeRunner{node},
	}
	cm.Teardown(context.Background())
	if node.hasCommand("kill") {
		t.Fatal("auto cleanup should NOT kill attached cluster")
	}
}

func TestClusterManager_Teardown_DestroyAttached_Kills(t *testing.T) {
	node := &mockNode{}
	cm := &ClusterManager{
		spec:    &ClusterSpec{Cleanup: "destroy"},
		logFunc: t.Logf,
		state:   ClusterState{Mode: ClusterModeAttached},
		attachedNodes: []NodeRunner{node},
	}
	cm.Teardown(context.Background())
	if !node.hasCommand("killall -9 weed") {
		t.Fatal("destroy cleanup should kill attached cluster processes")
	}
}

func TestClusterManager_Teardown_Keep_NoAction(t *testing.T) {
	node := &mockNode{}
	cm := &ClusterManager{
		spec:    &ClusterSpec{Cleanup: "keep"},
		logFunc: t.Logf,
		node:    node,
		state: ClusterState{
			Mode: ClusterModeManaged,
			Pids: []string{"111"},
		},
	}
	cm.Teardown(context.Background())
	if node.hasCommand("kill") {
		t.Fatal("keep cleanup should NOT kill anything")
	}
}

func TestClusterManager_MeetsRequirements(t *testing.T) {
	cm := &ClusterManager{
		spec: &ClusterSpec{
			Require: ClusterRequire{Servers: 2, BlockCapable: 1},
		},
	}
	tests := []struct {
		name   string
		state  ClusterState
		expect bool
	}{
		{"meets both", ClusterState{Servers: 3, BlockCap: 2}, true},
		{"meets exact", ClusterState{Servers: 2, BlockCap: 1}, true},
		{"servers short", ClusterState{Servers: 1, BlockCap: 1}, false},
		{"block short", ClusterState{Servers: 3, BlockCap: 0}, false},
		{"both short", ClusterState{Servers: 0, BlockCap: 0}, false},
	}
	for _, tt := range tests {
		if got := cm.meetsRequirements(tt.state); got != tt.expect {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.expect)
		}
	}
}
