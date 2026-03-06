package testrunner

import (
	"testing"
	"time"
)

func TestParse_ValidScenario(t *testing.T) {
	yaml := `
name: test-scenario
timeout: 5m
env:
  repo_dir: "/tmp/repo"

topology:
  nodes:
    node1:
      host: "192.168.1.1"
      user: testdev
      key: "/tmp/key"

targets:
  primary:
    node: node1
    vol_size: 100M
    iscsi_port: 3260
    admin_port: 8080
    iqn_suffix: test-primary
  replica:
    node: node1
    vol_size: 100M
    iscsi_port: 3261
    admin_port: 8081
    replica_data_port: 9011
    replica_ctrl_port: 9012
    rebuild_port: 9013
    iqn_suffix: test-replica

phases:
  - name: setup
    actions:
      - action: build_deploy
      - action: start_target
        target: primary
        create: "true"
  - name: cleanup
    always: true
    actions:
      - action: stop_all_targets
        ignore_error: true
`
	s, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if s.Name != "test-scenario" {
		t.Errorf("name = %q, want %q", s.Name, "test-scenario")
	}
	if s.Timeout.Duration != 5*time.Minute {
		t.Errorf("timeout = %v, want 5m", s.Timeout.Duration)
	}
	if len(s.Topology.Nodes) != 1 {
		t.Errorf("nodes = %d, want 1", len(s.Topology.Nodes))
	}
	if len(s.Targets) != 2 {
		t.Errorf("targets = %d, want 2", len(s.Targets))
	}
	if len(s.Phases) != 2 {
		t.Errorf("phases = %d, want 2", len(s.Phases))
	}

	// Check target IQN generation.
	if iqn := s.Targets["primary"].IQN(); iqn != "iqn.2024.com.seaweedfs:test-primary" {
		t.Errorf("primary IQN = %q", iqn)
	}

	// Check always flag.
	if !s.Phases[1].Always {
		t.Error("cleanup phase should have always=true")
	}
}

func TestParse_MissingName(t *testing.T) {
	yaml := `
phases:
  - name: test
    actions:
      - action: exec
        cmd: "echo hi"
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestParse_InvalidNodeRef(t *testing.T) {
	yaml := `
name: bad-ref
topology:
  nodes:
    node1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
targets:
  tgt:
    node: nonexistent
    iscsi_port: 3260
    iqn_suffix: test
phases:
  - name: test
    actions:
      - action: exec
        cmd: "echo"
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for invalid node ref")
	}
}

func TestParse_PortConflict(t *testing.T) {
	yaml := `
name: port-conflict
topology:
  nodes:
    node1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
targets:
  tgt1:
    node: node1
    iscsi_port: 3260
    iqn_suffix: t1
  tgt2:
    node: node1
    iscsi_port: 3260
    iqn_suffix: t2
phases:
  - name: test
    actions:
      - action: exec
        cmd: "echo"
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for port conflict")
	}
}

func TestParse_InvalidTargetRef(t *testing.T) {
	yaml := `
name: bad-target-ref
topology:
  nodes:
    node1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
targets:
  primary:
    node: node1
    iscsi_port: 3260
    iqn_suffix: test
phases:
  - name: test
    actions:
      - action: start_target
        target: nonexistent
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for invalid target ref in action")
	}
}

func TestParse_MissingIQNSuffix(t *testing.T) {
	yaml := `
name: missing-iqn
topology:
  nodes:
    node1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
targets:
  tgt:
    node: node1
    iscsi_port: 3260
phases:
  - name: test
    actions:
      - action: exec
        cmd: "echo"
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for missing iqn_suffix")
	}
}

func TestParse_NoPhases(t *testing.T) {
	yaml := `
name: no-phases
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for no phases")
	}
}

func TestParse_AgentTopology_Valid(t *testing.T) {
	yaml := `
name: agent-topo
topology:
  agents:
    tp01: "192.168.1.188:9100"
    m01: "192.168.1.181:9100"
  nodes:
    tp01_node:
      host: "192.168.1.188"
      agent: tp01
    m01_node:
      host: "192.168.1.181"
      agent: m01
targets:
  primary:
    node: tp01_node
    iscsi_port: 3260
    iqn_suffix: primary
phases:
  - name: test
    actions:
      - action: exec
        node: tp01_node
        cmd: "echo"
`
	s, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(s.Topology.Agents) != 2 {
		t.Errorf("agents = %d, want 2", len(s.Topology.Agents))
	}
	if s.Topology.Nodes["tp01_node"].Agent != "tp01" {
		t.Error("tp01_node should have agent=tp01")
	}
}

func TestParse_AgentTopology_InvalidAgentRef(t *testing.T) {
	yaml := `
name: bad-agent-ref
topology:
  agents:
    tp01: "192.168.1.188:9100"
  nodes:
    node1:
      host: "192.168.1.181"
      agent: nonexistent
phases:
  - name: test
    actions:
      - action: exec
        cmd: "echo"
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for invalid agent reference")
	}
}

func TestParse_ParallelPhase_SaveAsConflict(t *testing.T) {
	yaml := `
name: save-as-conflict
topology:
  nodes:
    node1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
phases:
  - name: parallel_phase
    parallel: true
    actions:
      - action: exec
        node: node1
        save_as: my_var
        cmd: "echo a"
      - action: exec
        node: node1
        save_as: my_var
        cmd: "echo b"
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for save_as conflict in parallel phase")
	}
}

func TestParse_SequentialPhase_SaveAsDuplicate_Allowed(t *testing.T) {
	yaml := `
name: save-as-sequential
topology:
  nodes:
    node1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
phases:
  - name: seq_phase
    actions:
      - action: exec
        node: node1
        save_as: device
        cmd: "echo a"
      - action: exec
        node: node1
        save_as: device
        cmd: "echo b"
`
	_, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("sequential save_as duplicate should be allowed, got: %v", err)
	}
}

func TestParse_ActionRetryAndTimeout(t *testing.T) {
	yaml := `
name: retry-test
topology:
  nodes:
    n1:
      host: "1.2.3.4"
      user: test
      key: "/tmp/key"
phases:
  - name: test
    actions:
      - action: start_target
        node: n1
        retry: 2
        timeout: 30s
        cmd: "echo"
`
	s, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	act := s.Phases[0].Actions[0]
	if act.Retry != 2 {
		t.Errorf("retry = %d, want 2", act.Retry)
	}
	if act.Timeout != "30s" {
		t.Errorf("timeout = %q, want 30s", act.Timeout)
	}
}

func TestExtractVarsFromString(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"{{ device }}", []string{"device"}},
		{"{{written_md5}}", []string{"written_md5"}},
		{"prefix {{ a }} middle {{ b }} suffix", []string{"a", "b"}},
		{"no vars here", nil},
		{"{{ }}", nil},
	}

	for _, tt := range tests {
		got := extractVarsFromString(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("extractVarsFromString(%q) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("extractVarsFromString(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
