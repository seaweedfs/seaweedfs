package testrunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInclude_Basic(t *testing.T) {
	dir := t.TempDir()

	// Template with one phase.
	writeFile(t, dir, "template.yaml", `
phases:
  - name: from_template
    actions:
      - action: print
        msg: "hello from template"
`)
	// Scenario that includes it.
	writeFile(t, dir, "scenario.yaml", `
name: include-test
timeout: 1m
phases:
  - include: template.yaml
  - name: inline
    actions:
      - action: print
        msg: "inline phase"
`)
	s, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(s.Phases) != 2 {
		t.Fatalf("phases: got %d, want 2", len(s.Phases))
	}
	if s.Phases[0].Name != "from_template" {
		t.Errorf("phase[0].Name = %q, want from_template", s.Phases[0].Name)
	}
	if s.Phases[1].Name != "inline" {
		t.Errorf("phase[1].Name = %q, want inline", s.Phases[1].Name)
	}
}

func TestInclude_Params(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "template.yaml", `
phases:
  - name: parameterized
    actions:
      - action: print
        msg: "size={{ size }} node={{ node }}"
`)
	writeFile(t, dir, "scenario.yaml", `
name: param-test
timeout: 1m
phases:
  - include: template.yaml
    include_params:
      size: "64K"
      node: "client"
`)
	s, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(s.Phases) != 1 {
		t.Fatalf("phases: got %d, want 1", len(s.Phases))
	}
	msg := s.Phases[0].Actions[0].Params["msg"]
	if msg != "size=64K node=client" {
		t.Errorf("msg = %q, want 'size=64K node=client'", msg)
	}
}

func TestInclude_NestedInclude(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	os.MkdirAll(sub, 0755)

	// Inner template.
	writeFile(t, sub, "inner.yaml", `
phases:
  - name: inner
    actions:
      - action: print
        msg: "from inner"
`)
	// Outer template includes inner.
	writeFile(t, dir, "outer.yaml", `
phases:
  - include: sub/inner.yaml
  - name: outer
    actions:
      - action: print
        msg: "from outer"
`)
	// Scenario includes outer.
	writeFile(t, dir, "scenario.yaml", `
name: nested-test
timeout: 1m
phases:
  - include: outer.yaml
`)
	s, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(s.Phases) != 2 {
		t.Fatalf("phases: got %d, want 2 (inner + outer)", len(s.Phases))
	}
	if s.Phases[0].Name != "inner" {
		t.Errorf("phase[0] = %q, want inner", s.Phases[0].Name)
	}
	if s.Phases[1].Name != "outer" {
		t.Errorf("phase[1] = %q, want outer", s.Phases[1].Name)
	}
}

func TestInclude_CircularDetected(t *testing.T) {
	dir := t.TempDir()

	// a.yaml includes b.yaml includes a.yaml.
	writeFile(t, dir, "a.yaml", `
phases:
  - include: b.yaml
`)
	writeFile(t, dir, "b.yaml", `
phases:
  - include: a.yaml
`)
	writeFile(t, dir, "scenario.yaml", `
name: circular-test
timeout: 1m
phases:
  - include: a.yaml
`)
	_, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err == nil {
		t.Fatal("expected error for circular include")
	}
	if !strings.Contains(err.Error(), "depth exceeds") {
		t.Errorf("error = %q, want 'depth exceeds'", err.Error())
	}
}

func TestInclude_MissingFile(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "scenario.yaml", `
name: missing-test
timeout: 1m
phases:
  - include: nonexistent.yaml
`)
	_, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err == nil {
		t.Fatal("expected error for missing include file")
	}
	if !strings.Contains(err.Error(), "nonexistent.yaml") {
		t.Errorf("error = %q, want to mention file name", err.Error())
	}
}

func TestInclude_MultiplePhases(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "multi.yaml", `
phases:
  - name: phase_a
    actions:
      - action: print
        msg: "a"
  - name: phase_b
    actions:
      - action: print
        msg: "b"
`)
	writeFile(t, dir, "scenario.yaml", `
name: multi-test
timeout: 1m
phases:
  - name: before
    actions:
      - action: print
        msg: "before"
  - include: multi.yaml
  - name: after
    actions:
      - action: print
        msg: "after"
`)
	s, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(s.Phases) != 4 {
		t.Fatalf("phases: got %d, want 4 (before + a + b + after)", len(s.Phases))
	}
	names := []string{s.Phases[0].Name, s.Phases[1].Name, s.Phases[2].Name, s.Phases[3].Name}
	want := []string{"before", "phase_a", "phase_b", "after"}
	for i, n := range names {
		if n != want[i] {
			t.Errorf("phase[%d] = %q, want %q", i, n, want[i])
		}
	}
}

func TestInclude_ParamsSubstituteNodeAndSaveAs(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "template.yaml", `
phases:
  - name: test
    actions:
      - action: kv_verify
        node: "{{ target_node }}"
        save_as: "{{ prefix }}_result"
`)
	writeFile(t, dir, "scenario.yaml", `
name: node-saveas-test
timeout: 1m
topology:
  nodes:
    m01:
      host: "127.0.0.1"
      is_local: true
phases:
  - include: template.yaml
    include_params:
      target_node: "m01"
      prefix: "kv"
`)
	s, err := ParseFile(filepath.Join(dir, "scenario.yaml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	act := s.Phases[0].Actions[0]
	if act.Node != "m01" {
		t.Errorf("node = %q, want m01", act.Node)
	}
	if act.SaveAs != "kv_result" {
		t.Errorf("save_as = %q, want kv_result", act.SaveAs)
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}
