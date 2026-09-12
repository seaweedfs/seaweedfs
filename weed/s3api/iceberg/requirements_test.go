package iceberg

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/table"
)

func TestNormalizeRequirementsAddsNullSnapshotID(t *testing.T) {
	// ClickHouse sends assert-ref-snapshot-id without snapshot-id when
	// asserting a branch does not yet exist. iceberg-go's parser rejects
	// an absent field; normalizeRequirements splices in an explicit null.
	raw := json.RawMessage(`[{"type":"assert-ref-snapshot-id","ref":"main"}]`)

	// Without normalization, iceberg-go rejects the requirement.
	var reqs table.Requirements
	if err := json.Unmarshal(raw, &reqs); err == nil {
		t.Fatal("iceberg-go accepted an assert-ref-snapshot-id without snapshot-id; expected an error")
	}

	normalized, err := normalizeRequirements(raw)
	if err != nil {
		t.Fatalf("normalizeRequirements: %v", err)
	}

	if err := json.Unmarshal(normalized, &reqs); err != nil {
		t.Fatalf("unmarshal normalized requirements: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("got %d requirements, want 1", len(reqs))
	}
	if reqs[0].GetType() != "assert-ref-snapshot-id" {
		t.Fatalf("requirement type = %s, want assert-ref-snapshot-id", reqs[0].GetType())
	}
}

func TestNormalizeRequirementsPreservesPresentSnapshotID(t *testing.T) {
	raw := json.RawMessage(`[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":42}]`)
	normalized, err := normalizeRequirements(raw)
	if err != nil {
		t.Fatalf("normalizeRequirements: %v", err)
	}

	var reqs table.Requirements
	if err := json.Unmarshal(normalized, &reqs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("got %d requirements, want 1", len(reqs))
	}
}

func TestNormalizeRequirementsPreservesExplicitNullSnapshotID(t *testing.T) {
	raw := json.RawMessage(`[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}]`)
	normalized, err := normalizeRequirements(raw)
	if err != nil {
		t.Fatalf("normalizeRequirements: %v", err)
	}

	var reqs table.Requirements
	if err := json.Unmarshal(normalized, &reqs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("got %d requirements, want 1", len(reqs))
	}
}

func TestNormalizeRequirementsLeavesOtherTypesUnchanged(t *testing.T) {
	raw := json.RawMessage(`[{"type":"assert-create"},{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000000"}]`)
	normalized, err := normalizeRequirements(raw)
	if err != nil {
		t.Fatalf("normalizeRequirements: %v", err)
	}

	var reqs table.Requirements
	if err := json.Unmarshal(normalized, &reqs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("got %d requirements, want 2", len(reqs))
	}
}

func TestNormalizeRequirementsEmptyInput(t *testing.T) {
	normalized, err := normalizeRequirements(nil)
	if err != nil {
		t.Fatalf("normalizeRequirements(nil): %v", err)
	}
	if normalized != nil {
		t.Fatalf("expected nil output for nil input, got %s", normalized)
	}
}
