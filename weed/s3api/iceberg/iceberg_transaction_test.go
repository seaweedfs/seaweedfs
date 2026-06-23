package iceberg

import (
	"encoding/json"
	"testing"
)

func TestDecodeCommitTransactionRequest(t *testing.T) {
	body := `{"table-changes":[
		{"identifier":{"namespace":["ns"],"name":"t1"},"requirements":[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}],"updates":[{"action":"set-properties","updates":{"k":"v"}}]},
		{"identifier":{"namespace":["ns","sub"],"name":"t2"},"requirements":[],"updates":[]}
	]}`

	var req CommitTransactionRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(req.TableChanges) != 2 {
		t.Fatalf("table-changes = %d, want 2", len(req.TableChanges))
	}
	if req.TableChanges[0].Identifier.Name != "t1" {
		t.Fatalf("change[0] name = %q, want t1", req.TableChanges[0].Identifier.Name)
	}
	if got := []string(req.TableChanges[1].Identifier.Namespace); len(got) != 2 || got[1] != "sub" {
		t.Fatalf("change[1] namespace = %v, want [ns sub]", got)
	}
}

func TestParseTableChangeSeparatesStatistics(t *testing.T) {
	change := tableChangeRequest{
		Requirements: json.RawMessage(`[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}]`),
		Updates: []json.RawMessage{
			json.RawMessage(`{"action":"set-properties","updates":{"k":"v"}}`),
			json.RawMessage(`{"action":"set-statistics","snapshot-id":10,"statistics-path":"s3://bucket/table/metadata/stats.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":20,"blob-metadata":[]}`),
		},
	}

	requirements, updates, statistics, reqErr := parseTableChange(change)
	if reqErr != nil {
		t.Fatalf("parseTableChange() error = %+v", reqErr)
	}
	if len(requirements) != 1 {
		t.Fatalf("requirements = %d, want 1", len(requirements))
	}
	if len(updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(updates))
	}
	if len(statistics) != 1 || statistics[0].set == nil || statistics[0].set.SnapshotID != 10 {
		t.Fatalf("unexpected statistics updates: %#v", statistics)
	}
}

func TestParseTableChangeRejectsInvalidRequirements(t *testing.T) {
	change := tableChangeRequest{Requirements: json.RawMessage(`{not json}`)}
	_, _, _, reqErr := parseTableChange(change)
	if reqErr == nil {
		t.Fatalf("parseTableChange() expected error for invalid requirements")
	}
	if reqErr.status != 400 {
		t.Fatalf("status = %d, want 400", reqErr.status)
	}
}
