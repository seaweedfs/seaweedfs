package erasure_coding

import (
	"strings"
	"testing"
)

func TestRequireFullShardSet_AllPresent(t *testing.T) {
	var bits ShardBits
	for id := 0; id < TotalShardsCount; id++ {
		bits = bits.Set(ShardId(id))
	}
	if err := RequireFullShardSet(42, bits); err != nil {
		t.Fatalf("unexpected error for full set: %v", err)
	}
}

func TestRequireFullShardSet_ReportsMissingIds(t *testing.T) {
	var bits ShardBits
	for id := 0; id < TotalShardsCount; id++ {
		if id == 3 || id == 7 {
			continue
		}
		bits = bits.Set(ShardId(id))
	}
	err := RequireFullShardSet(42, bits)
	if err == nil {
		t.Fatal("expected error for incomplete set, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "volume 42") {
		t.Errorf("error should name the volume id: %s", msg)
	}
	if !strings.Contains(msg, "[3 7]") {
		t.Errorf("error should list missing ids 3 and 7: %s", msg)
	}
	if !strings.Contains(msg, "12/14") {
		t.Errorf("error should report 12/14 shards present: %s", msg)
	}
}

func TestRequireFullShardSet_EmptyBitmap(t *testing.T) {
	err := RequireFullShardSet(1, 0)
	if err == nil {
		t.Fatal("expected error for empty bitmap")
	}
	if !strings.Contains(err.Error(), "0/14") {
		t.Errorf("error should report 0/14 shards: %s", err.Error())
	}
}

func TestSummarizeShardInventory_Deterministic(t *testing.T) {
	perServer := map[string]ServerShardInventory{
		"10.0.0.2:8080": {Bits: ShardBits(0).Set(4).Set(5).Set(6)},
		"10.0.0.1:8080": {Bits: ShardBits(0).Set(0).Set(1).Set(2).Set(3)},
	}
	got := SummarizeShardInventory(perServer)
	want := "10.0.0.1:8080=[0 1 2 3] 10.0.0.2:8080=[4 5 6]"
	if got != want {
		t.Errorf("summary mismatch\n  got:  %q\n  want: %q", got, want)
	}
}

func TestSummarizeShardInventory_IncludesError(t *testing.T) {
	perServer := map[string]ServerShardInventory{
		"10.0.0.1:8080": {Bits: ShardBits(0).Set(0).Set(1), QueryError: errStr("dial timeout")},
	}
	got := SummarizeShardInventory(perServer)
	if !strings.Contains(got, "ERR:dial timeout") {
		t.Errorf("expected error tag in summary, got %q", got)
	}
}

type errStr string

func (e errStr) Error() string { return string(e) }
