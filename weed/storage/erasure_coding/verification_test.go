package erasure_coding

import (
	"strings"
	"testing"
)

func TestRequireRecoverableShardSet_AllPresent(t *testing.T) {
	var bits ShardBits
	for id := 0; id < TotalShardsCount; id++ {
		bits = bits.Set(ShardId(id))
	}
	degraded, err := RequireRecoverableShardSet(42, bits, DataShardsCount, TotalShardsCount)
	if err != nil {
		t.Fatalf("unexpected error for full set: %v", err)
	}
	if degraded {
		t.Error("full set must not be reported as degraded")
	}
}

func TestRequireRecoverableShardSet_DegradedButRecoverable(t *testing.T) {
	// 12 of 14 shards: enough to reconstruct (>= 10), so the source may be
	// deleted, but the caller is told to warn and schedule a rebuild.
	var bits ShardBits
	for id := 0; id < TotalShardsCount; id++ {
		if id == 3 || id == 7 {
			continue
		}
		bits = bits.Set(ShardId(id))
	}
	degraded, err := RequireRecoverableShardSet(42, bits, DataShardsCount, TotalShardsCount)
	if err != nil {
		t.Fatalf("recoverable set must not error: %v", err)
	}
	if !degraded {
		t.Error("missing shards must be reported as degraded")
	}
}

func TestRequireRecoverableShardSet_BelowDataShards(t *testing.T) {
	// 9 of 14 shards: one short of reconstructable; the source must be kept.
	var bits ShardBits
	for id := 0; id < DataShardsCount-1; id++ {
		bits = bits.Set(ShardId(id))
	}
	_, err := RequireRecoverableShardSet(42, bits, DataShardsCount, TotalShardsCount)
	if err == nil {
		t.Fatal("expected error for unrecoverable set, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "volume 42") {
		t.Errorf("error should name the volume id: %s", msg)
	}
	if !strings.Contains(msg, "9/14") {
		t.Errorf("error should report 9/14 shards present: %s", msg)
	}
	if !strings.Contains(msg, "[9 10 11 12 13]") {
		t.Errorf("error should list the missing ids: %s", msg)
	}
}

func TestRequireRecoverableShardSet_EmptyBitmap(t *testing.T) {
	_, err := RequireRecoverableShardSet(1, 0, DataShardsCount, TotalShardsCount)
	if err == nil {
		t.Fatal("expected error for empty bitmap")
	}
	if !strings.Contains(err.Error(), "0/14") {
		t.Errorf("error should report 0/14 shards: %s", err.Error())
	}
}

func TestRequireRecoverableShardSet_CustomRatio(t *testing.T) {
	// 6+3 ratio: total=9, all present
	var bits ShardBits
	for id := 0; id < 9; id++ {
		bits = bits.Set(ShardId(id))
	}
	if degraded, err := RequireRecoverableShardSet(7, bits, 6, 9); err != nil || degraded {
		t.Fatalf("full 6+3 set: degraded=%v err=%v", degraded, err)
	}

	// 6+3, missing shard 5: 8 >= 6 remain, recoverable but degraded
	bits = bits.Clear(5)
	if degraded, err := RequireRecoverableShardSet(7, bits, 6, 9); err != nil || !degraded {
		t.Fatalf("8/9 shards of 6+3: degraded=%v err=%v", degraded, err)
	}

	// 6+3, only 5 shards left: below dataShards, must error
	bits = ShardBits(0)
	for id := 0; id < 5; id++ {
		bits = bits.Set(ShardId(id))
	}
	_, err := RequireRecoverableShardSet(7, bits, 6, 9)
	if err == nil {
		t.Fatal("expected error with 5/9 shards in 6+3 ratio")
	}
	if !strings.Contains(err.Error(), "5/9") {
		t.Errorf("error should report 5/9: %s", err.Error())
	}
}

func TestRequireRecoverableShardSet_RejectsInvalidParams(t *testing.T) {
	if _, err := RequireRecoverableShardSet(1, 0, DataShardsCount, 0); err == nil {
		t.Error("expected error for totalShards=0")
	}
	if _, err := RequireRecoverableShardSet(1, 0, DataShardsCount, MaxShardCount+1); err == nil {
		t.Errorf("expected error for totalShards > MaxShardCount")
	}
	if _, err := RequireRecoverableShardSet(1, 0, 0, TotalShardsCount); err == nil {
		t.Error("expected error for dataShards=0")
	}
	if _, err := RequireRecoverableShardSet(1, 0, TotalShardsCount+1, TotalShardsCount); err == nil {
		t.Error("expected error for dataShards > totalShards")
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
