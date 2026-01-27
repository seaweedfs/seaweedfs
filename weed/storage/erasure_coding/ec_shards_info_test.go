package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestShardsInfo_SetAndGet(t *testing.T) {
	si := NewShardsInfo()

	// Test setting shards
	si.Set(ShardInfo{Id: 0, Size: 1000})
	si.Set(ShardInfo{Id: 5, Size: 2000})
	si.Set(ShardInfo{Id: 13, Size: 3000})

	// Verify Has
	if !si.Has(0) {
		t.Error("Expected shard 0 to exist")
	}
	if !si.Has(5) {
		t.Error("Expected shard 5 to exist")
	}
	if !si.Has(13) {
		t.Error("Expected shard 13 to exist")
	}
	if si.Has(1) {
		t.Error("Expected shard 1 to not exist")
	}

	// Verify Size
	if got := si.Size(0); got != 1000 {
		t.Errorf("Expected size 1000, got %d", got)
	}
	if got := si.Size(5); got != 2000 {
		t.Errorf("Expected size 2000, got %d", got)
	}
	if got := si.Size(13); got != 3000 {
		t.Errorf("Expected size 3000, got %d", got)
	}

	// Verify Count
	if got := si.Count(); got != 3 {
		t.Errorf("Expected count 3, got %d", got)
	}

	// Verify Bitmap
	expectedBitmap := uint32((1 << 0) | (1 << 5) | (1 << 13))
	if got := si.Bitmap(); got != expectedBitmap {
		t.Errorf("Expected bitmap %b, got %b", expectedBitmap, got)
	}
}

func TestShardsInfo_SortedOrder(t *testing.T) {
	si := NewShardsInfo()

	// Add shards in non-sequential order
	si.Set(ShardInfo{Id: 10, Size: 1000})
	si.Set(ShardInfo{Id: 2, Size: 2000})
	si.Set(ShardInfo{Id: 7, Size: 3000})
	si.Set(ShardInfo{Id: 0, Size: 4000})

	// Verify Ids returns sorted order
	ids := si.Ids()
	expected := []ShardId{0, 2, 7, 10}
	if len(ids) != len(expected) {
		t.Fatalf("Expected %d ids, got %d", len(expected), len(ids))
	}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("Expected id[%d]=%d, got %d", i, expected[i], id)
		}
	}
}

func TestShardsInfo_Delete(t *testing.T) {
	si := NewShardsInfo()

	si.Set(ShardInfo{Id: 0, Size: 1000})
	si.Set(ShardInfo{Id: 5, Size: 2000})
	si.Set(ShardInfo{Id: 10, Size: 3000})

	// Delete middle shard
	si.Delete(5)

	if si.Has(5) {
		t.Error("Expected shard 5 to be deleted")
	}
	if !si.Has(0) || !si.Has(10) {
		t.Error("Expected other shards to remain")
	}
	if got := si.Count(); got != 2 {
		t.Errorf("Expected count 2, got %d", got)
	}

	// Verify slice is still sorted
	ids := si.Ids()
	if len(ids) != 2 || ids[0] != 0 || ids[1] != 10 {
		t.Errorf("Expected ids [0, 10], got %v", ids)
	}
}

func TestShardsInfo_Update(t *testing.T) {
	si := NewShardsInfo()

	si.Set(ShardInfo{Id: 5, Size: 1000})

	// Update existing shard
	si.Set(ShardInfo{Id: 5, Size: 2000})

	if got := si.Size(5); got != 2000 {
		t.Errorf("Expected updated size 2000, got %d", got)
	}
	if got := si.Count(); got != 1 {
		t.Errorf("Expected count to remain 1, got %d", got)
	}
}

func TestShardsInfo_TotalSize(t *testing.T) {
	si := NewShardsInfo()

	si.Set(ShardInfo{Id: 0, Size: 1000})
	si.Set(ShardInfo{Id: 5, Size: 2000})
	si.Set(ShardInfo{Id: 10, Size: 3000})

	expected := ShardSize(6000)
	if got := si.TotalSize(); got != expected {
		t.Errorf("Expected total size %d, got %d", expected, got)
	}
}

func TestShardsInfo_Sizes(t *testing.T) {
	si := NewShardsInfo()

	si.Set(ShardInfo{Id: 2, Size: 100})
	si.Set(ShardInfo{Id: 5, Size: 200})
	si.Set(ShardInfo{Id: 8, Size: 300})

	sizes := si.Sizes()
	expected := []ShardSize{100, 200, 300}

	if len(sizes) != len(expected) {
		t.Fatalf("Expected %d sizes, got %d", len(expected), len(sizes))
	}
	for i, size := range sizes {
		if size != expected[i] {
			t.Errorf("Expected size[%d]=%d, got %d", i, expected[i], size)
		}
	}
}

func TestShardsInfo_Copy(t *testing.T) {
	si := NewShardsInfo()
	si.Set(ShardInfo{Id: 0, Size: 1000})
	si.Set(ShardInfo{Id: 5, Size: 2000})

	siCopy := si.Copy()

	// Verify copy has same data
	if !siCopy.Has(0) || !siCopy.Has(5) {
		t.Error("Copy should have same shards")
	}
	if siCopy.Size(0) != 1000 || siCopy.Size(5) != 2000 {
		t.Error("Copy should have same sizes")
	}

	// Modify original
	si.Set(ShardInfo{Id: 10, Size: 3000})

	// Verify copy is independent
	if siCopy.Has(10) {
		t.Error("Copy should be independent of original")
	}
}

func TestShardsInfo_AddSubtract(t *testing.T) {
	si1 := NewShardsInfo()
	si1.Set(ShardInfo{Id: 0, Size: 1000})
	si1.Set(ShardInfo{Id: 2, Size: 2000})

	si2 := NewShardsInfo()
	si2.Set(ShardInfo{Id: 2, Size: 9999}) // Different size
	si2.Set(ShardInfo{Id: 5, Size: 3000})

	// Test Add
	si1.Add(si2)
	if !si1.Has(0) || !si1.Has(2) || !si1.Has(5) {
		t.Error("Add should merge shards")
	}
	if si1.Size(2) != 9999 {
		t.Error("Add should update existing shard size")
	}

	// Test Subtract
	si1.Subtract(si2)
	if si1.Has(2) || si1.Has(5) {
		t.Error("Subtract should remove shards")
	}
	if !si1.Has(0) {
		t.Error("Subtract should keep non-matching shards")
	}
}

func TestShardsInfo_PlusMinus(t *testing.T) {
	si1 := NewShardsInfo()
	si1.Set(ShardInfo{Id: 0, Size: 1000})
	si1.Set(ShardInfo{Id: 2, Size: 2000})

	si2 := NewShardsInfo()
	si2.Set(ShardInfo{Id: 2, Size: 2000})
	si2.Set(ShardInfo{Id: 5, Size: 3000})

	// Test Plus
	result := si1.Plus(si2)
	if !result.Has(0) || !result.Has(2) || !result.Has(5) {
		t.Error("Plus should merge into new instance")
	}
	if si1.Has(5) {
		t.Error("Plus should not modify original")
	}

	// Test Minus
	result = si1.Minus(si2)
	if !result.Has(0) || result.Has(2) {
		t.Error("Minus should subtract into new instance")
	}
	if !si1.Has(2) {
		t.Error("Minus should not modify original")
	}
}

func TestShardsInfo_DeleteParityShards(t *testing.T) {
	si := NewShardsInfo()

	// Add data shards (0-9)
	for i := 0; i < DataShardsCount; i++ {
		si.Set(ShardInfo{Id: ShardId(i), Size: ShardSize((i + 1) * 1000)})
	}

	// Add parity shards (10-13)
	for i := DataShardsCount; i < TotalShardsCount; i++ {
		si.Set(ShardInfo{Id: ShardId(i), Size: ShardSize((i + 1) * 1000)})
	}

	si.DeleteParityShards()

	// Verify only data shards remain
	for i := 0; i < DataShardsCount; i++ {
		if !si.Has(ShardId(i)) {
			t.Errorf("Expected data shard %d to remain", i)
		}
	}
	for i := DataShardsCount; i < TotalShardsCount; i++ {
		if si.Has(ShardId(i)) {
			t.Errorf("Expected parity shard %d to be deleted", i)
		}
	}
}

func TestShardsInfo_FromVolumeEcShardInformationMessage(t *testing.T) {
	tests := []struct {
		name      string
		msg       *master_pb.VolumeEcShardInformationMessage
		wantBits  uint32
		wantSizes []int64
	}{
		{
			name:      "nil message",
			msg:       nil,
			wantBits:  0,
			wantSizes: []int64{},
		},
		{
			name: "single shard",
			msg: &master_pb.VolumeEcShardInformationMessage{
				EcIndexBits: 1 << 5,
				ShardSizes:  []int64{12345},
			},
			wantBits:  1 << 5,
			wantSizes: []int64{12345},
		},
		{
			name: "multiple shards",
			msg: &master_pb.VolumeEcShardInformationMessage{
				EcIndexBits: (1 << 0) | (1 << 3) | (1 << 7),
				ShardSizes:  []int64{1000, 2000, 3000},
			},
			wantBits:  (1 << 0) | (1 << 3) | (1 << 7),
			wantSizes: []int64{1000, 2000, 3000},
		},
		{
			name: "missing sizes",
			msg: &master_pb.VolumeEcShardInformationMessage{
				EcIndexBits: (1 << 0) | (1 << 3),
				ShardSizes:  []int64{1000},
			},
			wantBits:  (1 << 0) | (1 << 3),
			wantSizes: []int64{1000, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			si := ShardsInfoFromVolumeEcShardInformationMessage(tt.msg)

			if got := si.Bitmap(); got != tt.wantBits {
				t.Errorf("Bitmap() = %b, want %b", got, tt.wantBits)
			}

			if got := si.SizesInt64(); len(got) != len(tt.wantSizes) {
				t.Errorf("SizesInt64() length = %d, want %d", len(got), len(tt.wantSizes))
			} else {
				for i, size := range got {
					if size != tt.wantSizes[i] {
						t.Errorf("SizesInt64()[%d] = %d, want %d", i, size, tt.wantSizes[i])
					}
				}
			}
		})
	}
}

func TestShardsInfo_String(t *testing.T) {
	si := NewShardsInfo()
	si.Set(ShardInfo{Id: 0, Size: 1024})
	si.Set(ShardInfo{Id: 5, Size: 2048})

	str := si.String()
	if str == "" {
		t.Error("String() should not be empty")
	}
	// Basic validation - should contain shard IDs
	if len(str) < 3 {
		t.Errorf("String() too short: %s", str)
	}
}

func BenchmarkShardsInfo_Set(b *testing.B) {
	si := NewShardsInfo()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.Set(ShardInfo{Id: ShardId(i % TotalShardsCount), Size: ShardSize(i * 1000)})
	}
}

func BenchmarkShardsInfo_Has(b *testing.B) {
	si := NewShardsInfo()
	for i := 0; i < TotalShardsCount; i++ {
		si.Set(ShardInfo{Id: ShardId(i), Size: ShardSize(i * 1000)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.Has(ShardId(i % TotalShardsCount))
	}
}

func BenchmarkShardsInfo_Size(b *testing.B) {
	si := NewShardsInfo()
	for i := 0; i < TotalShardsCount; i++ {
		si.Set(ShardInfo{Id: ShardId(i), Size: ShardSize(i * 1000)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.Size(ShardId(i % TotalShardsCount))
	}
}
