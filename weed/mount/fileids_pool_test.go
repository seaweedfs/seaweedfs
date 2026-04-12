package mount

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

// mockFilerClient simulates AssignVolume RPCs with configurable latency.
type mockFilerClient struct {
	latency   time.Duration
	nextVolId uint32
	nextKey   uint64
	mu        sync.Mutex
}

func (m *mockFilerClient) AssignVolume(_ context.Context, req *filer_pb.AssignVolumeRequest, _ ...grpc.CallOption) (*filer_pb.AssignVolumeResponse, error) {
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	count := int(req.Count)
	if count <= 0 {
		count = 1
	}
	vid := needle.VolumeId(m.nextVolId + 1)
	key := m.nextKey + 1000 // start at a non-zero key for valid hex encoding
	m.nextKey += uint64(count)
	m.nextVolId++

	fid := needle.NewFileId(vid, key, 0x12345678)
	return &filer_pb.AssignVolumeResponse{
		FileId: fid.String(),
		Count:  int32(count),
		Auth:   "test-jwt-token",
		Location: &filer_pb.Location{
			Url:       "127.0.0.1:8080",
			PublicUrl: "127.0.0.1:8080",
			GrpcPort:  18080,
		},
	}, nil
}

// TestFileIdPoolSequentialIds verifies that batch assignment generates
// correct sequential file IDs from a single AssignVolume response.
func TestFileIdPoolSequentialIds(t *testing.T) {
	mock := &mockFilerClient{}
	resp, err := mock.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{Count: 5})
	if err != nil {
		t.Fatal(err)
	}

	baseFid, parseErr := needle.ParseFileIdFromString(resp.FileId)
	if parseErr != nil {
		t.Fatal(parseErr)
	}

	baseKey := uint64(baseFid.Key)
	for i := 0; i < int(resp.Count); i++ {
		fid := needle.NewFileId(baseFid.VolumeId, baseKey+uint64(i), uint32(baseFid.Cookie))
		t.Logf("ID %d: %s", i, fid.String())
		// Verify each ID can be parsed back
		parsed, err := needle.ParseFileIdFromString(fid.String())
		if err != nil {
			t.Fatalf("Failed to parse sequential ID %d: %v", i, err)
		}
		if uint64(parsed.Key) != baseKey+uint64(i) {
			t.Fatalf("ID %d: expected key %d, got %d", i, baseKey+uint64(i), uint64(parsed.Key))
		}
	}
}

// TestFileIdPoolExpiry verifies that expired entries are evicted.
func TestFileIdPoolExpiry(t *testing.T) {
	pool := &FileIdPool{
		maxAge: 50 * time.Millisecond,
	}
	now := time.Now()
	pool.entries = []FileIdEntry{
		{FileId: "1,old", Time: now.Add(-100 * time.Millisecond)},
		{FileId: "2,old", Time: now.Add(-100 * time.Millisecond)},
		{FileId: "3,fresh", Time: now},
	}
	pool.evictExpired()
	if len(pool.entries) != 1 {
		t.Fatalf("expected 1 entry after eviction, got %d", len(pool.entries))
	}
	if pool.entries[0].FileId != "3,fresh" {
		t.Fatalf("expected fresh entry, got %s", pool.entries[0].FileId)
	}
}

// BenchmarkPoolGetVsDirectAssign measures the latency difference between
// getting a file ID from the pool vs a direct (simulated) AssignVolume RPC.
//
// This simulates the per-chunk overhead: each chunk upload needs a file ID.
// With the pool, Get() is a local mutex+slice operation.
// Without the pool, each chunk blocks on an AssignVolume RPC.
func BenchmarkPoolGetVsDirectAssign(b *testing.B) {
	assignLatencies := []time.Duration{
		0,
		100 * time.Microsecond,
		1 * time.Millisecond,
		5 * time.Millisecond,
	}

	for _, latency := range assignLatencies {
		name := "latency=" + latency.String()

		b.Run("DirectAssign/"+name, func(b *testing.B) {
			mock := &mockFilerClient{latency: latency}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := mock.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{Count: 1})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("PoolGet/"+name, func(b *testing.B) {
			// Pre-fill pool with enough entries
			pool := &FileIdPool{
				maxAge: 30 * time.Second,
			}
			now := time.Now()
			for i := 0; i < b.N+100; i++ {
				pool.entries = append(pool.entries, FileIdEntry{
					FileId: fmt.Sprintf("1,%x12345678", i),
					Host:   "127.0.0.1:8080",
					Auth:   security.EncodedJwt("test-jwt"),
					Time:   now,
				})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool.mu.Lock()
				if len(pool.entries) > 0 {
					_ = pool.entries[0]
					pool.entries = pool.entries[1:]
				}
				pool.mu.Unlock()
			}
		})
	}
}

// BenchmarkConcurrentPoolGet measures pool throughput under concurrent access,
// simulating multiple upload workers grabbing file IDs simultaneously.
func BenchmarkConcurrentPoolGet(b *testing.B) {
	for _, workers := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			pool := &FileIdPool{
				maxAge: 30 * time.Second,
			}
			// Pre-fill with enough entries
			now := time.Now()
			total := b.N * workers
			pool.entries = make([]FileIdEntry, total)
			for i := range pool.entries {
				pool.entries[i] = FileIdEntry{
					FileId: fmt.Sprintf("1,%x12345678", i),
					Host:   "127.0.0.1:8080",
					Auth:   security.EncodedJwt("test-jwt"),
					Time:   now,
				}
			}

			var ops atomic.Int64
			b.ResetTimer()

			var wg sync.WaitGroup
			perWorker := b.N
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						pool.mu.Lock()
						if len(pool.entries) > 0 {
							_ = pool.entries[0]
							pool.entries = pool.entries[1:]
							ops.Add(1)
						}
						pool.mu.Unlock()
					}
				}()
			}
			wg.Wait()
			b.ReportMetric(float64(ops.Load())/b.Elapsed().Seconds(), "ids/sec")
		})
	}
}

// BenchmarkBatchAssign measures the cost of batch vs individual assign RPCs,
// showing the amortization benefit of Count>1.
func BenchmarkBatchAssign(b *testing.B) {
	for _, batchSize := range []int{1, 8, 16, 32} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			mock := &mockFilerClient{latency: 1 * time.Millisecond}
			totalIds := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := mock.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{Count: int32(batchSize)})
				if err != nil {
					b.Fatal(err)
				}

				// Simulate generating sequential IDs
				baseFid, _ := needle.ParseFileIdFromString(resp.FileId)
				baseKey := uint64(baseFid.Key)
				for j := 0; j < int(resp.Count); j++ {
					_ = needle.NewFileId(baseFid.VolumeId, baseKey+uint64(j), uint32(baseFid.Cookie)).String()
					totalIds++
				}
			}
			b.ReportMetric(float64(totalIds)/b.Elapsed().Seconds(), "ids/sec")
		})
	}
}
