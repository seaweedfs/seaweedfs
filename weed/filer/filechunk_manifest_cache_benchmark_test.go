package filer

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func BenchmarkManifestResolutionRepeatedOpen(b *testing.B) {
	for _, delay := range []time.Duration{0, 5 * time.Millisecond, 20 * time.Millisecond} {
		b.Run(fmt.Sprintf("latency=%s", delay), func(b *testing.B) {
			fixture := newManifestReadFixture(b, map[string][]*filer_pb.FileChunk{
				"benchmark-cached": {resolveTestData("benchmark-data", 0)},
			}, map[string]time.Duration{"benchmark-cached": delay})
			var lookups atomic.Int32
			lookup := func(ctx context.Context, fileID string) ([]string, error) {
				lookups.Add(1)
				return fixture.lookup(ctx, fileID)
			}
			chunk := newManifestCacheTestChunk("benchmark-cached")
			cache := NewChunkManifestCache(MaxMountChunkManifestCacheEntries, MaxMountChunkManifestCacheBytes)
			_, err := resolveOneChunkManifest(context.Background(), lookup, chunk, nil, cache)
			if err != nil {
				b.Fatal(err)
			}
			fixture.loads.Store(0)
			lookups.Store(0)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := resolveOneChunkManifest(context.Background(), lookup, chunk, nil, cache); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.loads.Load())/float64(b.N), "fetches/op")
			b.ReportMetric(float64(lookups.Load())/float64(b.N), "lookups/op")
		})
	}
}

func BenchmarkManifestResolutionWithoutCache(b *testing.B) {
	for _, delay := range []time.Duration{0, 5 * time.Millisecond, 20 * time.Millisecond} {
		b.Run(fmt.Sprintf("latency=%s", delay), func(b *testing.B) {
			fixture := newManifestReadFixture(b, map[string][]*filer_pb.FileChunk{
				"benchmark-uncached": {resolveTestData("benchmark-data", 0)},
			}, map[string]time.Duration{"benchmark-uncached": delay})
			var lookups atomic.Int32
			lookup := func(ctx context.Context, fileID string) ([]string, error) {
				lookups.Add(1)
				return fixture.lookup(ctx, fileID)
			}
			chunk := newManifestCacheTestChunk("benchmark-uncached")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ResolveOneChunkManifest(context.Background(), lookup, chunk, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.loads.Load())/float64(b.N), "fetches/op")
			b.ReportMetric(float64(lookups.Load())/float64(b.N), "lookups/op")
		})
	}
}
