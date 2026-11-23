package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
)

// TestShouldCacheUploadedChunk pins the chunk-caching decision. The read-auth
// case is the one 0005 adds: a keyless mount must cache every uploaded chunk
// (not only the first) so a read-after-write of a non-first chunk is served
// from the cache before the chunk's entry is published and a token exists.
func TestShouldCacheUploadedChunk(t *testing.T) {
	cache := &chunk_cache.TieredChunkCache{}
	for _, tc := range []struct {
		name          string
		chunkCache    *chunk_cache.TieredChunkCache
		peerAnnouncer *PeerAnnouncer
		readAuth      *ReadAuth
		offset        int64
		want          bool
	}{
		{"no cache configured", nil, nil, nil, 0, false},
		{"first chunk, plain mount", cache, nil, nil, 0, true},
		{"non-first chunk, plain mount", cache, nil, nil, 1 << 20, false},
		{"non-first chunk, peer sharing", cache, &PeerAnnouncer{}, nil, 1 << 20, true},
		{"non-first chunk, read-auth mount", cache, nil, &ReadAuth{}, 1 << 20, true},
		{"read-auth but cache disabled", nil, nil, &ReadAuth{}, 1 << 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs := &WFS{chunkCache: tc.chunkCache, peerAnnouncer: tc.peerAnnouncer, readAuth: tc.readAuth}
			if got := wfs.shouldCacheUploadedChunk(tc.offset); got != tc.want {
				t.Fatalf("shouldCacheUploadedChunk(offset=%d)=%v, want %v", tc.offset, got, tc.want)
			}
		})
	}
}
