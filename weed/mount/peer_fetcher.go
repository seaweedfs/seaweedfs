package mount

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// peerLookupTimeout bounds the ChunkLookup RPC. Short because the result
// is consumed on the read critical path.
const peerLookupTimeout = 500 * time.Millisecond

// peerFetchTimeout bounds a single FetchChunk stream; on expiry we fall
// through to the next holder or the volume server.
const peerFetchTimeout = 5 * time.Second

// maxPeerFetchChunkBytes caps how much we will accept from a single
// FetchChunk stream. gRPC per-message size is already capped by the
// server option; this is belt-and-suspenders against a runaway peer.
const maxPeerFetchChunkBytes = 64 * 1024 * 1024

// tryPeerRead attempts to satisfy a read from a peer mount's chunk cache.
// Returns (bytesRead, modifiedTsNs, nil) on success. On any failure it
// returns (0, 0, err) so the caller falls through to
// entryChunkGroup.ReadDataAt (the volume-server path).
//
// Flow:
//   1. Resolve the offset's leaf chunk (flattening manifests).
//   2. Ask the HRW owner mount for current holders via ChunkLookup.
//   3. For each holder (LRU order from PR #5), open a FetchChunk stream,
//      assemble frames into a size-bounded buffer, and verify MD5
//      end-to-end against FileChunk.ETag.
//   4. On success, populate chunk_cache and enqueue an announce so
//      other mounts can discover us as a new holder.
func (fh *FileHandle) tryPeerRead(ctx context.Context, fileSize int64, buff []byte, offset int64, entry *LockedEntry) (int64, int64, error) {
	if fh.wfs.peerRegistrar == nil || fh.wfs.peerConnPool == nil {
		return 0, 0, fmt.Errorf("peer sharing not configured")
	}

	// Resolve offset → leaf chunk, flattening any manifest indirection.
	readStop := offset + int64(len(buff))
	if readStop > fileSize {
		readStop = fileSize
	}
	dataChunks, _, err := filer.ResolveChunkManifest(ctx, fh.wfs.LookupFn(), entry.GetEntry().Chunks, offset, readStop)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve manifest: %w", err)
	}
	targetChunk, chunkOffset := findChunkContaining(dataChunks, offset)
	if targetChunk == nil {
		return 0, 0, fmt.Errorf("no leaf chunk for offset %d", offset)
	}

	// Fail fast when the chunk is already cached locally: the fallback
	// ReadDataAt path will satisfy the read from chunkCache with no RPCs,
	// so dialing a peer (ChunkLookup + FetchChunk) would be pure overhead.
	if fh.wfs.chunkCache != nil && fh.wfs.chunkCache.IsInCache(targetChunk.FileId, true) {
		return 0, 0, fmt.Errorf("chunk already cached locally")
	}

	selfAddr := ""
	if fh.wfs.peerGrpcServer != nil {
		selfAddr = fh.wfs.peerGrpcServer.SelfAddr()
	}

	owner := fh.wfs.peerRegistrar.OwnerFor(targetChunk.FileId)
	if owner == "" || owner == selfAddr {
		return 0, 0, fmt.Errorf("no peer owner for fid %s", targetChunk.FileId)
	}

	holders, err := peerLookupHolders(ctx, fh.wfs.peerConnPool.Dialer(), owner, targetChunk.FileId)
	if err != nil {
		return 0, 0, fmt.Errorf("peer lookup: %w", err)
	}
	if len(holders) == 0 {
		return 0, 0, fmt.Errorf("no peer holder for fid %s", targetChunk.FileId)
	}

	// Re-rank holders by locality (same rack > same DC > elsewhere), keeping
	// the server's LRU order stable within each bucket. The server caps the
	// list at maxLookupHolders, so this is always a small N.
	sortHoldersByLocality(holders, fh.wfs.option.PeerDataCenter, fh.wfs.option.PeerRack)

	dial := fh.wfs.peerConnPool.Dialer()
	for _, h := range holders {
		if h.addr == selfAddr {
			continue
		}
		data, ferr := fetchChunkFromPeer(ctx, dial, h.addr, targetChunk.FileId, targetChunk.Size, targetChunk.ETag)
		if ferr != nil {
			glog.V(2).Infof("peer-fetch %s from %s: %v", targetChunk.FileId, h.addr, ferr)
			continue
		}
		if fh.wfs.chunkCache != nil {
			fh.wfs.chunkCache.SetChunk(targetChunk.FileId, data)
		}
		if fh.wfs.peerAnnouncer != nil {
			fh.wfs.peerAnnouncer.EnqueueAnnounce(targetChunk.FileId)
		}
		if chunkOffset >= int64(len(data)) {
			return 0, 0, fmt.Errorf("peer returned short chunk")
		}
		copied := copy(buff, data[chunkOffset:])
		return int64(copied), targetChunk.ModifiedTsNs, nil
	}
	return 0, 0, fmt.Errorf("no peer served fid %s", targetChunk.FileId)
}

func findChunkContaining(chunks []*filer_pb.FileChunk, offset int64) (*filer_pb.FileChunk, int64) {
	for _, c := range chunks {
		start := c.Offset
		stop := c.Offset + int64(c.Size)
		if offset >= start && offset < stop {
			return c, offset - start
		}
	}
	return nil, 0
}

// peerHolder is a holder entry carried through the fetcher: addr for
// dialing plus the DC/Rack labels the owner recorded at announce time, so
// the fetcher can re-rank by its own locality before dialing.
type peerHolder struct {
	addr string
	dc   string
	rack string
}

// peerLookupHolders calls ChunkLookup on the given HRW owner and returns
// the holders in the server-reported (LRU) order along with their locality
// labels. The pooled dialer is passed in from the caller.
func peerLookupHolders(ctx context.Context, dial MountPeerDialer, ownerAddr, fid string) ([]peerHolder, error) {
	client, closeFn, err := dial(ctx, ownerAddr)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	callCtx, cancel := context.WithTimeout(ctx, peerLookupTimeout)
	defer cancel()
	resp, err := client.ChunkLookup(callCtx, &mount_peer_pb.ChunkLookupRequest{FileIds: []string{fid}})
	if err != nil {
		return nil, err
	}
	set, ok := resp.PeersByFid[fid]
	if !ok || set == nil {
		return nil, nil
	}
	out := make([]peerHolder, 0, len(set.Peers))
	for _, p := range set.Peers {
		out = append(out, peerHolder{addr: p.PeerAddr, dc: p.DataCenter, rack: p.Rack})
	}
	return out, nil
}

// localityBucket scores how "close" a peer is to self. Lower is better:
//
//	0 = same rack in same DC      (shortest hop)
//	1 = same DC, different rack   (cross-rack but still intra-DC)
//	2 = different DC, or unknown  (anything else)
//
// Missing labels on either side fall to bucket 2 — we don't claim locality
// we can't prove.
func localityBucket(selfDC, selfRack, peerDC, peerRack string) int {
	if selfDC == "" || peerDC == "" || selfDC != peerDC {
		return 2
	}
	if selfRack != "" && peerRack != "" && selfRack == peerRack {
		return 0
	}
	return 1
}

// sortHoldersByLocality stable-sorts holders so the most local peers are
// tried first. The server-returned order is LRU (freshest holder first),
// and sort.SliceStable preserves that ordering within each locality
// bucket, so among equally-local peers we still prefer the freshest.
func sortHoldersByLocality(holders []peerHolder, selfDC, selfRack string) {
	if len(holders) < 2 {
		return
	}
	sort.SliceStable(holders, func(i, j int) bool {
		return localityBucket(selfDC, selfRack, holders[i].dc, holders[i].rack) <
			localityBucket(selfDC, selfRack, holders[j].dc, holders[j].rack)
	})
}

// fetchChunkFromPeer server-streams a chunk from the given peer and
// verifies the MD5 of the assembled bytes against expectedETag.
//
// expectedSize (when > 0) is the authoritative chunk length from the
// filer entry; we pre-allocate exactly that to avoid slice growth and
// reject streams that overshoot it. A zero expectedSize falls back to
// maxPeerFetchChunkBytes as a safety ceiling.
func fetchChunkFromPeer(ctx context.Context, dial MountPeerDialer, peerAddr, fid string, expectedSize uint64, expectedETag string) ([]byte, error) {
	client, closeFn, err := dial(ctx, peerAddr)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	callCtx, cancel := context.WithTimeout(ctx, peerFetchTimeout)
	defer cancel()

	stream, err := client.FetchChunk(callCtx, &mount_peer_pb.FetchChunkRequest{
		FileId:       fid,
		ExpectedEtag: expectedETag,
	})
	if err != nil {
		return nil, err
	}

	// capHint pre-sizes the assembly buffer from the filer-reported chunk
	// size. When the caller didn't know the size (expectedSize == 0), we
	// let append grow the buffer rather than reserve the 64 MiB ceiling —
	// typical chunks are a few MiB, and the maxPeerFetchChunkBytes check
	// during Recv is the real safety ceiling.
	capHint := expectedSize
	if capHint > maxPeerFetchChunkBytes {
		capHint = maxPeerFetchChunkBytes
	}
	buf := make([]byte, 0, capHint)

	for {
		resp, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			if status.Code(rerr) == codes.NotFound {
				return nil, fmt.Errorf("peer not cached")
			}
			return nil, rerr
		}
		if len(buf)+len(resp.Data) > int(maxPeerFetchChunkBytes) {
			return nil, fmt.Errorf("peer response exceeds max chunk size %d", maxPeerFetchChunkBytes)
		}
		buf = append(buf, resp.Data...)
	}

	if expectedSize > 0 && uint64(len(buf)) != expectedSize {
		return nil, fmt.Errorf("peer returned %d bytes, expected %d", len(buf), expectedSize)
	}

	// Per-chunk integrity check — the peer is treated as untrusted.
	if expectedETag != "" {
		sum := md5.Sum(buf)
		got := hex.EncodeToString(sum[:])
		if got != expectedETag {
			return nil, fmt.Errorf("etag mismatch: peer=%s got=%s want=%s", peerAddr, got, expectedETag)
		}
	}
	return buf, nil
}
