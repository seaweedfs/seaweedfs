package mount

import (
	"hash/fnv"
)

// SeedPeer is a mount server present in the filer's mount registry.
// DataCenter and Rack ride along so downstream locality-aware decisions
// (peer re-ranking in the fetcher) don't have to re-query the filer.
type SeedPeer struct {
	PeerAddr   string
	DataCenter string
	Rack       string
}

// OwnerFor returns the peer_addr of the mount that is the HRW-assigned
// directory owner for fid on the given seed list.
//
// Rendezvous (highest-random-weight) hashing is deterministic for a given
// (fid, seed set): every mount computes the same owner without any
// consensus. When a mount joins or leaves the seed set, only 1/N of fids
// change owner — the rest remain on their existing mount.
//
// If seeds is empty, returns "".
//
// See design-weed-mount-peer-chunk-sharing.md §4.2.2.
func OwnerFor(fid string, seeds []SeedPeer) string {
	if len(seeds) == 0 {
		return ""
	}
	var bestScore uint64
	var bestAddr string
	for _, s := range seeds {
		score := hrwScore(s.PeerAddr, fid)
		if bestAddr == "" || score > bestScore || (score == bestScore && s.PeerAddr < bestAddr) {
			bestScore = score
			bestAddr = s.PeerAddr
		}
	}
	return bestAddr
}

// hrwScore combines peer_addr and fid into a 64-bit score. Uses FNV-1a
// (cheap and fine for balancing; not a security primitive).
func hrwScore(peerAddr, fid string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(peerAddr))
	_, _ = h.Write([]byte{0}) // separator so "ab" + "c" != "a" + "bc"
	_, _ = h.Write([]byte(fid))
	return h.Sum64()
}
