package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeIdDigestHash spreads a volume id across the whole 64-bit range so that
// xoring a set of them together does not collide for the near-consecutive ids a
// cluster actually allocates.
func VolumeIdDigestHash(vid needle.VolumeId) uint64 {
	h := uint64(vid) + 0x9E3779B97F4A7C15
	h = (h ^ (h >> 30)) * 0xBF58476D1CE4E5B9
	h = (h ^ (h >> 27)) * 0x94D049BB133111EB
	return h ^ (h >> 31)
}

// VolumeIndexDigests returns the digest of the volumes the node's disks hold and
// the digest of the volumes the lookup index will serve from that node.
//
// The master keeps those two indexes separately, and a disconnect racing a
// reconnect has been seen to drop a volume from the lookup index while leaving
// it on the node: the volume stays visible in volume.list and the admin UI while
// LookupVolume answers "volume id not found". A volume server cannot detect that
// -- its own report is identical either way -- so the master has to notice it
// on its own rather than rely on the heartbeat digest.
func (dn *DataNode) VolumeIndexDigests() (held, servable uint64) {
	dn.RLock()
	for _, c := range dn.children {
		held ^= c.(*Disk).VolumeIdDigest()
	}
	dn.RUnlock()
	return held, dn.lookupDigest.Load()
}

// HasConsistentVolumeIndex reports whether every volume on the node is reachable
// through the lookup index, and nothing else is.
func (dn *DataNode) HasConsistentVolumeIndex() bool {
	held, servable := dn.VolumeIndexDigests()
	return held == servable
}

// releaseLookupOwnership clears the lookup digest bit of every location this
// layout serves. Used when the layout is dropped wholesale with its collection
// rather than volume by volume.
func (vl *VolumeLayout) releaseLookupOwnership() {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()
	for vid, location := range vl.vid2location {
		for _, dn := range location.list {
			moveLookupOwnership(vid, dn, nil)
		}
	}
	vl.vid2location = make(map[needle.VolumeId]*VolumeLocationList)
}

// moveLookupOwnership transfers the digest bit for vid from the node a lookup
// entry used to name to the node it now names. Either may be nil, for an entry
// being created or dropped.
func moveLookupOwnership(vid needle.VolumeId, from, to *DataNode) {
	if from == to {
		return
	}
	if from != nil {
		from.trackLookupChange(vid)
	}
	if to != nil {
		to.trackLookupChange(vid)
	}
}

// trackLookupChange records that vid became reachable through this node, or
// stopped being: xor is its own inverse, so both are the same update. Volume
// layouts for different collections share the node, so it cannot assume the
// caller's lock.
func (dn *DataNode) trackLookupChange(vid needle.VolumeId) {
	delta := VolumeIdDigestHash(vid)
	for {
		old := dn.lookupDigest.Load()
		if dn.lookupDigest.CompareAndSwap(old, old^delta) {
			return
		}
	}
}
