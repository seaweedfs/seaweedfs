package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// TrackPeerForTesting registers a peer as if the master had announced it, but
// without starting its subscription goroutine, so loop tests can drive the
// low-watermarks by hand. Test support only.
func (ma *MetaAggregator) TrackPeerForTesting(peer pb.ServerAddress) {
	ma.peerChansLock.Lock()
	ma.peerChans[peer] = make(chan struct{})
	ma.peerChansLock.Unlock()
	ma.initPeerWatermark(peer)
}

// ReportPeerWatermarksForTesting stands in for what a peer's stream reports:
// its delivery watermark (events and idle heartbeats) and its flush
// watermark. Test support only.
func (ma *MetaAggregator) ReportPeerWatermarksForTesting(peer pb.ServerAddress, deliveredTsNs, flushedTsNs int64) {
	ma.advancePeerWatermark(peer, deliveredTsNs)
	ma.advancePeerFlushWatermark(peer, flushedTsNs)
}
