package weed_server

import engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"

// StoreProjection keeps the latest published projection in the server-owned
// adapter cache used by outward/product-facing surfaces.
func (bs *BlockService) StoreProjection(volumeID string, projection engine.PublicationProjection) {
	if bs == nil {
		return
	}
	bs.coreProjMu.Lock()
	if bs.coreProj == nil {
		bs.coreProj = make(map[string]engine.PublicationProjection)
	}
	bs.coreProj[volumeID] = projection
	bs.coreProjMu.Unlock()
}
