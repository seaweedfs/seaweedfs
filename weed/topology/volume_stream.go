package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// StreamVolumes hands every volume the filter selects to send, in batches, so
// that neither end holds the whole cluster to move it. Batches are built under
// their disk's lock and handed over outside it, so a slow reader stalls the
// stream rather than the topology.
//
// Only the disks `listed` names are streamed. That listing went out first, and
// the topology is walked again here, so without it a disk registering in
// between would have its volumes sent to a client with nowhere to put them.
// Bounding the walk by what was already announced makes the two agree by
// construction: a disk arriving mid-listing is in neither, and is reported by
// the next one.
//
// The batches still do not share one instant. Neither did a single listing,
// which takes each disk's lock in turn, so a volume that moves while either is
// running can be seen twice or not at all.
func (t *Topology) StreamVolumes(listed *master_pb.TopologyInfo, filter VolumeFilter, batchSize int, send func(*master_pb.VolumeListStreamResponse) error) error {
	if batchSize <= 0 {
		batchSize = defaultVolumeStreamBatch
	}
	announced := announcedDisks(listed)
	for _, dcNode := range t.Children() {
		dc := dcNode.(*DataCenter)
		for _, rackNode := range dc.Children() {
			rack := rackNode.(*Rack)
			for _, dnNode := range rack.Children() {
				dn := dnNode.(*DataNode)
				for _, diskNode := range dn.Children() {
					disk := diskNode.(*Disk)
					if !announced[[4]string{string(dc.Id()), string(rack.Id()), string(dn.Id()), string(disk.Id())}] {
						continue
					}
					batch := func() *master_pb.VolumeListStreamResponse {
						return &master_pb.VolumeListStreamResponse{
							DataCenter: string(dc.Id()),
							Rack:       string(rack.Id()),
							DataNode:   string(dn.Id()),
							DiskType:   string(disk.Id()),
						}
					}
					if err := disk.streamVolumes(filter, batchSize, batch, send); err != nil {
						return err
					}
					if err := disk.streamEcShards(filter, batchSize, batch, send); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

const defaultVolumeStreamBatch = 10000

// announcedDisks names the disks a listing carried.
func announcedDisks(listed *master_pb.TopologyInfo) map[[4]string]bool {
	announced := make(map[[4]string]bool)
	if listed == nil {
		return announced
	}
	for _, dc := range listed.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for diskType := range node.DiskInfos {
					announced[[4]string{dc.Id, rack.Id, node.Id, diskType}] = true
				}
			}
		}
	}
	return announced
}

// streamVolumes sends this disk's volumes a batch at a time. The ids are taken
// in one pass and the messages built in later ones, so the lock is held for a
// batch rather than for the disk, and only 4 bytes per volume are carried
// between passes. A volume that leaves in between is simply not sent.
func (d *Disk) streamVolumes(filter VolumeFilter, batchSize int, newBatch func() *master_pb.VolumeListStreamResponse, send func(*master_pb.VolumeListStreamResponse) error) error {
	d.RLock()
	ids := make([]needle.VolumeId, 0, len(d.volumes))
	for id := range d.volumes {
		ids = append(ids, id)
	}
	d.RUnlock()

	for start := 0; start < len(ids); start += batchSize {
		end := min(start+batchSize, len(ids))
		batch := newBatch()

		d.RLock()
		for _, id := range ids[start:end] {
			v, found := d.volumes[id]
			if !found || !filter.matches(v.Collection, v.Id) {
				continue
			}
			batch.VolumeInfos = append(batch.VolumeInfos, v.ToVolumeInformationMessage())
		}
		d.RUnlock()

		if len(batch.VolumeInfos) == 0 {
			continue
		}
		if err := send(batch); err != nil {
			return err
		}
	}
	return nil
}

func (d *Disk) streamEcShards(filter VolumeFilter, batchSize int, newBatch func() *master_pb.VolumeListStreamResponse, send func(*master_pb.VolumeListStreamResponse) error) error {
	// GetEcShards already copies under the lock, and a cluster holds far fewer
	// ec shards than volumes, so these only need cutting into batches.
	shards := d.GetEcShards()
	for start := 0; start < len(shards); start += batchSize {
		end := min(start+batchSize, len(shards))
		batch := newBatch()
		for _, ecv := range shards[start:end] {
			if !filter.matches(ecv.Collection, ecv.VolumeId) {
				continue
			}
			batch.EcShardInfos = append(batch.EcShardInfos, ecv.ToVolumeEcShardInformationMessage())
		}
		if len(batch.EcShardInfos) == 0 {
			continue
		}
		if err := send(batch); err != nil {
			return err
		}
	}
	return nil
}
