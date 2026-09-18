package weed_server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) ScrubVolume(ctx context.Context, req *volume_server_pb.ScrubVolumeRequest) (*volume_server_pb.ScrubVolumeResponse, error) {
	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		return nil, err
	}
	vids := []needle.VolumeId{}
	explicit := len(req.GetVolumeIds()) != 0
	if !explicit {
		for _, l := range vs.store.Locations {
			vids = append(vids, l.VolumeIds()...)
		}
	} else {
		for _, vid := range req.GetVolumeIds() {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	return vs.scrubVolumes(ctx, req, vids, explicit)
}

// scrubVolumes walks an already-resolved volume list. Split out from
// ScrubVolume so the vanished-volume handling below is reachable from a test:
// the list and the store are read under the same lock nowhere, so a test
// cannot otherwise arrange for an id to disappear between the two.
//
// explicit says the caller named the volumes. A named volume that is absent is
// a caller error and still fails the request; an id that came from the node's
// own listing is not, because nothing holds a lock across the scan and the
// volume set is free to change under it — the heartbeat drops a volume with an
// I/O error, and a delete or an unmount can land at any point. Failing there
// would discard every result accumulated so far and leave every later volume
// unscrubbed, which is the opposite of what a node-wide scrub is for.
func (vs *VolumeServer) scrubVolumes(ctx context.Context, req *volume_server_pb.ScrubVolumeRequest, vids []needle.VolumeId, explicit bool) (*volume_server_pb.ScrubVolumeResponse, error) {
	var details []string
	var totalVolumes, totalFiles uint64
	var brokenVolumes []*storage.Volume
	var brokenVolumeIds []uint32
	for _, vid := range vids {
		v := vs.store.GetVolume(vid)
		if v == nil {
			if explicit {
				return nil, fmt.Errorf("volume id %d not found", vid)
			}
			glog.V(0).Infof("scrub: volume %d is no longer mounted, skipping", vid)
			continue
		}

		var files int64
		var serrs []error
		switch m := req.GetMode(); m {
		case volume_server_pb.VolumeScrubMode_INDEX:
			files, serrs = v.ScrubIndex()
		case volume_server_pb.VolumeScrubMode_LOCAL, volume_server_pb.VolumeScrubMode_READS:
			// both are equivalent to FULL for regular volumes: there are no shards to
			// stay local to, and nothing to reconstruct from
			fallthrough
		case volume_server_pb.VolumeScrubMode_FULL:
			files, serrs = v.Scrub()
		default:
			return nil, fmt.Errorf("unsupported volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += uint64(files)
		if len(serrs) != 0 {
			brokenVolumes = append(brokenVolumes, v)
			brokenVolumeIds = append(brokenVolumeIds, uint32(v.Id))
			for _, err := range serrs {
				details = append(details, err.Error())
			}
		}
	}

	errs := []error{}
	if req.GetMarkBrokenVolumesReadonly() {
		for _, v := range brokenVolumes {
			if err := vs.makeVolumeReadonly(ctx, v, false, true); err != nil {
				errs = append(errs, err)
				details = append(details, err.Error())
			} else {
				details = append(details, fmt.Sprintf("volume %d is now read-only", v.Id))
			}
		}
	}

	scrubLabels := prometheus.Labels{"mode": req.GetMode().String()}
	stats.VolumeServerScrubLastTimeSeconds.With(scrubLabels).Set(float64(time.Now().Unix()))
	stats.VolumeServerScrubVolumeFailures.With(scrubLabels).Add(float64(len(brokenVolumes)))

	if len(errs) != 0 {
		return nil, errors.Join(errs...)
	}

	res := &volume_server_pb.ScrubVolumeResponse{
		TotalVolumes:    totalVolumes,
		TotalFiles:      totalFiles,
		BrokenVolumeIds: brokenVolumeIds,
		Details:         details,
	}
	return res, nil
}

func (vs *VolumeServer) ScrubEcVolume(ctx context.Context, req *volume_server_pb.ScrubEcVolumeRequest) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		return nil, err
	}
	if m := req.GetMode(); req.GetForceDeletedNeedlesCheck() &&
		m != volume_server_pb.VolumeScrubMode_FULL && m != volume_server_pb.VolumeScrubMode_READS {
		return nil, fmt.Errorf("deleted needle checks are only supported for FULL and READS scrubs")
	}

	vids := []needle.VolumeId{}
	explicit := len(req.GetVolumeIds()) != 0
	if !explicit {
		// A split-disk volume is mounted once per disk, so a node-wide
		// listing would otherwise scrub it once per location. Dedupe in
		// location order so the merged view still sees every runtime.
		seen := map[needle.VolumeId]struct{}{}
		for _, l := range vs.store.Locations {
			for _, vid := range l.EcVolumeIds() {
				if _, ok := seen[vid]; ok {
					continue
				}
				seen[vid] = struct{}{}
				vids = append(vids, vid)
			}
		}
	} else {
		for _, vid := range req.GetVolumeIds() {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	return vs.scrubEcVolumes(req, vids, explicit)
}

// scrubEcVolumes walks an already-resolved EC volume list. Same split, and same
// vanished-volume rule, as scrubVolumes — see its comment.
func (vs *VolumeServer) scrubEcVolumes(req *volume_server_pb.ScrubEcVolumeRequest, vids []needle.VolumeId, explicit bool) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	var details []string
	var totalVolumes, totalFiles uint64
	var brokenVolumeIds []uint32
	var brokenShardInfos []*volume_server_pb.EcShardInfo
	for _, vid := range vids {
		// Resolve every per-disk runtime, not just the first: a reconciled
		// volume's shards are split across runtimes, and a scrub that only
		// sees the first disk misses the rest. The merged view fences on
		// encode generation and geometry so incompatible runtimes are
		// reported rather than verified together.
		runtimes := vs.store.FindAllEcVolumes(vid)
		merged := erasure_coding.MergeEcRuntimes(runtimes)
		if merged == nil {
			if explicit {
				return nil, fmt.Errorf("EC volume id %d not found", vid)
			}
			glog.V(0).Infof("ec scrub: volume %d is no longer mounted, skipping", vid)
			continue
		}

		var files int64
		var shardInfos []*volume_server_pb.EcShardInfo
		var serrs []error
		switch m := req.GetMode(); m {
		case volume_server_pb.VolumeScrubMode_INDEX:
			files, serrs = merged.Anchor.ScrubIndex()
			for _, sk := range merged.Skipped {
				serrs = append(serrs, fmt.Errorf("%s", sk))
			}
		case volume_server_pb.VolumeScrubMode_LOCAL:
			files, shardInfos, serrs = merged.ScrubLocal()
		case volume_server_pb.VolumeScrubMode_FULL, volume_server_pb.VolumeScrubMode_READS:
			files, shardInfos, serrs = vs.store.ScrubEcVolumeMerged(merged, m, req.GetForceDeletedNeedlesCheck())
		case volume_server_pb.VolumeScrubMode_CHECKSUM:
			// Verify each local shard's raw bytes against the bitrot sidecar,
			// exercising cold parity shards. Read-only. The first return is
			// blocks scanned, not files — discard it so TotalFiles (a
			// needle/file count) isn't inflated by the block count.
			_, shardInfos, serrs = merged.ChecksumScrub()
		default:
			return nil, fmt.Errorf("unsupported EC volume scrub mode %d", m)
		}

		totalVolumes += 1
		totalFiles += uint64(files)
		if len(serrs) != 0 || len(shardInfos) != 0 {
			brokenVolumeIds = append(brokenVolumeIds, uint32(vid))
			brokenShardInfos = append(brokenShardInfos, shardInfos...)
			for _, err := range serrs {
				details = append(details, err.Error())
			}
		}
	}

	scrubLabels := prometheus.Labels{"mode": req.GetMode().String()}
	stats.VolumeServerScrubLastTimeSeconds.With(scrubLabels).Set(float64(time.Now().Unix()))
	stats.VolumeServerScrubVolumeFailures.With(scrubLabels).Add(float64(len(brokenVolumeIds)))
	stats.VolumeServerScrubShardFailures.With(scrubLabels).Add(float64(len(brokenShardInfos)))

	res := &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes:     totalVolumes,
		TotalFiles:       totalFiles,
		BrokenVolumeIds:  brokenVolumeIds,
		BrokenShardInfos: brokenShardInfos,
		Details:          details,
	}
	return res, nil
}
