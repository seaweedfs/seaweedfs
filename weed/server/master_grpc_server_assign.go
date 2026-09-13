package weed_server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (ms *MasterServer) StreamAssign(server master_pb.Seaweed_StreamAssignServer) error {
	for {
		req, err := server.Recv()
		if err != nil {
			glog.Errorf("StreamAssign failed to receive: %v", err)
			return err
		}
		resp, err := ms.Assign(server.Context(), req)
		if err != nil {
			// Return transient errors (warmup, growth-in-progress shed) as in-band
			// error responses instead of killing the stream, so pooled connections
			// survive.
			if st, ok := status.FromError(err); ok && (st.Code() == codes.Unavailable || st.Code() == codes.ResourceExhausted) {
				glog.V(1).Infof("StreamAssign transient error: %v", err)
				resp = &master_pb.AssignResponse{Error: st.Message()}
			} else {
				glog.Errorf("StreamAssign failed to assign: %v", err)
				return err
			}
		}
		if err = server.Send(resp); err != nil {
			glog.Errorf("StreamAssign failed to send: %v", err)
			return err
		}
	}
}
func (ms *MasterServer) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Count == 0 {
		req.Count = 1
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	if ms.Topo.IsWarmingUp() {
		return nil, status.Errorf(codes.Unavailable, "master is warming up, topology is still loading")
	}
	diskType := types.ToDiskType(req.DiskType)

	ver := needle.GetCurrentVersion()
	option := &topology.VolumeGrowOption{
		Collection:         req.Collection,
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		DiskType:           diskType,
		Preallocate:        ms.preallocateSize,
		DataCenter:         req.DataCenter,
		Rack:               req.Rack,
		DataNode:           req.DataNode,
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,
		Version:            uint32(ver),
	}

	if !ms.Topo.DataCenterExists(option.DataCenter) {
		return nil, fmt.Errorf("data center %v not found in topology", option.DataCenter)
	}

	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)
	if req.DiskType == "" {
		if writable, _ := vl.GetWritableVolumeCount(); writable == 0 {
			if hddVl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, types.ToDiskType(types.HddType)); hddVl != nil {
				if writable, _ := hddVl.GetWritableVolumeCount(); writable > 0 {
					option.DiskType = types.ToDiskType(types.HddType)
					vl = hddVl
				}
			}
		}
	}
	vl.SetLastGrowCount(req.WritableVolumeCount)

	var (
		lastErr              error
		maxTimeout           = time.Second * 10
		startTime            = time.Now()
		initiatedGrow        bool
		repickedAfterGrow    bool
		unservedLayoutLogged bool
	)

	for time.Now().Sub(startTime) < maxTimeout {
		fid, count, dnList, shouldGrow, err := ms.Topo.PickForWrite(req.Count, option, vl, req.ExpectedDataSize)
		if shouldGrow && !initiatedGrow && !ms.option.VolumeGrowthDisabled && vl.AddGrowRequestIfAbsent() {
			initiatedGrow = true
			if err != nil && ms.Topo.AvailableSpaceFor(option) <= 0 && ms.Topo.CapacityFor(option) > 0 {
				err = fmt.Errorf("%s and no free volumes left for %s", err.Error(), option.String())
			}
			ms.volumeGrowthRequestChan <- &topology.VolumeGrowRequest{
				Option: option,
				Count:  req.WritableVolumeCount,
				Reason: "grpc assign",
			}
		}
		if err != nil {
			glog.V(1).Infof("assign %v %v: %v", req, option.String(), err)
			stats.MasterPickForWriteErrorCounter.Inc()
			lastErr = err
			if (req.DataCenter != "" || req.Rack != "") && strings.Contains(err.Error(), topology.NoWritableVolumes) {
				glog.V(0).Infof("assign %v %v: %v", req, option.String(), err)
				return nil, err
			}
			if shouldGrow {
				if ms.Topo.AvailableSpaceFor(option) <= 0 {
					// Fail fast whenever any capacity is registered: full for
					// this medium, or a medium no volume server serves — a
					// state a heartbeat won't change, so a retryable shed
					// would loop until the client's deadline. Shed retryably
					// only while nothing at all has registered, a just-started
					// cluster whose volume servers have not heartbeated, so
					// the first write rides out the startup window instead of
					// failing outright.
					if ms.Topo.CapacityForAnyDisk() > 0 {
						if ms.Topo.CapacityFor(option) <= 0 {
							// Wrapped here, not beside the "no free volumes left"
							// wrap above, so followers and growth-disabled
							// masters name the unserved medium too — the
							// initiator block is skipped for both.
							// The empty disk type is the legacy unlabeled
							// layout; naming it "hdd" here sends operators
							// looking for servers that were never labeled.
							lastErr = fmt.Errorf("%s and no volume server carries the %s disk layout for %s", err.Error(), describeDiskLayout(req.DiskType), option.String())
							assignUnservedLayoutWarning.Do(option.String(), lastErr)
							unservedLayoutLogged = true
						}
						break // surface the real error, not a retryable shed
					}
					return nil, status.Errorf(codes.ResourceExhausted, "no volume server capacity registered yet for %s", option.String())
				}
				// Only the initiator waits, and only while the growth it triggered
				// is still pending: followers shed fast so a herd doesn't pin a
				// goroutine each, and an initiator whose growth concluded without
				// yielding a writable volume sheds so client retries re-trigger
				// growth instead of looping it here. ResourceExhausted, not
				// Unavailable: clients retry it (assign_file_id.go) without
				// invalidating the shared master connection.
				if initiatedGrow != vl.HasGrowRequest() {
					// The failed pick above may predate the growth concluding, so
					// re-pick once before shedding: growth registers its volumes
					// before clearing the flag, and shedding here would bounce the
					// client off a volume that just landed.
					if initiatedGrow && !repickedAfterGrow {
						repickedAfterGrow = true
						continue
					}
					return nil, status.Errorf(codes.ResourceExhausted, "no writable volumes for %s, volume growth in progress", option.String())
				}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		dn := dnList.Head()
		if dn == nil {
			continue
		}
		var replicas []*master_pb.Location
		for _, r := range dnList.Rest() {
			replicas = append(replicas, &master_pb.Location{
				Url:        r.Url(),
				PublicUrl:  r.PublicUrl,
				GrpcPort:   uint32(r.GrpcPort),
				DataCenter: r.GetDataCenterId(),
			})
		}
		return &master_pb.AssignResponse{
			Fid: fid,
			Location: &master_pb.Location{
				Url:        dn.Url(),
				PublicUrl:  dn.PublicUrl,
				GrpcPort:   uint32(dn.GrpcPort),
				DataCenter: dn.GetDataCenterId(),
			},
			Count:    count,
			Auth:     string(security.GenJwtForVolumeServer(ms.guard.SigningKey(), ms.guard.ExpiresAfterSec(), fid)),
			Replicas: replicas,
		}, nil
	}
	// The initiator timed out with its growth still pending: shed retryably
	// rather than failing the write that growth is about to satisfy.
	if initiatedGrow && vl.HasGrowRequest() && ms.Topo.AvailableSpaceFor(option) > 0 {
		return nil, status.Errorf(codes.ResourceExhausted, "no writable volumes for %s, volume growth in progress", option.String())
	}
	if lastErr != nil && !unservedLayoutLogged {
		// The unserved-layout branch already logged this once per option via
		// assignUnservedLayoutWarning; repeating it here would flood the log
		// on every retry of a state a retry cannot change.
		glog.V(0).Infof("assign %v %v: %v", req, option.String(), lastErr)
	}
	return nil, lastErr
}

// describeDiskLayout names the disk layout an assign targets. The empty disk
// type is the legacy unlabeled layout on servers that were never started with
// -disk; naming it "hdd" sends operators looking for servers that were never
// labeled.
//
// Pass the original request disk type, not the canonicalized option.DiskType:
// ToDiskType folds both "" and "hdd" into HardDriveType, so only the request
// string can tell an unlabeled request from an explicit hdd one.
func describeDiskLayout(reqDiskType string) string {
	if reqDiskType == "" {
		return "default (unlabeled)"
	}
	return fmt.Sprintf("%q", strings.ToLower(reqDiskType))
}

// assignUnservedLayoutWarning logs a repeated assign failure at most once per
// option per interval instead of once per write attempt: a layout no volume
// server serves is a state a retry cannot change, so repeating it only buries
// the actionable first warning.
//
// The remembered set is bounded and expires: option keys embed
// request-derived fields (collection, disk type), so an unbounded, permanent
// dedupe map would let a client grow master memory at will and would also
// suppress the warning if the same option goes unserved again after the
// topology recovers.
const (
	unservedLayoutWarnInterval = time.Hour
	unservedLayoutWarnMaxKeys  = 1024
)

type unservedLayoutWarning struct {
	mu   sync.Mutex
	now  func() time.Time
	last map[string]time.Time
}

var assignUnservedLayoutWarning = &unservedLayoutWarning{
	now:  time.Now,
	last: make(map[string]time.Time),
}

func (w *unservedLayoutWarning) Do(optionKey string, lastErr error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	if t, ok := w.last[optionKey]; ok && now.Sub(t) < unservedLayoutWarnInterval {
		return
	}
	if len(w.last) >= unservedLayoutWarnMaxKeys {
		// Client-driven keys must never grow the map without bound; a full
		// reset trades a burst of repeated warnings for bounded memory.
		w.last = make(map[string]time.Time)
	}
	w.last[optionKey] = now
	glog.Warningf("assign requests for %s will keep failing until a volume server registers that disk layout or clients change their assignment disk type: %v", optionKey, lastErr)
}
