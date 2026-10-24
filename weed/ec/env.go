// Package ec holds the EC (erasure coding) cluster orchestration logic shared
// by the weed shell commands and the maintenance workers: topology analysis of
// EC shards, the encode/balance pipelines, and the volume-server RPC wrappers
// they drive. The placement policy itself lives in
// weed/storage/erasure_coding/ecbalancer; low-level shard mechanics live in
// weed/storage/erasure_coding.
package ec

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

// Env carries the cluster access hooks EC operations need, decoupled from any
// particular caller (shell CommandEnv, worker task, admin server).
type Env struct {
	GrpcDialOption grpc.DialOption
	// FetchTopology returns a fresh master topology snapshot (and the master's
	// volume size limit in MB) after an optional delay.
	FetchTopology func(delay time.Duration) (*master_pb.TopologyInfo, uint64, error)
	// GetVolumeLocations returns the current replica locations for a volume id,
	// or false if the volume is unknown.
	GetVolumeLocations func(vid uint32) ([]wdclient.Location, bool)
	// IsLocked reports whether the caller still holds the cluster admin lock.
	// Callers without a lock concept return true.
	IsLocked func() bool
}

// isLocked treats a nil Env or nil hook as locked, matching the shell's
// nil-receiver behavior so dry-run paths work without a cluster connection.
func (env *Env) isLocked() bool {
	if env == nil || env.IsLocked == nil {
		return true
	}
	return env.IsLocked()
}
