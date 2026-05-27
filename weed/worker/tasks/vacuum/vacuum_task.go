package vacuum

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// VacuumTask implements the Task interface.
//
// One task covers all replicas of a volume so behavior matches the master
// built-in vacuum (see topology.Topology.vacuumOneVolumeId): Check across
// every replica → filter to those whose garbage ratio meets the threshold
// → Compact/Commit/Cleanup that subset. Treating one replica per task (the
// prior behavior) drops the other N-1 replicas because the dispatcher
// gates duplicate tasks per volume via ActiveTopology.HasAnyTask.
type VacuumTask struct {
	*base.BaseTask
	servers          []string
	volumeID         uint32
	collection       string
	garbageThreshold float64
	progress         float64
	grpcDialOption   grpc.DialOption
	volumeSize       uint64
	vacuumTargets    []string // populated by checkVacuumEligibility — subset of servers that pass the per-replica garbage re-check and proceed to Compact/Commit/Cleanup
}

// NewVacuumTask creates a new unified vacuum task instance covering every
// replica server reported by the dispatcher.
func NewVacuumTask(id string, servers []string, volumeID uint32, collection string, grpcDialOption grpc.DialOption) *VacuumTask {
	deduped := dedupePreserveOrder(servers)
	return &VacuumTask{
		BaseTask:         base.NewBaseTask(id, types.TaskTypeVacuum),
		servers:          deduped,
		volumeID:         volumeID,
		collection:       collection,
		garbageThreshold: 0.3, // Default 30% threshold
		grpcDialOption:   grpcDialOption,
	}
}

// Execute implements the UnifiedTask interface
func (t *VacuumTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	vacuumParams := params.GetVacuumParams()
	if vacuumParams == nil {
		return fmt.Errorf("vacuum parameters are required")
	}

	t.garbageThreshold = vacuumParams.GarbageThreshold
	t.volumeSize = params.VolumeSize

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":         t.volumeID,
		"servers":           t.servers,
		"collection":        t.collection,
		"garbage_threshold": t.garbageThreshold,
	}).Info("Starting vacuum task")

	if len(t.servers) == 0 {
		return fmt.Errorf("no source servers configured for vacuum task")
	}

	// Step 1: Check vacuum eligibility for each replica. Mirrors
	// topology.batchVacuumVolumeCheck — only replicas whose garbage is at
	// or above the threshold proceed to Compact/Commit/Cleanup.
	t.ReportProgress(10.0)
	t.GetLogger().Info("Checking volume status")
	targets, currentGarbageRatios, err := t.checkVacuumEligibility(ctx)
	if err != nil {
		return fmt.Errorf("failed to check vacuum eligibility: %v", err)
	}

	if len(targets) == 0 {
		t.GetLogger().WithFields(map[string]interface{}{
			"garbage_ratios":     currentGarbageRatios,
			"required_threshold": t.garbageThreshold,
		}).Info("No replica meets vacuum criteria, skipping")
		t.ReportProgress(100.0)
		return nil
	}
	t.vacuumTargets = targets

	// Step 2: Perform vacuum (compact + commit + cleanup) across every
	// target replica.
	t.ReportProgress(50.0)
	t.GetLogger().WithFields(map[string]interface{}{
		"vacuum_targets": targets,
		"garbage_ratios": currentGarbageRatios,
		"threshold":      t.garbageThreshold,
	}).Info("Performing vacuum operation")

	if err := t.performVacuum(ctx); err != nil {
		return fmt.Errorf("failed to perform vacuum: %v", err)
	}

	// Step 3: Verify vacuum results on each target replica.
	t.ReportProgress(90.0)
	t.GetLogger().Info("Verifying vacuum results")
	if err := t.verifyVacuumResults(ctx); err != nil {
		glog.Warningf("Vacuum verification failed: %v", err)
		// Don't fail the task - vacuum operation itself succeeded
	}

	t.ReportProgress(100.0)
	glog.Infof("Vacuum task completed successfully: volume %d on %v (garbage ratios %v)",
		t.volumeID, targets, currentGarbageRatios)
	return nil
}

// Validate implements the UnifiedTask interface
func (t *VacuumTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	vacuumParams := params.GetVacuumParams()
	if vacuumParams == nil {
		return fmt.Errorf("vacuum parameters are required")
	}

	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}

	// Every server the task was created with must appear in the params'
	// Sources list. The dispatcher fills Sources from the detection-time
	// replica set, so a mismatch means the worker received stale routing.
	sourceSet := make(map[string]struct{}, len(params.Sources))
	for _, source := range params.Sources {
		sourceSet[source.Node] = struct{}{}
	}
	for _, server := range t.servers {
		if _, ok := sourceSet[server]; !ok {
			return fmt.Errorf("task server %s not present in params.Sources", server)
		}
	}

	if vacuumParams.GarbageThreshold < 0 || vacuumParams.GarbageThreshold > 1.0 {
		return fmt.Errorf("invalid garbage threshold: %f (must be between 0.0 and 1.0)", vacuumParams.GarbageThreshold)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *VacuumTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 14 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *VacuumTask) GetProgress() float64 {
	return t.progress
}

// vacuumTimeout returns a dynamic timeout scaled by volume size, matching the
// topology vacuum approach. base is the per-GB multiplier (e.g. 1 minute for
// check, 3 minutes for compact).
func (t *VacuumTask) vacuumTimeout(base time.Duration) time.Duration {
	if t.volumeSize == 0 {
		glog.V(1).Infof("volume %d has no size metric, using minimum timeout", t.volumeID)
	}
	sizeGB := int64(t.volumeSize/1024/1024/1024) + 1
	return base * time.Duration(sizeGB)
}

// Helper methods for real vacuum operations

// checkVacuumEligibility checks each replica's garbage ratio. Returns the
// subset of servers whose garbage is at or above the configured threshold,
// alongside a per-server ratio map for logging. The returned error is
// non-nil only when every replica check failed — partial check errors are
// logged and treated as "ineligible" so the task can still vacuum the
// replicas that responded.
func (t *VacuumTask) checkVacuumEligibility(ctx context.Context) ([]string, map[string]float64, error) {
	ratios := make(map[string]float64, len(t.servers))
	var errCount int
	var lastErr error
	for _, server := range t.servers {
		ratio, err := t.checkOneVacuumEligibility(ctx, server)
		if err != nil {
			glog.Warningf("vacuum check failed for volume %d on %s: %v", t.volumeID, server, err)
			errCount++
			lastErr = err
			continue
		}
		ratios[server] = ratio
		glog.V(1).Infof("Volume %d on %s garbage ratio: %.2f%%, threshold: %.2f%%",
			t.volumeID, server, ratio*100, t.garbageThreshold*100)
	}

	if errCount == len(t.servers) {
		return nil, ratios, fmt.Errorf("vacuum check failed on all replicas: %v", lastErr)
	}

	eligible := make([]string, 0, len(ratios))
	for _, server := range t.servers {
		if ratio, ok := ratios[server]; ok && ratio >= t.garbageThreshold {
			eligible = append(eligible, server)
		}
	}
	return eligible, ratios, nil
}

func (t *VacuumTask) checkOneVacuumEligibility(ctx context.Context, server string) (float64, error) {
	var garbageRatio float64
	err := operation.WithVolumeServerClient(false, pb.ServerAddress(server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			checkCtx, cancel := context.WithTimeout(ctx, t.vacuumTimeout(time.Minute))
			defer cancel()
			resp, err := client.VacuumVolumeCheck(checkCtx, &volume_server_pb.VacuumVolumeCheckRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("failed to check volume vacuum status: %v", err)
			}
			garbageRatio = resp.GarbageRatio
			return nil
		})
	return garbageRatio, err
}

// performVacuum runs the two-phase vacuum protocol that master built-in
// vacuum uses (topology.vacuumOneVolumeId):
//
//   Phase 1 (Compact): build the new .cpd/.cpx files on every target.
//   If any replica fails, roll back by Cleanup'ing the .cp* temp files
//   on every target and abort — no replica has yet swapped its active
//   files, so no replica is committed.
//
//   Phase 2 (Commit): swap each target's active files with its .cp*
//   files. Best-effort, matching batchVacuumVolumeCommit: per-replica
//   errors are logged and surfaced together, but once any replica has
//   swapped there is no clean rollback for the others, so we do not
//   retry or undo. An operator must reconcile a partial commit
//   failure.
//
// Interleaving Compact→Commit→Cleanup per replica (the prior behavior)
// could leave a committed first replica beside an uncompacted second
// replica when Compact on the second failed — replica divergence with
// no automatic recovery.
func (t *VacuumTask) performVacuum(ctx context.Context) error {
	// Phase 1: Compact all targets.
	for _, server := range t.vacuumTargets {
		if err := t.compactOne(ctx, server); err != nil {
			t.cleanupAll(ctx)
			return fmt.Errorf("vacuum compact on %s: %w", server, err)
		}
	}

	// Phase 2: Commit all targets.
	var commitErrors []error
	for _, server := range t.vacuumTargets {
		if err := t.commitOne(ctx, server); err != nil {
			glog.Errorf("vacuum commit on %s for volume %d: %v", server, t.volumeID, err)
			commitErrors = append(commitErrors, fmt.Errorf("%s: %w", server, err))
		}
	}
	if len(commitErrors) > 0 {
		return fmt.Errorf("vacuum commit failed on %d/%d replicas: %v",
			len(commitErrors), len(t.vacuumTargets), commitErrors)
	}
	return nil
}

func (t *VacuumTask) compactOne(ctx context.Context, server string) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			t.GetLogger().Info("Compacting volume on %s", server)
			compactCtx, cancel := context.WithTimeout(ctx, t.vacuumTimeout(3*time.Minute))
			defer cancel()
			stream, err := client.VacuumVolumeCompact(compactCtx, &volume_server_pb.VacuumVolumeCompactRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("vacuum compact start: %v", err)
			}
			for {
				resp, recvErr := stream.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					}
					return fmt.Errorf("vacuum compact stream: %v", recvErr)
				}
				glog.V(2).Infof("Volume %d on %s compact progress: %d bytes", t.volumeID, server, resp.ProcessedBytes)
			}
			return nil
		})
}

func (t *VacuumTask) commitOne(ctx context.Context, server string) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			t.GetLogger().Info("Committing vacuum on %s", server)
			commitCtx, cancel := context.WithTimeout(ctx, t.vacuumTimeout(time.Minute))
			defer cancel()
			_, err := client.VacuumVolumeCommit(commitCtx, &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("vacuum commit: %v", err)
			}
			return nil
		})
}

func (t *VacuumTask) cleanupOne(ctx context.Context, server string) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			cleanupCtx, cancel := context.WithTimeout(ctx, t.vacuumTimeout(time.Minute))
			defer cancel()
			_, err := client.VacuumVolumeCleanup(cleanupCtx, &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumeId: t.volumeID,
			})
			return err
		})
}

// cleanupAll removes the .cpd/.cpx/.cpldb temp files on every target.
// Used to roll back when Compact fails on one replica after others
// have already created their temp files. Per-target failures are
// logged but never bubble up — the rollback is best-effort.
func (t *VacuumTask) cleanupAll(ctx context.Context) {
	for _, server := range t.vacuumTargets {
		if err := t.cleanupOne(ctx, server); err != nil {
			glog.Warningf("rollback cleanup on %s for volume %d: %v", server, t.volumeID, err)
		}
	}
}

// verifyVacuumResults checks each target replica's post-vacuum garbage
// ratio. Failures are logged at WARN — the task does not fail because the
// vacuum itself already succeeded.
func (t *VacuumTask) verifyVacuumResults(ctx context.Context) error {
	for _, server := range t.vacuumTargets {
		err := operation.WithVolumeServerClient(false, pb.ServerAddress(server), t.grpcDialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				verifyCtx, cancel := context.WithTimeout(ctx, t.vacuumTimeout(time.Minute))
				defer cancel()
				resp, err := client.VacuumVolumeCheck(verifyCtx, &volume_server_pb.VacuumVolumeCheckRequest{
					VolumeId: t.volumeID,
				})
				if err != nil {
					return fmt.Errorf("failed to verify vacuum results: %v", err)
				}
				glog.V(1).Infof("Volume %d on %s post-vacuum garbage ratio: %.2f%%",
					t.volumeID, server, resp.GarbageRatio*100)
				return nil
			})
		if err != nil {
			glog.Warningf("post-vacuum verify on %s: %v", server, err)
		}
	}
	return nil
}

// dedupePreserveOrder returns servers with duplicates removed, keeping the
// first occurrence's position. Detection sometimes hands the same node
// address in multiple Sources (e.g. EC variants); we coalesce them so each
// physical replica is vacuumed exactly once.
func dedupePreserveOrder(servers []string) []string {
	seen := make(map[string]struct{}, len(servers))
	out := make([]string, 0, len(servers))
	for _, s := range servers {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
