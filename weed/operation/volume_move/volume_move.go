package volume_move

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrSourceKeptReadonly marks a move failure whose recovery deliberately keeps
// the source volume readonly — the target copy may hold data the source lacks
// (an ambiguous source delete, or an undeletable stale copy). Callers that
// froze the source themselves must not thaw it on this error.
var ErrSourceKeptReadonly = errors.New("the source volume is deliberately kept readonly")

// VolumeMoveOptions control LiveMoveVolume.
type VolumeMoveOptions struct {
	// DiskType changes the volume's disk type on the target ("" keeps it).
	DiskType string
	// IoBytePerSecond throttles the copy (0 = unlimited).
	IoBytePerSecond int64
	// IdleTimeout is how long the tail phase waits for in-flight requests to
	// drain (default 5s).
	IdleTimeout time.Duration
	// Writer receives human-readable progress lines (nil discards them).
	Writer io.Writer
	// Progress, when set, receives percent/stage callbacks as the move advances.
	Progress func(percent float64, stage string)
}

func (o *VolumeMoveOptions) fillDefaults() {
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 5 * time.Second
	}
	if o.Writer == nil {
		o.Writer = io.Discard
	}
	if o.Progress == nil {
		o.Progress = func(float64, string) {}
	}
}

// LiveMoveVolume moves one volume between volume servers while it keeps serving
// reads: freeze the source (readonly), copy, tail to drain in-flight requests,
// verify the target matches the source, then delete the source. A failure
// before the source delete restores the source's writability if this move is
// what froze it.
func (m *Mover) LiveMoveVolume(ctx context.Context, volumeId needle.VolumeId, source, target pb.ServerAddress, opts VolumeMoveOptions) (err error) {
	opts.fillDefaults()

	// VolumeCopy tears down any existing copy on the target first, so a
	// same-server "move" would delete the volume and then fail to read it.
	// SameServer, not ==: "node:8080" and "node:8080.18080" are one server.
	if SameServer(source, target) {
		return fmt.Errorf("refusing to move volume %d onto its own server %s", volumeId, source)
	}

	opts.Progress(10, fmt.Sprintf("marking volume %d readonly on %s", volumeId, source))
	sourceWasWritable, err := m.ensureVolumeReadonly(ctx, volumeId, source, true)
	// The source stays authoritative until its delete succeeds; on any earlier
	// failure undo the freeze this move added and clean up the target copy.
	// Installed before the error check: the marking RPC itself can fail after
	// the server applied the mark (e.g. its master notification failed).
	var copyStarted, copyCompleted, sourceDeleteStarted bool
	var targetHadVolume, targetStateKnown bool
	defer func() {
		if err != nil {
			if sourceDeleteStarted {
				// Past verification: the target is complete and may already hold
				// new writes, and the delete may or may not have reached the
				// source. Deleting the target or reopening the source could fork
				// the volume; keep the source readonly.
				fmt.Fprintf(opts.Writer, "volume %d is left readonly on %s: the source delete failed with the verified copy on %s mounted\n", volumeId, source, target)
				glog.Warningf("volume %d is left readonly on %s: the source delete failed with the verified copy on %s mounted", volumeId, source, target)
				err = fmt.Errorf("%w: %w", ErrSourceKeptReadonly, err)
				return
			}
			// The cleanup runs on its own deadline so an abort via cancelled
			// context still cleans up.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cleanupCancel()
			cleanupTarget := copyCompleted
			if copyStarted && !copyCompleted {
				// The server can finish the copy and mount the target even when
				// the client loses the stream, so probe rather than assume.
				exists, known := m.probeVolume(cleanupCtx, volumeId, target)
				switch {
				case known && !exists:
					// nothing mounted on the target; just undo the freeze below
				case known && exists && targetStateKnown && !targetHadVolume:
					// the failed copy created it; remove it so the source stays
					// the only replica
					cleanupTarget = true
				default:
					// A copy may sit mounted on the target and its provenance
					// cannot be proven: the prior state is unknown, the target
					// held a replica before the move, or the probe failed.
					// Deleting it risks someone else's replica; reopening the
					// source beside it risks two writable replicas taking
					// divergent writes. Keep the source readonly.
					fmt.Fprintf(opts.Writer, "volume %d is left readonly on %s: a copy may exist on %s but its origin cannot be determined; delete one side explicitly, then re-run the move\n", volumeId, source, target)
					glog.Warningf("volume %d is left readonly on %s: a copy may exist on %s but its origin cannot be determined; delete one side explicitly, then re-run the move", volumeId, source, target)
					err = fmt.Errorf("%w: %w", ErrSourceKeptReadonly, err)
					return
				}
			}
			if cleanupTarget {
				// The target copy may be missing tailed entries; remove it so
				// the source stays the only replica.
				if dErr := m.DeleteVolume(cleanupCtx, volumeId, target, false, true); dErr != nil {
					// Restoring the source while the stale target stays mounted
					// risks divergent replicas; keep the source readonly. A
					// re-run refuses while the copy exists, so name the fix.
					fmt.Fprintf(opts.Writer, "volume %d is left readonly on %s: failed to delete the incomplete copy on %s: %v; delete that copy, then re-run the move\n", volumeId, source, target, dErr)
					glog.Warningf("volume %d is left readonly on %s: failed to delete the incomplete copy on %s: %v; delete that copy, then re-run the move", volumeId, source, target, dErr)
					err = fmt.Errorf("%w: %w", ErrSourceKeptReadonly, err)
					return
				}
			}
			if sourceWasWritable {
				m.restoreVolumeWritable(volumeId, source)
			} else {
				// The hard freeze added on top of a readonly-reporting status is
				// kept: it is indistinguishable from an operator's mark. It is
				// not persisted, so a volume server restart clears it.
				fmt.Fprintf(opts.Writer, "volume %d on %s keeps the readonly mark the move added; volume.mark -writable clears it if the prior readonly state was transient\n", volumeId, source)
			}
		}
	}()
	if err != nil {
		return fmt.Errorf("mark volume %d readonly on %s: %v", volumeId, source, err)
	}

	targetHadVolume, targetStateKnown = m.probeVolume(ctx, volumeId, target)
	if !sourceWasWritable {
		// A readonly source next to an existing copy is the signature of a
		// previous move whose source delete failed — that copy may be the
		// authoritative one, serving writes the source never saw, and the
		// copy below would tear it down. No client-side observation proves
		// otherwise: compaction revision is shared by ordinary replicas,
		// aggregate sizes shrink under compaction, and any snapshot can be
		// invalidated by a write right after it. Fail closed on any existing
		// copy or unknown state; the operator deletes one side explicitly.
		if !targetStateKnown {
			err = fmt.Errorf("%w: cannot determine whether %s already holds a copy of volume %d; refusing to overwrite it while the source is readonly", ErrSourceKeptReadonly, target, volumeId)
			return err
		}
		if targetHadVolume {
			err = fmt.Errorf("%w: volume %d already has a copy on %s while the source on %s is readonly — likely a previous move that did not finish; delete the source to keep that copy, or delete the copy before re-running the move", ErrSourceKeptReadonly, volumeId, target, source)
			return err
		}
	}

	opts.Progress(20, fmt.Sprintf("copying volume %d from %s to %s", volumeId, source, target))
	fmt.Fprintf(opts.Writer, "copying volume %d from %s to %s\n", volumeId, source, target)
	copyStarted = true
	lastAppendAtNs, err := m.copyVolumeData(ctx, volumeId, source, target, opts.DiskType, opts.IoBytePerSecond, opts.Writer)
	if err != nil {
		return fmt.Errorf("copy volume %d from %s to %s: %v", volumeId, source, target, err)
	}
	copyCompleted = true

	opts.Progress(70, fmt.Sprintf("tailing volume %d from %s to %s", volumeId, source, target))
	fmt.Fprintf(opts.Writer, "tailing volume %d from %s to %s\n", volumeId, source, target)
	tailFailed := false
	if tailErr := m.TailVolume(ctx, volumeId, source, target, lastAppendAtNs, opts.IdleTimeout); tailErr != nil {
		// A tail failure is tolerable only when the volume was already readonly
		// before this move began: frozen for the whole copy, it should have no
		// in-flight writes for the tail to drain — and the stability check
		// below still proves it. A volume this move froze can have stragglers
		// admitted just before the freeze that only the tail delivers, so
		// losing the tail there must abort the move.
		if sourceWasWritable {
			return fmt.Errorf("tail volume %d from %s to %s: %v", volumeId, source, target, tailErr)
		}
		tailFailed = true
		fmt.Fprintf(opts.Writer, "tail volume %d from %s to %s: %v\n", volumeId, source, target, tailErr)
		glog.Warningf("tail volume %d from %s to %s: %v", volumeId, source, target, tailErr)
	}

	// Verify before the point of no return: the source is deleted only when the
	// target holds at least everything the source does. The source status is
	// read here, after the tail — a write in flight when the source was frozen
	// can still land after an earlier read, and a stale snapshot would wave
	// through an incomplete target.
	opts.Progress(85, fmt.Sprintf("verifying volume %d on %s", volumeId, target))
	sourceStatus, err := m.ReadVolumeFileStatus(ctx, volumeId, source)
	if err != nil {
		return fmt.Errorf("read volume %d status on %s: %v", volumeId, source, err)
	}
	if tailFailed {
		// A successful tail proves the source held still for the idle window; a
		// tolerated tail failure proved nothing, so substitute the same drain
		// barrier here: the source status must not change across the window.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(opts.IdleTimeout):
		}
		secondStatus, statusErr := m.ReadVolumeFileStatus(ctx, volumeId, source)
		if statusErr != nil {
			return fmt.Errorf("re-read volume %d status on %s: %v", volumeId, source, statusErr)
		}
		if secondStatus.DatFileSize != sourceStatus.DatFileSize || secondStatus.IdxFileSize != sourceStatus.IdxFileSize || secondStatus.FileCount != sourceStatus.FileCount {
			return fmt.Errorf("volume %d on %s is still changing after a failed tail; aborting the move", volumeId, source)
		}
		sourceStatus = secondStatus
	}
	targetStatus, err := m.ReadVolumeFileStatus(ctx, volumeId, target)
	if err != nil {
		return fmt.Errorf("verify volume %d on target %s before deleting source: %v", volumeId, target, err)
	}
	if err = verifyTargetNotBehind(volumeId, sourceStatus, targetStatus); err != nil {
		return err
	}
	if targetStatus.DatFileSize > sourceStatus.DatFileSize || targetStatus.IdxFileSize > sourceStatus.IdxFileSize || targetStatus.FileCount > sourceStatus.FileCount {
		// The target announced itself writable when the copy mounted it, so
		// clients may have written to it during the tail. Those writes live on
		// the surviving copy; committing keeps them.
		fmt.Fprintf(opts.Writer, "volume %d on %s has writes beyond the source; they stay with the moved volume\n", volumeId, target)
	}

	opts.Progress(90, fmt.Sprintf("deleting volume %d from %s", volumeId, source))
	fmt.Fprintf(opts.Writer, "deleting volume %d from %s\n", volumeId, source)
	sourceDeleteStarted = true
	if err = m.DeleteVolume(ctx, volumeId, source, false, true); err != nil {
		return fmt.Errorf("delete volume %d from %s: %v", volumeId, source, err)
	}

	opts.Progress(100, fmt.Sprintf("moved volume %d from %s to %s", volumeId, source, target))
	fmt.Fprintf(opts.Writer, "moved volume %d from %s to %s\n", volumeId, source, target)
	return nil
}

// verifyTargetNotBehind fails when the target holds less than the (frozen)
// source — the copy or tail missed data and deleting the source would lose it.
// A target that is ahead is not an error: the target serves writes during the
// tail, and those belong to the copy that survives the move.
func verifyTargetNotBehind(volumeId needle.VolumeId, source, target *volume_server_pb.ReadVolumeFileStatusResponse) error {
	if target.DatFileSize < source.DatFileSize {
		return fmt.Errorf("volume %d target is behind the source: .dat %d < %d bytes", volumeId, target.DatFileSize, source.DatFileSize)
	}
	if target.IdxFileSize < source.IdxFileSize {
		return fmt.Errorf("volume %d target is behind the source: .idx %d < %d bytes", volumeId, target.IdxFileSize, source.IdxFileSize)
	}
	if target.FileCount < source.FileCount {
		return fmt.Errorf("volume %d target is behind the source: %d < %d files", volumeId, target.FileCount, source.FileCount)
	}
	return nil
}

// CopyVolume freezes the volume on source, copies it to target, and reports the
// stamp of the last entry copied. restoreWritable also restores the source's
// writability on success, for copies that leave the source serving; a failed
// copy always undoes the freeze this call added.
func (m *Mover) CopyVolume(ctx context.Context, volumeId needle.VolumeId, source, target pb.ServerAddress, diskType string, ioBytePerSecond int64, restoreWritable bool, writer io.Writer) (lastAppendAtNs uint64, err error) {
	if writer == nil {
		writer = io.Discard
	}
	if SameServer(source, target) {
		return 0, fmt.Errorf("refusing to copy volume %d onto its own server %s", volumeId, source)
	}
	// The copy is non-destructive, so a readonly-reporting source is taken as
	// is (force=false): missed concurrent deletes only make the new replica
	// trivially stale, and hard-marking here could pin a transiently readonly
	// (e.g. low-disk) source readonly with nothing to gate on it.
	sourceWasWritable, err := m.ensureVolumeReadonly(ctx, volumeId, source, false)
	defer func() {
		if sourceWasWritable && (err != nil || restoreWritable) {
			m.restoreVolumeWritable(volumeId, source)
		}
	}()
	if err != nil {
		return 0, err
	}
	return m.copyVolumeData(ctx, volumeId, source, target, diskType, ioBytePerSecond, writer)
}

// ensureVolumeReadonly freezes the volume on server and reports whether it was
// writable beforehand — only then may a failure path undo the freeze, since a
// volume that was already readonly (e.g. full, or operator-set) must stay so.
// With force, the mark is issued even when the status already reports
// readonly: that answer also covers transient low-disk state and the
// readonly-but-can-delete flag, and neither blocks needle deletes, so only the
// hard mark makes the volume immutable — required before deleting the source
// of a move. Without force, a readonly-reporting volume is left untouched.
func (m *Mover) ensureVolumeReadonly(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress, force bool) (wasWritable bool, err error) {
	err = m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		resp, statusErr := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
			VolumeId: uint32(volumeId),
		})
		if statusErr != nil {
			return statusErr
		}
		wasWritable = !resp.IsReadOnly
		if !wasWritable && !force {
			return nil
		}
		_, readonlyErr := client.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
			VolumeId: uint32(volumeId),
			Persist:  false,
		})
		return readonlyErr
	})
	return
}

// probeVolume reports whether server currently has the volume, and whether the
// answer is trustworthy. The Go server answers a missing volume with a plain
// error (gRPC code Unknown — the only way this RPC fails there); the Rust
// server answers with codes.NotFound. Both mean the server responded and the
// volume is absent. A transport-level failure means the state is unknown, and
// callers deciding to delete must treat unknown as hands-off.
func (m *Mover) probeVolume(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress) (exists bool, known bool) {
	_, err := m.ReadVolumeFileStatus(ctx, volumeId, server)
	if err == nil {
		return true, true
	}
	if s, ok := status.FromError(err); ok && (s.Code() == codes.Unknown || s.Code() == codes.NotFound) {
		return false, true
	}
	return false, false
}

// restoreVolumeWritable undoes a readonly mark after a failed move. It runs on
// its own deadline so an abort via cancelled context still restores the source,
// and only logs a failure — the caller is already returning the move error.
func (m *Mover) restoreVolumeWritable(volumeId needle.VolumeId, server pb.ServerAddress) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m.MarkVolumeWritable(ctx, volumeId, server, true, false); err != nil {
		glog.Warningf("restore volume %d writable on %s: %v", volumeId, server, err)
	}
}

// copyVolumeData streams a VolumeCopy on the target, which pulls the volume
// from source and mounts it, and returns the stamp of the last entry copied
// for the tail phase.
func (m *Mover) copyVolumeData(ctx context.Context, volumeId needle.VolumeId, source, target pb.ServerAddress, diskType string, ioBytePerSecond int64, writer io.Writer) (lastAppendAtNs uint64, err error) {
	// The target dials the embedded source itself; a malformed one would
	// abort the target server, not this client.
	if err = checkDialable(source); err != nil {
		return 0, err
	}
	err = m.withClient(true, target, func(client volume_server_pb.VolumeServerClient) error {
		stream, replicateErr := client.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
			VolumeId:        uint32(volumeId),
			SourceDataNode:  string(source),
			DiskType:        diskType,
			IoBytePerSecond: ioBytePerSecond,
		})
		if replicateErr != nil {
			return replicateErr
		}
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return recvErr
			}
			if resp.LastAppendAtNs != 0 {
				lastAppendAtNs = resp.LastAppendAtNs
			} else {
				fmt.Fprintf(writer, "%s => %s volume %d processed %s\n", source, target, volumeId, util.BytesToHumanReadable(uint64(resp.ProcessedBytes)))
			}
		}
		return nil
	})
	return
}

// TailVolume has the target follow the source's appends since sinceNs until the
// source stays idle for idleTimeout, draining requests in flight when the move
// froze the source.
func (m *Mover) TailVolume(ctx context.Context, volumeId needle.VolumeId, source, target pb.ServerAddress, sinceNs uint64, idleTimeout time.Duration) error {
	// The target dials the embedded source itself; a malformed one would
	// abort the target server, not this client.
	if err := checkDialable(source); err != nil {
		return err
	}
	return m.withClient(true, target, func(client volume_server_pb.VolumeServerClient) error {
		_, replicateErr := client.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
			VolumeId:           uint32(volumeId),
			SinceNs:            sinceNs,
			IdleTimeoutSeconds: uint32(idleTimeout.Seconds()),
			SourceVolumeServer: string(source),
		})
		return replicateErr
	})
}

// ReadVolumeFileStatus reads the volume's file sizes and needle count on server.
func (m *Mover) ReadVolumeFileStatus(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress) (resp *volume_server_pb.ReadVolumeFileStatusResponse, err error) {
	err = m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		var statusErr error
		resp, statusErr = client.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
			VolumeId: uint32(volumeId),
		})
		return statusErr
	})
	return
}

// DeleteVolume removes the volume from server. When keepRemoteData is true, the
// cloud-tier object backing the volume is left intact — used on the source side
// of a move where another server is taking over the same .vif.
func (m *Mover) DeleteVolume(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress, onlyEmpty bool, keepRemoteData bool) error {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		_, deleteErr := client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
			VolumeId:       uint32(volumeId),
			OnlyEmpty:      onlyEmpty,
			KeepRemoteData: keepRemoteData,
		})
		return deleteErr
	})
}

// MarkVolumeWritable marks the volume writable (or readonly when writable is
// false, persisted per persist) on server.
func (m *Mover) MarkVolumeWritable(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress, writable, persist bool) (err error) {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		if writable {
			_, err = client.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{
				VolumeId: uint32(volumeId),
			})
		} else {
			_, err = client.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: uint32(volumeId),
				Persist:  persist,
			})
		}
		return err
	})
}

// ReplicateVolume copies a volume from source to target without touching the
// source — the replica-creation half of a move.
func (m *Mover) ReplicateVolume(ctx context.Context, volumeId needle.VolumeId, source, target pb.ServerAddress, diskType string, writer io.Writer) error {
	if writer == nil {
		writer = io.Discard
	}
	if SameServer(source, target) {
		return fmt.Errorf("refusing to replicate volume %d onto its own server %s", volumeId, source)
	}
	// The target dials the embedded source itself; a malformed one would
	// abort the target server, not this client.
	if err := checkDialable(source); err != nil {
		return err
	}
	return m.withClient(false, target, func(client volume_server_pb.VolumeServerClient) error {
		stream, replicateErr := client.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
			VolumeId:       uint32(volumeId),
			SourceDataNode: string(source),
			DiskType:       diskType,
		})
		if replicateErr != nil {
			return replicateErr
		}
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return recvErr
			}
			if resp.ProcessedBytes > 0 {
				fmt.Fprintf(writer, "volume %d processed %s bytes\n", volumeId, util.BytesToHumanReadable(uint64(resp.ProcessedBytes)))
			}
		}
		return nil
	})
}

// ConfigureVolumeReplication sets the replication setting on the volume at server.
func (m *Mover) ConfigureVolumeReplication(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress, replication string) error {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		resp, configureErr := client.VolumeConfigure(ctx, &volume_server_pb.VolumeConfigureRequest{
			VolumeId:    uint32(volumeId),
			Replication: replication,
		})
		if configureErr != nil {
			return configureErr
		}
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		return nil
	})
}
