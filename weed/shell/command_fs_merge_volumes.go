package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func init() {
	Commands = append(Commands, &commandFsMergeVolumes{})
}

type commandFsMergeVolumes struct {
	volumes         map[needle.VolumeId]*master_pb.VolumeInformationMessage
	volumeSizeLimit uint64
}

func (c *commandFsMergeVolumes) Name() string {
	return "fs.mergeVolumes"
}

func (c *commandFsMergeVolumes) Help() string {
	return `re-locate chunks into target volumes and try to clear lighter volumes.

	This would help clear half-full volumes and let vacuum system to delete them later.

	fs.mergeVolumes [-toVolumeId=y[,z]] [-fromVolumeId=x] [-collection="*"] [-dir=/] [-apply]

	-toVolumeId accepts a comma-separated list. With -fromVolumeId, the source
	chunks are distributed across the listed volumes by remaining capacity, so a
	volume that does not fit into any single target can still be cleared.
`
}

func (c *commandFsMergeVolumes) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMergeVolumes) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMergeVolumesCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	dirArg := fsMergeVolumesCommand.String("dir", "/", "base directory to find and update files")
	fromVolumeArg := fsMergeVolumesCommand.Uint("fromVolumeId", 0, "move chunks with this volume id")
	toVolumeArg := fsMergeVolumesCommand.String("toVolumeId", "", "change chunks to this volume id, or distribute across a comma-separated list of volume ids")
	collectionArg := fsMergeVolumesCommand.String("collection", "*", "Name of collection to merge")
	apply := fsMergeVolumesCommand.Bool("apply", false, "applying the metadata changes")
	if err = fsMergeVolumesCommand.Parse(args); err != nil {
		return err
	}

	dir := *dirArg
	if dir != "/" {
		dir = strings.TrimRight(dir, "/")
	}

	// flag.Uint is a 64-bit uint on amd64 but needle.VolumeId is uint32, so a
	// value that overflows (e.g. 4294967297) silently wraps to a valid id
	// like 1. Reject instead of wrapping.
	const maxVolumeID = uint(^uint32(0))
	if *fromVolumeArg > maxVolumeID {
		return fmt.Errorf("fromVolumeId %d exceeds max volume id %d", *fromVolumeArg, maxVolumeID)
	}

	fromVolumeId := needle.VolumeId(*fromVolumeArg)
	toVolumeIds, err := parseTargetVolumeIds(*toVolumeArg)
	if err != nil {
		return err
	}

	if err = c.reloadVolumesInfo(commandEnv.MasterClient); err != nil {
		return fmt.Errorf("reload volumes info: %w", err)
	}

	// Reject unknown ids before createMergePlan silently produces an empty plan
	// and we print just the "max volume size" header. That output is
	// indistinguishable from a legitimate "nothing to merge" and hides typos,
	// already-deleted volumes, and stale scripts.
	if fromVolumeId != 0 {
		if _, err := c.getVolumeInfoById(fromVolumeId); err != nil {
			return fmt.Errorf("fromVolumeId %d not found on master", fromVolumeId)
		}
	}
	for _, toVolumeId := range toVolumeIds {
		if _, err := c.getVolumeInfoById(toVolumeId); err != nil {
			return fmt.Errorf("toVolumeId %d not found on master", toVolumeId)
		}
	}

	plan, err := c.createMergePlan(*collectionArg, toVolumeIds, fromVolumeId)

	if err != nil {
		return err
	}
	c.printPlan(plan)

	if len(plan.targets) == 0 {
		return nil
	}

	defer util_http.GetGlobalHttpClient().CloseIdleConnections()

	lookupFn := filer.LookupFn(commandEnv)

	// Hard-linked siblings share ONE chunk list via a KV blob keyed by
	// HardLinkId (see weed/filer/filerstore_hardlink.go): UpdateEntry's
	// setHardLink() rewrites that blob, and every sibling read goes through
	// maybeReadHardLink() which overrides the per-entry chunks with the
	// blob's. So moving a chunk and calling UpdateEntry on one sibling
	// propagates the new fids to every other sibling automatically —
	// provided we do it exactly once per HardLinkId. Processing every
	// sibling would race: the first succeeds, the next would either
	// re-download an already-moved (and possibly already-deleted) source
	// needle or double-queue the same fid for deletion. Track the ids we
	// have already handled so BFS workers in different directories can
	// synchronize without a global lock.
	var processedHardLinks sync.Map

	return commandEnv.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		return filer_pb.TraverseBfs(context.Background(), commandEnv, util.FullPath(dir), func(parentPath util.FullPath, entry *filer_pb.Entry) error {
			if entry.IsDirectory {
				return nil
			}
			entryPath := parentPath.Child(entry.Name)
			if len(entry.HardLinkId) > 0 {
				if _, seen := processedHardLinks.LoadOrStore(string(entry.HardLinkId), struct{}{}); seen {
					// Another sibling already carried the HardLinkId through
					// the move + UpdateEntry path; the shared KV blob has the
					// new fids, so this sibling is already correct on read.
					return nil
				}
			}
			entryChanged := false
			// Every successful moveChunk or rewriteManifestChunk leaves the old
			// needle sitting on its source volume as a silent orphan — until
			// now the source only shrank after a separate volume.fsck +
			// volume.vacuum cycle, which is what made #9116 (comment 4282692876)
			// look like mergeVolumes hadn't done anything. Track the old fids
			// and delete them below after the filer update commits, so the
			// filer never points at a fid we already deleted.
			var movedSources []movedSourceNeedle
			for i, chunk := range entry.Chunks {
				if chunk.IsChunkManifest {
					oldManifestFid := chunk.GetFileIdString()
					oldManifestVid := chunk.Fid.VolumeId
					newChunk, changed, subSources, mErr := c.rewriteManifestChunk(context.Background(), commandEnv, lookupFn, plan, entryPath, chunk, *apply)
					if mErr != nil {
						fmt.Printf("failed to rewrite manifest %s(%s): %v\n", entryPath, oldManifestFid, mErr)
						continue
					}
					if !changed || !*apply {
						continue
					}
					entry.Chunks[i] = newChunk
					entryChanged = true
					movedSources = append(movedSources, subSources...)
					// The old manifest needle is always orphaned when we
					// replace it with a freshly uploaded one, even when the
					// rewrite was triggered by sub-chunk moves rather than the
					// manifest volume itself being in the plan.
					movedSources = append(movedSources, movedSourceNeedle{volumeId: oldManifestVid, fileId: oldManifestFid})
					continue
				}

				chunkVolumeId := needle.VolumeId(chunk.Fid.VolumeId)
				if !plan.isSource(chunkVolumeId) {
					continue
				}

				oldFid := chunk.GetFileIdString()
				oldVid := chunk.Fid.VolumeId
				toVolumeId, ok := plan.allocate(chunkVolumeId, chunk.Size)
				if !ok {
					fmt.Printf("skip %s(%s): no target volume has room\n", entryPath, oldFid)
					continue
				}
				fmt.Printf("move %s(%s) => volume %d\n", entryPath, oldFid, toVolumeId)
				if !*apply {
					continue
				}
				if mvErr := moveChunk(chunk, toVolumeId, commandEnv.MasterClient); mvErr != nil {
					fmt.Printf("failed to move %s(%s): %v\n", entryPath, oldFid, mvErr)
					plan.release(chunkVolumeId, toVolumeId, chunk.Size)
					continue
				}
				entryChanged = true
				movedSources = append(movedSources, movedSourceNeedle{volumeId: oldVid, fileId: oldFid})
			}
			if entryChanged {
				if uErr := filer_pb.UpdateEntry(context.Background(), filerClient, &filer_pb.UpdateEntryRequest{
					Directory: string(parentPath),
					Entry:     entry,
				}); uErr != nil {
					fmt.Printf("failed to update %s: %v\n", entryPath, uErr)
					// Filer still references the source fids. Deleting them
					// now would lose data — abandon the cleanup for this
					// entry and let fsck reconcile later.
					return nil
				}
				c.deleteMovedSourceNeedles(commandEnv, entryPath, movedSources)
			}
			return nil
		})
	})
}

// movedSourceNeedle is a needle that was copied out of its source volume by
// a move/rewrite operation and is safe to delete once the filer update that
// re-pointed references to the new location has committed.
type movedSourceNeedle struct {
	volumeId uint32
	fileId   string
}

// deleteMovedSourceNeedles fans out BatchDelete RPCs to every replica of each
// source volume. Errors are logged but never returned — the source data is
// already orphan at this point, so a failed cleanup just leaves work for a
// later fsck. Propagating an error here would abort TraverseBfs and strand
// the remaining entries mid-merge, which is strictly worse.
func (c *commandFsMergeVolumes) deleteMovedSourceNeedles(commandEnv *CommandEnv, entryPath util.FullPath, sources []movedSourceNeedle) {
	if len(sources) == 0 {
		return
	}
	byVolume := make(map[uint32][]string)
	for _, s := range sources {
		byVolume[s.volumeId] = append(byVolume[s.volumeId], s.fileId)
	}
	for vid, fids := range byVolume {
		locations, found := commandEnv.MasterClient.GetLocations(vid)
		if !found {
			fmt.Printf("source cleanup %s: no locations for volume %d\n", entryPath, vid)
			continue
		}
		for _, loc := range locations {
			results := operation.DeleteFileIdsAtOneVolumeServer(loc.ServerAddress(), commandEnv.option.GrpcDialOption, fids, false)
			// Summarize per server: an unreachable volume server returns one
			// error per needle, which for manifest-heavy files can mean
			// hundreds of near-identical lines. Keep the first error as the
			// example and report a single line with the total count.
			var firstErr, firstFid string
			errCount := 0
			for _, r := range results {
				// StatusNotModified (304) means DeleteVolumeNeedle returned
				// size 0 — the needle was already gone when we arrived.
				// StatusNotFound (404) comes from the cookie-check path when
				// ReadVolumeNeedle can't find the needle. Both are benign
				// races against a concurrent fsck purge or a replica that
				// had already reconciled, so skip them. Cast to int because
				// r.Status is an int32 protobuf field and linters flag the
				// mixed-type compare even though Go's untyped-constant rules
				// make it valid.
				status := int(r.Status)
				if r.Error == "" || status == http.StatusNotModified || status == http.StatusNotFound {
					continue
				}
				if errCount == 0 {
					firstErr = r.Error
					firstFid = r.FileId
				}
				errCount++
			}
			if errCount == 1 {
				fmt.Printf("source cleanup %s: delete %s on %v: %s\n", entryPath, firstFid, loc.ServerAddress(), firstErr)
			} else if errCount > 1 {
				fmt.Printf("source cleanup %s: %d/%d needles failed on %v (e.g. %s: %s)\n",
					entryPath, errCount, len(fids), loc.ServerAddress(), firstFid, firstErr)
			}
		}
	}
}

func (c *commandFsMergeVolumes) getVolumeInfoById(vid needle.VolumeId) (*master_pb.VolumeInformationMessage, error) {
	info := c.volumes[vid]
	var err error
	if info == nil {
		err = errors.New("cannot find volume")
	}
	return info, err
}

func (c *commandFsMergeVolumes) volumesAreCompatible(src needle.VolumeId, dest needle.VolumeId) (bool, error) {
	srcInfo, err := c.getVolumeInfoById(src)
	if err != nil {
		return false, err
	}
	destInfo, err := c.getVolumeInfoById(dest)
	if err != nil {
		return false, err
	}
	return (srcInfo.Collection == destInfo.Collection &&
		srcInfo.Ttl == destInfo.Ttl &&
		srcInfo.ReplicaPlacement == destInfo.ReplicaPlacement), nil
}

func (c *commandFsMergeVolumes) reloadVolumesInfo(masterClient *wdclient.MasterClient) error {
	c.volumes = make(map[needle.VolumeId]*master_pb.VolumeInformationMessage)

	return masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		volumes, err := pb.CollectVolumeList(context.Background(), client, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		c.volumeSizeLimit = volumes.GetVolumeSizeLimitMb() * 1024 * 1024

		for _, dc := range volumes.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for _, disk := range node.DiskInfos {
						for _, volume := range disk.VolumeInfos {
							vid := needle.VolumeId(volume.Id)
							if found := c.volumes[vid]; found == nil {
								c.volumes[vid] = volume
							}
						}
					}
				}
			}
		}
		return nil
	})
}

// mergePlan maps each source volume to candidate targets: a single-target
// source sends every chunk there (historic behavior), a multi-target source
// allocates per chunk under mu since TraverseBfs callbacks run in parallel.
type mergePlan struct {
	mu              sync.Mutex
	targets         map[needle.VolumeId][]needle.VolumeId
	plannedSize     map[needle.VolumeId]uint64
	volumeSizeLimit uint64
}

func newMergePlan(volumeSizeLimit uint64) *mergePlan {
	return &mergePlan{
		targets:         make(map[needle.VolumeId][]needle.VolumeId),
		plannedSize:     make(map[needle.VolumeId]uint64),
		volumeSizeLimit: volumeSizeLimit,
	}
}

func (p *mergePlan) isSource(vid needle.VolumeId) bool {
	_, found := p.targets[vid]
	return found
}

// allocate picks the candidate with the most remaining capacity that still
// fits the chunk, and reserves the chunk size against it.
func (p *mergePlan) allocate(src needle.VolumeId, size uint64) (needle.VolumeId, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	candidates := p.targets[src]
	if len(candidates) == 0 {
		return 0, false
	}
	if len(candidates) == 1 {
		return candidates[0], true
	}
	var best needle.VolumeId
	var bestRemaining uint64
	found := false
	for _, t := range candidates {
		used := p.plannedSize[t]
		if used+size > p.volumeSizeLimit {
			continue
		}
		if remaining := p.volumeSizeLimit - used; !found || remaining > bestRemaining {
			best, bestRemaining, found = t, remaining, true
		}
	}
	if !found {
		return 0, false
	}
	p.plannedSize[best] += size
	return best, true
}

// release returns a failed move's reservation so later chunks can use it.
// Single-target sources reserve at plan time, not per chunk.
func (p *mergePlan) release(src, target needle.VolumeId, size uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.targets[src]) <= 1 {
		return
	}
	if p.plannedSize[target] >= size {
		p.plannedSize[target] -= size
	}
}

// Empty or "0" means unset, matching the old numeric flag's default.
func parseTargetVolumeIds(arg string) ([]needle.VolumeId, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" || arg == "0" {
		return nil, nil
	}
	var ids []needle.VolumeId
	seen := make(map[needle.VolumeId]bool)
	for _, part := range strings.Split(arg, ",") {
		part = strings.TrimSpace(part)
		v, err := strconv.ParseUint(part, 10, 32)
		if err != nil || v == 0 {
			return nil, fmt.Errorf("invalid toVolumeId %q", part)
		}
		vid := needle.VolumeId(v)
		if seen[vid] {
			return nil, fmt.Errorf("duplicate toVolumeId %d", vid)
		}
		seen[vid] = true
		ids = append(ids, vid)
	}
	return ids, nil
}

func (c *commandFsMergeVolumes) createMergePlan(collection string, toVolumeIds []needle.VolumeId, fromVolumeId needle.VolumeId) (*mergePlan, error) {
	// When the user names both endpoints, honor that exact direction. The
	// heuristic below only ever merges a smaller volume into a larger one, so
	// an explicit "merge larger into smaller" request would otherwise yield an
	// empty plan and silently do nothing.
	if fromVolumeId != 0 && len(toVolumeIds) > 0 {
		return c.createDirectedMergePlan(collection, fromVolumeId, toVolumeIds)
	}

	plan := newMergePlan(c.volumeSizeLimit)
	volumeIds := maps.Keys(c.volumes)
	sort.Slice(volumeIds, func(a, b int) bool {
		return c.volumes[volumeIds[b]].Size < c.volumes[volumeIds[a]].Size
	})

	l := len(volumeIds)
	for i := 0; i < l; i++ {
		volume := c.volumes[volumeIds[i]]
		if volume.GetReadOnly() || c.getVolumeSize(volume) == 0 || (collection != "*" && collection != volume.GetCollection()) {

			if fromVolumeId != 0 && volumeIds[i] == fromVolumeId || slices.Contains(toVolumeIds, volumeIds[i]) {
				if volume.GetReadOnly() {
					return nil, fmt.Errorf("volume %d is readonly", volumeIds[i])
				}
				if c.getVolumeSize(volume) == 0 {
					return nil, fmt.Errorf("volume %d is empty", volumeIds[i])
				}
			}
			volumeIds = slices.Delete(volumeIds, i, i+1)
			i--
			l--
		}
	}
	for i := l - 1; i >= 0; i-- {
		src := volumeIds[i]
		if fromVolumeId != 0 && src != fromVolumeId {
			continue
		}
		for j := 0; j < i; j++ {
			candidate := volumeIds[j]
			if len(toVolumeIds) > 0 && !slices.Contains(toVolumeIds, candidate) {
				continue
			}
			if _, moving := plan.targets[candidate]; moving {
				continue
			}
			compatible, err := c.volumesAreCompatible(src, candidate)
			if err != nil {
				return nil, err
			}
			if !compatible {
				fmt.Printf("volume %d is not compatible with volume %d\n", src, candidate)
				continue
			}
			if _, tracked := plan.plannedSize[candidate]; !tracked {
				plan.plannedSize[candidate] = c.getVolumeSizeById(candidate)
			}
			candidatePlannedSize := plan.plannedSize[candidate]
			if candidatePlannedSize+c.getVolumeSizeById(src) > c.volumeSizeLimit {
				fmt.Printf("volume %d (%d MB) merge into volume %d (%d MB, %d MB with plan) exceeds volume size limit (%d MB)\n",
					src, c.getVolumeSizeById(src)/1024/1024,
					candidate, c.getVolumeSizeById(candidate)/1024/1024, candidatePlannedSize/1024/1024,
					c.volumeSizeLimit/1024/1024)
				continue
			}
			plan.targets[src] = []needle.VolumeId{candidate}
			plan.plannedSize[candidate] += c.getVolumeSizeById(src)
			break
		}
	}

	return plan, nil
}

// createDirectedMergePlan honors the exact direction the user named, skipping
// the heuristic planner's smaller-into-larger ordering.
func (c *commandFsMergeVolumes) createDirectedMergePlan(collection string, from needle.VolumeId, toIds []needle.VolumeId) (*mergePlan, error) {
	if slices.Contains(toIds, from) {
		return nil, fmt.Errorf("no volume id changes, %d is both source and target", from)
	}
	for _, vid := range append([]needle.VolumeId{from}, toIds...) {
		volume, err := c.getVolumeInfoById(vid)
		if err != nil {
			return nil, err
		}
		if volume.GetReadOnly() {
			return nil, fmt.Errorf("volume %d is readonly", vid)
		}
		if collection != "*" && collection != volume.GetCollection() {
			return nil, fmt.Errorf("volume %d is not in collection %q", vid, collection)
		}
		// Merging into an empty target is valid (e.g. a freshly vacuumed
		// volume); only an empty source has nothing to move.
		if vid == from && c.getVolumeSize(volume) == 0 {
			return nil, fmt.Errorf("volume %d is empty", vid)
		}
		if vid != from {
			compatible, err := c.volumesAreCompatible(from, vid)
			if err != nil {
				return nil, err
			}
			if !compatible {
				return nil, fmt.Errorf("volume %d is not compatible with volume %d", from, vid)
			}
		}
	}

	plan := newMergePlan(c.volumeSizeLimit)
	fromSize := c.getVolumeSizeById(from)
	var totalFree uint64
	for _, to := range toIds {
		toSize := c.getVolumeSizeById(to)
		plan.plannedSize[to] = toSize
		if toSize < c.volumeSizeLimit {
			totalFree += c.volumeSizeLimit - toSize
		}
	}
	if fromSize > totalFree {
		return nil, fmt.Errorf(
			"volume %d (%d MB) cannot merge into volumes %v (%d MB free) due to volume size limit (%d MB)",
			from, fromSize/1024/1024,
			toIds, totalFree/1024/1024,
			c.volumeSizeLimit/1024/1024,
		)
	}
	plan.targets[from] = toIds
	return plan, nil
}

// getVolumeSize is the volume's live data size, clamped since
// DeletedByteCount can transiently exceed Size.
func (c *commandFsMergeVolumes) getVolumeSize(volume *master_pb.VolumeInformationMessage) uint64 {
	if volume.Size < volume.DeletedByteCount {
		return 0
	}
	return volume.Size - volume.DeletedByteCount
}

func (c *commandFsMergeVolumes) getVolumeSizeById(vid needle.VolumeId) uint64 {
	return c.getVolumeSize(c.volumes[vid])
}

func (c *commandFsMergeVolumes) printPlan(plan *mergePlan) {
	fmt.Printf("max volume size: %d MB\n", c.volumeSizeLimit/1024/1024)
	reversePlan := make(map[needle.VolumeId][]needle.VolumeId)
	for src, dests := range plan.targets {
		if len(dests) > 1 {
			fmt.Printf("volume %d (%d MB) distribute across volumes %v by remaining capacity\n",
				src, c.getVolumeSizeById(src)/1024/1024, dests)
			continue
		}
		reversePlan[dests[0]] = append(reversePlan[dests[0]], src)
	}
	for dest, srcs := range reversePlan {
		currentSize := c.getVolumeSizeById(dest)
		for _, src := range srcs {
			srcSize := c.getVolumeSizeById(src)
			newSize := currentSize + srcSize
			fmt.Printf(
				"volume %d (%d MB) merge into volume %d (%d MB => %d MB)\n",
				src, srcSize/1024/1024,
				dest, currentSize/1024/1024, newSize/1024/1024,
			)
			currentSize = newSize

		}
		fmt.Println()
	}
}

// rewriteManifestChunk walks the sub-chunks referenced by a manifest chunk and
// moves any that live in a source volume from the merge plan. If any sub-chunk
// moves, or the manifest chunk itself lives in a source volume, the manifest
// blob is re-serialized and uploaded to a freshly assigned file id.
//
// The returned movedSourceNeedle slice lists every source needle the caller
// should delete once the filer update commits — sub-chunks that were moved and
// nested manifest chunks that got rewritten. The OUTER manifest needle is the
// caller's responsibility to record, since only the caller knows its pre-move
// fid (this function's own chunk argument still reports the old fid on return,
// but that couples manifest-nesting logic to a fact that is easier to capture
// at the top-level callsite).
func (c *commandFsMergeVolumes) rewriteManifestChunk(
	ctx context.Context,
	commandEnv *CommandEnv,
	lookupFn wdclient.LookupFileIdFunctionType,
	plan *mergePlan,
	entryPath util.FullPath,
	chunk *filer_pb.FileChunk,
	apply bool,
) (*filer_pb.FileChunk, bool, []movedSourceNeedle, error) {
	if !chunk.IsChunkManifest {
		return chunk, false, nil, fmt.Errorf("not a manifest chunk: %s", chunk.GetFileIdString())
	}

	subChunks, err := filer.ResolveOneChunkManifest(ctx, lookupFn, chunk)
	if err != nil {
		return chunk, false, nil, err
	}

	var movedSources []movedSourceNeedle
	anySubChanged := false
	for i, sub := range subChunks {
		if sub.IsChunkManifest {
			oldSubManifestFid := sub.GetFileIdString()
			oldSubManifestVid := sub.Fid.VolumeId
			newSub, changed, nestedSources, rErr := c.rewriteManifestChunk(ctx, commandEnv, lookupFn, plan, entryPath, sub, apply)
			if rErr != nil {
				return chunk, false, nil, rErr
			}
			if changed {
				subChunks[i] = newSub
				anySubChanged = true
				if apply {
					movedSources = append(movedSources, nestedSources...)
					// Nested manifest got replaced — its old needle is now
					// orphan on the same volume it used to live on.
					movedSources = append(movedSources, movedSourceNeedle{volumeId: oldSubManifestVid, fileId: oldSubManifestFid})
				}
			}
			continue
		}
		subVid := needle.VolumeId(sub.Fid.VolumeId)
		if !plan.isSource(subVid) {
			continue
		}
		oldSubFid := sub.GetFileIdString()
		oldSubVid := sub.Fid.VolumeId
		toVid, ok := plan.allocate(subVid, sub.Size)
		if !ok {
			fmt.Printf("skip %s(%s) [inside manifest %s]: no target volume has room\n", entryPath, oldSubFid, chunk.GetFileIdString())
			continue
		}
		fmt.Printf("move %s(%s) => volume %d [inside manifest %s]\n", entryPath, oldSubFid, toVid, chunk.GetFileIdString())
		if !apply {
			anySubChanged = true
			continue
		}
		if mErr := moveChunk(sub, toVid, commandEnv.MasterClient); mErr != nil {
			fmt.Printf("failed to move %s(%s): %v\n", entryPath, oldSubFid, mErr)
			plan.release(subVid, toVid, sub.Size)
			continue
		}
		anySubChanged = true
		movedSources = append(movedSources, movedSourceNeedle{volumeId: oldSubVid, fileId: oldSubFid})
	}

	manifestVid := needle.VolumeId(chunk.Fid.VolumeId)
	manifestMustMove := plan.isSource(manifestVid)

	if !anySubChanged && !manifestMustMove {
		return chunk, false, nil, nil
	}

	fmt.Printf("rewrite manifest %s(%s)\n", entryPath, chunk.GetFileIdString())
	if !apply {
		// Propagate "would change" so nested callers also announce their
		// rewrites in dry-run mode. The top-level caller gates any actual
		// filer writes on *apply, so returning true here is safe.
		return chunk, true, nil, nil
	}

	filer_pb.BeforeEntrySerialization(subChunks)
	defer filer_pb.AfterEntryDeserialization(subChunks)
	data, err := proto.Marshal(&filer_pb.FileChunkManifest{Chunks: subChunks})
	if err != nil {
		return chunk, false, nil, fmt.Errorf("marshal manifest: %w", err)
	}

	collection := ""
	if info, ok := c.volumes[manifestVid]; ok {
		collection = info.Collection
	}
	newChunk, err := c.uploadManifestChunk(ctx, commandEnv, entryPath, collection, plan, data)
	if err != nil {
		return chunk, false, nil, fmt.Errorf("upload new manifest: %w", err)
	}

	newChunk.IsChunkManifest = true
	newChunk.Offset = chunk.Offset
	newChunk.Size = chunk.Size
	if chunk.ModifiedTsNs != 0 {
		newChunk.ModifiedTsNs = chunk.ModifiedTsNs
	}
	newChunk.FileId = ""

	return newChunk, true, movedSources, nil
}

// uploadManifestChunk assigns a fresh file id via the filer and uploads the
// given manifest bytes to the chosen volume server. If the filer picks a
// volume that is a source in the merge plan, the assignment is rejected and
// retried up to manifestAssignAttempts times — otherwise the replacement
// manifest would land on the very volume this command is trying to empty.
func (c *commandFsMergeVolumes) uploadManifestChunk(
	ctx context.Context,
	commandEnv *CommandEnv,
	entryPath util.FullPath,
	collection string,
	plan *mergePlan,
	data []byte,
) (*filer_pb.FileChunk, error) {
	const manifestAssignAttempts = 10
	var assignResp *filer_pb.AssignVolumeResponse
	if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for attempt := 1; attempt <= manifestAssignAttempts; attempt++ {
			resp, err := client.AssignVolume(ctx, &filer_pb.AssignVolumeRequest{
				Count:      1,
				Collection: collection,
				// entryPath is built from entry.Name returned by the filer. Filers
				// written through gRPC already hold valid UTF-8, but legacy or
				// directly-imported entries may not — sanitize so one bad name
				// does not fail the whole merge pass.
				Path:             entryPath.Sanitized(),
				ExpectedDataSize: uint64(len(data)),
			})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("%s", resp.Error)
			}
			fid, parseErr := filer_pb.ToFileIdObject(resp.FileId)
			if parseErr != nil {
				return fmt.Errorf("parse assigned fid %q: %w", resp.FileId, parseErr)
			}
			if !plan.isSource(needle.VolumeId(fid.VolumeId)) {
				assignResp = resp
				return nil
			}
			fmt.Printf("rejecting manifest assignment to merge-source volume %d (attempt %d/%d)\n",
				fid.VolumeId, attempt, manifestAssignAttempts)
		}
		return fmt.Errorf("filer kept assigning manifest uploads to merge-source volumes after %d attempts", manifestAssignAttempts)
	}); err != nil {
		return nil, fmt.Errorf("assign volume: %w", err)
	}
	if assignResp.Location == nil {
		return nil, fmt.Errorf("assign volume returned no location")
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, err
	}

	uploadUrl := fmt.Sprintf("http://%s/%s", commandEnv.AdjustedUrl(assignResp.Location), assignResp.FileId)

	jwt := security.EncodedJwt(assignResp.Auth)
	if jwt == "" {
		v := util.GetViper()
		if signingKey := v.GetString("jwt.signing.key"); signingKey != "" {
			expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
			jwt = security.GenJwtForVolumeServer(security.SigningKey(signingKey), expiresAfterSec, assignResp.FileId)
		}
	}

	uploadResult, err := uploader.UploadData(ctx, data, &operation.UploadOption{
		UploadUrl: uploadUrl,
		Jwt:       jwt,
	})
	if err != nil {
		return nil, err
	}
	if uploadResult.Error != "" {
		return nil, fmt.Errorf("upload: %s", uploadResult.Error)
	}

	return uploadResult.ToPbFileChunk(assignResp.FileId, 0, time.Now().UnixNano()), nil
}

func moveChunk(chunk *filer_pb.FileChunk, toVolumeId needle.VolumeId, masterClient *wdclient.MasterClient) error {
	fromFid := needle.NewFileId(needle.VolumeId(chunk.Fid.VolumeId), chunk.Fid.FileKey, chunk.Fid.Cookie)
	toFid := needle.NewFileId(toVolumeId, chunk.Fid.FileKey, chunk.Fid.Cookie)

	downloadURLs, err := masterClient.LookupVolumeServerUrl(fromFid.VolumeId.String())
	if err != nil {
		return err
	}

	downloadURL := fmt.Sprintf("http://%s/%s?readDeleted=true", downloadURLs[0], fromFid.String())

	uploadURLs, err := masterClient.LookupVolumeServerUrl(toVolumeId.String())
	if err != nil {
		return err
	}
	uploadURL := fmt.Sprintf("http://%s/%s", uploadURLs[0], toFid.String())

	resp, reader, err := readUrl(downloadURL, filer.JwtForVolumeServer(fromFid.String()))
	if err != nil {
		return err
	}
	defer util_http.CloseResponse(resp)
	defer reader.Close()

	var filename string

	contentDisposition := resp.Header.Get("Content-Disposition")
	if len(contentDisposition) > 0 {
		idx := strings.Index(contentDisposition, "filename=")
		if idx != -1 {
			filename = contentDisposition[idx+len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}

	contentType := resp.Header.Get("Content-Type")
	isCompressed := resp.Header.Get("Content-Encoding") == "gzip"
	md5 := resp.Header.Get("Content-MD5")

	uploader, err := operation.NewUploader()
	if err != nil {
		return err
	}

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	var jwt security.EncodedJwt
	if signingKey != "" {
		expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
		jwt = security.GenJwtForVolumeServer(security.SigningKey(signingKey), expiresAfterSec, toFid.String())
	}

	_, err, _ = uploader.Upload(context.Background(), reader, &operation.UploadOption{
		UploadUrl:         uploadURL,
		Filename:          filename,
		IsInputCompressed: isCompressed,
		Cipher:            false,
		MimeType:          contentType,
		PairMap:           nil,
		Md5:               md5,
		Jwt:               security.EncodedJwt(jwt),
	})
	if err != nil {
		return err
	}
	chunk.Fid.VolumeId = uint32(toVolumeId)
	chunk.FileId = ""

	return nil
}

func readUrl(fileUrl string, jwt string) (*http.Response, io.ReadCloser, error) {

	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("Accept-Encoding", "gzip")
	if jwt != "" {
		req.Header.Set("Authorization", security.BearerPrefix+jwt)
	}

	r, err := util_http.GetGlobalHttpClient().Do(req)
	if err != nil {
		return nil, nil, err
	}
	if r.StatusCode >= 400 {
		util_http.CloseResponse(r)
		return nil, nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	return r, r.Body, nil
}
