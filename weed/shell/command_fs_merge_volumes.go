package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/operation"
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

	fs.mergeVolumes [-toVolumeId=y] [-fromVolumeId=x] [-collection="*"] [-dir=/] [-apply]
`
}

func (c *commandFsMergeVolumes) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMergeVolumes) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMergeVolumesCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	dirArg := fsMergeVolumesCommand.String("dir", "/", "base directory to find and update files")
	fromVolumeArg := fsMergeVolumesCommand.Uint("fromVolumeId", 0, "move chunks with this volume id")
	toVolumeArg := fsMergeVolumesCommand.Uint("toVolumeId", 0, "change chunks to this volume id")
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
	if *toVolumeArg > maxVolumeID {
		return fmt.Errorf("toVolumeId %d exceeds max volume id %d", *toVolumeArg, maxVolumeID)
	}

	fromVolumeId := needle.VolumeId(*fromVolumeArg)
	toVolumeId := needle.VolumeId(*toVolumeArg)

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
	if toVolumeId != 0 {
		if _, err := c.getVolumeInfoById(toVolumeId); err != nil {
			return fmt.Errorf("toVolumeId %d not found on master", toVolumeId)
		}
	}

	if fromVolumeId != 0 && toVolumeId != 0 {
		if fromVolumeId == toVolumeId {
			return fmt.Errorf("no volume id changes, %d == %d", fromVolumeId, toVolumeId)
		}
		compatible, err := c.volumesAreCompatible(fromVolumeId, toVolumeId)
		if err != nil {
			return fmt.Errorf("cannot determine volumes are compatible: %d and %d", fromVolumeId, toVolumeId)
		}
		if !compatible {
			return fmt.Errorf("volume %d is not compatible with volume %d", fromVolumeId, toVolumeId)
		}
		fromSize := c.getVolumeSizeById(fromVolumeId)
		toSize := c.getVolumeSizeById(toVolumeId)
		if fromSize+toSize > c.volumeSizeLimit {
			return fmt.Errorf(
				"volume %d (%d MB) cannot merge into volume %d (%d MB_ due to volume size limit (%d MB)",
				fromVolumeId, fromSize/1024/1024,
				toVolumeId, toSize/1024/1024,
				c.volumeSizeLimit/1024/1024,
			)
		}
	}

	plan, err := c.createMergePlan(*collectionArg, toVolumeId, fromVolumeId)

	if err != nil {
		return err
	}
	c.printPlan(plan)

	if len(plan) == 0 {
		return nil
	}

	defer util_http.GetGlobalHttpClient().CloseIdleConnections()

	lookupFn := filer.LookupFn(commandEnv)

	return commandEnv.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		return filer_pb.TraverseBfs(context.Background(), commandEnv, util.FullPath(dir), func(parentPath util.FullPath, entry *filer_pb.Entry) error {
			if entry.IsDirectory {
				return nil
			}
			entryPath := parentPath.Child(entry.Name)
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
				toVolumeId, found := plan[chunkVolumeId]
				if !found {
					continue
				}

				oldFid := chunk.GetFileIdString()
				oldVid := chunk.Fid.VolumeId
				fmt.Printf("move %s(%s)\n", entryPath, oldFid)
				if !*apply {
					continue
				}
				if mvErr := moveChunk(chunk, toVolumeId, commandEnv.MasterClient); mvErr != nil {
					fmt.Printf("failed to move %s(%s): %v\n", entryPath, oldFid, mvErr)
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
			for _, r := range results {
				// StatusNotModified means the needle was already deleted
				// (e.g. a concurrent fsck purge or a replica that had
				// already reconciled). That's the desired end state, so
				// don't warn about it. Cast to int because r.Status is
				// an int32 protobuf field and linters flag the mixed-type
				// compare even though Go's untyped-constant rules make it
				// valid.
				if r.Error != "" && int(r.Status) != http.StatusNotModified {
					fmt.Printf("source cleanup %s: delete %s on %v: %s\n", entryPath, r.FileId, loc.ServerAddress(), r.Error)
				}
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
		volumes, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
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

func (c *commandFsMergeVolumes) createMergePlan(collection string, toVolumeId needle.VolumeId, fromVolumeId needle.VolumeId) (map[needle.VolumeId]needle.VolumeId, error) {
	plan := make(map[needle.VolumeId]needle.VolumeId)
	volumeIds := maps.Keys(c.volumes)
	sort.Slice(volumeIds, func(a, b int) bool {
		return c.volumes[volumeIds[b]].Size < c.volumes[volumeIds[a]].Size
	})

	l := len(volumeIds)
	for i := 0; i < l; i++ {
		volume := c.volumes[volumeIds[i]]
		if volume.GetReadOnly() || c.getVolumeSize(volume) == 0 || (collection != "*" && collection != volume.GetCollection()) {

			if fromVolumeId != 0 && volumeIds[i] == fromVolumeId || toVolumeId != 0 && volumeIds[i] == toVolumeId {
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
			if toVolumeId != 0 && candidate != toVolumeId {
				continue
			}
			if _, moving := plan[candidate]; moving {
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
			candidatePlannedSize := c.getVolumeSizeBasedOnPlan(plan, candidate)
			if candidatePlannedSize+c.getVolumeSizeById(src) > c.volumeSizeLimit {
				fmt.Printf("volume %d (%d MB) merge into volume %d (%d MB, %d MB with plan) exceeds volume size limit (%d MB)\n",
					src, c.getVolumeSizeById(src)/1024/1024,
					candidate, c.getVolumeSizeById(candidate)/1024/1024, candidatePlannedSize/1024/1024,
					c.volumeSizeLimit/1024/1024)
				continue
			}
			plan[src] = candidate
			break
		}
	}

	return plan, nil
}

func (c *commandFsMergeVolumes) getVolumeSizeBasedOnPlan(plan map[needle.VolumeId]needle.VolumeId, vid needle.VolumeId) uint64 {
	size := c.getVolumeSizeById(vid)
	for src, dest := range plan {
		if dest == vid {
			size += c.getVolumeSizeById(src)
		}
	}
	return size
}

func (c *commandFsMergeVolumes) getVolumeSize(volume *master_pb.VolumeInformationMessage) uint64 {
	return volume.Size - volume.DeletedByteCount
}

func (c *commandFsMergeVolumes) getVolumeSizeById(vid needle.VolumeId) uint64 {
	return c.getVolumeSize(c.volumes[vid])
}

func (c *commandFsMergeVolumes) printPlan(plan map[needle.VolumeId]needle.VolumeId) {
	fmt.Printf("max volume size: %d MB\n", c.volumeSizeLimit/1024/1024)
	reversePlan := make(map[needle.VolumeId][]needle.VolumeId)
	for src, dest := range plan {
		reversePlan[dest] = append(reversePlan[dest], src)
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
	plan map[needle.VolumeId]needle.VolumeId,
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
		toVid, ok := plan[subVid]
		if !ok {
			continue
		}
		oldSubFid := sub.GetFileIdString()
		oldSubVid := sub.Fid.VolumeId
		fmt.Printf("move %s(%s) [inside manifest %s]\n", entryPath, oldSubFid, chunk.GetFileIdString())
		if !apply {
			anySubChanged = true
			continue
		}
		if mErr := moveChunk(sub, toVid, commandEnv.MasterClient); mErr != nil {
			fmt.Printf("failed to move %s(%s): %v\n", entryPath, oldSubFid, mErr)
			continue
		}
		anySubChanged = true
		movedSources = append(movedSources, movedSourceNeedle{volumeId: oldSubVid, fileId: oldSubFid})
	}

	manifestVid := needle.VolumeId(chunk.Fid.VolumeId)
	_, manifestMustMove := plan[manifestVid]

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
	plan map[needle.VolumeId]needle.VolumeId,
	data []byte,
) (*filer_pb.FileChunk, error) {
	const manifestAssignAttempts = 10
	var assignResp *filer_pb.AssignVolumeResponse
	if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for attempt := 1; attempt <= manifestAssignAttempts; attempt++ {
			resp, err := client.AssignVolume(ctx, &filer_pb.AssignVolumeRequest{
				Count:            1,
				Collection:       collection,
				Path:             string(entryPath),
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
			if _, isSource := plan[needle.VolumeId(fid.VolumeId)]; !isSource {
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

	resp, reader, err := readUrl(downloadURL)
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

func readUrl(fileUrl string) (*http.Response, io.ReadCloser, error) {

	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("Accept-Encoding", "gzip")

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
