package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	Commands = append(Commands, &commandFsVerify{})
}

type commandFsVerify struct {
	env                *CommandEnv
	volumeServers      []pb.ServerAddress
	volumeIds          map[uint32][]pb.ServerAddress
	verbose            *bool
	metadataFromLog    *bool
	pruneEntries       *bool
	concurrency        *int
	modifyTimeAgoAtSec int64
	writer             io.Writer
	waitChan           map[string]chan struct{}
	waitChanLock       sync.RWMutex
}

func (c *commandFsVerify) Name() string {
	return "fs.verify"
}

func (c *commandFsVerify) Help() string {
	return `recursively verify all files under a directory

	fs.verify [-v] [-modifyTimeAgo 1h] [-pruneEntries] [-concurrency 4] /buckets/dir

	-pruneEntries deletes filer entries whose needles are missing from every
	volume location holding the volume (data lost, e.g. after a volume
	truncation). The delete only applies when the entry is unchanged since
	verification, so a re-uploaded file is never pruned.

`
}

func (c *commandFsVerify) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsVerify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	c.env = commandEnv
	c.writer = writer
	fsVerifyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.verbose = fsVerifyCommand.Bool("v", false, "print out each processed files")
	modifyTimeAgo := fsVerifyCommand.Duration("modifyTimeAgo", 0, "only include files after this modify time to verify")
	c.concurrency = fsVerifyCommand.Int("concurrency", 0, "number of parallel verification per volume server")
	c.metadataFromLog = fsVerifyCommand.Bool("metadataFromLog", false, "Using  filer log to get metadata")
	c.pruneEntries = fsVerifyCommand.Bool("pruneEntries", false, "delete filer entries whose needles are missing from all volume locations (data lost); a changed entry is never deleted")
	if err = fsVerifyCommand.Parse(args); err != nil {
		return err
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsVerifyCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	c.modifyTimeAgoAtSec = int64(modifyTimeAgo.Seconds())
	c.volumeIds = make(map[uint32][]pb.ServerAddress)
	c.waitChan = make(map[string]chan struct{})
	c.volumeServers = []pb.ServerAddress{}
	defer func() {
		c.modifyTimeAgoAtSec = 0
		c.volumeIds = nil
		c.waitChan = nil
		c.volumeServers = nil
	}()

	if err := c.collectVolumeIds(); err != nil {
		return err
	}

	if *c.concurrency > 0 {
		for _, volumeServer := range c.volumeServers {
			volumeServerStr := string(volumeServer)
			c.waitChan[volumeServerStr] = make(chan struct{}, *c.concurrency)
			defer close(c.waitChan[volumeServerStr])
		}
	}
	var fCount, eCount, pCount uint64
	if *c.metadataFromLog {
		var wg sync.WaitGroup
		fCount, eCount, pCount, err = c.verifyProcessMetadata(path, &wg)
		wg.Wait()
	} else {
		fCount, eCount, pCount, err = c.verifyTraverseBfs(path)
	}
	fmt.Fprintf(writer, "verified %d files, error %d files, pruned %d entries \n", fCount, eCount, pCount)
	return err
}

func (c *commandFsVerify) collectVolumeIds() error {
	topologyInfo, _, err := collectTopologyInfo(c.env, 0)
	if err != nil {
		return err
	}
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, nodeInfo *master_pb.DataNodeInfo) {
		for _, diskInfo := range nodeInfo.DiskInfos {
			for _, vi := range diskInfo.VolumeInfos {
				volumeServer := pb.NewServerAddressFromDataNode(nodeInfo)
				c.volumeIds[vi.Id] = append(c.volumeIds[vi.Id], volumeServer)
				if !slices.Contains(c.volumeServers, volumeServer) {
					c.volumeServers = append(c.volumeServers, volumeServer)
				}
			}
			for _, vi := range diskInfo.EcShardInfos {
				volumeServer := pb.NewServerAddressFromDataNode(nodeInfo)
				c.volumeIds[vi.Id] = append(c.volumeIds[vi.Id], volumeServer)
				if !slices.Contains(c.volumeServers, volumeServer) {
					c.volumeServers = append(c.volumeServers, volumeServer)
				}
			}
		}
	})
	return nil
}

func (c *commandFsVerify) verifyChunk(volumeServer pb.ServerAddress, fileId *filer_pb.FileId) error {
	err := operation.WithVolumeServerClient(false, volumeServer, c.env.option.GrpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeNeedleStatus(context.Background(),
				&volume_server_pb.VolumeNeedleStatusRequest{
					VolumeId: fileId.VolumeId,
					NeedleId: fileId.FileKey})
			return err
		},
	)
	if err != nil && !strings.Contains(err.Error(), storage.ErrorDeleted.Error()) {
		return err
	}
	return nil
}

type ItemEntry struct {
	chunks    []*filer_pb.FileChunk
	rawChunks []*filer_pb.FileChunk
	path      util.FullPath
	mtimeSec  int64
	md5       []byte
}

// resolveAndVerify expands chunk manifests and verifies the entry's needles.
// An unresolvable manifest is an entry-level failure even when raw chunks are
// healthy; raw chunks are still checked so a missing manifest needle can be pruned.
func (c *commandFsVerify) resolveAndVerify(entryPath string, chunks []*filer_pb.FileChunk, errorChunksCount *atomic.Uint64, wg *sync.WaitGroup) (verified bool, hasMissingNeedles bool) {
	dataChunks := chunks
	manifestResolveFailed := false
	if resolved, manifestChunks, resolveErr := filer.ResolveChunkManifest(context.Background(), filer.LookupFn(c.env), chunks, 0, math.MaxInt64, nil); resolveErr == nil {
		dataChunks = append(resolved, manifestChunks...)
	} else {
		manifestResolveFailed = true
		fmt.Fprintf(c.writer, "file: %s failed to resolve chunk manifest (%v), verifying raw chunks\n", entryPath, resolveErr)
	}
	verified, hasMissingNeedles = c.verifyEntry(entryPath, dataChunks, errorChunksCount, wg)
	if manifestResolveFailed && verified {
		// an unreadable manifest is an entry-level failure even with healthy raw needles
		verified = false
		errorChunksCount.CompareAndSwap(0, 1)
	}
	return verified, hasMissingNeedles
}

func (c *commandFsVerify) verifyProcessMetadata(path string, wg *sync.WaitGroup) (fileCount uint64, errCount uint64, prunedCount uint64, err error) {
	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if resp.EventNotification.NewEntry == nil {
			return nil
		}
		chunkCount := len(message.NewEntry.Chunks)
		if chunkCount == 0 {
			return nil
		}
		entryPath := fmt.Sprintf("%s/%s", message.NewParentPath, message.NewEntry.Name)
		errorChunksCount := atomic.NewUint64(0)
		verified, hasMissingNeedles := c.resolveAndVerify(entryPath, message.NewEntry.Chunks, errorChunksCount, wg)
		if !verified {
			if err = c.env.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				entryResp, errReq := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
					Directory: message.NewParentPath,
					Name:      message.NewEntry.Name,
				})
				if errReq != nil {
					if strings.HasSuffix(errReq.Error(), "no entry is found in filer store") {
						return nil
					}
					return errReq
				}
				if entryResp.Entry.Attributes.Mtime == message.NewEntry.Attributes.Mtime &&
					bytes.Equal(entryResp.Entry.Attributes.Md5, message.NewEntry.Attributes.Md5) {
					fmt.Fprintf(c.writer, "file: %s needles:%d failed:%d\n", entryPath, chunkCount, errorChunksCount.Load())
					errCount++
					if *c.pruneEntries && hasMissingNeedles {
						pruned, pruneErr := c.pruneEntry(
							util.NewFullPath(message.NewParentPath, message.NewEntry.Name),
							message.NewEntry.Attributes.Mtime,
							message.NewEntry.Attributes.Md5,
							message.NewEntry.Chunks)
						if pruneErr != nil {
							fmt.Fprintf(c.writer, "prune %s failed: %v\n", entryPath, pruneErr)
						} else if pruned {
							prunedCount++
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
			return nil
		}
		if *c.verbose {
			fmt.Fprintf(c.writer, "file: %s needles:%d verified\n", entryPath, chunkCount)
		}
		fileCount++
		return nil
	}
	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "shell_verify",
		ClientId:               util.RandomInt32(),
		ClientEpoch:            0,
		SelfSignature:          0,
		PathPrefix:             path,
		AdditionalPathPrefixes: nil,
		DirectoriesToWatch:     nil,
		StartTsNs:              time.Now().Add(-1 * time.Second * time.Duration(c.modifyTimeAgoAtSec)).UnixNano(),
		StopTsNs:               time.Now().UnixNano(),
		EventErrorType:         pb.DontLogError,
	}
	err = pb.FollowMetadata(c.env.option.FilerAddress, c.env.option.GrpcDialOption, metadataFollowOption, processEventFn)
	return fileCount, errCount, prunedCount, err
}

// isNeedleMissingError reports whether a VolumeNeedleStatus error means the
// needle data is lost at that location. It must NOT match "volume not found".
// Newer Go servers and the Rust server return codes.NotFound; older Go
// servers return codes.Unknown with "needle not found <id>" or "EOF" (and EC
// volumes wrap it as "locate in local ec volume: ... needle not found").
// Legacy shapes are matched anchored so unrelated errors don't classify as missing.
func isNeedleMissingError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return strings.HasPrefix(st.Message(), "needle not found ")
		case codes.Unknown:
			if st.Message() != "" {
				msg = st.Message()
			}
		default:
			return false
		}
	}
	return strings.HasPrefix(msg, "needle not found ") || msg == "EOF" ||
		strings.Contains(msg, "locate in local ec volume:") && strings.HasSuffix(msg, "needle not found")
}

// verifyEntry verifies all chunks of an entry. It returns verified=false when
// any chunk failed. hasMissingNeedles is true when at least one needle is
// missing from every volume location holding its volume, i.e. the data is
// lost rather than a transient or routing error.
func (c *commandFsVerify) verifyEntry(path string, chunks []*filer_pb.FileChunk, errorCount *atomic.Uint64, wg *sync.WaitGroup) (verified bool, hasMissingNeedles bool) {
	fileMsg := fmt.Sprintf("file:%s", path)
	itemIsVerifed := atomic.NewBool(true)
	hasMissingNeedlesFlag := atomic.NewBool(false)
	// tracks this entry's in-flight verifications so the missing-needle
	// decision at the end of each chunk is deterministic
	itemWg := &sync.WaitGroup{}

	for _, chunk := range chunks {
		if volumeIds, ok := c.volumeIds[chunk.Fid.VolumeId]; ok {
			chunkMissingLocations := atomic.NewUint64(0)
			for _, volumeServer := range volumeIds {
				if *c.concurrency == 0 {
					if err := c.verifyChunk(volumeServer, chunk.Fid); err != nil {
						if isNeedleMissingError(err) {
							chunkMissingLocations.Add(1)
						}
						if !(*c.metadataFromLog && strings.HasSuffix(err.Error(), "not found")) {
							fmt.Fprintf(c.writer, "%s failed verify fileId %s: %+v, at volume server %v\n",
								fileMsg, chunk.GetFileIdString(), err, volumeServer)
						}
						if itemIsVerifed.Load() {
							itemIsVerifed.Store(false)
							errorCount.Add(1)
						}
					}
					continue
				}
				c.waitChanLock.RLock()
				waitChan, ok := c.waitChan[string(volumeServer)]
				c.waitChanLock.RUnlock()
				if !ok {
					fmt.Fprintf(c.writer, "%s failed to get channel for %s fileId: %s\n",
						string(volumeServer), fileMsg, chunk.GetFileIdString())
					if itemIsVerifed.Load() {
						itemIsVerifed.Store(false)
						errorCount.Add(1)
					}
					continue
				}
				wg.Add(1)
				itemWg.Add(1)
				waitChan <- struct{}{}
				go func(fChunk *filer_pb.FileChunk, path string, volumeServer pb.ServerAddress, msg string, chunkMissingLocations *atomic.Uint64) {
					defer wg.Done()
					defer itemWg.Done()
					if err := c.verifyChunk(volumeServer, fChunk.Fid); err != nil {
						if isNeedleMissingError(err) {
							chunkMissingLocations.Add(1)
						}
						if !(*c.metadataFromLog && strings.HasSuffix(err.Error(), "not found")) {
							fmt.Fprintf(c.writer, "%s failed verify fileId %s: %+v, at volume server %v\n",
								msg, fChunk.GetFileIdString(), err, volumeServer)
						}
						if itemIsVerifed.Load() {
							itemIsVerifed.Store(false)
							errorCount.Add(1)
						}
					}
					<-waitChan
				}(chunk, path, volumeServer, fileMsg, chunkMissingLocations)
			}
			itemWg.Wait()
			if chunkMissingLocations.Load() == uint64(len(volumeIds)) {
				hasMissingNeedlesFlag.Store(true)
			}
		} else {
			if !*c.metadataFromLog {
				err := fmt.Errorf("volumeId %d not found", chunk.Fid.VolumeId)
				fmt.Fprintf(c.writer, "%s failed verify fileId %s: %+v\n",
					fileMsg, chunk.GetFileIdString(), err)
			}
			if itemIsVerifed.Load() {
				itemIsVerifed.Store(false)
				errorCount.Add(1)
			}
			break
		}
	}
	return itemIsVerifed.Load(), hasMissingNeedlesFlag.Load()
}

// pruneEntry deletes an entry whose needles are gone, unless the entry
// changed since verification. Three guards protect a concurrent repair:
// re-lookup with mtime/md5 comparison, an identity check of the chunk set
// (an append or rewrite in the same second as the original mtime changes the
// chunks but not the timestamps), and IfNotModifiedAfter on the delete. The
// entry is only counted as pruned when a follow-up lookup confirms it is gone.
func (c *commandFsVerify) pruneEntry(path util.FullPath, mtimeSec int64, md5 []byte, expectedChunks []*filer_pb.FileChunk) (pruned bool, err error) {
	dir, name := path.DirAndName()
	expectedIds := chunkFingerprints(expectedChunks)
	lookupErr := c.env.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			if strings.Contains(err.Error(), "no entry is found in filer store") {
				return nil // already gone
			}
			return err
		}
		att := lookupResp.Entry.GetAttributes()
		if att.GetMtime() != mtimeSec {
			fmt.Fprintf(c.writer, "skip pruning %s: entry changed since verification\n", path)
			return nil
		}
		if len(md5) > 0 && !bytes.Equal(att.GetMd5(), md5) {
			fmt.Fprintf(c.writer, "skip pruning %s: entry changed since verification\n", path)
			return nil
		}
		if !chunksEqual(chunkFingerprints(lookupResp.Entry.GetChunks()), expectedIds) {
			fmt.Fprintf(c.writer, "skip pruning %s: entry changed since verification\n", path)
			return nil
		}
		deleteResp, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:          dir,
			Name:               name,
			IfNotModifiedAfter: mtimeSec,
		})
		if err != nil {
			return err
		}
		if deleteResp.Error != "" {
			return fmt.Errorf("delete entry %s: %s", path, deleteResp.Error)
		}
		// the filer silently skips the delete when the entry advanced past
		// IfNotModifiedAfter between our lookup and the delete; confirm
		confirm, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			if strings.Contains(err.Error(), "no entry is found in filer store") {
				pruned = true
				fmt.Fprintf(c.writer, "pruned entry with missing needles: %s\n", path)
				return nil
			}
			// the delete outcome is unknown — never count it as pruned
			fmt.Fprintf(c.writer, "skip pruning %s: delete not confirmed (%v)\n", path, err)
			return nil
		}
		if confirm.Entry != nil {
			fmt.Fprintf(c.writer, "skip pruning %s: entry changed since verification\n", path)
			return nil
		}
		pruned = true
		fmt.Fprintf(c.writer, "pruned entry with missing needles: %s\n", path)
		return nil
	})
	return pruned, lookupErr
}

// chunkFingerprints maps the file id of every chunk to a set, so two chunk
// lists count as identical when they reference the same needles.
func chunkFingerprints(chunks []*filer_pb.FileChunk) map[string]struct{} {
	ids := make(map[string]struct{}, len(chunks))
	for _, chunk := range chunks {
		if chunk == nil || chunk.Fid == nil {
			continue
		}
		ids[chunk.GetFileIdString()] = struct{}{}
	}
	return ids
}

func chunksEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

func (c *commandFsVerify) verifyTraverseBfs(path string) (fileCount uint64, errCount uint64, prunedCount uint64, err error) {
	timeNowAtSec := time.Now().Unix()
	return fileCount, errCount, prunedCount, doTraverseBfsAndSaving(c.env, c.writer, path, false, false,
		func(ctx context.Context, entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
			if c.modifyTimeAgoAtSec > 0 {
				if entry.Entry.Attributes != nil && c.modifyTimeAgoAtSec < timeNowAtSec-entry.Entry.Attributes.Mtime {
					return nil
				}
			}
			dataChunks, manifestChunks, resolveErr := filer.ResolveChunkManifest(context.Background(), filer.LookupFn(c.env), entry.Entry.GetChunks(), 0, math.MaxInt64, nil)
			if resolveErr != nil {
				return fmt.Errorf("failed to ResolveChunkManifest: %+v", resolveErr)
			}
			dataChunks = append(dataChunks, manifestChunks...)
			if len(dataChunks) > 0 {
				select {
				case outputChan <- &ItemEntry{
					chunks:    dataChunks,
					rawChunks: entry.Entry.GetChunks(),
					path:      util.NewFullPath(entry.Dir, entry.Entry.Name),
					mtimeSec:  entry.Entry.GetAttributes().GetMtime(),
					md5:       entry.Entry.GetAttributes().GetMd5(),
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		func(outputChan chan interface{}) error {
			var wg sync.WaitGroup
			itemErrCount := atomic.NewUint64(0)
			for itemEntry := range outputChan {
				i := itemEntry.(*ItemEntry)
				itemPath := string(i.path)
				if verified, hasMissingNeedles := c.verifyEntry(itemPath, i.chunks, itemErrCount, &wg); verified {
					if *c.verbose {
						fmt.Fprintf(c.writer, "file: %s needles:%d verified\n", itemPath, len(i.chunks))
					}
					fileCount++
				} else if *c.pruneEntries && hasMissingNeedles {
					pruned, pruneErr := c.pruneEntry(i.path, i.mtimeSec, i.md5, i.rawChunks)
					if pruneErr != nil {
						fmt.Fprintf(c.writer, "prune %s failed: %v\n", itemPath, pruneErr)
					} else if pruned {
						prunedCount++
					}
				}
			}
			wg.Wait()
			errCount = itemErrCount.Load()
			return nil
		})
}
