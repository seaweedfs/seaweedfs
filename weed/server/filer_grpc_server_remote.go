package weed_server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func (fs *FilerServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	// Use singleflight to deduplicate concurrent caching requests for the same object.
	// This benefits all clients: S3 API, filer HTTP, Hadoop, etc.
	cacheKey := req.Directory + "/" + req.Name

	// Detach from caller ctx: on failure the error path deletes every chunk
	// already written, so cancelling mid-download loses all progress. For
	// blobs large enough that the download outlasts the caller's timeout
	// the retry loop never converges.
	bgCtx := context.WithoutCancel(ctx)

	// DoChan (vs Do) so the caller can bail out on ctx.Done() while the
	// singleflight goroutine keeps caching on bgCtx; otherwise this handler
	// goroutine stays blocked for the full download after the client is gone.
	ch := fs.remoteCacheGroup.DoChan(cacheKey, func() (interface{}, error) {
		return fs.doCacheRemoteObjectToLocalCluster(bgCtx, req)
	})

	select {
	case <-ctx.Done():
		// Caller gave up; the detached cache keeps running and a later
		// request will find the entry cached (or join the same singleflight).
		return nil, ctx.Err()
	case res := <-ch:
		if res.Shared {
			glog.V(2).Infof("CacheRemoteObjectToLocalCluster: shared result for %s", cacheKey)
		}
		if res.Err != nil {
			return nil, res.Err
		}
		if res.Val == nil {
			return nil, fmt.Errorf("unexpected nil result from singleflight")
		}
		resp, ok := res.Val.(*filer_pb.CacheRemoteObjectToLocalClusterResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected result type from singleflight")
		}
		return resp, nil
	}
}

// doCacheRemoteObjectToLocalCluster performs the actual caching operation.
// This is called from singleflight, so only one instance runs per object.
func (fs *FilerServer) doCacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	// find the entry first to check if already cached
	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))
	if err == filer_pb.ErrNotFound {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("find entry %s/%s: %v", req.Directory, req.Name, err)
	}

	resp := &filer_pb.CacheRemoteObjectToLocalClusterResponse{}

	// Early return if not a remote-only object or already cached
	if entry.Remote == nil || entry.Remote.RemoteSize == 0 {
		resp.Entry = entry.ToProtoEntry()
		return resp, nil
	}
	if len(entry.GetChunks()) > 0 {
		// Already has local chunks - already cached
		glog.V(2).Infof("CacheRemoteObjectToLocalCluster: %s/%s already cached (%d chunks)", req.Directory, req.Name, len(entry.GetChunks()))
		resp.Entry = entry.ToProtoEntry()
		return resp, nil
	}

	glog.V(1).Infof("CacheRemoteObjectToLocalCluster: caching %s/%s (remote size: %d)", req.Directory, req.Name, entry.Remote.RemoteSize)

	// load all mappings
	mappingEntry, err := fs.filer.FindEntry(ctx, util.JoinPath(filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE))
	if err != nil {
		return nil, err
	}
	mappings, err := filer.UnmarshalRemoteStorageMappings(mappingEntry.Content)
	if err != nil {
		return nil, err
	}

	// find mapping
	var remoteStorageMountedLocation *remote_pb.RemoteStorageLocation
	var localMountedDir string
	for k, loc := range mappings.Mappings {
		if strings.HasPrefix(req.Directory, k) {
			localMountedDir, remoteStorageMountedLocation = k, loc
		}
	}
	if localMountedDir == "" {
		return nil, fmt.Errorf("%s is not mounted", req.Directory)
	}

	// find storage configuration
	storageConfEntry, err := fs.filer.FindEntry(ctx, util.JoinPath(filer.DirectoryEtcRemote, remoteStorageMountedLocation.Name+filer.REMOTE_STORAGE_CONF_SUFFIX))
	if err != nil {
		return nil, err
	}
	storageConf := &remote_pb.RemoteConf{}
	if unMarshalErr := proto.Unmarshal(storageConfEntry.Content, storageConf); unMarshalErr != nil {
		return nil, fmt.Errorf("unmarshal remote storage conf %s/%s: %v", filer.DirectoryEtcRemote, remoteStorageMountedLocation.Name+filer.REMOTE_STORAGE_CONF_SUFFIX, unMarshalErr)
	}

	// detect storage option
	so, err := fs.detectStorageOption(ctx, req.Directory, "", "", 0, "", "", "", "")
	if err != nil {
		return resp, err
	}
	assignRequest, altRequest := so.ToAssignRequests(1)

	// adaptive chunk size: target ~32 chunks per file to balance
	// per-chunk overhead (volume assign, gRPC, needle write) against parallelism
	chunkSize := int64(5 * 1024 * 1024) // 5MB floor
	maxChunkSize := int64(fs.option.MaxMB) * 1024 * 1024
	if maxChunkSize < chunkSize {
		maxChunkSize = chunkSize
	}
	targetChunks := int64(32)
	if entry.Remote.RemoteSize/targetChunks > chunkSize {
		chunkSize = entry.Remote.RemoteSize / targetChunks
		if chunkSize > maxChunkSize {
			chunkSize = maxChunkSize
		}
	}
	// final safety check: ensure no more than 1000 chunks
	if (entry.Remote.RemoteSize+chunkSize-1)/chunkSize > 1000 {
		chunkSize = (entry.Remote.RemoteSize + 999) / 1000
	}

	// Now that chunkSize is known, hint it to the master so per-chunk
	// assigns don't fall back to the 1 MB default estimate. Slightly over-
	// estimates for the final partial chunk (< chunkSize) by design.
	assignRequest.ExpectedDataSize = uint64(chunkSize)
	if altRequest != nil {
		altRequest.ExpectedDataSize = uint64(chunkSize)
	}

	dest := util.FullPath(remoteStorageMountedLocation.Path).Child(string(util.FullPath(req.Directory).Child(req.Name))[len(localMountedDir):])

	var chunks []*filer_pb.FileChunk
	var chunksMu sync.Mutex
	var fetchAndWriteErr error
	var wg sync.WaitGroup

	chunkConcurrency := int(req.ChunkConcurrency)
	if chunkConcurrency <= 0 {
		chunkConcurrency = 8
	} else if chunkConcurrency > 1024 {
		glog.V(0).Infof("capping chunkConcurrency from %d to 1024", chunkConcurrency)
		chunkConcurrency = 1024
	}
	downloadConcurrency := req.DownloadConcurrency
	if downloadConcurrency > 1024 {
		glog.V(0).Infof("capping downloadConcurrency from %d to 1024", downloadConcurrency)
		downloadConcurrency = 1024
	}

	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(chunkConcurrency)
	for offset := int64(0); offset < entry.Remote.RemoteSize; offset += chunkSize {
		localOffset := offset

		wg.Add(1)
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()
			size := chunkSize
			if localOffset+chunkSize > entry.Remote.RemoteSize {
				size = entry.Remote.RemoteSize - localOffset
			}

			// assign one volume server
			assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
			if err != nil {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = err
				}
				chunksMu.Unlock()
				return
			}
			if assignResult.Error != "" {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = fmt.Errorf("assign: %v", assignResult.Error)
				}
				chunksMu.Unlock()
				return
			}
			fileId, parseErr := needle.ParseFileIdFromString(assignResult.Fid)
			if parseErr != nil {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = fmt.Errorf("unrecognized file id %s: %v", assignResult.Fid, parseErr)
				}
				chunksMu.Unlock()
				return
			}

			var replicas []*volume_server_pb.FetchAndWriteNeedleRequest_Replica
			for _, r := range assignResult.Replicas {
				replicas = append(replicas, &volume_server_pb.FetchAndWriteNeedleRequest_Replica{
					Url:       r.Url,
					PublicUrl: r.PublicUrl,
					GrpcPort:  int32(r.GrpcPort),
				})
			}

			// tell filer to tell volume server to download into needles
			assignedServerAddress := pb.NewServerAddressWithGrpcPort(assignResult.Url, assignResult.GrpcPort)
			var etag string
			err = operation.WithVolumeServerClient(false, assignedServerAddress, fs.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, fetchErr := volumeServerClient.FetchAndWriteNeedle(context.Background(), &volume_server_pb.FetchAndWriteNeedleRequest{
					VolumeId:            uint32(fileId.VolumeId),
					NeedleId:            uint64(fileId.Key),
					Cookie:              uint32(fileId.Cookie),
					Offset:              localOffset,
					Size:                size,
					Replicas:            replicas,
					Auth:                string(assignResult.Auth),
					DownloadConcurrency: downloadConcurrency,
					RemoteConf:          storageConf,
					RemoteLocation: &remote_pb.RemoteStorageLocation{
						Name:   remoteStorageMountedLocation.Name,
						Bucket: remoteStorageMountedLocation.Bucket,
						Path:   string(dest),
					},
				})
				if fetchErr != nil {
					return fmt.Errorf("volume server %s fetchAndWrite %s: %v", assignResult.Url, dest, fetchErr)
				}
				etag = resp.ETag
				return nil
			})

			if err != nil {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = err
				}
				chunksMu.Unlock()
				return
			}

			chunk := &filer_pb.FileChunk{
				FileId:       assignResult.Fid,
				Offset:       localOffset,
				Size:         uint64(size),
				ModifiedTsNs: time.Now().UnixNano(),
				ETag:         etag,
				Fid: &filer_pb.FileId{
					VolumeId: uint32(fileId.VolumeId),
					FileKey:  uint64(fileId.Key),
					Cookie:   uint32(fileId.Cookie),
				},
			}
			chunksMu.Lock()
			chunks = append(chunks, chunk)
			chunksMu.Unlock()
		})
	}

	wg.Wait()

	chunksMu.Lock()
	err = fetchAndWriteErr
	// Sort chunks by offset to maintain file order
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Offset < chunks[j].Offset
	})
	chunksMu.Unlock()
	if err != nil {
		// Clean up any chunks that were successfully written before the error.
		// Without this, partial downloads leave orphaned needles in volume servers
		// that accumulate across retry cycles and cannot be reclaimed by vacuum.
		if len(chunks) > 0 {
			fs.filer.DeleteUncommittedChunks(ctx, chunks)
		}
		return nil, err
	}

	garbage := entry.GetChunks()

	newEntry := entry.ShallowClone()
	newEntry.Chunks = chunks
	newEntry.Remote = proto.Clone(entry.Remote).(*filer_pb.RemoteEntry)
	newEntry.Remote.LastLocalSyncTsNs = time.Now().UnixNano()

	// this skips meta data log events

	if err := fs.filer.Store.UpdateEntry(context.Background(), newEntry); err != nil {
		fs.filer.DeleteUncommittedChunks(ctx, chunks)
		return nil, err
	}
	fs.filer.DeleteChunks(ctx, entry.FullPath, garbage)

	ctx, eventSink := filer.WithMetadataEventSink(ctx)
	fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, false, nil)

	resp.Entry = newEntry.ToProtoEntry()
	resp.MetadataEvent = eventSink.Last()

	return resp, nil

}
