package weed_server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
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

	// find the entry
	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))
	if err == filer_pb.ErrNotFound {
		return nil, err
	}

	resp := &filer_pb.CacheRemoteObjectToLocalClusterResponse{}
	if entry.Remote == nil || entry.Remote.RemoteSize == 0 {
		return resp, nil
	}

	// detect storage option
	so, err := fs.detectStorageOption(req.Directory, "", "", 0, "", "", "", "")
	if err != nil {
		return resp, err
	}
	assignRequest, altRequest := so.ToAssignRequests(1)

	// find a good chunk size
	chunkSize := int64(5 * 1024 * 1024)
	chunkCount := entry.Remote.RemoteSize/chunkSize + 1
	for chunkCount > 1000 && chunkSize < int64(fs.option.MaxMB)*1024*1024/2 {
		chunkSize *= 2
		chunkCount = entry.Remote.RemoteSize/chunkSize + 1
	}

	dest := util.FullPath(remoteStorageMountedLocation.Path).Child(string(util.FullPath(req.Directory).Child(req.Name))[len(localMountedDir):])

	var chunks []*filer_pb.FileChunk
	var fetchAndWriteErr error
	var wg sync.WaitGroup

	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(8)
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
			assignResult, err := operation.Assign(fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
			if err != nil {
				fetchAndWriteErr = err
				return
			}
			if assignResult.Error != "" {
				fetchAndWriteErr = fmt.Errorf("assign: %v", assignResult.Error)
				return
			}
			fileId, parseErr := needle.ParseFileIdFromString(assignResult.Fid)
			if assignResult.Error != "" {
				fetchAndWriteErr = fmt.Errorf("unrecognized file id %s: %v", assignResult.Fid, parseErr)
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
				resp, fetchAndWriteErr := volumeServerClient.FetchAndWriteNeedle(context.Background(), &volume_server_pb.FetchAndWriteNeedleRequest{
					VolumeId:   uint32(fileId.VolumeId),
					NeedleId:   uint64(fileId.Key),
					Cookie:     uint32(fileId.Cookie),
					Offset:     localOffset,
					Size:       size,
					Replicas:   replicas,
					Auth:       string(assignResult.Auth),
					RemoteConf: storageConf,
					RemoteLocation: &remote_pb.RemoteStorageLocation{
						Name:   remoteStorageMountedLocation.Name,
						Bucket: remoteStorageMountedLocation.Bucket,
						Path:   string(dest),
					},
				})
				if fetchAndWriteErr != nil {
					return fmt.Errorf("volume server %s fetchAndWrite %s: %v", assignResult.Url, dest, fetchAndWriteErr)
				} else {
					etag = resp.ETag
				}
				return nil
			})

			if err != nil && fetchAndWriteErr == nil {
				fetchAndWriteErr = err
				return
			}

			chunks = append(chunks, &filer_pb.FileChunk{

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
			})
		})
	}

	wg.Wait()
	if fetchAndWriteErr != nil {
		return nil, fetchAndWriteErr
	}

	garbage := entry.GetChunks()

	newEntry := entry.ShallowClone()
	newEntry.Chunks = chunks
	newEntry.Remote = proto.Clone(entry.Remote).(*filer_pb.RemoteEntry)
	newEntry.Remote.LastLocalSyncTsNs = time.Now().UnixNano()

	// this skips meta data log events

	if err := fs.filer.Store.UpdateEntry(context.Background(), newEntry); err != nil {
		fs.filer.DeleteUncommittedChunks(chunks)
		return nil, err
	}
	fs.filer.DeleteChunks(entry.FullPath, garbage)

	fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, false, nil)

	resp.Entry = newEntry.ToProtoEntry()

	return resp, nil

}
