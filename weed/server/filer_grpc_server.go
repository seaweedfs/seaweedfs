package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	glog.V(4).Infof("LookupDirectoryEntry %s", filepath.Join(req.Directory, req.Name))

	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))
	if err == filer_pb.ErrNotFound {
		return &filer_pb.LookupDirectoryEntryResponse{}, err
	}
	if err != nil {
		glog.V(3).Infof("LookupDirectoryEntry %s: %+v, ", filepath.Join(req.Directory, req.Name), err)
		return nil, err
	}

	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: entry.ToProtoEntry(),
	}, nil
}

func (fs *FilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) (err error) {

	glog.V(4).Infof("ListEntries %v", req)

	limit := int(req.Limit)
	if limit == 0 {
		limit = fs.option.DirListingLimit
	}

	paginationLimit := filer.PaginationSize
	if limit < paginationLimit {
		paginationLimit = limit
	}

	lastFileName := req.StartFromFileName
	includeLastFile := req.InclusiveStartFrom
	var listErr error
	for limit > 0 {
		var hasEntries bool
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(stream.Context(), util.FullPath(req.Directory), lastFileName, includeLastFile, int64(paginationLimit), req.Prefix, "", "", func(entry *filer.Entry) bool {
			hasEntries = true
			if err = stream.Send(&filer_pb.ListEntriesResponse{
				Entry: entry.ToProtoEntry(),
			}); err != nil {
				return false
			}

			limit--
			if limit == 0 {
				return false
			}
			return true
		})

		if listErr != nil {
			return listErr
		}
		if err != nil {
			return err
		}
		if !hasEntries {
			return nil
		}

		includeLastFile = false

	}

	return nil
}

func (fs *FilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {

	resp := &filer_pb.LookupVolumeResponse{
		LocationsMap: make(map[string]*filer_pb.Locations),
	}

	for _, vidString := range req.VolumeIds {
		vid, err := strconv.Atoi(vidString)
		if err != nil {
			glog.V(1).Infof("Unknown volume id %d", vid)
			return nil, err
		}
		var locs []*filer_pb.Location
		locations, found := fs.filer.MasterClient.GetLocations(uint32(vid))
		if !found {
			continue
		}
		for _, loc := range locations {
			locs = append(locs, &filer_pb.Location{
				Url:        loc.Url,
				PublicUrl:  loc.PublicUrl,
				GrpcPort:   uint32(loc.GrpcPort),
				DataCenter: loc.DataCenter,
			})
		}
		resp.LocationsMap[vidString] = &filer_pb.Locations{
			Locations: locs,
		}
	}

	return resp, nil
}

func (fs *FilerServer) lookupFileId(ctx context.Context, fileId string) (targetUrls []string, err error) {
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return nil, err
	}
	locations, found := fs.filer.MasterClient.GetLocations(uint32(fid.VolumeId))
	if !found || len(locations) == 0 {
		return nil, fmt.Errorf("not found volume %d in %s", fid.VolumeId, fileId)
	}
	for _, loc := range locations {
		targetUrls = append(targetUrls, fmt.Sprintf("http://%s/%s", loc.Url, fileId))
	}
	return
}

func (fs *FilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {

	glog.V(4).Infof("CreateEntry %v/%v", req.Directory, req.Entry.Name)

	resp = &filer_pb.CreateEntryResponse{}

	chunks, garbage, err2 := fs.cleanupChunks(ctx, util.Join(req.Directory, req.Entry.Name), nil, req.Entry)
	if err2 != nil {
		return &filer_pb.CreateEntryResponse{}, fmt.Errorf("CreateEntry cleanupChunks %s %s: %v", req.Directory, req.Entry.Name, err2)
	}

	so, err := fs.detectStorageOption(ctx, string(util.NewFullPath(req.Directory, req.Entry.Name)), "", "", 0, "", "", "", "")
	if err != nil {
		return nil, err
	}
	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks
	newEntry.TtlSec = so.TtlSeconds

	createErr := fs.filer.CreateEntry(ctx, newEntry, req.OExcl, req.IsFromOtherCluster, req.Signatures, req.SkipCheckParentDirectory, so.MaxFileNameLength)

	if createErr == nil {
		fs.filer.DeleteChunksNotRecursive(garbage)
	} else {
		glog.V(3).Infof("CreateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), createErr)
		resp.Error = createErr.Error()
	}

	return
}

func (fs *FilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {

	glog.V(4).Infof("UpdateEntry %v", req)

	fullpath := util.Join(req.Directory, req.Entry.Name)
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullpath))
	if err != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("not found %s: %v", fullpath, err)
	}

	chunks, garbage, err2 := fs.cleanupChunks(ctx, fullpath, entry, req.Entry)
	if err2 != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("UpdateEntry cleanupChunks %s: %v", fullpath, err2)
	}

	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks

	if filer.EqualEntry(entry, newEntry) {
		return &filer_pb.UpdateEntryResponse{}, err
	}

	if err = fs.filer.UpdateEntry(ctx, entry, newEntry); err == nil {
		fs.filer.DeleteChunksNotRecursive(garbage)

		fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, req.IsFromOtherCluster, req.Signatures)

	} else {
		glog.V(3).Infof("UpdateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), err)
	}

	return &filer_pb.UpdateEntryResponse{}, err
}

func (fs *FilerServer) cleanupChunks(ctx context.Context, fullpath string, existingEntry *filer.Entry, newEntry *filer_pb.Entry) (chunks, garbage []*filer_pb.FileChunk, err error) {

	// remove old chunks if not included in the new ones
	if existingEntry != nil {
		garbage, err = filer.MinusChunks(ctx, fs.lookupFileId, existingEntry.GetChunks(), newEntry.GetChunks())
		if err != nil {
			return newEntry.GetChunks(), nil, fmt.Errorf("MinusChunks: %v", err)
		}
	}

	// files with manifest chunks are usually large and append only, skip calculating covered chunks
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(newEntry.GetChunks())

	chunks, coveredChunks := filer.CompactFileChunks(ctx, fs.lookupFileId, nonManifestChunks)
	garbage = append(garbage, coveredChunks...)

	if newEntry.Attributes != nil {
		so, _ := fs.detectStorageOption(ctx, fullpath,
			"",
			"",
			newEntry.Attributes.TtlSec,
			"",
			"",
			"",
			"",
		) // ignore readonly error for capacity needed to manifestize
		chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), chunks)
		if err != nil {
			// not good, but should be ok
			glog.V(0).Infof("MaybeManifestize: %v", err)
		}
	}

	chunks = append(manifestChunks, chunks...)

	return
}

func (fs *FilerServer) AppendToEntry(ctx context.Context, req *filer_pb.AppendToEntryRequest) (*filer_pb.AppendToEntryResponse, error) {

	glog.V(4).Infof("AppendToEntry %v", req)
	fullpath := util.NewFullPath(req.Directory, req.EntryName)

	lockClient := cluster.NewLockClient(fs.grpcDialOption, fs.option.Host)
	lock := lockClient.NewShortLivedLock(string(fullpath), string(fs.option.Host))
	defer lock.StopShortLivedLock()

	var offset int64 = 0
	entry, err := fs.filer.FindEntry(ctx, fullpath)
	if err == filer_pb.ErrNotFound {
		entry = &filer.Entry{
			FullPath: fullpath,
			Attr: filer.Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))
	}

	for _, chunk := range req.Chunks {
		chunk.Offset = offset
		offset += int64(chunk.Size)
	}

	entry.Chunks = append(entry.GetChunks(), req.Chunks...)
	so, err := fs.detectStorageOption(ctx, string(fullpath), "", "", entry.TtlSec, "", "", "", "")
	if err != nil {
		glog.Warningf("detectStorageOption: %v", err)
		return &filer_pb.AppendToEntryResponse{}, err
	}
	entry.Chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), entry.GetChunks())
	if err != nil {
		// not good, but should be ok
		glog.V(0).Infof("MaybeManifestize: %v", err)
	}

	err = fs.filer.CreateEntry(context.Background(), entry, false, false, nil, false, fs.filer.MaxFilenameLength)

	return &filer_pb.AppendToEntryResponse{}, err
}

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {

	glog.V(4).Infof("DeleteEntry %v", req)

	err = fs.filer.DeleteEntryMetaAndData(ctx, util.JoinPath(req.Directory, req.Name), req.IsRecursive, req.IgnoreRecursiveError, req.IsDeleteData, req.IsFromOtherCluster, req.Signatures, req.IfNotModifiedAfter)
	resp = &filer_pb.DeleteEntryResponse{}
	if err != nil && err != filer_pb.ErrNotFound {
		resp.Error = err.Error()
	}
	return resp, nil
}

func (fs *FilerServer) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (resp *filer_pb.AssignVolumeResponse, err error) {

	if req.DiskType == "" {
		req.DiskType = fs.option.DiskType
	}

	so, err := fs.detectStorageOption(ctx, req.Path, req.Collection, req.Replication, req.TtlSec, req.DiskType, req.DataCenter, req.Rack, req.DataNode)
	if err != nil {
		glog.V(3).Infof("AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}

	assignRequest, altRequest := so.ToAssignRequests(int(req.Count))

	assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
	if err != nil {
		glog.V(3).Infof("AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}
	if assignResult.Error != "" {
		glog.V(3).Infof("AssignVolume error: %v", assignResult.Error)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume result: %v", assignResult.Error)}, nil
	}

	return &filer_pb.AssignVolumeResponse{
		FileId: assignResult.Fid,
		Count:  int32(assignResult.Count),
		Location: &filer_pb.Location{
			Url:       assignResult.Url,
			PublicUrl: assignResult.PublicUrl,
			GrpcPort:  uint32(assignResult.GrpcPort),
		},
		Auth:        string(assignResult.Auth),
		Collection:  so.Collection,
		Replication: so.Replication,
	}, nil
}

func (fs *FilerServer) CollectionList(ctx context.Context, req *filer_pb.CollectionListRequest) (resp *filer_pb.CollectionListResponse, err error) {

	glog.V(4).Infof("CollectionList %v", req)
	resp = &filer_pb.CollectionListResponse{}

	err = fs.filer.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		masterResp, err := client.CollectionList(context.Background(), &master_pb.CollectionListRequest{
			IncludeNormalVolumes: req.IncludeNormalVolumes,
			IncludeEcVolumes:     req.IncludeEcVolumes,
		})
		if err != nil {
			return err
		}
		for _, c := range masterResp.Collections {
			resp.Collections = append(resp.Collections, &filer_pb.Collection{Name: c.Name})
		}
		return nil
	})

	return
}

func (fs *FilerServer) DeleteCollection(ctx context.Context, req *filer_pb.DeleteCollectionRequest) (resp *filer_pb.DeleteCollectionResponse, err error) {

	glog.V(4).Infof("DeleteCollection %v", req)

	err = fs.filer.DoDeleteCollection(req.GetCollection())

	return &filer_pb.DeleteCollectionResponse{}, err
}
