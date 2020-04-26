package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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
		Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: entry.IsDirectory(),
			Attributes:  filer2.EntryAttributeToPb(entry),
			Chunks:      entry.Chunks,
			Extended:    entry.Extended,
		},
	}, nil
}

func (fs *FilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {

	glog.V(4).Infof("ListEntries %v", req)

	limit := int(req.Limit)
	if limit == 0 {
		limit = fs.option.DirListingLimit
	}

	paginationLimit := filer2.PaginationSize
	if limit < paginationLimit {
		paginationLimit = limit
	}

	lastFileName := req.StartFromFileName
	includeLastFile := req.InclusiveStartFrom
	for limit > 0 {
		entries, err := fs.filer.ListDirectoryEntries(stream.Context(), util.FullPath(req.Directory), lastFileName, includeLastFile, paginationLimit)

		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}

		includeLastFile = false

		for _, entry := range entries {

			lastFileName = entry.Name()

			if req.Prefix != "" {
				if !strings.HasPrefix(entry.Name(), req.Prefix) {
					continue
				}
			}

			if err := stream.Send(&filer_pb.ListEntriesResponse{
				Entry: &filer_pb.Entry{
					Name:        entry.Name(),
					IsDirectory: entry.IsDirectory(),
					Chunks:      entry.Chunks,
					Attributes:  filer2.EntryAttributeToPb(entry),
					Extended:    entry.Extended,
				},
			}); err != nil {
				return err
			}

			limit--
			if limit == 0 {
				return nil
			}
		}

		if len(entries) < paginationLimit {
			break
		}

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
				Url:       loc.Url,
				PublicUrl: loc.PublicUrl,
			})
		}
		resp.LocationsMap[vidString] = &filer_pb.Locations{
			Locations: locs,
		}
	}

	return resp, nil
}

func (fs *FilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {

	glog.V(4).Infof("CreateEntry %v", req)

	resp = &filer_pb.CreateEntryResponse{}

	chunks, garbages := filer2.CompactFileChunks(req.Entry.Chunks)

	if req.Entry.Attributes == nil {
		glog.V(3).Infof("CreateEntry %s: nil attributes", filepath.Join(req.Directory, req.Entry.Name))
		resp.Error = fmt.Sprintf("can not create entry with empty attributes")
		return
	}

	createErr := fs.filer.CreateEntry(ctx, &filer2.Entry{
		FullPath: util.JoinPath(req.Directory, req.Entry.Name),
		Attr:     filer2.PbToEntryAttribute(req.Entry.Attributes),
		Chunks:   chunks,
	}, req.OExcl)

	if createErr == nil {
		fs.filer.DeleteChunks(garbages)
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

	// remove old chunks if not included in the new ones
	unusedChunks := filer2.MinusChunks(entry.Chunks, req.Entry.Chunks)

	chunks, garbages := filer2.CompactFileChunks(req.Entry.Chunks)

	newEntry := &filer2.Entry{
		FullPath: util.JoinPath(req.Directory, req.Entry.Name),
		Attr:     entry.Attr,
		Extended: req.Entry.Extended,
		Chunks:   chunks,
	}

	glog.V(3).Infof("updating %s: %+v, chunks %d: %v => %+v, chunks %d: %v, extended: %v => %v",
		fullpath, entry.Attr, len(entry.Chunks), entry.Chunks,
		req.Entry.Attributes, len(req.Entry.Chunks), req.Entry.Chunks,
		entry.Extended, req.Entry.Extended)

	if req.Entry.Attributes != nil {
		if req.Entry.Attributes.Mtime != 0 {
			newEntry.Attr.Mtime = time.Unix(req.Entry.Attributes.Mtime, 0)
		}
		if req.Entry.Attributes.FileMode != 0 {
			newEntry.Attr.Mode = os.FileMode(req.Entry.Attributes.FileMode)
		}
		newEntry.Attr.Uid = req.Entry.Attributes.Uid
		newEntry.Attr.Gid = req.Entry.Attributes.Gid
		newEntry.Attr.Mime = req.Entry.Attributes.Mime
		newEntry.Attr.UserName = req.Entry.Attributes.UserName
		newEntry.Attr.GroupNames = req.Entry.Attributes.GroupName

	}

	if filer2.EqualEntry(entry, newEntry) {
		return &filer_pb.UpdateEntryResponse{}, err
	}

	if err = fs.filer.UpdateEntry(ctx, entry, newEntry); err == nil {
		fs.filer.DeleteChunks(unusedChunks)
		fs.filer.DeleteChunks(garbages)
	} else {
		glog.V(3).Infof("UpdateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), err)
	}

	fs.filer.NotifyUpdateEvent(entry, newEntry, true)

	return &filer_pb.UpdateEntryResponse{}, err
}

func (fs *FilerServer) AppendToEntry(ctx context.Context, req *filer_pb.AppendToEntryRequest) (*filer_pb.AppendToEntryResponse, error) {

	glog.V(4).Infof("AppendToEntry %v", req)

	fullpath := util.NewFullPath(req.Directory, req.EntryName)
	var offset int64 = 0
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullpath))
	if err == filer_pb.ErrNotFound {
		entry = &filer2.Entry{
			FullPath: fullpath,
			Attr: filer2.Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else {
		offset = int64(filer2.TotalSize(entry.Chunks))
	}

	for _, chunk := range req.Chunks {
		chunk.Offset = offset
		offset += int64(chunk.Size)
	}

	entry.Chunks = append(entry.Chunks, req.Chunks...)

	err = fs.filer.CreateEntry(context.Background(), entry, false)

	return &filer_pb.AppendToEntryResponse{}, err
}

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {

	glog.V(4).Infof("DeleteEntry %v", req)

	err = fs.filer.DeleteEntryMetaAndData(ctx, util.JoinPath(req.Directory, req.Name), req.IsRecursive, req.IgnoreRecursiveError, req.IsDeleteData)
	resp = &filer_pb.DeleteEntryResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

func (fs *FilerServer) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (resp *filer_pb.AssignVolumeResponse, err error) {

	ttlStr := ""
	if req.TtlSec > 0 {
		ttlStr = strconv.Itoa(int(req.TtlSec))
	}
	collection, replication, _ := fs.detectCollection(req.ParentPath, req.Collection, req.Replication)

	var altRequest *operation.VolumeAssignRequest

	dataCenter := req.DataCenter
	if dataCenter == "" {
		dataCenter = fs.option.DataCenter
	}

	assignRequest := &operation.VolumeAssignRequest{
		Count:       uint64(req.Count),
		Replication: replication,
		Collection:  collection,
		Ttl:         ttlStr,
		DataCenter:  dataCenter,
	}
	if dataCenter != "" {
		altRequest = &operation.VolumeAssignRequest{
			Count:       uint64(req.Count),
			Replication: replication,
			Collection:  collection,
			Ttl:         ttlStr,
			DataCenter:  "",
		}
	}
	assignResult, err := operation.Assign(fs.filer.GetMaster(), fs.grpcDialOption, assignRequest, altRequest)
	if err != nil {
		glog.V(3).Infof("AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}
	if assignResult.Error != "" {
		glog.V(3).Infof("AssignVolume error: %v", assignResult.Error)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume result: %v", assignResult.Error)}, nil
	}

	return &filer_pb.AssignVolumeResponse{
		FileId:      assignResult.Fid,
		Count:       int32(assignResult.Count),
		Url:         assignResult.Url,
		PublicUrl:   assignResult.PublicUrl,
		Auth:        string(assignResult.Auth),
		Collection:  collection,
		Replication: replication,
	}, nil
}

func (fs *FilerServer) DeleteCollection(ctx context.Context, req *filer_pb.DeleteCollectionRequest) (resp *filer_pb.DeleteCollectionResponse, err error) {

	glog.V(4).Infof("DeleteCollection %v", req)

	err = fs.filer.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: req.GetCollection(),
		})
		return err
	})

	return &filer_pb.DeleteCollectionResponse{}, err
}

func (fs *FilerServer) Statistics(ctx context.Context, req *filer_pb.StatisticsRequest) (resp *filer_pb.StatisticsResponse, err error) {

	var output *master_pb.StatisticsResponse

	err = fs.filer.MasterClient.WithClient(func(masterClient master_pb.SeaweedClient) error {
		grpcResponse, grpcErr := masterClient.Statistics(context.Background(), &master_pb.StatisticsRequest{
			Replication: req.Replication,
			Collection:  req.Collection,
			Ttl:         req.Ttl,
		})
		if grpcErr != nil {
			return grpcErr
		}

		output = grpcResponse
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &filer_pb.StatisticsResponse{
		TotalSize: output.TotalSize,
		UsedSize:  output.UsedSize,
		FileCount: output.FileCount,
	}, nil
}

func (fs *FilerServer) GetFilerConfiguration(ctx context.Context, req *filer_pb.GetFilerConfigurationRequest) (resp *filer_pb.GetFilerConfigurationResponse, err error) {

	t := &filer_pb.GetFilerConfigurationResponse{
		Masters:     fs.option.Masters,
		Collection:  fs.option.Collection,
		Replication: fs.option.DefaultReplication,
		MaxMb:       uint32(fs.option.MaxMB),
		DirBuckets:  fs.filer.DirBucketsPath,
		Cipher:      fs.filer.Cipher,
	}

	glog.V(4).Infof("GetFilerConfiguration: %v", t)

	return t, nil
}
