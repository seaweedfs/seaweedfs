package weed_server

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"path/filepath"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"time"
	"os"
)

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	found, entry, err := fs.filer.FindEntry(filer2.FullPath(filepath.Join(req.Directory, req.Name)))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%s not found under %s", req.Name, req.Directory)
	}

	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: entry.IsDirectory(),
			Chunks:      entry.Chunks,
		},
	}, nil
}

func (fs *FilerServer) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest) (*filer_pb.ListEntriesResponse, error) {

	entries, err := fs.filer.ListDirectoryEntries(filer2.FullPath(req.Directory), "", false, 1000)
	if err != nil {
		return nil, err
	}

	resp := &filer_pb.ListEntriesResponse{}
	for _, entry := range entries {

		glog.V(0).Infof("%s attr=%v size=%d", entry.Name(), entry.Attr, filer2.Chunks(entry.Chunks).TotalSize())
		resp.Entries = append(resp.Entries, &filer_pb.Entry{
			Name:        entry.Name(),
			IsDirectory: entry.IsDirectory(),
			Chunks:      entry.Chunks,
			Attributes: &filer_pb.FuseAttributes{
				FileSize: filer2.Chunks(entry.Chunks).TotalSize(),
				Mtime:    entry.Mtime.Unix(),
				Gid:      entry.Gid,
				Uid:      entry.Uid,
				FileMode: uint32(entry.Mode),
			},
		})
	}

	return resp, nil
}

func (fs *FilerServer) GetFileAttributes(ctx context.Context, req *filer_pb.GetFileAttributesRequest) (*filer_pb.GetFileAttributesResponse, error) {

	attributes := &filer_pb.FuseAttributes{}

	found, entry, err := fs.filer.FindEntry(filer2.NewFullPath(req.ParentDir, req.Name))
	if err != nil {
		return nil, err
	}
	if !found {
		attributes.FileSize = 0
	} else {
		attributes.FileSize = filer2.Chunks(entry.Chunks).TotalSize()
		attributes.FileMode = uint32(entry.Mode)
		attributes.Uid = entry.Uid
		attributes.Gid = entry.Gid
		attributes.Mtime = entry.Mtime.Unix()
	}

	return &filer_pb.GetFileAttributesResponse{
		Attributes: attributes,
	}, nil
}

func (fs *FilerServer) GetFileContent(ctx context.Context, req *filer_pb.GetFileContentRequest) (*filer_pb.GetFileContentResponse, error) {

	server, err := operation.LookupFileId(fs.getMasterNode(), req.FileId)
	if err != nil {
		return nil, err
	}
	content, err := util.Get(server)
	if err != nil {
		return nil, err
	}

	return &filer_pb.GetFileContentResponse{
		Content: content,
	}, nil
}

func (fs *FilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {
	err = fs.filer.CreateEntry(&filer2.Entry{
		FullPath: filer2.FullPath(filepath.Join(req.Directory, req.Entry.Name)),
		Attr: filer2.Attr{
			Mtime:  time.Unix(req.Entry.Attributes.Mtime, 0),
			Crtime: time.Unix(req.Entry.Attributes.Mtime, 0),
			Mode:   os.FileMode(req.Entry.Attributes.FileMode),
			Uid:    req.Entry.Attributes.Uid,
			Gid:    req.Entry.Attributes.Gid,
		},
	})

	if err == nil {
	}

	return &filer_pb.CreateEntryResponse{}, err
}

func (fs *FilerServer) AppendFileChunks(ctx context.Context, req *filer_pb.AppendFileChunksRequest) (*filer_pb.AppendFileChunksResponse, error) {
	err := fs.filer.AppendFileChunk(
		filer2.FullPath(filepath.Join(req.Directory, req.Entry.Name)),
		req.Entry.Chunks,
	)

	return &filer_pb.AppendFileChunksResponse{}, err
}

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {
	entry, err := fs.filer.DeleteEntry(filer2.FullPath(filepath.Join(req.Directory, req.Name)))
	if err == nil {
		for _, chunk := range entry.Chunks {
			if err = operation.DeleteFile(fs.getMasterNode(), chunk.FileId, fs.jwt(chunk.FileId)); err != nil {
				glog.V(0).Infof("deleting file %s: %v", chunk.FileId, err)
			}
		}
	}
	return &filer_pb.DeleteEntryResponse{}, err
}

func (fs *FilerServer) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (resp *filer_pb.AssignVolumeResponse, err error) {

	assignResult, err := operation.Assign(fs.master, &operation.VolumeAssignRequest{
		Count:       uint64(req.Count),
		Replication: req.Replication,
		Collection:  req.Collection,
	})
	if err != nil {
		return nil, fmt.Errorf("assign volume: %v", err)
	}
	if assignResult.Error != "" {
		return nil, fmt.Errorf("assign volume result: %v", assignResult.Error)
	}

	return &filer_pb.AssignVolumeResponse{
		FileId:    assignResult.Fid,
		Count:     int32(assignResult.Count),
		Url:       assignResult.Url,
		PublicUrl: assignResult.PublicUrl,
	}, err
}
