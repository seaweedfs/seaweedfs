package weed_server

import (
	"context"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"path/filepath"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	found, entry, err := fs.filer.FindEntry(filer2.FullPath(filepath.Join(req.Directory, req.Name)))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%s not found under %s", req.Name, req.Directory)
	}

	var fileId string
	if !entry.IsDirectory() && len(entry.Chunks) > 0 {
		fileId = string(entry.Chunks[0].Fid)
	}

	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: entry.IsDirectory(),
			FileId:      fileId,
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
		var fileId string
		if !entry.IsDirectory() && len(entry.Chunks) > 0 {
			fileId = string(entry.Chunks[0].Fid)
		}
		resp.Entries = append(resp.Entries, &filer_pb.Entry{
			Name:        entry.Name(),
			IsDirectory: entry.IsDirectory(),
			FileId:      fileId,
		})
	}

	return resp, nil
}

func (fs *FilerServer) GetFileAttributes(ctx context.Context, req *filer_pb.GetFileAttributesRequest) (*filer_pb.GetFileAttributesResponse, error) {

	attributes := &filer_pb.FuseAttributes{}

	server, err := operation.LookupFileId(fs.getMasterNode(), req.FileId)
	if err != nil {
		return nil, err
	}
	head, err := util.Head(server)
	if err != nil {
		return nil, err
	}
	attributes.FileSize, err = strconv.ParseUint(head.Get("Content-Length"), 10, 0)
	if err != nil {
		return nil, err
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

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {
	entry, err := fs.filer.DeleteEntry(filer2.FullPath(filepath.Join(req.Directory, req.Name)))
	if err == nil {
		for _, chunk := range entry.Chunks {
			fid := string(chunk.Fid)
			if err = operation.DeleteFile(fs.getMasterNode(), fid, fs.jwt(fid)); err != nil {
				glog.V(0).Infof("deleting file %s: %v", fid, err)
			}
		}
	}
	return &filer_pb.DeleteEntryResponse{}, err
}
