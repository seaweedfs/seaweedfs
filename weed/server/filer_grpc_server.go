package weed_server

import (
	"context"
	"strconv"

	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	found, fileId, err := fs.filer.LookupDirectoryEntry(req.Directory, req.Name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fuse.ENOENT
	}

	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: fileId == "",
			FileId:      fileId,
		},
	}, nil
}

func (fs *FilerServer) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest) (*filer_pb.ListEntriesResponse, error) {

	directoryNames, err := fs.filer.ListDirectories(req.Directory)
	if err != nil {
		return nil, err
	}
	files, err := fs.filer.ListFiles(req.Directory, "", 1000)
	if err != nil {
		return nil, err
	}

	resp := &filer_pb.ListEntriesResponse{}
	for _, dir := range directoryNames {
		resp.Entries = append(resp.Entries, &filer_pb.Entry{
			Name:        string(dir),
			IsDirectory: true,
		})
	}
	for _, fileEntry := range files {
		resp.Entries = append(resp.Entries, &filer_pb.Entry{
			Name:        fileEntry.Name,
			IsDirectory: false,
			FileId:      string(fileEntry.Id),
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
	if req.IsDirectory {
		err = fs.filer.DeleteDirectory(req.Directory+req.Name, false)
	} else {
		fid, err := fs.filer.DeleteFile(req.Directory + req.Name)
		if err == nil && fid != "" {
			err = operation.DeleteFile(fs.getMasterNode(), fid, fs.jwt(fid))
		}
	}
	return nil, err
}
