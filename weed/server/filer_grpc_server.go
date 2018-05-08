package weed_server

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strconv"
)

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer.LookupDirectoryEntryRequest) (*filer.LookupDirectoryEntryResponse, error) {

	found, fileId, err := fs.filer.LookupDirectoryEntry(req.Directory, req.Name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fuse.ENOENT
	}

	return &filer.LookupDirectoryEntryResponse{
		Entry: &filer.Entry{
			Name:        req.Name,
			IsDirectory: fileId == "",
			FileId:      fileId,
		},
	}, nil
}

func (fs *FilerServer) ListEntries(ctx context.Context, req *filer.ListEntriesRequest) (*filer.ListEntriesResponse, error) {

	directoryNames, err := fs.filer.ListDirectories(req.Directory)
	if err != nil {
		return nil, err
	}
	files, err := fs.filer.ListFiles(req.Directory, "", 1000)
	if err != nil {
		return nil, err
	}

	resp := &filer.ListEntriesResponse{}
	for _, dir := range directoryNames {
		resp.Entries = append(resp.Entries, &filer.Entry{
			Name:        string(dir),
			IsDirectory: true,
		})
	}
	for _, fileEntry := range files {
		resp.Entries = append(resp.Entries, &filer.Entry{
			Name:        fileEntry.Name,
			IsDirectory: false,
			FileId:      string(fileEntry.Id),
		})
	}

	return resp, nil
}

func (fs *FilerServer) GetFileAttributes(ctx context.Context, req *filer.GetFileAttributesRequest) (*filer.GetFileAttributesResponse, error) {

	attributes := &filer.FuseAttributes{}

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

	return &filer.GetFileAttributesResponse{
		Attributes: attributes,
	}, nil
}

func (fs *FilerServer) GetFileContent(ctx context.Context, req *filer.GetFileContentRequest) (*filer.GetFileContentResponse, error) {

	server, err := operation.LookupFileId(fs.getMasterNode(), req.FileId)
	if err != nil {
		return nil, err
	}
	content, err := util.Get(server)
	if err != nil {
		return nil, err
	}

	return &filer.GetFileContentResponse{
		Content: content,
	}, nil
}

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer.DeleteEntryRequest) (resp *filer.DeleteEntryResponse, err error) {
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
