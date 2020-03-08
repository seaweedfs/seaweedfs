package filer_pb

import (
	"context"
	"fmt"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func toFileIdObject(fileIdStr string) (*FileId, error) {
	t, err := needle.ParseFileIdFromString(fileIdStr)
	if err != nil {
		return nil, err
	}
	return &FileId{
		VolumeId: uint32(t.VolumeId),
		Cookie:   uint32(t.Cookie),
		FileKey:  uint64(t.Key),
	}, nil

}

func (fid *FileId) toFileIdString() string {
	return needle.NewFileId(needle.VolumeId(fid.VolumeId), fid.FileKey, fid.Cookie).String()
}

func (c *FileChunk) GetFileIdString() string {
	if c.FileId != "" {
		return c.FileId
	}
	if c.Fid != nil {
		c.FileId = c.Fid.toFileIdString()
		return c.FileId
	}
	return ""
}

func BeforeEntrySerialization(chunks []*FileChunk) {

	for _, chunk := range chunks {

		if chunk.FileId != "" {
			if fid, err := toFileIdObject(chunk.FileId); err == nil {
				chunk.Fid = fid
				chunk.FileId = ""
			}
		}

		if chunk.SourceFileId != "" {
			if fid, err := toFileIdObject(chunk.SourceFileId); err == nil {
				chunk.SourceFid = fid
				chunk.SourceFileId = ""
			}
		}

	}
}

func AfterEntryDeserialization(chunks []*FileChunk) {

	for _, chunk := range chunks {

		if chunk.Fid != nil && chunk.FileId == "" {
			chunk.FileId = chunk.Fid.toFileIdString()
		}

		if chunk.SourceFid != nil && chunk.SourceFileId == "" {
			chunk.SourceFileId = chunk.SourceFid.toFileIdString()
		}

	}
}

func CreateEntry(client SeaweedFilerClient, request *CreateEntryRequest) error {
	resp, err := client.CreateEntry(context.Background(), request)
	if err != nil {
		glog.V(1).Infof("create entry %s/%s %v: %v", request.Directory, request.Entry.Name, request.OExcl, err)
		return fmt.Errorf("CreateEntry: %v", err)
	}
	if resp.Error != "" {
		glog.V(1).Infof("create entry %s/%s %v: %v", request.Directory, request.Entry.Name, request.OExcl, err)
		return fmt.Errorf("CreateEntry : %v", resp.Error)
	}
	return nil
}

func LookupEntry(client SeaweedFilerClient, request *LookupDirectoryEntryRequest) (*LookupDirectoryEntryResponse, error) {
	resp, err := filer_pb.LookupEntry(client, request)
	if err != nil {
		if err == filer2.ErrNotFound || strings.Contains(err.Error(), ErrNotFound.Error()) {
			return nil, filer2.ErrNotFound
		}
		glog.V(3).Infof("read %s/%v: %v", request.Directory, request.Entry.Name, err)
		return nil, fmt.Errorf("LookupEntry1: %v", err)
	}
	if resp.Error != "" && strings.Contains(resp.Error, ErrNotFound.Error()) {
		return nil, filer2.ErrNotFound
	}
	if resp.Error != "" {
		glog.V(3).Infof("lookup %s/%v: %v", request.Directory, request.Entry.Name, err)
		return nil, fmt.Errorf("LookupEntry2: %v", err)
	}
	if resp.Entry == nil {
		return nil, filer2.ErrNotFound
	}
	return resp, nil
}
