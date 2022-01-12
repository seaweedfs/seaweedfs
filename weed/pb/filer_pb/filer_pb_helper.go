package filer_pb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/golang/protobuf/proto"
	"github.com/viant/ptrie"
)

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.Chunks) == 0 && entry.RemoteEntry != nil && entry.RemoteEntry.RemoteSize > 0
}

func (entry *Entry) FileMode() (fileMode os.FileMode) {
	if entry != nil && entry.Attributes != nil {
		fileMode = os.FileMode(entry.Attributes.FileMode)
	}
	return
}

func ToFileIdObject(fileIdStr string) (*FileId, error) {
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
			if fid, err := ToFileIdObject(chunk.FileId); err == nil {
				chunk.Fid = fid
				chunk.FileId = ""
			}
		}

		if chunk.SourceFileId != "" {
			if fid, err := ToFileIdObject(chunk.SourceFileId); err == nil {
				chunk.SourceFid = fid
				chunk.SourceFileId = ""
			}
		}

	}
}

func EnsureFid(chunk *FileChunk) {
	if chunk.Fid != nil {
		return
	}
	if fid, err := ToFileIdObject(chunk.FileId); err == nil {
		chunk.Fid = fid
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
		glog.V(1).Infof("create entry %s/%s %v: %v", request.Directory, request.Entry.Name, request.OExcl, resp.Error)
		return fmt.Errorf("CreateEntry : %v", resp.Error)
	}
	return nil
}

func UpdateEntry(client SeaweedFilerClient, request *UpdateEntryRequest) error {
	_, err := client.UpdateEntry(context.Background(), request)
	if err != nil {
		glog.V(1).Infof("update entry %s/%s :%v", request.Directory, request.Entry.Name, err)
		return fmt.Errorf("UpdateEntry: %v", err)
	}
	return nil
}

func LookupEntry(client SeaweedFilerClient, request *LookupDirectoryEntryRequest) (*LookupDirectoryEntryResponse, error) {
	resp, err := client.LookupDirectoryEntry(context.Background(), request)
	if err != nil {
		if err == ErrNotFound || strings.Contains(err.Error(), ErrNotFound.Error()) {
			return nil, ErrNotFound
		}
		glog.V(3).Infof("read %s/%v: %v", request.Directory, request.Name, err)
		return nil, fmt.Errorf("LookupEntry1: %v", err)
	}
	if resp.Entry == nil {
		return nil, ErrNotFound
	}
	return resp, nil
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")

func IsCreate(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry != nil && event.EventNotification.OldEntry == nil
}
func IsUpdate(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry != nil &&
		event.EventNotification.OldEntry != nil &&
		event.Directory == event.EventNotification.NewParentPath
}
func IsDelete(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry == nil && event.EventNotification.OldEntry != nil
}
func IsRename(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry != nil &&
		event.EventNotification.OldEntry != nil &&
		event.Directory != event.EventNotification.NewParentPath
}

var _ = ptrie.KeyProvider(&FilerConf_PathConf{})

func (fp *FilerConf_PathConf) Key() interface{} {
	key, _ := proto.Marshal(fp)
	return string(key)
}
