package filer_pb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/viant/ptrie"
	"google.golang.org/protobuf/proto"
)

const cutoffTimeNewEmptyDir = 3

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.GetChunks()) == 0 && entry.RemoteEntry != nil && entry.RemoteEntry.RemoteSize > 0
}

func (entry *Entry) IsDirectoryKeyObject() bool {
	return entry.IsDirectory && entry.Attributes != nil && entry.Attributes.Mime != ""
}

func (entry *Entry) GetExpiryTime() (expiryTime int64) {
	// For S3 objects with lifecycle expiration, use Mtime (modification time)
	// For regular TTL entries, use Crtime (creation time) for backward compatibility
	if entry.Extended != nil {
		if _, hasS3Expiry := entry.Extended[s3_constants.SeaweedFSExpiresS3]; hasS3Expiry {
			// S3 lifecycle expiration: base TTL on modification time
			expiryTime = entry.Attributes.Mtime
			if expiryTime == 0 {
				expiryTime = entry.Attributes.Crtime
			}
			expiryTime += int64(entry.Attributes.TtlSec)
			return expiryTime
		}
	}

	// Regular TTL expiration: base on creation time only
	expiryTime = entry.Attributes.Crtime + int64(entry.Attributes.TtlSec)
	return expiryTime
}

func (entry *Entry) IsExpired() bool {
	return entry != nil && entry.Attributes != nil && entry.Attributes.TtlSec > 0 &&
		time.Now().Unix() >= entry.GetExpiryTime()
}

func (entry *Entry) FileMode() (fileMode os.FileMode) {
	if entry != nil && entry.Attributes != nil {
		fileMode = os.FileMode(entry.Attributes.FileMode)
	}
	return
}

func (entry *Entry) IsOlderDir() bool {
	return entry.IsDirectory && entry.Attributes != nil && entry.Attributes.Mime == "" && entry.Attributes.GetCrtime() <= time.Now().Unix()-cutoffTimeNewEmptyDir
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

func CreateEntry(ctx context.Context, client SeaweedFilerClient, request *CreateEntryRequest) error {
	_, err := CreateEntryWithResponse(ctx, client, request)
	return err
}

func CreateEntryWithResponse(ctx context.Context, client SeaweedFilerClient, request *CreateEntryRequest) (*CreateEntryResponse, error) {
	resp, err := client.CreateEntry(ctx, request)
	if err != nil {
		glog.V(1).InfofCtx(ctx, "create entry %s/%s %v: %v", request.Directory, request.Entry.Name, request.OExcl, err)
		return nil, fmt.Errorf("CreateEntry: %w", err)
	}
	if resp.ErrorCode != FilerError_OK {
		glog.V(1).InfofCtx(ctx, "create entry %s/%s %v: %v (code %v)", request.Directory, request.Entry.Name, request.OExcl, resp.Error, resp.ErrorCode)
		if sentinel := FilerErrorToSentinel(resp.ErrorCode); sentinel != nil {
			return nil, fmt.Errorf("CreateEntry %s/%s: %w", request.Directory, request.Entry.Name, sentinel)
		}
		return nil, fmt.Errorf("CreateEntry: %w", errors.New(resp.Error))
	}
	if resp.Error != "" {
		glog.V(1).InfofCtx(ctx, "create entry %s/%s %v: %v", request.Directory, request.Entry.Name, request.OExcl, resp.Error)
		return nil, fmt.Errorf("CreateEntry: %w", errors.New(resp.Error))
	}
	return resp, nil
}

func UpdateEntry(ctx context.Context, client SeaweedFilerClient, request *UpdateEntryRequest) error {
	_, err := UpdateEntryWithResponse(ctx, client, request)
	return err
}

func UpdateEntryWithResponse(ctx context.Context, client SeaweedFilerClient, request *UpdateEntryRequest) (*UpdateEntryResponse, error) {
	resp, err := client.UpdateEntry(ctx, request)
	if err != nil {
		glog.V(1).InfofCtx(ctx, "update entry %s/%s :%v", request.Directory, request.Entry.Name, err)
		return nil, fmt.Errorf("UpdateEntry: %w", err)
	}
	return resp, nil
}

func LookupEntry(ctx context.Context, client SeaweedFilerClient, request *LookupDirectoryEntryRequest) (*LookupDirectoryEntryResponse, error) {
	resp, err := client.LookupDirectoryEntry(ctx, request)
	if err != nil {
		if err == ErrNotFound || strings.Contains(err.Error(), ErrNotFound.Error()) {
			return nil, ErrNotFound
		}
		glog.V(3).InfofCtx(ctx, "read %s/%v: %v", request.Directory, request.Name, err)
		return nil, fmt.Errorf("LookupEntry1: %w", err)
	}
	if resp.Entry == nil {
		return nil, ErrNotFound
	}
	return resp, nil
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")

// Sentinel errors for filer entry operations.
// These are set by the filer and reconstructed from FilerError codes after
// crossing the gRPC boundary, so consumers can use errors.Is() instead of
// parsing error strings.
var (
	ErrEntryNameTooLong    = errors.New("entry name too long")
	ErrParentIsFile        = errors.New("parent path is a file")
	ErrExistingIsDirectory = errors.New("existing entry is a directory")
	ErrExistingIsFile      = errors.New("existing entry is a file")
	ErrEntryAlreadyExists  = errors.New("entry already exists")
)

// FilerErrorToSentinel maps a proto FilerError code to its sentinel error.
// Returns nil for OK or unknown codes.
func FilerErrorToSentinel(code FilerError) error {
	switch code {
	case FilerError_ENTRY_NAME_TOO_LONG:
		return ErrEntryNameTooLong
	case FilerError_PARENT_IS_FILE:
		return ErrParentIsFile
	case FilerError_EXISTING_IS_DIRECTORY:
		return ErrExistingIsDirectory
	case FilerError_EXISTING_IS_FILE:
		return ErrExistingIsFile
	case FilerError_ENTRY_ALREADY_EXISTS:
		return ErrEntryAlreadyExists
	default:
		return nil
	}
}

func IsEmpty(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry == nil && event.EventNotification.OldEntry == nil
}

func IsCreate(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry != nil && event.EventNotification.OldEntry == nil
}

func IsUpdate(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry != nil &&
		event.EventNotification.OldEntry != nil &&
		event.Directory == event.EventNotification.NewParentPath &&
		event.EventNotification.NewEntry.Name == event.EventNotification.OldEntry.Name
}

func IsDelete(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry == nil && event.EventNotification.OldEntry != nil
}

func IsRename(event *SubscribeMetadataResponse) bool {
	return event.EventNotification.NewEntry != nil &&
		event.EventNotification.OldEntry != nil &&
		(event.Directory != event.EventNotification.NewParentPath ||
			event.EventNotification.NewEntry.Name != event.EventNotification.OldEntry.Name)
}

func MetadataEventSourceDirectory(event *SubscribeMetadataResponse) string {
	if event == nil {
		return ""
	}
	return event.Directory
}

func MetadataEventTargetDirectory(event *SubscribeMetadataResponse) string {
	if event == nil {
		return ""
	}
	if event.EventNotification != nil && event.EventNotification.NewParentPath != "" {
		return event.EventNotification.NewParentPath
	}
	return event.Directory
}

func metadataEventSourceEntryName(event *SubscribeMetadataResponse) string {
	if event == nil || event.EventNotification == nil {
		return ""
	}
	if event.EventNotification.OldEntry != nil {
		return event.EventNotification.OldEntry.Name
	}
	if event.EventNotification.NewEntry != nil {
		return event.EventNotification.NewEntry.Name
	}
	return ""
}

func metadataEventTargetEntryName(event *SubscribeMetadataResponse) string {
	if event == nil || event.EventNotification == nil {
		return ""
	}
	if event.EventNotification.NewEntry != nil {
		return event.EventNotification.NewEntry.Name
	}
	if event.EventNotification.OldEntry != nil {
		return event.EventNotification.OldEntry.Name
	}
	return ""
}

func MetadataEventSourceFullPath(event *SubscribeMetadataResponse) string {
	return util.Join(MetadataEventSourceDirectory(event), metadataEventSourceEntryName(event))
}

func MetadataEventTargetFullPath(event *SubscribeMetadataResponse) string {
	return util.Join(MetadataEventTargetDirectory(event), metadataEventTargetEntryName(event))
}

func MetadataEventTouchesDirectory(event *SubscribeMetadataResponse, dir string) bool {
	if MetadataEventSourceDirectory(event) == dir {
		return true
	}
	return event != nil &&
		event.EventNotification != nil &&
		event.EventNotification.NewEntry != nil &&
		MetadataEventTargetDirectory(event) == dir
}

func MetadataEventMatchesSubscription(event *SubscribeMetadataResponse, pathPrefix string, pathPrefixes []string, directories []string) bool {
	if event == nil {
		return false
	}

	if metadataEventMatchesPath(MetadataEventSourceFullPath(event), MetadataEventSourceDirectory(event), pathPrefix, pathPrefixes, directories) {
		return true
	}

	return event.EventNotification != nil &&
		event.EventNotification.NewEntry != nil &&
		metadataEventMatchesPath(MetadataEventTargetFullPath(event), MetadataEventTargetDirectory(event), pathPrefix, pathPrefixes, directories)
}

func metadataEventMatchesPath(fullPath, dirPath, pathPrefix string, pathPrefixes []string, directories []string) bool {
	if hasPrefixIn(fullPath, pathPrefixes) {
		return true
	}
	if matchByDirectory(dirPath, directories) {
		return true
	}
	return strings.HasPrefix(fullPath, pathPrefix)
}

func hasPrefixIn(text string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(text, p) {
			return true
		}
	}
	return false
}

func matchByDirectory(dirPath string, directories []string) bool {
	for _, dir := range directories {
		if dirPath == dir {
			return true
		}
	}
	return false
}

var _ = ptrie.KeyProvider(&FilerConf_PathConf{})

func (fp *FilerConf_PathConf) Key() interface{} {
	key, _ := proto.Marshal(fp)
	return string(key)
}
