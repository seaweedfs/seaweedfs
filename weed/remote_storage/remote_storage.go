package remote_storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

const slash = "/"

func ParseLocationName(remote string) (locationName string) {
	remote = strings.TrimSuffix(remote, slash)
	parts := strings.SplitN(remote, slash, 2)
	if len(parts) >= 1 {
		return parts[0]
	}
	return
}

// Describes reports whether a stored remote entry's stamp still describes the
// live remote object: same size, and equal ETags when both are known.
func Describes(stamp, object *filer_pb.RemoteEntry) bool {
	if stamp == nil || object == nil || stamp.RemoteSize != object.RemoteSize {
		return false
	}
	return stamp.RemoteETag == "" || object.RemoteETag == "" || stamp.RemoteETag == object.RemoteETag
}

// StillCurrent reports whether live can still be the object the stored stamp
// described: not newer than the stamp, matching it on size, and not having
// dropped an ETag the stamp carried.
func StillCurrent(stamp, live *filer_pb.RemoteEntry) bool {
	if live != nil && live.RemoteMtime > stamp.GetRemoteMtime() {
		return false
	}
	if !Describes(stamp, live) {
		return false
	}
	return stamp.GetRemoteETag() == "" || live.RemoteETag != ""
}

func parseBucketLocation(remote string) (loc *remote_pb.RemoteStorageLocation) {
	loc = &remote_pb.RemoteStorageLocation{}
	remote = strings.TrimSuffix(remote, slash)
	parts := strings.SplitN(remote, slash, 3)
	if len(parts) >= 1 {
		loc.Name = parts[0]
	}
	if len(parts) >= 2 {
		loc.Bucket = parts[1]
	}
	loc.Path = remote[len(loc.Name)+1+len(loc.Bucket):]
	if loc.Path == "" {
		loc.Path = slash
	}
	return
}

func parseNoBucketLocation(remote string) (loc *remote_pb.RemoteStorageLocation) {
	loc = &remote_pb.RemoteStorageLocation{}
	remote = strings.TrimSuffix(remote, slash)
	parts := strings.SplitN(remote, slash, 2)
	if len(parts) >= 1 {
		loc.Name = parts[0]
	}
	loc.Path = remote[len(loc.Name):]
	if loc.Path == "" {
		loc.Path = slash
	}
	return
}

func FormatLocation(loc *remote_pb.RemoteStorageLocation) string {
	if loc.Bucket == "" {
		return fmt.Sprintf("%s%s", loc.Name, loc.Path)
	}
	return fmt.Sprintf("%s/%s%s", loc.Name, loc.Bucket, loc.Path)
}

type VisitFunc func(dir string, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error

// EntryContentEncoding returns the Content-Encoding stored in the entry
// extended attributes, for clients to set on uploaded remote objects.
func EntryContentEncoding(entry *filer_pb.Entry) string {
	if entry == nil {
		return ""
	}
	return string(entry.Extended["Content-Encoding"])
}

type Bucket struct {
	Name      string
	CreatedAt time.Time
}

// ErrRemoteObjectNotFound is returned by StatFile and the ReadFile variants when the object does not exist in the remote storage backend.
var ErrRemoteObjectNotFound = errors.New("remote object not found")

type RemoteStorageClient interface {
	Traverse(loc *remote_pb.RemoteStorageLocation, visitFn VisitFunc) error
	ListDirectory(ctx context.Context, loc *remote_pb.RemoteStorageLocation, visitFn VisitFunc) error
	StatFile(loc *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error)
	ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error)
	WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error)
	RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error)
	WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error)
	// UpdateFileMetadata returns the stamp of the object the update produced —
	// a backend that rewrites the object to change its metadata can give it a
	// new ETag or mtime — or nil when the object was not rewritten. Callers
	// restamp their entries from the result and keep the previous stamp on nil.
	UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (remoteEntry *filer_pb.RemoteEntry, err error)
	DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error)
	ListBuckets() ([]*Bucket, error)
	CreateBucket(name string) (err error)
	DeleteBucket(name string) (err error)
}

// RemoteStorageConcurrentReader is an optional interface for remote storage clients
// that support configurable download concurrency for multipart downloads.
type RemoteStorageConcurrentReader interface {
	ReadFileWithConcurrency(loc *remote_pb.RemoteStorageLocation, offset int64, size int64, concurrency int) (data []byte, err error)
}

// RemoteStorageStreamReader is an optional interface for remote storage clients
// that support streaming reads with io.Reader for efficient memory usage.
type RemoteStorageStreamReader interface {
	ReadFileAsStream(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (reader io.ReadCloser, err error)
}

// RemoteStorageObjectCopier is an optional interface for remote storage clients
// that copy an object to another key within the storage, without the content
// passing through the client. A rename of an entry whose content exists only
// on the remote needs it.
type RemoteStorageObjectCopier interface {
	CopyFile(src *remote_pb.RemoteStorageLocation, dst *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error)
}

// RemoteStorageConditionalDeleter is an optional interface for remote storage
// clients whose backend can delete an object only while it is still the one a
// stat described, enforced with a backend version precondition (S3 and Azure:
// ETag, GCS: generation). deleted is false without an error when the object
// differs from stamp or changed between the stat and the delete.
type RemoteStorageConditionalDeleter interface {
	DeleteFileIfUnchanged(loc *remote_pb.RemoteStorageLocation, stamp *filer_pb.RemoteEntry) (deleted bool, err error)
}

// CacheWaitTimeout is how long a read of an uncached remote-only object waits
// for the local cache before serving another way: small files wait longer since
// their cache completes quickly, large files fail fast for better TTFB. A mount
// carrying cache_wait_ms replaces the size tiers, and 0 means never wait.
func CacheWaitTimeout(remoteSize int64, mountedLocation *remote_pb.RemoteStorageLocation) time.Duration {
	if mountedLocation != nil && mountedLocation.CacheWaitMs != nil {
		return max(0, time.Duration(*mountedLocation.CacheWaitMs)*time.Millisecond)
	}
	switch {
	case remoteSize > 500*1024*1024:
		return 2 * time.Second
	case remoteSize > 0 && remoteSize < 50*1024*1024:
		return 10 * time.Second
	default:
		return 5 * time.Second
	}
}

type RemoteStorageClientMaker interface {
	Make(remoteConf *remote_pb.RemoteConf) (RemoteStorageClient, error)
	HasBucket() bool
}

var RemoteStorageClientMakers = make(map[string]RemoteStorageClientMaker)

func GetAllRemoteStorageNames() string {
	var storageNames []string
	for k := range RemoteStorageClientMakers {
		storageNames = append(storageNames, k)
	}
	sort.Strings(storageNames)
	return strings.Join(storageNames, "|")
}

func ParseRemoteLocation(remoteConfType string, remote string) (remoteStorageLocation *remote_pb.RemoteStorageLocation, err error) {
	maker, found := RemoteStorageClientMakers[remoteConfType]
	if !found {
		return nil, fmt.Errorf("remote storage type %s not found", remoteConfType)
	}

	if !maker.HasBucket() {
		return parseNoBucketLocation(remote), nil
	}
	return parseBucketLocation(remote), nil
}

func makeRemoteStorageClient(remoteConf *remote_pb.RemoteConf) (RemoteStorageClient, error) {
	maker, found := RemoteStorageClientMakers[remoteConf.Type]
	if !found {
		return nil, fmt.Errorf("remote storage type %s not found", remoteConf.Type)
	}
	return maker.Make(remoteConf)
}
