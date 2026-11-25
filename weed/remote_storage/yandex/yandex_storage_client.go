package yandex

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

// Yandex Disk has no buckets and no place to store a file's modification time.
// A top-level folder under yandex_root plays the bucket, and the entry's
// modification time (and content encoding) travel as custom resource
// properties. The sync compares the stored stamp with the entry's mtime, so a
// stamp taken from the service's own "modified" would make every upload look
// stale.
const (
	propModified        = "seaweedfs_modified"
	propContentEncoding = "seaweedfs_content_encoding"
	// propRcloneModified is the property rclone stores its upload mtimes
	// under; reading it keeps the mtime of objects rclone put there.
	propRcloneModified = "rclone_modified"
)

func init() {
	remote_storage.RemoteStorageClientMakers["yandex"] = &yandexRemoteStorageMaker{}
}

type yandexRemoteStorageMaker struct{}

func (m *yandexRemoteStorageMaker) HasBucket() bool {
	return true
}

func (m *yandexRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}

// MakeWithHTTPClient builds a Yandex Disk client. Callers that need to pin the
// dial path against a caller-supplied endpoint pass a client whose transport
// has a guarded DialContext; it then carries the API calls, the token
// exchange, and the object transfers alike.
func MakeWithHTTPClient(conf *remote_pb.RemoteConf, httpClient *http.Client) (remote_storage.RemoteStorageClient, error) {
	return makeClient(conf, httpClient, "")
}

func makeClient(conf *remote_pb.RemoteConf, httpClient *http.Client, tokenURL string) (remote_storage.RemoteStorageClient, error) {
	if conf.GetYandexToken() == "" {
		return nil, fmt.Errorf("yandex token is not configured: remote.configure -name=%s -type=yandex -yandex.token=<token>", conf.GetName())
	}
	tokens, err := sharedTokenSource(conf, httpClient, tokenURL)
	if err != nil {
		return nil, err
	}
	prefix := "disk"
	if conf.GetYandexAppFolder() {
		prefix = "app"
	}
	return &yandexRemoteStorageClient{
		conf:   conf,
		api:    newAPIClient(conf.GetYandexEndpoint(), httpClient, tokens),
		prefix: prefix,
		root:   strings.Trim(conf.GetYandexRoot(), "/"),
	}, nil
}

type yandexRemoteStorageClient struct {
	conf   *remote_pb.RemoteConf
	api    *apiClient
	prefix string
	root   string
}

var _ = remote_storage.RemoteStorageClient(&yandexRemoteStorageClient{})

// rootPath is the folder the buckets live in, e.g. "disk:/" or
// "disk:/seaweedfs".
func (y *yandexRemoteStorageClient) rootPath() string {
	return y.prefix + ":/" + y.root
}

// bucketPath is the folder one bucket maps to.
func (y *yandexRemoteStorageClient) bucketPath(bucket string) string {
	path := y.rootPath()
	if bucket = strings.Trim(bucket, "/"); bucket != "" {
		path = strings.TrimSuffix(path, "/") + "/" + bucket
	}
	return path
}

// resourcePath is the API path of a location.
func (y *yandexRemoteStorageClient) resourcePath(loc *remote_pb.RemoteStorageLocation) string {
	path := y.bucketPath(loc.GetBucket())
	if rest := strings.Trim(loc.GetPath(), "/"); rest != "" {
		path = strings.TrimSuffix(path, "/") + "/" + rest
	}
	return path
}

// relativeDir is a directory path as the visit callback reports it: relative
// to the bucket root, so "/" is the bucket itself.
func (y *yandexRemoteStorageClient) relativeDir(loc *remote_pb.RemoteStorageLocation) string {
	bucket := y.bucketPath(loc.GetBucket())
	dir := y.resourcePath(loc)
	if !strings.HasPrefix(dir, bucket) {
		return "/"
	}
	if rest := strings.TrimPrefix(dir, bucket); rest != "" {
		return rest
	}
	return "/"
}

func (y *yandexRemoteStorageClient) stampOf(res *resource) *filer_pb.RemoteEntry {
	stamp := &filer_pb.RemoteEntry{
		StorageName: y.conf.GetName(),
		RemoteSize:  res.Size,
		RemoteETag:  res.Md5,
		RemoteMtime: mtimeOf(res),
	}
	if encoding, found := stringProperty(res, propContentEncoding); found {
		stamp.RemoteContentEncoding = proto.String(encoding)
	}
	return stamp
}

// mtimeOf reads the modification time the upload recorded. Falling back to the
// service's "modified" keeps objects written by other clients usable.
func mtimeOf(res *resource) int64 {
	for _, key := range []string{propModified, propRcloneModified} {
		if value, found := stringProperty(res, key); found {
			if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
				return t.Unix()
			}
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, res.Modified); err == nil {
		return t.Unix()
	}
	return 0
}

func stringProperty(res *resource, key string) (string, bool) {
	if res.CustomProperties == nil {
		return "", false
	}
	value, found := res.CustomProperties[key]
	if !found {
		return "", false
	}
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	return text, true
}

func (y *yandexRemoteStorageClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	return y.walkTree(context.Background(), y.resourcePath(loc), y.relativeDir(loc), visitFn)
}

// walkTree visits every file and folder under dirPath, depth first. The Disk
// API lists one folder at a time, so a deep tree costs one request per folder.
func (y *yandexRemoteStorageClient) walkTree(ctx context.Context, dirPath, relDir string, visitFn remote_storage.VisitFunc) error {
	var offset uint64
	for {
		page, err := y.api.list(ctx, dirPath, offset)
		if err != nil {
			return fmt.Errorf("list yandex %s: %w", dirPath, err)
		}
		items := page.items()
		for i := range items {
			item := &items[i]
			isDirectory := item.Type == "dir"
			var stamp *filer_pb.RemoteEntry
			if !isDirectory {
				stamp = y.stampOf(item)
			}
			if err = visitFn(relDir, item.Name, isDirectory, stamp); err != nil {
				return err
			}
			if isDirectory {
				childRel := strings.TrimSuffix(relDir, "/") + "/" + item.Name
				if err = y.walkTree(ctx, dirPath+"/"+item.Name, childRel, visitFn); err != nil {
					return err
				}
			}
		}
		offset += uint64(len(items))
		if len(items) < listPageSize || offset >= page.total() {
			return nil
		}
	}
}

func (y *yandexRemoteStorageClient) ListDirectory(ctx context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	dirPath := y.resourcePath(loc)
	relDir := y.relativeDir(loc)
	var offset uint64
	for {
		page, err := y.api.list(ctx, dirPath, offset)
		if err != nil {
			return fmt.Errorf("list yandex %s: %w", dirPath, err)
		}
		if page.Type != "dir" {
			return fmt.Errorf("list yandex %s: %w", dirPath, ErrDirectory)
		}
		items := page.items()
		for i := range items {
			item := &items[i]
			isDirectory := item.Type == "dir"
			var stamp *filer_pb.RemoteEntry
			if !isDirectory {
				stamp = y.stampOf(item)
			}
			if err = visitFn(relDir, item.Name, isDirectory, stamp); err != nil {
				return err
			}
		}
		offset += uint64(len(items))
		if len(items) < listPageSize || offset >= page.total() {
			return nil
		}
	}
}

func (y *yandexRemoteStorageClient) StatFile(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	return y.statFile(context.Background(), loc)
}

func (y *yandexRemoteStorageClient) statFile(ctx context.Context, loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	path := y.resourcePath(loc)
	res, err := y.api.stat(ctx, path)
	if err != nil {
		if isNotFound(err) {
			return nil, y.missingObjectError(ctx, loc, path)
		}
		return nil, fmt.Errorf("stat yandex %s: %w", path, err)
	}
	if res.Type == "dir" {
		return nil, fmt.Errorf("stat yandex %s: %w", path, ErrDirectory)
	}
	return y.stampOf(res), nil
}

// missingObjectError confirms that the bucket folder still exists before
// reporting the object gone: a vanished bucket must not read as every object
// under it having been deleted. Only the bucket is checked, as the gcs client
// does: a subfolder is part of the key, and a key whose subfolder is missing
// (a rename destination the remote has no folder for yet, a folder removed on
// the disk) is simply a missing object.
func (y *yandexRemoteStorageClient) missingObjectError(ctx context.Context, loc *remote_pb.RemoteStorageLocation, path string) error {
	bucket := y.bucketPath(loc.GetBucket())
	if _, err := y.api.stat(ctx, bucket); err != nil {
		if isNotFound(err) {
			return fmt.Errorf("yandex %s: %w", path, ErrObjectMissingWithParent)
		}
		return fmt.Errorf("yandex %s: object not found and bucket folder unconfirmed: %w", path, err)
	}
	return remote_storage.ErrRemoteObjectNotFound
}

func parentPath(path string) string {
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return ""
	}
	parent := path[:index]
	if strings.HasSuffix(parent, ":") {
		return parent + "/"
	}
	if parent == "" {
		return "/"
	}
	return parent
}

func (y *yandexRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) ([]byte, error) {
	return y.readFile(context.Background(), loc, offset, size)
}

func (y *yandexRemoteStorageClient) readFile(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) ([]byte, error) {
	path := y.resourcePath(loc)
	resp, err := y.api.openRange(ctx, path, offset, size)
	if err != nil {
		if isNotFound(err) {
			return nil, y.missingObjectError(ctx, loc, path)
		}
		return nil, fmt.Errorf("read yandex %s: %w", path, err)
	}
	defer resp.Body.Close()

	if size <= 0 {
		data, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("read yandex %s: %w", path, readErr)
		}
		return data, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, size))
	if err != nil {
		return nil, fmt.Errorf("read yandex %s: %w", path, err)
	}
	// a pre-sized buffer would zero-pad the tail of a short body and cache it
	// as valid content; the same check the S3 client makes
	if int64(len(data)) != size {
		return nil, fmt.Errorf("short read from yandex %s at offset %d: got %d bytes, want %d", path, offset, len(data), size)
	}
	return data, nil
}

func (y *yandexRemoteStorageClient) ReadFileAsStream(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (io.ReadCloser, error) {
	path := y.resourcePath(loc)
	resp, err := y.api.openRange(ctx, path, offset, size)
	if err != nil {
		if isNotFound(err) {
			return nil, y.missingObjectError(ctx, loc, path)
		}
		return nil, fmt.Errorf("open yandex %s: %w", path, err)
	}
	return resp.Body, nil
}

func (y *yandexRemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) error {
	return y.api.mkdir(context.Background(), y.resourcePath(loc))
}

func (y *yandexRemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) error {
	if strings.Trim(loc.GetPath(), "/") == "" {
		// the mount root maps to the whole bucket folder; wiping it from one
		// directory event is too destructive, so keep it
		glog.Warningf("yandex %s: skip removing the directory mapped to the bucket root", y.resourcePath(loc))
		return nil
	}
	err := y.api.remove(context.Background(), y.resourcePath(loc), y.conf.GetYandexTrash())
	if isNotFound(err) {
		return nil
	}
	return err
}

func (y *yandexRemoteStorageClient) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (*filer_pb.RemoteEntry, error) {
	return y.writeFile(context.Background(), loc, entry, reader)
}

func (y *yandexRemoteStorageClient) writeFile(ctx context.Context, loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (*filer_pb.RemoteEntry, error) {
	path := y.resourcePath(loc)

	// the parent folders an S3-style key never needed are created only when
	// the service says the path does not exist, so the common case costs no
	// extra requests
	upload, err := y.api.uploadHref(ctx, path, true)
	if err != nil {
		if !isMissingParent(err) {
			return nil, fmt.Errorf("yandex upload link for %s: %w", path, err)
		}
		if mkErr := y.mkdirAll(ctx, parentPath(path)); mkErr != nil {
			return nil, mkErr
		}
		if upload, err = y.api.uploadHref(ctx, path, true); err != nil {
			return nil, fmt.Errorf("yandex upload link for %s: %w", path, err)
		}
	}

	size := int64(filer.FileSize(entry))
	contentType := entry.GetAttributes().GetMime()
	written, err := y.api.upload(ctx, upload.Href, reader, size, contentType)
	if err != nil {
		return nil, fmt.Errorf("upload to yandex %s: %w", path, err)
	}
	if size > 0 && written != size {
		return nil, fmt.Errorf("upload to yandex %s: wrote %d bytes, want %d: %w", path, written, size, io.ErrUnexpectedEOF)
	}
	if upload.OperationID != "" {
		if err = y.api.waitOperation(ctx, y.api.operationURL(upload.OperationID)); err != nil {
			return nil, err
		}
	}
	return y.recordStamp(ctx, path, entry)
}

// recordStamp stores the entry's modification time and content encoding on the
// object. The service occasionally drops a property set right after an upload,
// so the value is read back and set again, and the stamp returned carries the
// mtime that is actually stored.
func (y *yandexRemoteStorageClient) recordStamp(ctx context.Context, path string, entry *filer_pb.Entry) (*filer_pb.RemoteEntry, error) {
	want := formatMtime(entryMtime(entry))
	encoding := remote_storage.EntryContentEncoding(entry)

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, y.api.pollEvery); err != nil {
				return nil, err
			}
		}
		if lastErr = y.api.setProperties(ctx, path, map[string]string{
			propModified:        want,
			propContentEncoding: encoding,
		}); lastErr != nil {
			continue
		}
		res, err := y.api.statAfterWrite(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("stat yandex %s after upload: %w", path, err)
		}
		if stored, found := stringProperty(res, propModified); found && stored == want {
			return y.stampOf(res), nil
		}
		lastErr = fmt.Errorf("yandex %s: the modification time was not stored", path)
	}
	return nil, lastErr
}

func (y *yandexRemoteStorageClient) mkdirAll(ctx context.Context, dirPath string) error {
	prefix, rest, found := strings.Cut(dirPath, ":")
	if !found {
		prefix, rest = "disk", dirPath
	}
	current := prefix + ":/"
	for _, segment := range strings.Split(strings.Trim(rest, "/"), "/") {
		if segment == "" {
			continue
		}
		current = strings.TrimSuffix(current, "/") + "/" + segment
		if err := y.api.mkdir(ctx, current); err != nil {
			return fmt.Errorf("create yandex folder %s: %w", current, err)
		}
	}
	return nil
}

func (y *yandexRemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (*filer_pb.RemoteEntry, error) {
	oldMtime := entryMtime(oldEntry)
	newMtime := entryMtime(newEntry)
	oldEncoding := remote_storage.EntryContentEncoding(oldEntry)
	newEncoding := remote_storage.EntryContentEncoding(newEntry)
	if oldMtime.Equal(newMtime) && oldEncoding == newEncoding {
		return nil, nil
	}
	path := y.resourcePath(loc)
	err := y.api.setProperties(context.Background(), path, map[string]string{
		propModified:        formatMtime(newMtime),
		propContentEncoding: newEncoding,
	})
	if err != nil {
		return nil, fmt.Errorf("update yandex metadata of %s: %w", path, err)
	}
	return y.statFile(context.Background(), loc)
}

func (y *yandexRemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) error {
	err := y.api.remove(context.Background(), y.resourcePath(loc), y.conf.GetYandexTrash())
	if isNotFound(err) {
		return remote_storage.ErrRemoteObjectNotFound
	}
	if err != nil {
		return fmt.Errorf("delete yandex %s: %w", y.resourcePath(loc), err)
	}
	return nil
}

// CopyFile copies a remote-only object within the remote, so a rename does not
// have to move the bytes through the client. Yandex does not reliably honor
// overwrite on a server-side copy, so an existing destination is removed
// first, and the source's properties are re-applied: a copy keeps the content,
// not the custom properties that carry its modification time.
func (y *yandexRemoteStorageClient) CopyFile(src *remote_pb.RemoteStorageLocation, dst *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	ctx := context.Background()
	srcPath := y.resourcePath(src)
	dstPath := y.resourcePath(dst)

	source, err := y.api.stat(ctx, srcPath)
	if err != nil {
		if isNotFound(err) {
			return nil, y.missingObjectError(ctx, src, srcPath)
		}
		return nil, fmt.Errorf("stat yandex %s: %w", srcPath, err)
	}
	properties := map[string]string{}
	for _, key := range []string{propModified, propContentEncoding} {
		if value, found := stringProperty(source, key); found {
			properties[key] = value
		}
	}

	if err = y.api.remove(ctx, dstPath, false); err != nil && !isNotFound(err) {
		return nil, fmt.Errorf("clear yandex copy destination %s: %w", dstPath, err)
	}
	err = y.api.copyResource(ctx, srcPath, dstPath, true)
	if isMissingParent(err) {
		// a rename into a folder the remote does not have yet: the sync may
		// not have created it, or may not run at all
		if mkErr := y.mkdirAll(ctx, parentPath(dstPath)); mkErr != nil {
			return nil, mkErr
		}
		err = y.api.copyResource(ctx, srcPath, dstPath, true)
	}
	if err != nil {
		return nil, fmt.Errorf("copy yandex %s to %s: %w", srcPath, dstPath, err)
	}
	if len(properties) > 0 {
		if err = y.api.setProperties(ctx, dstPath, properties); err != nil {
			return nil, fmt.Errorf("stamp yandex copy %s: %w", dstPath, err)
		}
	}
	return y.statFile(ctx, dst)
}

func (y *yandexRemoteStorageClient) ListBuckets() ([]*remote_storage.Bucket, error) {
	var buckets []*remote_storage.Bucket
	ctx := context.Background()
	rootPath := y.rootPath()
	var offset uint64
	for {
		page, err := y.api.list(ctx, rootPath, offset)
		if err != nil {
			if isNotFound(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("list yandex %s: %w", rootPath, err)
		}
		items := page.items()
		for i := range items {
			item := &items[i]
			if item.Type != "dir" {
				continue
			}
			bucket := &remote_storage.Bucket{Name: item.Name}
			if created, parseErr := time.Parse(time.RFC3339Nano, item.Created); parseErr == nil {
				bucket.CreatedAt = created
			}
			buckets = append(buckets, bucket)
		}
		offset += uint64(len(items))
		if len(items) < listPageSize || offset >= page.total() {
			return buckets, nil
		}
	}
}

func (y *yandexRemoteStorageClient) CreateBucket(name string) error {
	ctx := context.Background()
	// the root folder itself may be missing: a fresh disk has no folders
	if err := y.mkdirAll(ctx, y.rootPath()); err != nil {
		return err
	}
	if err := y.api.mkdir(ctx, y.bucketPath(name)); err != nil {
		return fmt.Errorf("create yandex bucket %s: %w", name, err)
	}
	return nil
}

func (y *yandexRemoteStorageClient) DeleteBucket(name string) error {
	err := y.api.remove(context.Background(), y.bucketPath(name), y.conf.GetYandexTrash())
	if err == nil || isNotFound(err) {
		return nil
	}
	return fmt.Errorf("delete yandex bucket %s: %w", name, err)
}

func entryMtime(entry *filer_pb.Entry) time.Time {
	return time.Unix(entry.GetAttributes().GetMtime(), int64(entry.GetAttributes().GetMtimeNs()))
}

func formatMtime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
