package yandex

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func newTestClient(t *testing.T, disk *fakeDisk) (*yandexRemoteStorageClient, *httptest.Server) {
	t.Helper()
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)
	server := disk.server()
	t.Cleanup(server.Close)

	client, err := makeClient(&remote_pb.RemoteConf{
		Name:           "yd",
		Type:           "yandex",
		YandexToken:    `{"access_token":"test-token","token_type":"bearer"}`,
		YandexEndpoint: server.URL + "/v1/disk",
		YandexRoot:     "/root",
	}, server.Client(), server.URL+"/token")
	if err != nil {
		t.Fatalf("make client: %v", err)
	}
	yandexClient := client.(*yandexRemoteStorageClient)
	yandexClient.api.pollEvery = time.Millisecond
	yandexClient.api.retryBase = time.Millisecond
	return yandexClient, server
}

func testLocation(bucket, path string) *remote_pb.RemoteStorageLocation {
	return &remote_pb.RemoteStorageLocation{Name: "yd", Bucket: bucket, Path: path}
}

func TestListDirectoryReportsDirsAndFiles(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{
		data: []byte("hello"),
		props: map[string]string{
			propModified:        "2024-01-02T03:04:05.5Z",
			propContentEncoding: "gzip",
		},
	})
	disk.put("/root/bucket/empty", &fakeNode{isDir: true})
	client, _ := newTestClient(t, disk)

	var files, dirs int
	var stamp *filer_pb.RemoteEntry
	err := client.ListDirectory(context.Background(), testLocation("bucket", "/"), func(dir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		if dir != "/" {
			t.Errorf("dir = %q, want %q", dir, "/")
		}
		if isDirectory {
			dirs++
			return nil
		}
		files++
		stamp = remoteEntry
		return nil
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if files != 1 || dirs != 1 {
		t.Fatalf("files = %d, dirs = %d, want 1 and 1", files, dirs)
	}
	if stamp == nil {
		t.Fatal("file reported without a stamp")
	}
	if stamp.RemoteSize != 5 || stamp.RemoteETag != "5d41402abc4b2a76b9719d911017c592" {
		t.Errorf("stamp = %+v, want size 5 and the md5", stamp)
	}
	if stamp.RemoteMtime != 1704164645 {
		t.Errorf("mtime = %d, want the stored 2024-01-02T03:04:05.5Z", stamp.RemoteMtime)
	}
	if stamp.RemoteContentEncoding == nil || *stamp.RemoteContentEncoding != "gzip" {
		t.Errorf("encoding = %v, want gzip", stamp.RemoteContentEncoding)
	}
}

func TestListDirectoryPages(t *testing.T) {
	previous := listPageSize
	listPageSize = 2
	t.Cleanup(func() { listPageSize = previous })

	disk := newFakeDisk(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		disk.put("/root/bucket/"+name+".txt", &fakeNode{data: []byte(name)})
	}
	client, _ := newTestClient(t, disk)

	var seen int
	err := client.ListDirectory(context.Background(), testLocation("bucket", "/"), func(dir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if seen != 5 {
		t.Fatalf("seen = %d, want 5", seen)
	}
	var offsets []string
	for _, request := range disk.requests {
		if request.method == http.MethodGet && request.path == "/v1/disk/resources" && strings.Contains(request.query, "offset=") {
			offsets = append(offsets, request.query)
		}
	}
	if len(offsets) != 3 {
		t.Fatalf("paged requests = %d (%v), want 3", len(offsets), offsets)
	}
}

func TestListDirectoryOfAMissingFolder(t *testing.T) {
	disk := newFakeDisk(t)
	client, _ := newTestClient(t, disk)

	err := client.ListDirectory(context.Background(), testLocation("bucket", "/"), func(dir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "DiskNotFoundError") {
		t.Fatalf("err = %v, want the missing folder error", err)
	}
}

func TestStatFile(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello"), props: map[string]string{propModified: "2024-01-02T03:04:05Z"}})
	disk.put("/root/bucket/dir", &fakeNode{isDir: true})
	client, _ := newTestClient(t, disk)

	stamp, err := client.StatFile(testLocation("bucket", "/a.txt"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stamp.RemoteMtime != 1704164645 || stamp.RemoteSize != 5 {
		t.Fatalf("stamp = %+v", stamp)
	}

	if _, err = client.StatFile(testLocation("bucket", "/missing.txt")); !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		t.Fatalf("missing file err = %v, want ErrRemoteObjectNotFound", err)
	}
	if _, err = client.StatFile(testLocation("missing-bucket", "/a.txt")); !errors.Is(err, ErrObjectMissingWithParent) {
		t.Fatalf("missing bucket err = %v, want ErrObjectMissingWithParent", err)
	}
	if _, err = client.StatFile(testLocation("bucket", "/dir")); !errors.Is(err, ErrDirectory) {
		t.Fatalf("directory err = %v, want ErrDirectory", err)
	}
}

func TestStatFileFallsBackToRcloneModified(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("x"), props: map[string]string{propRcloneModified: "2023-05-06T07:08:09Z"}})
	client, _ := newTestClient(t, disk)

	stamp, err := client.StatFile(testLocation("bucket", "/a.txt"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stamp.RemoteMtime != 1683356889 {
		t.Fatalf("mtime = %d, want the rclone property", stamp.RemoteMtime)
	}
}

func TestWriteFileUploadsAndStamps(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	client, _ := newTestClient(t, disk)

	entry := &filer_pb.Entry{
		Name: "a.txt",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    1700000000,
			MtimeNs:  500,
			FileSize: 5,
			Mime:     "text/plain",
		},
	}
	stamp, err := client.WriteFile(testLocation("bucket", "/a.txt"), entry, strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if string(disk.lastUploadBody) != "hello" || disk.lastUploadLength != 5 {
		t.Errorf("uploaded %q with length %d, want %q with 5", disk.lastUploadBody, disk.lastUploadLength, "hello")
	}
	if disk.lastUploadAuth != "" {
		t.Errorf("the upload link carried authorization %q", disk.lastUploadAuth)
	}
	node := disk.nodes["/root/bucket/a.txt"]
	if node == nil {
		t.Fatal("the object was not stored")
	}
	if want := "2023-11-14T22:13:20.0000005Z"; node.props[propModified] != want {
		t.Errorf("stored mtime property = %q, want %q", node.props[propModified], want)
	}
	if stamp.RemoteMtime != 1700000000 || stamp.RemoteSize != 5 {
		t.Errorf("stamp = %+v", stamp)
	}
	if stamp.RemoteContentEncoding == nil || *stamp.RemoteContentEncoding != "" {
		t.Errorf("encoding = %v, want an authoritative empty value", stamp.RemoteContentEncoding)
	}
}

func TestWriteFileCreatesParentFolders(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	client, _ := newTestClient(t, disk)

	entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{FileSize: 3, Mtime: 1700000000}}
	if _, err := client.WriteFile(testLocation("bucket", "/a/b/c.txt"), entry, strings.NewReader("abc")); err != nil {
		t.Fatalf("write: %v", err)
	}
	for _, path := range []string{"/root/bucket/a", "/root/bucket/a/b", "/root/bucket/a/b/c.txt"} {
		if disk.nodes[path] == nil {
			t.Errorf("%s was not created", path)
		}
	}
}

func TestWriteFileRejectsAShortBody(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	client, _ := newTestClient(t, disk)

	entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{FileSize: 10, Mtime: 1700000000}}
	_, err := client.WriteFile(testLocation("bucket", "/a.txt"), entry, strings.NewReader("short"))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriteFileRestampsADroppedProperty(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	client, _ := newTestClient(t, disk)

	entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{FileSize: 5, Mtime: 1700000000}}
	// the service silently drops the first write's property
	disk.hidePropertyOnce = true

	stamp, err := client.WriteFile(testLocation("bucket", "/a.txt"), entry, strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if stamp.RemoteMtime != 1700000000 {
		t.Fatalf("mtime = %d, want the entry mtime", stamp.RemoteMtime)
	}
	patches := 0
	for _, request := range disk.requests {
		if request.method == http.MethodPatch {
			patches++
		}
	}
	if patches != 2 {
		t.Fatalf("properties written %d times, want 2", patches)
	}
}

func TestWriteFileWaitsForTheUploadOperation(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	disk.uploadOperation = true
	disk.ops["up1"] = 1
	client, _ := newTestClient(t, disk)

	entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{FileSize: 5, Mtime: 1700000000}}
	if _, err := client.WriteFile(testLocation("bucket", "/a.txt"), entry, strings.NewReader("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	operations := 0
	for _, request := range disk.requests {
		if strings.HasPrefix(request.path, "/v1/disk/operations/") {
			operations++
		}
	}
	if operations != 2 {
		t.Fatalf("operation polled %d times, want 2", operations)
	}
}

func TestReadFileRange(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello world")})
	client, _ := newTestClient(t, disk)

	data, err := client.ReadFile(testLocation("bucket", "/a.txt"), 6, 5)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "world" {
		t.Fatalf("read %q, want %q", data, "world")
	}
	var hrefRequest *recordedRequest
	for i := range disk.requests {
		if strings.HasPrefix(disk.requests[i].path, "/download/") {
			hrefRequest = &disk.requests[i]
		}
	}
	if hrefRequest == nil {
		t.Fatal("no request to the download link")
	}
	if hrefRequest.auth != "" {
		t.Errorf("the download link carried authorization %q", hrefRequest.auth)
	}
	if hrefRequest.range_ != "bytes=6-10" {
		t.Errorf("range = %q, want bytes=6-10", hrefRequest.range_)
	}

	if _, err = client.ReadFile(testLocation("bucket", "/missing.txt"), 0, 5); !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		t.Fatalf("missing err = %v, want ErrRemoteObjectNotFound", err)
	}
}

func TestReadFileRefusesAnIgnoredRange(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello world")})
	disk.ignoreRange = true
	client, _ := newTestClient(t, disk)

	if _, err := client.ReadFile(testLocation("bucket", "/a.txt"), 6, 5); err == nil || !strings.Contains(err.Error(), "ignored the byte range") {
		t.Fatalf("err = %v, want the ignored range error", err)
	}
}

func TestReadFileRejectsAShortBody(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello world")})
	disk.shortBody = true
	client, _ := newTestClient(t, disk)

	if _, err := client.ReadFile(testLocation("bucket", "/a.txt"), 0, 5); err == nil || !strings.Contains(err.Error(), "short read") {
		t.Fatalf("err = %v, want a short read error", err)
	}
}

func TestReadFileAsStream(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello world")})
	client, _ := newTestClient(t, disk)

	reader, err := client.ReadFileAsStream(context.Background(), testLocation("bucket", "/a.txt"), 0, 5)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("streamed %q, want %q", data, "hello")
	}
}

func TestCopyFileKeepsTheSourceStamp(t *testing.T) {
	disk := newFakeDisk(t)
	mtime := "2024-01-02T03:04:05Z"
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello"), props: map[string]string{propModified: mtime, propContentEncoding: "gzip"}})
	disk.put("/root/bucket/b.txt", &fakeNode{data: []byte("old")})
	client, _ := newTestClient(t, disk)

	stamp, err := client.CopyFile(testLocation("bucket", "/a.txt"), testLocation("bucket", "/b.txt"))
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if stamp.RemoteMtime != 1704164645 {
		t.Errorf("mtime = %d, want the source mtime", stamp.RemoteMtime)
	}
	node := disk.nodes["/root/bucket/b.txt"]
	if node == nil || string(node.data) != "hello" {
		t.Fatalf("destination = %+v", node)
	}
	if node.props[propModified] != mtime || node.props[propContentEncoding] != "gzip" {
		t.Errorf("destination properties = %v", node.props)
	}
}

func TestCopyFileOfAMissingSource(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	client, _ := newTestClient(t, disk)

	if _, err := client.CopyFile(testLocation("bucket", "/a.txt"), testLocation("bucket", "/b.txt")); !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		t.Fatalf("err = %v, want ErrRemoteObjectNotFound", err)
	}
}

func TestDeleteFile(t *testing.T) {
	disk := newFakeDisk(t)
	disk.deleteAccepted = true
	disk.ops["del"] = 1
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	client, _ := newTestClient(t, disk)

	if err := client.DeleteFile(testLocation("bucket", "/a.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if disk.nodes["/root/bucket/a.txt"] != nil {
		t.Fatal("the object was not deleted")
	}
	if err := client.DeleteFile(testLocation("bucket", "/a.txt")); !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		t.Fatalf("second delete err = %v, want ErrRemoteObjectNotFound", err)
	}
	operations := 0
	for _, request := range disk.requests {
		if strings.HasPrefix(request.path, "/v1/disk/operations/") {
			operations++
		}
	}
	if operations != 2 {
		t.Fatalf("operation polled %d times, want 2", operations)
	}
}

func TestRemoveDirectoryKeepsTheBucketRoot(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	client, _ := newTestClient(t, disk)

	if err := client.RemoveDirectory(testLocation("bucket", "/")); err != nil {
		t.Fatalf("remove root: %v", err)
	}
	if disk.nodes["/root/bucket/a.txt"] == nil {
		t.Fatal("the bucket root was removed")
	}
	if err := client.RemoveDirectory(testLocation("bucket", "/dir")); err != nil {
		t.Fatalf("remove missing dir: %v", err)
	}
}

func TestRemoveDirectoryDeletesRecursively(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/dir/a.txt", &fakeNode{data: []byte("hello")})
	client, _ := newTestClient(t, disk)

	if err := client.RemoveDirectory(testLocation("bucket", "/dir")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if disk.nodes["/root/bucket/dir"] != nil || disk.nodes["/root/bucket/dir/a.txt"] != nil {
		t.Fatal("the folder was not removed recursively")
	}
}

func TestWriteDirectoryIsIdempotent(t *testing.T) {
	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	client, _ := newTestClient(t, disk)

	entry := &filer_pb.Entry{IsDirectory: true}
	if err := client.WriteDirectory(testLocation("bucket", "/dir"), entry); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := client.WriteDirectory(testLocation("bucket", "/dir"), entry); err != nil {
		t.Fatalf("mkdir twice: %v", err)
	}
}

func TestWriteDirectoryRefusesToReplaceAFile(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/dir", &fakeNode{data: []byte("hello")})
	client, _ := newTestClient(t, disk)

	err := client.WriteDirectory(testLocation("bucket", "/dir"), &filer_pb.Entry{IsDirectory: true})
	if err == nil || !strings.Contains(err.Error(), "existing file") {
		t.Fatalf("err = %v, want the existing file error", err)
	}
}

func TestUpdateFileMetadata(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello"), props: map[string]string{propModified: "2024-01-02T03:04:05Z"}})
	client, _ := newTestClient(t, disk)

	oldEntry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 1704164645}}
	newEntry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 1704164650}}

	unchanged, err := client.UpdateFileMetadata(testLocation("bucket", "/a.txt"), oldEntry, oldEntry)
	if err != nil || unchanged != nil {
		t.Fatalf("unchanged metadata: stamp %+v, err %v", unchanged, err)
	}
	stamp, err := client.UpdateFileMetadata(testLocation("bucket", "/a.txt"), oldEntry, newEntry)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if stamp.RemoteMtime != 1704164650 {
		t.Fatalf("mtime = %d, want the new mtime", stamp.RemoteMtime)
	}
	if got := disk.nodes["/root/bucket/a.txt"].props[propModified]; got != "2024-01-02T03:04:10Z" {
		t.Fatalf("stored mtime = %q", got)
	}
}

func TestTraverseVisitsNestedFolders(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("a")})
	disk.put("/root/bucket/dir/b.txt", &fakeNode{data: []byte("b")})
	disk.put("/root/bucket/dir/empty", &fakeNode{isDir: true})
	client, _ := newTestClient(t, disk)

	visited := map[string]bool{}
	err := client.Traverse(testLocation("bucket", "/"), func(dir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		visited[strings.TrimSuffix(dir, "/")+"/"+name] = isDirectory
		return nil
	})
	if err != nil {
		t.Fatalf("traverse: %v", err)
	}
	want := map[string]bool{"/a.txt": false, "/dir": true, "/dir/b.txt": false, "/dir/empty": true}
	for path, isDir := range want {
		if got, found := visited[path]; !found || got != isDir {
			t.Errorf("%s: visited=%v isDir=%v, want isDir=%v", path, found, got, isDir)
		}
	}
	if len(visited) != len(want) {
		t.Errorf("visited %v, want %v", visited, want)
	}
}

func TestListBuckets(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/one", &fakeNode{isDir: true})
	disk.put("/root/two", &fakeNode{isDir: true})
	disk.put("/root/stray.txt", &fakeNode{data: []byte("x")})
	client, _ := newTestClient(t, disk)

	buckets, err := client.ListBuckets()
	if err != nil {
		t.Fatalf("buckets: %v", err)
	}
	if len(buckets) != 2 || buckets[0].Name != "one" || buckets[1].Name != "two" {
		t.Fatalf("buckets = %+v", buckets)
	}
	if buckets[0].CreatedAt.IsZero() {
		t.Error("the bucket creation time was not read")
	}

	empty, _ := newTestClient(t, newFakeDisk(t))
	if _, err = empty.ListBuckets(); err != nil {
		t.Fatalf("a disk without the root folder: %v", err)
	}
}

func TestCreateAndDeleteBucket(t *testing.T) {
	disk := newFakeDisk(t)
	client, _ := newTestClient(t, disk)

	if err := client.CreateBucket("fresh"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if disk.nodes["/root/fresh"] == nil {
		t.Fatal("the bucket folder was not created")
	}
	if err := client.CreateBucket("fresh"); err != nil {
		t.Fatalf("create twice: %v", err)
	}
	if err := client.DeleteBucket("fresh"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := client.DeleteBucket("fresh"); err != nil {
		t.Fatalf("delete twice: %v", err)
	}
}

func TestRequestsGoThroughTheProvidedHTTPClient(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	server := disk.server()
	t.Cleanup(server.Close)
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	var counted int64
	transport := &countingTransport{next: server.Client().Transport, counted: &counted}
	client, err := makeClient(&remote_pb.RemoteConf{
		Name:           "yd",
		Type:           "yandex",
		YandexToken:    `{"access_token":"test-token"}`,
		YandexEndpoint: server.URL + "/v1/disk",
		YandexRoot:     "/root",
	}, &http.Client{Transport: transport}, server.URL+"/token")
	if err != nil {
		t.Fatalf("make client: %v", err)
	}
	yandexClient := client.(*yandexRemoteStorageClient)
	yandexClient.api.pollEvery = time.Millisecond

	if _, err = yandexClient.ReadFile(testLocation("bucket", "/a.txt"), 0, 5); err != nil {
		t.Fatalf("read: %v", err)
	}
	// the API call and the download link both went through the client
	if got := atomic.LoadInt64(&counted); got < 2 {
		t.Fatalf("requests through the provided transport = %d, want at least 2", got)
	}
}

type countingTransport struct {
	next    http.RoundTripper
	counted *int64
}

func (t *countingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	atomic.AddInt64(t.counted, 1)
	return t.next.RoundTrip(request)
}
