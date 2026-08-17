package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

func init() {
	remote_storage.RemoteStorageClientMakers["gcs"] = new(gcsRemoteStorageMaker)
}

// defaultTokenURL is where the SDK sends the token request when the credentials
// leave token_uri unset.
const defaultTokenURL = "https://oauth2.googleapis.com/token"

// ParseInlineCredentials reports the credential type of an inline credentials
// document and the token endpoint it makes the SDK dial.
func ParseInlineCredentials(creds string) (credType string, tokenURL string, err error) {
	var doc struct {
		Type     string `json:"type"`
		TokenURI string `json:"token_uri"`
	}
	if err := json.Unmarshal([]byte(creds), &doc); err != nil {
		return "", "", fmt.Errorf("parse gcs credentials: %w", err)
	}
	if doc.TokenURI == "" {
		return doc.Type, defaultTokenURL, nil
	}
	return doc.Type, doc.TokenURI, nil
}

type gcsRemoteStorageMaker struct{}

func (s gcsRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s gcsRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}

// MakeWithHTTPClient builds a gcs client whose token exchange and object reads
// both go through the supplied *http.Client (or the SDK default when nil).
// Callers that need to pin the dial path against DNS rebinding pass a client
// whose transport has a guarded DialContext, mirroring the S3 backend.
func MakeWithHTTPClient(conf *remote_pb.RemoteConf, httpClient *http.Client) (remote_storage.RemoteStorageClient, error) {
	client := &gcsRemoteStorageClient{
		conf: conf,
	}

	ctx := context.Background()
	if httpClient != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	}

	googleApplicationCredentials := conf.GcsGoogleApplicationCredentials

	if googleApplicationCredentials == "" {
		if creds, found := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); found {
			googleApplicationCredentials = creds
		} else {
			glog.Warningf("no GOOGLE_APPLICATION_CREDENTIALS env variable found, falling back to Application Default Credentials")
		}
	}

	projectID := conf.GcsProjectId
	if projectID == "" {
		if pid, found := os.LookupEnv("GOOGLE_CLOUD_PROJECT"); found {
			projectID = pid
		} else {
			glog.Warningf("need to specify GOOGLE_CLOUD_PROJECT env variable")
		}
	}

	var clientOpts []option.ClientOption

	if googleApplicationCredentials != "" {
		googleApplicationCredentials = util.ResolvePath(googleApplicationCredentials)
		var data []byte
		var err error
		if strings.HasPrefix(googleApplicationCredentials, "{") {
			data = []byte(googleApplicationCredentials)
		} else {
			data, err = os.ReadFile(googleApplicationCredentials)
			if err != nil {
				return nil, fmt.Errorf("failed to read credentials file %s: %w", googleApplicationCredentials, err)
			}
		}
		creds, err := google.CredentialsFromJSON(ctx, data, storage.ScopeFullControl)
		if err != nil {
			return nil, fmt.Errorf("failed to parse credentials: %w", err)
		}
		clientOpts = append(clientOpts, option.WithHTTPClient(oauth2.NewClient(ctx, creds.TokenSource)), option.WithoutAuthentication())
	}

	c, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	client.client = c
	client.projectID = projectID
	return client, nil
}

type gcsRemoteStorageClient struct {
	conf      *remote_pb.RemoteConf
	client    *storage.Client
	projectID string
}

var _ = remote_storage.RemoteStorageClient(&gcsRemoteStorageClient{})

func (gcs *gcsRemoteStorageClient) toRemoteEntry(attr *storage.ObjectAttrs) *filer_pb.RemoteEntry {
	return &filer_pb.RemoteEntry{
		StorageName:           gcs.conf.Name,
		RemoteMtime:           attr.Updated.Unix(),
		RemoteSize:            attr.Size,
		RemoteETag:            attr.Etag,
		RemoteContentEncoding: proto.String(attr.ContentEncoding),
	}
}

func (gcs *gcsRemoteStorageClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

	pathKey := loc.Path[1:]

	objectIterator := gcs.client.Bucket(loc.Bucket).Objects(context.Background(), &storage.Query{
		Delimiter: "",
		Prefix:    pathKey,
		Versions:  false,
	})

	var objectAttr *storage.ObjectAttrs
	for err == nil {
		objectAttr, err = objectIterator.Next()
		if err != nil {
			if err == iterator.Done {
				return nil
			}
			return err
		}

		key := objectAttr.Name
		key = "/" + key
		dir, name := util.FullPath(key).DirAndName()
		err = visitFn(dir, name, false, gcs.toRemoteEntry(objectAttr))
	}
	return
}

const defaultGCSOpTimeout = 30 * time.Second

func (gcs *gcsRemoteStorageClient) ListDirectory(ctx context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {
	pathKey := loc.Path[1:]
	if pathKey != "" && !strings.HasSuffix(pathKey, "/") {
		pathKey += "/"
	}

	objectIterator := gcs.client.Bucket(loc.Bucket).Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    pathKey,
		Versions:  false,
	})

	for {
		objectAttr, iterErr := objectIterator.Next()
		if iterErr != nil {
			if iterErr == iterator.Done {
				return nil
			}
			return fmt.Errorf("list directory %s%s: %w", loc.Bucket, loc.Path, iterErr)
		}

		if objectAttr.Prefix != "" {
			// Common prefix → subdirectory
			dirKey := "/" + strings.TrimSuffix(objectAttr.Prefix, "/")
			dir, name := util.FullPath(dirKey).DirAndName()
			if err = visitFn(dir, name, true, nil); err != nil {
				return err
			}
		} else {
			key := "/" + objectAttr.Name
			if strings.HasSuffix(key, "/") {
				continue // skip directory markers
			}
			dir, name := util.FullPath(key).DirAndName()
			if err = visitFn(dir, name, false, gcs.toRemoteEntry(objectAttr)); err != nil {
				return err
			}
		}
	}
}

func (gcs *gcsRemoteStorageClient) StatFile(loc *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	key := loc.Path[1:]
	ctx, cancel := context.WithTimeout(context.Background(), defaultGCSOpTimeout)
	defer cancel()
	attr, err := gcs.client.Bucket(loc.Bucket).Object(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("stat gcs %s%s: %w", loc.Bucket, loc.Path, err)
	}
	return gcs.toRemoteEntry(attr), nil
}

func (gcs *gcsRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {

	key := loc.Path[1:]
	// read the stored bytes: decompressive transcoding of gzip-encoded objects
	// breaks range reads and returns sizes that disagree with RemoteSize
	rangeReader, readErr := gcs.client.Bucket(loc.Bucket).Object(key).ReadCompressed(true).NewRangeReader(context.Background(), offset, size)
	if readErr != nil {
		return nil, readErr
	}
	data, err = io.ReadAll(rangeReader)

	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %v", loc.Bucket, loc.Path, err)
	}

	return
}

func (gcs *gcsRemoteStorageClient) ReadFileAsStream(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (reader io.ReadCloser, err error) {
	key := loc.Path[1:]
	return gcs.client.Bucket(loc.Bucket).Object(key).ReadCompressed(true).NewRangeReader(ctx, offset, size)
}

func (gcs *gcsRemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (gcs *gcsRemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error) {
	// the trailing slash keeps sibling prefixes that share the name intact
	prefix := loc.Path[1:]
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix == "" {
		// the mount root maps to the whole bucket; wiping every object from a
		// single namespace event is too destructive, so keep them
		glog.Warningf("gcs %s: skip removing directory mapped to the bucket root", loc.Bucket)
		return nil
	}

	bucket := gcs.client.Bucket(loc.Bucket)
	objectIterator := bucket.Objects(context.Background(), &storage.Query{
		Prefix: prefix,
	})
	for {
		objectAttr, iterErr := objectIterator.Next()
		if iterErr == iterator.Done {
			return nil
		}
		if iterErr != nil {
			return fmt.Errorf("gcs list %s/%s: %w", loc.Bucket, prefix, iterErr)
		}
		if delErr := bucket.Object(objectAttr.Name).Delete(context.Background()); delErr != nil && !errors.Is(delErr, storage.ErrObjectNotExist) {
			return fmt.Errorf("gcs delete %s/%s: %w", loc.Bucket, objectAttr.Name, delErr)
		}
	}
}

func (gcs *gcsRemoteStorageClient) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

	key := loc.Path[1:]

	metadata := toMetadata(entry.Extended)
	wc := gcs.client.Bucket(loc.Bucket).Object(key).NewWriter(context.Background())
	wc.Metadata = metadata
	if entry.Attributes != nil && entry.Attributes.Mime != "" {
		wc.ContentType = entry.Attributes.Mime
	}
	wc.ContentEncoding = remote_storage.EntryContentEncoding(entry)
	if _, err = io.Copy(wc, reader); err != nil {
		return nil, fmt.Errorf("upload to gcs %s/%s%s: %v", loc.Name, loc.Bucket, loc.Path, err)
	}
	if err = wc.Close(); err != nil {
		return nil, fmt.Errorf("close gcs %s/%s%s: %v", loc.Name, loc.Bucket, loc.Path, err)
	}

	// read back the remote entry
	return gcs.readFileRemoteEntry(loc)

}

func (gcs *gcsRemoteStorageClient) readFileRemoteEntry(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	return gcs.StatFile(loc)
}

func toMetadata(attributes map[string][]byte) map[string]string {
	metadata := make(map[string]string)
	for k, v := range attributes {
		if strings.HasPrefix(k, "X-") {
			continue
		}
		metadata[k] = string(v)
	}
	return metadata
}

func (gcs *gcsRemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error) {
	if reflect.DeepEqual(oldEntry.Extended, newEntry.Extended) {
		return nil
	}
	attrsToUpdate := storage.ObjectAttrsToUpdate{}
	if metadata := toMetadata(newEntry.Extended); len(metadata) > 0 {
		attrsToUpdate.Metadata = metadata
	} else {
		// no way to delete the metadata yet
	}
	if encoding := remote_storage.EntryContentEncoding(newEntry); encoding != remote_storage.EntryContentEncoding(oldEntry) {
		attrsToUpdate.ContentEncoding = encoding // empty clears the header
	}

	if attrsToUpdate.Metadata == nil && attrsToUpdate.ContentEncoding == nil {
		return nil
	}
	key := loc.Path[1:]
	_, err = gcs.client.Bucket(loc.Bucket).Object(key).Update(context.Background(), attrsToUpdate)
	return
}
func (gcs *gcsRemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error) {
	key := loc.Path[1:]
	if err = gcs.client.Bucket(loc.Bucket).Object(key).Delete(context.Background()); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return remote_storage.ErrRemoteObjectNotFound
		}
		return fmt.Errorf("gcs delete %s%s: %v", loc.Bucket, key, err)
	}
	return
}

func (gcs *gcsRemoteStorageClient) ListBuckets() (buckets []*remote_storage.Bucket, err error) {
	if gcs.projectID == "" {
		return nil, fmt.Errorf("gcs project id or GOOGLE_CLOUD_PROJECT env variable not set")
	}
	iter := gcs.client.Buckets(context.Background(), gcs.projectID)
	for {
		b, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return buckets, err
		}
		buckets = append(buckets, &remote_storage.Bucket{
			Name:      b.Name,
			CreatedAt: b.Created,
		})
	}
	return
}

func (gcs *gcsRemoteStorageClient) CreateBucket(name string) (err error) {
	if gcs.projectID == "" {
		return fmt.Errorf("gcs project id or GOOGLE_CLOUD_PROJECT env variable not set")
	}
	err = gcs.client.Bucket(name).Create(context.Background(), gcs.projectID, &storage.BucketAttrs{})
	if err != nil {
		return fmt.Errorf("create bucket %s: %v", name, err)
	}
	return
}

func (gcs *gcsRemoteStorageClient) DeleteBucket(name string) (err error) {
	err = gcs.client.Bucket(name).Delete(context.Background())
	if err != nil {
		return fmt.Errorf("delete bucket %s: %v", name, err)
	}
	return
}
