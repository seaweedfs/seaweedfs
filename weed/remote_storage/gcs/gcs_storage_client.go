package gcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	remote_storage.RemoteStorageClientMakers["gcs"] = new(gcsRemoteStorageMaker)
}

type gcsRemoteStorageMaker struct{}

func (s gcsRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s gcsRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &gcsRemoteStorageClient{
		conf: conf,
	}

	googleApplicationCredentials := conf.GcsGoogleApplicationCredentials

	if googleApplicationCredentials == "" {
		found := false
		googleApplicationCredentials, found = os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
		if !found {
			return nil, fmt.Errorf("need to specific GOOGLE_APPLICATION_CREDENTIALS env variable")
		}
	}

	projectID := conf.GcsProjectId
	if projectID == "" {
		found := false
		projectID, found = os.LookupEnv("GOOGLE_CLOUD_PROJECT")
		if !found {
			glog.Warningf("need to specific GOOGLE_CLOUD_PROJECT env variable")
		}
	}

	googleApplicationCredentials = util.ResolvePath(googleApplicationCredentials)

	c, err := storage.NewClient(context.Background(), option.WithCredentialsFile(googleApplicationCredentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
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
		err = visitFn(dir, name, false, &filer_pb.RemoteEntry{
			RemoteMtime: objectAttr.Updated.Unix(),
			RemoteSize:  objectAttr.Size,
			RemoteETag:  objectAttr.Etag,
			StorageName: gcs.conf.Name,
		})
	}
	return
}
func (gcs *gcsRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {

	key := loc.Path[1:]
	rangeReader, readErr := gcs.client.Bucket(loc.Bucket).Object(key).NewRangeReader(context.Background(), offset, size)
	if readErr != nil {
		return nil, readErr
	}
	data, err = io.ReadAll(rangeReader)

	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %v", loc.Bucket, loc.Path, err)
	}

	return
}

func (gcs *gcsRemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (gcs *gcsRemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error) {
	return nil
}

func (gcs *gcsRemoteStorageClient) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

	key := loc.Path[1:]

	metadata := toMetadata(entry.Extended)
	wc := gcs.client.Bucket(loc.Bucket).Object(key).NewWriter(context.Background())
	wc.Metadata = metadata
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
	key := loc.Path[1:]
	attr, err := gcs.client.Bucket(loc.Bucket).Object(key).Attrs(context.Background())

	if err != nil {
		return nil, err
	}

	return &filer_pb.RemoteEntry{
		RemoteMtime: attr.Updated.Unix(),
		RemoteSize:  attr.Size,
		RemoteETag:  attr.Etag,
		StorageName: gcs.conf.Name,
	}, nil

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
	metadata := toMetadata(newEntry.Extended)

	key := loc.Path[1:]

	if len(metadata) > 0 {
		_, err = gcs.client.Bucket(loc.Bucket).Object(key).Update(context.Background(), storage.ObjectAttrsToUpdate{
			Metadata: metadata,
		})
	} else {
		// no way to delete the metadata yet
	}

	return
}
func (gcs *gcsRemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error) {
	key := loc.Path[1:]
	if err = gcs.client.Bucket(loc.Bucket).Object(key).Delete(context.Background()); err != nil {
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
