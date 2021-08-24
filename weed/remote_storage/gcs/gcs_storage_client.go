package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
	"os"
	"reflect"
)

func init() {
	remote_storage.RemoteStorageClientMakers["gcs"] = new(gcsRemoteStorageMaker)
}

type gcsRemoteStorageMaker struct{}

func (s gcsRemoteStorageMaker) Make(conf *filer_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
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

	googleApplicationCredentials = util.ResolvePath(googleApplicationCredentials)

	c, err := storage.NewClient(context.Background(), option.WithCredentialsFile(googleApplicationCredentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	client.client = c
	return client, nil
}

type gcsRemoteStorageClient struct {
	conf *filer_pb.RemoteConf
	client        *storage.Client
}

var _ = remote_storage.RemoteStorageClient(&gcsRemoteStorageClient{})

func (gcs *gcsRemoteStorageClient) Traverse(loc *filer_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

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
func (gcs *gcsRemoteStorageClient) ReadFile(loc *filer_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {

	key := loc.Path[1:]
	rangeReader, readErr := gcs.client.Bucket(loc.Bucket).Object(key).NewRangeReader(context.Background(), offset, size)
	if readErr != nil {
		return nil, readErr
	}
	data, err = ioutil.ReadAll(rangeReader)

	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %v", loc.Bucket, loc.Path, err)
	}

	return
}

func (gcs *gcsRemoteStorageClient) WriteDirectory(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (gcs *gcsRemoteStorageClient) WriteFile(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

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

func (gcs *gcsRemoteStorageClient) readFileRemoteEntry(loc *filer_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
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
		metadata[k] = string(v)
	}
	return metadata
}

func (gcs *gcsRemoteStorageClient) UpdateFileMetadata(loc *filer_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error) {
	if reflect.DeepEqual(oldEntry.Extended, newEntry.Extended) {
		return nil
	}
	metadata := toMetadata(newEntry.Extended)

	key := loc.Path[1:]

	if len(metadata) > 0 {
		_, err = gcs.client.Bucket(loc.Bucket).Object(key).Update(context.Background(), storage.ObjectAttrsToUpdate{
			Metadata:           metadata,
		})
	} else {
		// no way to delete the metadata yet
	}

	return
}
func (gcs *gcsRemoteStorageClient) DeleteFile(loc *filer_pb.RemoteStorageLocation) (err error) {
	key := loc.Path[1:]
	if err = gcs.client.Bucket(loc.Bucket).Object(key).Delete(context.Background()); err != nil {
		return fmt.Errorf("gcs delete %s%s: %v", loc.Bucket, key, err)
	}
	return
}
