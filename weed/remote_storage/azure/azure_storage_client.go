package azure

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	remote_storage.RemoteStorageClientMakers["azure"] = new(azureRemoteStorageMaker)
}

type azureRemoteStorageMaker struct{}

func (s azureRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s azureRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {

	client := &azureRemoteStorageClient{
		conf: conf,
	}

	accountName, accountKey := conf.AzureAccountName, conf.AzureAccountKey
	if len(accountName) == 0 || len(accountKey) == 0 {
		accountName, accountKey = os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
		if len(accountName) == 0 || len(accountKey) == 0 {
			return nil, fmt.Errorf("either AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
		}
	}

	// Use your Storage account's name and key to create a credential object.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("invalid Azure credential with account name:%s: %v", accountName, err)
	}

	// Create a request pipeline that is used to process HTTP(S) requests and responses.
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	client.serviceURL = azblob.NewServiceURL(*u, p)

	return client, nil
}

type azureRemoteStorageClient struct {
	conf       *remote_pb.RemoteConf
	serviceURL azblob.ServiceURL
}

var _ = remote_storage.RemoteStorageClient(&azureRemoteStorageClient{})

func (az *azureRemoteStorageClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

	pathKey := loc.Path[1:]
	containerURL := az.serviceURL.NewContainerURL(loc.Bucket)

	// List the container that we have created above
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, azblob.ListBlobsSegmentOptions{
			Prefix: pathKey,
		})
		if err != nil {
			return fmt.Errorf("azure traverse %s%s: %v", loc.Bucket, loc.Path, err)
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			key := blobInfo.Name
			key = "/" + key
			dir, name := util.FullPath(key).DirAndName()
			err = visitFn(dir, name, false, &filer_pb.RemoteEntry{
				RemoteMtime: blobInfo.Properties.LastModified.Unix(),
				RemoteSize:  *blobInfo.Properties.ContentLength,
				RemoteETag:  string(blobInfo.Properties.Etag),
				StorageName: az.conf.Name,
			})
			if err != nil {
				return fmt.Errorf("azure processing %s%s: %v", loc.Bucket, loc.Path, err)
			}
		}
	}

	return
}
func (az *azureRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {

	key := loc.Path[1:]
	containerURL := az.serviceURL.NewContainerURL(loc.Bucket)
	blobURL := containerURL.NewBlockBlobURL(key)

	downloadResponse, readErr := blobURL.Download(context.Background(), offset, size, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if readErr != nil {
		return nil, readErr
	}
	// NOTE: automatically retries are performed if the connection fails
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	defer bodyStream.Close()

	data, err = io.ReadAll(bodyStream)

	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %v", loc.Bucket, loc.Path, err)
	}

	return
}

func (az *azureRemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (az *azureRemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error) {
	return nil
}

func (az *azureRemoteStorageClient) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

	key := loc.Path[1:]
	containerURL := az.serviceURL.NewContainerURL(loc.Bucket)
	blobURL := containerURL.NewBlockBlobURL(key)

	readerAt, ok := reader.(io.ReaderAt)
	if !ok {
		return nil, fmt.Errorf("unexpected reader: readerAt expected")
	}
	fileSize := int64(filer.FileSize(entry))

	_, err = uploadReaderAtToBlockBlob(context.Background(), readerAt, fileSize, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:       4 * 1024 * 1024,
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{ContentType: entry.Attributes.Mime},
		Metadata:        toMetadata(entry.Extended),
		Parallelism:     16,
	})
	if err != nil {
		return nil, fmt.Errorf("azure upload to %s%s: %v", loc.Bucket, loc.Path, err)
	}

	// read back the remote entry
	return az.readFileRemoteEntry(loc)
}

func (az *azureRemoteStorageClient) readFileRemoteEntry(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	key := loc.Path[1:]
	containerURL := az.serviceURL.NewContainerURL(loc.Bucket)
	blobURL := containerURL.NewBlockBlobURL(key)

	attr, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})

	if err != nil {
		return nil, err
	}

	return &filer_pb.RemoteEntry{
		RemoteMtime: attr.LastModified().Unix(),
		RemoteSize:  attr.ContentLength(),
		RemoteETag:  string(attr.ETag()),
		StorageName: az.conf.Name,
	}, nil

}

func toMetadata(attributes map[string][]byte) map[string]string {
	metadata := make(map[string]string)
	for k, v := range attributes {
		if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
			metadata[k[len(s3_constants.AmzUserMetaPrefix):]] = string(v)
		}
	}
	parsed_metadata := make(map[string]string)
	for k, v := range metadata {
		parsed_metadata[strings.Replace(k, "-", "_", -1)] = v
	}
	return parsed_metadata
}

func (az *azureRemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error) {
	if reflect.DeepEqual(oldEntry.Extended, newEntry.Extended) {
		return nil
	}
	metadata := toMetadata(newEntry.Extended)

	key := loc.Path[1:]
	containerURL := az.serviceURL.NewContainerURL(loc.Bucket)

	_, err = containerURL.NewBlobURL(key).SetMetadata(context.Background(), metadata, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})

	return
}

func (az *azureRemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error) {
	key := loc.Path[1:]
	containerURL := az.serviceURL.NewContainerURL(loc.Bucket)
	if _, err = containerURL.NewBlobURL(key).Delete(context.Background(),
		azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{}); err != nil {
		return fmt.Errorf("azure delete %s%s: %v", loc.Bucket, loc.Path, err)
	}
	return
}

func (az *azureRemoteStorageClient) ListBuckets() (buckets []*remote_storage.Bucket, err error) {
	ctx := context.Background()
	for containerMarker := (azblob.Marker{}); containerMarker.NotDone(); {
		listContainer, err := az.serviceURL.ListContainersSegment(ctx, containerMarker, azblob.ListContainersSegmentOptions{})
		if err == nil {
			for _, v := range listContainer.ContainerItems {
				buckets = append(buckets, &remote_storage.Bucket{
					Name:      v.Name,
					CreatedAt: v.Properties.LastModified,
				})
			}
		} else {
			return buckets, err
		}
		containerMarker = listContainer.NextMarker
	}
	return
}

func (az *azureRemoteStorageClient) CreateBucket(name string) (err error) {
	containerURL := az.serviceURL.NewContainerURL(name)
	if _, err = containerURL.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone); err != nil {
		return fmt.Errorf("create bucket %s: %v", name, err)
	}
	return
}

func (az *azureRemoteStorageClient) DeleteBucket(name string) (err error) {
	containerURL := az.serviceURL.NewContainerURL(name)
	if _, err = containerURL.Delete(context.Background(), azblob.ContainerAccessConditions{}); err != nil {
		return fmt.Errorf("delete bucket %s: %v", name, err)
	}
	return
}
