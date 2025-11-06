package azure

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	defaultBlockSize   = 4 * 1024 * 1024
	defaultConcurrency = 16

	// DefaultAzureOpTimeout is the timeout for individual Azure blob operations.
	// This should be larger than the maximum time the Azure SDK client will spend
	// retrying. With MaxRetries=3 (4 total attempts) and TryTimeout=10s, the maximum
	// time is roughly 4*10s + delays(~7s) = 47s. We use 60s to provide a reasonable
	// buffer while still failing faster than indefinite hangs.
	DefaultAzureOpTimeout = 60 * time.Second
)

// DefaultAzBlobClientOptions returns the default Azure blob client options
// with consistent retry configuration across the application.
// This centralizes the retry policy to ensure uniform behavior between
// remote storage and replication sink implementations.
//
// Related: Use DefaultAzureOpTimeout for context.WithTimeout when calling Azure operations
// to ensure the timeout accommodates all retry attempts configured here.
func DefaultAzBlobClientOptions() *azblob.ClientOptions {
	return &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    3,                // Reasonable retry count - aggressive retries mask configuration errors
				TryTimeout:    10 * time.Second, // Reduced from 1 minute to fail faster on auth issues
				RetryDelay:    1 * time.Second,
				MaxRetryDelay: 10 * time.Second,
			},
		},
	}
}

// invalidMetadataChars matches any character that is not valid in Azure metadata keys.
// Azure metadata keys must be valid C# identifiers: letters, digits, and underscores only.
var invalidMetadataChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// sanitizeMetadataKey converts an S3 metadata key to a valid Azure metadata key.
// Azure metadata keys must be valid C# identifiers (letters, digits, underscores only, cannot start with digit).
// To prevent collisions, invalid characters are replaced with their hex representation (_XX_).
// Examples:
//   - "my-key" -> "my_2d_key"
//   - "my.key" -> "my_2e_key"
//   - "key@value" -> "key_40_value"
func sanitizeMetadataKey(key string) string {
	// Replace each invalid character with _XX_ where XX is the hex code
	result := invalidMetadataChars.ReplaceAllStringFunc(key, func(s string) string {
		return fmt.Sprintf("_%02x_", s[0])
	})

	// Azure metadata keys cannot start with a digit
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "_" + result
	}

	return result
}

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

	// Create credential and client
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("invalid Azure credential with account name:%s: %w", accountName, err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	azClient, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, DefaultAzBlobClientOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure client: %w", err)
	}

	client.client = azClient

	return client, nil
}

type azureRemoteStorageClient struct {
	conf   *remote_pb.RemoteConf
	client *azblob.Client
}

var _ = remote_storage.RemoteStorageClient(&azureRemoteStorageClient{})

func (az *azureRemoteStorageClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

	pathKey := loc.Path[1:]
	containerClient := az.client.ServiceClient().NewContainerClient(loc.Bucket)

	// List blobs with pager
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &pathKey,
	})

	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return fmt.Errorf("azure traverse %s%s: %w", loc.Bucket, loc.Path, err)
		}

		for _, blobItem := range resp.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}
			key := "/" + *blobItem.Name
			dir, name := util.FullPath(key).DirAndName()

			remoteEntry := &filer_pb.RemoteEntry{
				StorageName: az.conf.Name,
			}
			if blobItem.Properties != nil {
				if blobItem.Properties.LastModified != nil {
					remoteEntry.RemoteMtime = blobItem.Properties.LastModified.Unix()
				}
				if blobItem.Properties.ContentLength != nil {
					remoteEntry.RemoteSize = *blobItem.Properties.ContentLength
				}
				if blobItem.Properties.ETag != nil {
					remoteEntry.RemoteETag = string(*blobItem.Properties.ETag)
				}
			}

			err = visitFn(dir, name, false, remoteEntry)
			if err != nil {
				return fmt.Errorf("azure processing %s%s: %w", loc.Bucket, loc.Path, err)
			}
		}
	}

	return
}

func (az *azureRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {

	key := loc.Path[1:]
	blobClient := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlockBlobClient(key)

	count := size
	if count == 0 {
		count = blob.CountToEnd
	}
	downloadResp, err := blobClient.DownloadStream(context.Background(), &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  count,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %w", loc.Bucket, loc.Path, err)
	}
	defer downloadResp.Body.Close()

	data, err = io.ReadAll(downloadResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read download stream %s%s: %w", loc.Bucket, loc.Path, err)
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
	blobClient := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlockBlobClient(key)

	// Upload from reader
	metadata := toMetadata(entry.Extended)
	httpHeaders := &blob.HTTPHeaders{}
	if entry.Attributes != nil && entry.Attributes.Mime != "" {
		httpHeaders.BlobContentType = &entry.Attributes.Mime
	}

	_, err = blobClient.UploadStream(context.Background(), reader, &blockblob.UploadStreamOptions{
		BlockSize:   defaultBlockSize,
		Concurrency: defaultConcurrency,
		HTTPHeaders: httpHeaders,
		Metadata:    metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("azure upload to %s%s: %w", loc.Bucket, loc.Path, err)
	}

	// read back the remote entry
	return az.readFileRemoteEntry(loc)
}

func (az *azureRemoteStorageClient) readFileRemoteEntry(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	key := loc.Path[1:]
	blobClient := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlockBlobClient(key)

	props, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	remoteEntry := &filer_pb.RemoteEntry{
		StorageName: az.conf.Name,
	}

	if props.LastModified != nil {
		remoteEntry.RemoteMtime = props.LastModified.Unix()
	}
	if props.ContentLength != nil {
		remoteEntry.RemoteSize = *props.ContentLength
	}
	if props.ETag != nil {
		remoteEntry.RemoteETag = string(*props.ETag)
	}

	return remoteEntry, nil
}

func toMetadata(attributes map[string][]byte) map[string]*string {
	metadata := make(map[string]*string)
	for k, v := range attributes {
		if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
			// S3 stores metadata keys in lowercase; normalize for consistency.
			key := strings.ToLower(k[len(s3_constants.AmzUserMetaPrefix):])

			// Sanitize key to prevent collisions and ensure Azure compliance
			key = sanitizeMetadataKey(key)

			val := string(v)
			metadata[key] = &val
		}
	}
	return metadata
}

func (az *azureRemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error) {
	if reflect.DeepEqual(oldEntry.Extended, newEntry.Extended) {
		return nil
	}
	metadata := toMetadata(newEntry.Extended)

	key := loc.Path[1:]
	blobClient := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlobClient(key)

	_, err = blobClient.SetMetadata(context.Background(), metadata, nil)

	return
}

func (az *azureRemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error) {
	key := loc.Path[1:]
	blobClient := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlobClient(key)

	_, err = blobClient.Delete(context.Background(), &blob.DeleteOptions{
		DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude),
	})
	if err != nil {
		// Make delete idempotent - don't return error if blob doesn't exist
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil
		}
		return fmt.Errorf("azure delete %s%s: %w", loc.Bucket, loc.Path, err)
	}
	return
}

func (az *azureRemoteStorageClient) ListBuckets() (buckets []*remote_storage.Bucket, err error) {
	pager := az.client.NewListContainersPager(nil)

	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return buckets, err
		}

		for _, containerItem := range resp.ContainerItems {
			if containerItem.Name != nil {
				bucket := &remote_storage.Bucket{
					Name: *containerItem.Name,
				}
				if containerItem.Properties != nil && containerItem.Properties.LastModified != nil {
					bucket.CreatedAt = *containerItem.Properties.LastModified
				}
				buckets = append(buckets, bucket)
			}
		}
	}
	return
}

func (az *azureRemoteStorageClient) CreateBucket(name string) (err error) {
	containerClient := az.client.ServiceClient().NewContainerClient(name)
	_, err = containerClient.Create(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("create bucket %s: %w", name, err)
	}
	return
}

func (az *azureRemoteStorageClient) DeleteBucket(name string) (err error) {
	containerClient := az.client.ServiceClient().NewContainerClient(name)
	_, err = containerClient.Delete(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("delete bucket %s: %w", name, err)
	}
	return
}
