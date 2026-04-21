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
	defaultBlockSize       = 4 * 1024 * 1024
	defaultConcurrency     = 16
	defaultReadConcurrency = 16

	// DefaultAzureOpTimeout is the timeout for short metadata operations
	// (GetProperties, Delete, conditional updates, etc.). Blob body transfers
	// (ReadFile / WriteFile) are NOT bounded by this timeout: they use
	// parallel 4 MiB block transfers and rely on the SDK's per-try TryTimeout
	// to bound each individual block.
	DefaultAzureOpTimeout = 60 * time.Second
)

// DefaultAzBlobClientOptions returns the default Azure blob client options
// with consistent retry configuration across the application.
// This centralizes the retry policy to ensure uniform behavior between
// remote storage and replication sink implementations.
//
// TryTimeout bounds a single HTTP attempt (headers + body) in the Azure SDK.
// With parallel 4 MiB block transfers (DownloadBuffer / UploadStream) a 60s
// TryTimeout is comfortable. A previous regression set this to 10s "to fail
// faster on auth issues" — that silently broke large-blob reads: a single
// non-parallelized Download of more than ~40 MiB over a typical WAN link
// cannot finish in 10s, so re-caching a large remote object (e.g. a 2 GB
// blob mounted via `remote.mount` and fetched through the S3 gateway) failed
// every attempt with a generic context-deadline error.
func DefaultAzBlobClientOptions() *azblob.ClientOptions {
	return &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    3,
				TryTimeout:    60 * time.Second,
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
var _ = remote_storage.RemoteStorageConcurrentReader(&azureRemoteStorageClient{})

func (az *azureRemoteStorageClient) ListDirectory(ctx context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {
	pathKey := loc.Path[1:]
	if pathKey != "" && !strings.HasSuffix(pathKey, "/") {
		pathKey += "/"
	}

	containerClient := az.client.ServiceClient().NewContainerClient(loc.Bucket)
	pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Prefix: &pathKey,
	})

	for pager.More() {
		resp, pageErr := pager.NextPage(ctx)
		if pageErr != nil {
			return fmt.Errorf("azure list directory %s%s: %w", loc.Bucket, loc.Path, pageErr)
		}

		for _, prefix := range resp.Segment.BlobPrefixes {
			if prefix.Name == nil {
				continue
			}
			dirKey := "/" + strings.TrimSuffix(*prefix.Name, "/")
			dir, name := util.FullPath(dirKey).DirAndName()
			if err = visitFn(dir, name, true, nil); err != nil {
				return fmt.Errorf("azure processing directory prefix %s: %w", *prefix.Name, err)
			}
		}

		for _, blobItem := range resp.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}
			key := "/" + *blobItem.Name
			if strings.HasSuffix(key, "/") {
				continue // skip directory markers
			}
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

			if err = visitFn(dir, name, false, remoteEntry); err != nil {
				return fmt.Errorf("azure processing blob %s: %w", *blobItem.Name, err)
			}
		}
	}

	return nil
}

func (az *azureRemoteStorageClient) StatFile(loc *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	key := loc.Path[1:]
	ctx, cancel := context.WithTimeout(context.Background(), DefaultAzureOpTimeout)
	defer cancel()
	resp, err := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("stat azure %s%s: %w", loc.Bucket, loc.Path, err)
	}
	remoteEntry = &filer_pb.RemoteEntry{
		StorageName: az.conf.Name,
	}
	if resp.ContentLength != nil {
		remoteEntry.RemoteSize = *resp.ContentLength
	}
	if resp.LastModified != nil {
		remoteEntry.RemoteMtime = resp.LastModified.Unix()
	}
	if resp.ETag != nil {
		remoteEntry.RemoteETag = string(*resp.ETag)
	}
	return remoteEntry, nil
}

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
	return az.ReadFileWithConcurrency(loc, offset, size, defaultReadConcurrency)
}

// ReadFileWithConcurrency fetches a byte range of a blob using the Azure SDK's
// parallel block downloader. The blob is split into `defaultBlockSize` (4 MiB)
// blocks that are fetched in parallel up to `concurrency` at a time. This
// keeps each individual HTTP GET small enough to complete well within the
// SDK's per-try TryTimeout, so large-blob reads remain reliable even on
// slower WAN links.
func (az *azureRemoteStorageClient) ReadFileWithConcurrency(loc *remote_pb.RemoteStorageLocation, offset int64, size int64, concurrency int) (data []byte, err error) {

	if concurrency <= 0 {
		concurrency = defaultReadConcurrency
	}

	key := loc.Path[1:]
	blobClient := az.client.ServiceClient().NewContainerClient(loc.Bucket).NewBlockBlobClient(key)

	// DownloadBuffer requires a pre-sized destination. If the caller asked for
	// "read to end" (size == 0), discover the blob length via GetProperties
	// first so we can allocate accurately.
	if size == 0 {
		propsCtx, cancelProps := context.WithTimeout(context.Background(), DefaultAzureOpTimeout)
		props, propsErr := blobClient.GetProperties(propsCtx, nil)
		cancelProps()
		if propsErr != nil {
			return nil, fmt.Errorf("get properties %s%s: %w", loc.Bucket, loc.Path, propsErr)
		}
		if props.ContentLength == nil {
			return nil, fmt.Errorf("azure %s%s: missing ContentLength", loc.Bucket, loc.Path)
		}
		size = *props.ContentLength - offset
		if size <= 0 {
			return []byte{}, nil
		}
	}

	data = make([]byte, size)
	_, err = blobClient.DownloadBuffer(context.Background(), data, &blob.DownloadBufferOptions{
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  size,
		},
		BlockSize:   defaultBlockSize,
		Concurrency: uint16(concurrency),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %w", loc.Bucket, loc.Path, err)
	}

	return data, nil
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
	return az.StatFile(loc)
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
