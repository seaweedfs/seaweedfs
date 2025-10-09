package azuresink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/repl_util"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type AzureSink struct {
	client        *azblob.Client
	container     string
	dir           string
	filerSource   *source.FilerSource
	isIncremental bool
}

func init() {
	sink.Sinks = append(sink.Sinks, &AzureSink{})
}

func (g *AzureSink) GetName() string {
	return "azure"
}

func (g *AzureSink) GetSinkToDirectory() string {
	return g.dir
}

func (g *AzureSink) IsIncremental() bool {
	return g.isIncremental
}

func (g *AzureSink) Initialize(configuration util.Configuration, prefix string) error {
	g.isIncremental = configuration.GetBool(prefix + "is_incremental")
	return g.initialize(
		configuration.GetString(prefix+"account_name"),
		configuration.GetString(prefix+"account_key"),
		configuration.GetString(prefix+"container"),
		configuration.GetString(prefix+"directory"),
	)
}

func (g *AzureSink) SetSourceFiler(s *source.FilerSource) {
	g.filerSource = s
}

func (g *AzureSink) initialize(accountName, accountKey, container, dir string) error {
	g.container = container
	g.dir = dir

	// Create credential and client
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return fmt.Errorf("failed to create Azure credential with account name:%s: %w", accountName, err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    10, // Increased from default 3 for replication sink resiliency
				TryTimeout:    time.Minute,
				RetryDelay:    2 * time.Second,
				MaxRetryDelay: time.Minute,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create Azure client: %w", err)
	}

	g.client = client

	return nil
}

func (g *AzureSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	blobClient := g.client.ServiceClient().NewContainerClient(g.container).NewBlobClient(key)
	_, err := blobClient.Delete(context.Background(), &blob.DeleteOptions{
		DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude),
	})
	if err != nil {
		// Make delete idempotent - don't return error if blob doesn't exist
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil
		}
		return fmt.Errorf("azure delete %s/%s: %w", g.container, key, err)
	}

	return nil
}

func (g *AzureSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {

	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer.FileSize(entry)
	chunkViews := filer.ViewFromChunks(context.Background(), g.filerSource.LookupFileId, entry.GetChunks(), 0, int64(totalSize))

	// Create append blob client
	appendBlobClient := g.client.ServiceClient().NewContainerClient(g.container).NewAppendBlobClient(key)

	// Create blob with access conditions
	accessConditions := &blob.AccessConditions{}
	if entry.Attributes != nil && entry.Attributes.Mtime > 0 {
		modifiedTime := time.Unix(entry.Attributes.Mtime, 0)
		accessConditions.ModifiedAccessConditions = &blob.ModifiedAccessConditions{
			IfUnmodifiedSince: &modifiedTime,
		}
	}

	_, err := appendBlobClient.Create(context.Background(), &appendblob.CreateOptions{
		AccessConditions: accessConditions,
	})

	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
			// Blob already exists, which is fine for an append blob - we can append to it
		} else {
			// Check if this is a precondition failed error (HTTP 412)
			var respErr *azcore.ResponseError
			if ok := errors.As(err, &respErr); ok && respErr.StatusCode == http.StatusPreconditionFailed {
				glog.V(0).Infof("skip overwriting %s/%s: precondition failed", g.container, key)
				return nil
			}
			return fmt.Errorf("azure create append blob %s/%s: %w", g.container, key, err)
		}
	}

	writeFunc := func(data []byte) error {
		_, writeErr := appendBlobClient.AppendBlock(context.Background(), streaming.NopCloser(bytes.NewReader(data)), &appendblob.AppendBlockOptions{})
		return writeErr
	}

	if len(entry.Content) > 0 {
		return writeFunc(entry.Content)
	}

	if err := repl_util.CopyFromChunkViews(chunkViews, g.filerSource, writeFunc); err != nil {
		return err
	}

	return nil
}

func (g *AzureSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {
	key = cleanKey(key)
	return true, g.CreateEntry(key, newEntry, signatures)
}

func cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	return key
}
