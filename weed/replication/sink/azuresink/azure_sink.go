package azuresink

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage/azure"
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
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, azure.DefaultAzBlobClientOptions())
	if err != nil {
		return fmt.Errorf("failed to create Azure client: %w", err)
	}

	g.client = client

	// Validate that the container exists early to catch configuration errors
	containerClient := client.ServiceClient().NewContainerClient(container)
	ctxValidate, cancelValidate := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
	defer cancelValidate()
	_, err = containerClient.GetProperties(ctxValidate, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return fmt.Errorf("Azure container '%s' does not exist. Please create it first", container)
		}
		return fmt.Errorf("failed to validate Azure container '%s': %w", container, err)
	}

	return nil
}

func (g *AzureSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	blobClient := g.client.ServiceClient().NewContainerClient(g.container).NewBlobClient(key)
	ctxDelete, cancelDelete := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
	defer cancelDelete()
	_, err := blobClient.Delete(ctxDelete, &blob.DeleteOptions{
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

	// Try to create the blob first (without access conditions for initial creation)
	ctxCreate, cancelCreate := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
	defer cancelCreate()
	_, err := appendBlobClient.Create(ctxCreate, nil)

	needsWrite := true
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
			// Handle existing blob - check if overwrite is needed and perform it if necessary
			var handleErr error
			needsWrite, handleErr = g.handleExistingBlob(appendBlobClient, key, entry, totalSize)
			if handleErr != nil {
				return handleErr
			}
		} else {
			return fmt.Errorf("azure create append blob %s/%s: %w", g.container, key, err)
		}
	}

	// If we don't need to write (blob is up-to-date), return early
	if !needsWrite {
		return nil
	}

	writeFunc := func(data []byte) error {
		ctxWrite, cancelWrite := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
		defer cancelWrite()
		_, writeErr := appendBlobClient.AppendBlock(ctxWrite, streaming.NopCloser(bytes.NewReader(data)), &appendblob.AppendBlockOptions{})
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

// handleExistingBlob determines whether an existing blob needs to be overwritten and performs the overwrite if necessary.
// It returns:
//   - needsWrite: true if the caller should write data to the blob, false if the blob is already up-to-date
//   - error: any error encountered during the operation
func (g *AzureSink) handleExistingBlob(appendBlobClient *appendblob.Client, key string, entry *filer_pb.Entry, totalSize uint64) (needsWrite bool, err error) {
	// Get the blob's properties to decide whether to overwrite.
	// Use a timeout to fail fast on network issues.
	ctxProps, cancelProps := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
	defer cancelProps()
	props, propErr := appendBlobClient.GetProperties(ctxProps, nil)

	// Fail fast if we cannot fetch properties - we should not proceed to delete without knowing the blob state.
	if propErr != nil {
		return false, fmt.Errorf("azure get properties %s/%s: %w", g.container, key, propErr)
	}

	// Check if we can skip writing based on modification time and size.
	if entry.Attributes != nil && entry.Attributes.Mtime > 0 && props.LastModified != nil && props.ContentLength != nil {
		const clockSkewTolerance = int64(2) // seconds - allow small clock differences
		remoteMtime := props.LastModified.Unix()
		localMtime := entry.Attributes.Mtime
		// Skip if remote is newer/same (within skew tolerance) and has the SAME size.
		// This prevents skipping partial/corrupted files that may have a newer mtime.
		if remoteMtime >= localMtime-clockSkewTolerance && *props.ContentLength == int64(totalSize) {
			glog.V(2).Infof("skip overwriting %s/%s: remote is up-to-date (remote mtime: %d >= local mtime: %d, size: %d)",
				g.container, key, remoteMtime, localMtime, *props.ContentLength)
			return false, nil
		}
	}

	// Blob is empty or outdated - we need to delete and recreate it.
	// REQUIRE ETag for conditional delete to avoid race conditions and data loss.
	if props.ETag == nil {
		return false, fmt.Errorf("azure blob %s/%s: missing ETag; refusing to delete without conditional", g.container, key)
	}

	deleteOpts := &blob.DeleteOptions{
		DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude),
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: props.ETag,
			},
		},
	}

	// Delete existing blob with conditional delete and timeout.
	ctxDel, cancelDel := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
	defer cancelDel()
	_, delErr := appendBlobClient.Delete(ctxDel, deleteOpts)

	if delErr != nil {
		// If the precondition fails, the blob was modified by another process after we checked it.
		// Failing here is safe; replication will retry.
		if bloberror.HasCode(delErr, bloberror.ConditionNotMet) {
			return false, fmt.Errorf("azure blob %s/%s was modified concurrently, preventing overwrite: %w", g.container, key, delErr)
		}
		// Ignore BlobNotFound, as the goal is to delete it anyway.
		if !bloberror.HasCode(delErr, bloberror.BlobNotFound) {
			return false, fmt.Errorf("azure delete existing blob %s/%s: %w", g.container, key, delErr)
		}
	}

	// Recreate the blob with timeout.
	ctxRecreate, cancelRecreate := context.WithTimeout(context.Background(), azure.DefaultAzureOpTimeout)
	defer cancelRecreate()
	_, createErr := appendBlobClient.Create(ctxRecreate, nil)

	if createErr != nil {
		// It's possible another process recreated it after our delete.
		// Failing is safe, as a retry of the whole function will handle it.
		return false, fmt.Errorf("azure recreate append blob %s/%s: %w", g.container, key, createErr)
	}

	return true, nil
}

func (g *AzureSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {
	key = cleanKey(key)
	return true, g.CreateEntry(key, newEntry, signatures)
}

func cleanKey(key string) string {
	// Remove all leading slashes (TrimLeft handles multiple slashes, unlike TrimPrefix)
	return strings.TrimLeft(key, "/")
}
