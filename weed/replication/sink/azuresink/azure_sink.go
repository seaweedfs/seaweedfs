package azuresink

import (
	"bytes"
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/replication/repl_util"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type AzureSink struct {
	containerURL  azblob.ContainerURL
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

	// Use your Storage account's name and key to create a credential object.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		glog.Fatalf("failed to create Azure credential with account name:%s: %v", accountName, err)
	}

	// Create a request pipeline that is used to process HTTP(S) requests and responses.
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := azblob.NewServiceURL(*u, p)

	g.containerURL = serviceURL.NewContainerURL(g.container)

	return nil
}

func (g *AzureSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	if _, err := g.containerURL.NewBlobURL(key).Delete(context.Background(),
		azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{}); err != nil {
		return fmt.Errorf("azure delete %s/%s: %v", g.container, key, err)
	}

	return nil

}

func (g *AzureSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {

	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer.FileSize(entry)
	chunkViews := filer.ViewFromChunks(g.filerSource.LookupFileId, entry.Chunks, 0, int64(totalSize))

	// Create a URL that references a to-be-created blob in your
	// Azure Storage account's container.
	appendBlobURL := g.containerURL.NewAppendBlobURL(key)

	_, err := appendBlobURL.Create(context.Background(), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return err
	}

	writeFunc := func(data []byte) error {
		_, writeErr := appendBlobURL.AppendBlock(context.Background(), bytes.NewReader(data), azblob.AppendBlobAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})
		return writeErr
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
