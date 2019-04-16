package B2Sink

import (
	"context"
	"strings"

	"github.com/HZ89/seaweedfs/weed/filer2"
	"github.com/HZ89/seaweedfs/weed/pb/filer_pb"
	"github.com/HZ89/seaweedfs/weed/replication/sink"
	"github.com/HZ89/seaweedfs/weed/replication/source"
	"github.com/HZ89/seaweedfs/weed/util"
	"github.com/kurin/blazer/b2"
)

type B2Sink struct {
	client      *b2.Client
	bucket      string
	dir         string
	filerSource *source.FilerSource
}

func init() {
	sink.Sinks = append(sink.Sinks, &B2Sink{})
}

func (g *B2Sink) GetName() string {
	return "backblaze"
}

func (g *B2Sink) GetSinkToDirectory() string {
	return g.dir
}

func (g *B2Sink) Initialize(configuration util.Configuration) error {
	return g.initialize(
		configuration.GetString("b2_account_id"),
		configuration.GetString("b2_master_application_key"),
		configuration.GetString("bucket"),
		configuration.GetString("directory"),
	)
}

func (g *B2Sink) SetSourceFiler(s *source.FilerSource) {
	g.filerSource = s
}

func (g *B2Sink) initialize(accountId, accountKey, bucket, dir string) error {
	ctx := context.Background()
	client, err := b2.NewClient(ctx, accountId, accountKey)
	if err != nil {
		return err
	}

	g.client = client
	g.bucket = bucket
	g.dir = dir

	return nil
}

func (g *B2Sink) DeleteEntry(ctx context.Context, key string, isDirectory, deleteIncludeChunks bool) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	bucket, err := g.client.Bucket(ctx, g.bucket)
	if err != nil {
		return err
	}

	targetObject := bucket.Object(key)

	return targetObject.Delete(ctx)

}

func (g *B2Sink) CreateEntry(ctx context.Context, key string, entry *filer_pb.Entry) error {

	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer2.TotalSize(entry.Chunks)
	chunkViews := filer2.ViewFromChunks(entry.Chunks, 0, int(totalSize))

	bucket, err := g.client.Bucket(ctx, g.bucket)
	if err != nil {
		return err
	}

	targetObject := bucket.Object(key)
	writer := targetObject.NewWriter(ctx)

	for _, chunk := range chunkViews {

		fileUrl, err := g.filerSource.LookupFileId(ctx, chunk.FileId)
		if err != nil {
			return err
		}

		var writeErr error
		_, readErr := util.ReadUrlAsStream(fileUrl, chunk.Offset, int(chunk.Size), func(data []byte) {
			_, err := writer.Write(data)
			if err != nil {
				writeErr = err
			}
		})

		if readErr != nil {
			return readErr
		}
		if writeErr != nil {
			return writeErr
		}

	}

	return writer.Close()

}

func (g *B2Sink) UpdateEntry(ctx context.Context, key string, oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error) {

	key = cleanKey(key)

	// TODO improve efficiency
	return false, nil
}

func cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	return key
}
