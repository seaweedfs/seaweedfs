package B2Sink

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/replication/repl_util"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/kurin/blazer/b2"
)

type B2Sink struct {
	client        *b2.Client
	bucket        string
	dir           string
	filerSource   *source.FilerSource
	isIncremental bool
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

func (g *B2Sink) IsIncremental() bool {
	return g.isIncremental
}

func (g *B2Sink) Initialize(configuration util.Configuration, prefix string) error {
	g.isIncremental = configuration.GetBool(prefix + "is_incremental")
	return g.initialize(
		configuration.GetString(prefix+"b2_account_id"),
		configuration.GetString(prefix+"b2_master_application_key"),
		configuration.GetString(prefix+"bucket"),
		configuration.GetString(prefix+"directory"),
	)
}

func (g *B2Sink) SetSourceFiler(s *source.FilerSource) {
	g.filerSource = s
}

func (g *B2Sink) initialize(accountId, accountKey, bucket, dir string) error {
	client, err := b2.NewClient(context.Background(), accountId, accountKey)
	if err != nil {
		return err
	}

	g.client = client
	g.bucket = bucket
	g.dir = dir

	return nil
}

func (g *B2Sink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	bucket, err := g.client.Bucket(context.Background(), g.bucket)
	if err != nil {
		return err
	}

	targetObject := bucket.Object(key)

	return targetObject.Delete(context.Background())

}

func (g *B2Sink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {

	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer.FileSize(entry)
	chunkViews := filer.ViewFromChunks(g.filerSource.LookupFileId, entry.Chunks, 0, int64(totalSize))

	bucket, err := g.client.Bucket(context.Background(), g.bucket)
	if err != nil {
		return err
	}

	targetObject := bucket.Object(key)
	writer := targetObject.NewWriter(context.Background())

	writeFunc := func(data []byte) error {
		_, writeErr := writer.Write(data)
		return writeErr
	}

	defer writer.Close()

	if err := repl_util.CopyFromChunkViews(chunkViews, g.filerSource, writeFunc); err != nil {
		return err
	}

	return nil

}

func (g *B2Sink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {

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
