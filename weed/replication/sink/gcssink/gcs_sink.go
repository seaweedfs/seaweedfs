package gcssink

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/replication/repl_util"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type GcsSink struct {
	client        *storage.Client
	bucket        string
	dir           string
	filerSource   *source.FilerSource
	isIncremental bool
}

func init() {
	sink.Sinks = append(sink.Sinks, &GcsSink{})
}

func (g *GcsSink) GetName() string {
	return "google_cloud_storage"
}

func (g *GcsSink) GetSinkToDirectory() string {
	return g.dir
}

func (g *GcsSink) IsIncremental() bool {
	return g.isIncremental
}

func (g *GcsSink) Initialize(configuration util.Configuration, prefix string) error {
	g.isIncremental = configuration.GetBool(prefix + "is_incremental")
	return g.initialize(
		configuration.GetString(prefix+"google_application_credentials"),
		configuration.GetString(prefix+"bucket"),
		configuration.GetString(prefix+"directory"),
	)
}

func (g *GcsSink) SetSourceFiler(s *source.FilerSource) {
	g.filerSource = s
}

func (g *GcsSink) initialize(google_application_credentials, bucketName, dir string) error {
	g.bucket = bucketName
	g.dir = dir

	// Creates a client.
	if google_application_credentials == "" {
		var found bool
		google_application_credentials, found = os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
		if !found {
			glog.Fatalf("need to specific GOOGLE_APPLICATION_CREDENTIALS env variable or google_application_credentials in replication.toml")
		}
	}
	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(google_application_credentials))
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	g.client = client

	return nil
}

func (g *GcsSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	if isDirectory {
		key = key + "/"
	}

	if err := g.client.Bucket(g.bucket).Object(key).Delete(context.Background()); err != nil {
		return fmt.Errorf("gcs delete %s%s: %v", g.bucket, key, err)
	}

	return nil

}

func (g *GcsSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer.FileSize(entry)
	chunkViews := filer.ViewFromChunks(context.Background(), g.filerSource.LookupFileId, entry.GetChunks(), 0, int64(totalSize))

	wc := g.client.Bucket(g.bucket).Object(key).NewWriter(context.Background())
	defer wc.Close()

	writeFunc := func(data []byte) error {
		_, writeErr := wc.Write(data)
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

func (g *GcsSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {
	return true, g.CreateEntry(key, newEntry, signatures)
}
