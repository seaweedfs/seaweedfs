package gcssink

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type GcsSink struct {
	client      *storage.Client
	bucket      string
	dir         string
	filerSource *source.FilerSource
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

func (g *GcsSink) Initialize(configuration util.Configuration, prefix string) error {
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

func (g *GcsSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool) error {

	if isDirectory {
		key = key + "/"
	}

	if err := g.client.Bucket(g.bucket).Object(key).Delete(context.Background()); err != nil {
		return fmt.Errorf("gcs delete %s%s: %v", g.bucket, key, err)
	}

	return nil

}

func (g *GcsSink) CreateEntry(key string, entry *filer_pb.Entry) error {

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer2.TotalSize(entry.Chunks)
	chunkViews := filer2.ViewFromChunks(entry.Chunks, 0, int64(totalSize))

	wc := g.client.Bucket(g.bucket).Object(key).NewWriter(context.Background())

	for _, chunk := range chunkViews {

		fileUrl, err := g.filerSource.LookupFileId(chunk.FileId)
		if err != nil {
			return err
		}

		err = util.ReadUrlAsStream(fileUrl, nil, false, chunk.IsFullChunk(), chunk.Offset, int(chunk.Size), func(data []byte) {
			wc.Write(data)
		})

		if err != nil {
			return err
		}

	}

	if err := wc.Close(); err != nil {
		return err
	}

	return nil

}

func (g *GcsSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error) {
	// TODO improve efficiency
	return false, nil
}
