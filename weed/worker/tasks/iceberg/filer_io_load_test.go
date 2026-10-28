package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// A commit that outgrew the request-size limit stores no inline metadata: the
// record keeps only metadataLocation. Maintenance must follow it or the table
// is skipped on every scan.
func TestLoadCurrentMetadataFollowsMetadataLocation(t *testing.T) {
	fs, client := startFakeFiler(t)
	ctx := context.Background()

	setup := tableSetup{BucketName: "b", Namespace: "ns", TableName: "t"}
	meta := buildTestMetadata(t, nil, nil, 0, nil, nil, nil)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}

	bucketPath := path.Join(s3tables.TablesPath, setup.BucketName)
	nsPath := path.Join(bucketPath, setup.Namespace)
	tableDir := path.Join(bucketPath, setup.tablePath())
	metadataFileName := "v1.metadata.json"
	metadataLocation := fmt.Sprintf("s3://%s/%s/metadata/%s", setup.BucketName, setup.tablePath(), metadataFileName)

	fs.putEntry(s3tables.TablesPath, setup.BucketName, &filer_pb.Entry{
		Name:        setup.BucketName,
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("true")},
	})
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{
		Name:        setup.Namespace,
		IsDirectory: true,
	})
	fs.putEntry(path.Join(tableDir, "metadata"), metadataFileName, &filer_pb.Entry{
		Name:       metadataFileName,
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(metaJSON))},
		Content:    metaJSON,
	})

	xattr, err := json.Marshal(map[string]interface{}{
		"metadataVersion":  7,
		"metadataLocation": metadataLocation,
	})
	if err != nil {
		t.Fatalf("marshal xattr: %v", err)
	}
	fs.putEntry(nsPath, setup.TableName, &filer_pb.Entry{
		Name:        setup.TableName,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyMetadata:        xattr,
			s3tables.ExtendedKeyMetadataVersion: metadataVersionXattr(7),
		},
	})

	state, err := loadCurrentMetadata(ctx, client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	if state.Metadata == nil {
		t.Fatal("loadCurrentMetadata returned no metadata for a location-only record")
	}
	if got := state.Metadata.Location(); got != "s3://test-bucket/test-table" {
		t.Fatalf("metadata location = %q, want the file's own location", got)
	}
	if state.MetadataFileName != metadataFileName {
		t.Fatalf("MetadataFileName = %q, want %q", state.MetadataFileName, metadataFileName)
	}
	if state.DataPath != setup.tablePath() {
		t.Fatalf("DataPath = %q, want %q", state.DataPath, setup.tablePath())
	}
}
