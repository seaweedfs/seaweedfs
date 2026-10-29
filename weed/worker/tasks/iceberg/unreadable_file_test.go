package iceberg

import (
	"bytes"
	"context"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type positionDeleteTestRow struct {
	FilePath string `parquet:"file_path"`
	Pos      int64  `parquet:"pos"`
}

func positionDeleteParquet(t *testing.T, rows []positionDeleteTestRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[positionDeleteTestRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close parquet: %v", err)
	}
	return buf.Bytes()
}

// noPanic runs fn and turns a panic into a test failure. A panic in a
// maintenance goroutine is not recovered anywhere and ends the process
// hosting the worker.
func noPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	fn()
}

// putInlineFile stores body as the file at an Iceberg path of the table.
func putInlineFile(t *testing.T, fs *fakeFilerServer, bucket, dataPath, file string, body []byte) {
	t.Helper()
	full, err := icebergFilerPath(bucket, dataPath, file)
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	fs.putEntry(path.Dir(full), path.Base(full), &filer_pb.Entry{
		Name:       path.Base(full),
		Content:    body,
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(body))},
	})
}

// corruptPageFile is a position delete file whose footer is intact but some
// bytes of its data pages are altered (the writer recorded no page CRC). It
// opens; decoding its rows panics inside parquet-go.
func corruptPageFile(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/corrupt_page_deletes.parquet")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	return b
}

func TestUnopenableParquetFileIsAnErrorNotAPanic(t *testing.T) {
	good := positionDeleteParquet(t, []positionDeleteTestRow{{"s3://b/ns/t/data/f1.parquet", 1}})
	for name, body := range map[string][]byte{
		"wrong magic":      append([]byte("XXXX"), good[4:]...),
		"truncated footer": good[:len(good)-6],
		"not parquet":      []byte(`{"not": "parquet"}`),
		"empty":            {},
	} {
		t.Run(name, func(t *testing.T) {
			fs, client := startFakeFiler(t)
			const bucket, dataPath, file = "b", "ns/t", "data/bad-deletes.parquet"
			putInlineFile(t, fs, bucket, dataPath, file, body)

			var err error
			noPanic(t, func() {
				_, err = readPositionDeleteFile(context.Background(), client, bucket, dataPath, file)
			})
			if err == nil || !strings.Contains(err.Error(), file) {
				t.Fatalf("position delete read: err = %v, want an error naming %s", err, file)
			}
			noPanic(t, func() {
				_, err = readEqualityDeleteFile(context.Background(), client, bucket, dataPath, file, []int{1}, newTestSchema())
			})
			if err == nil || !strings.Contains(err.Error(), file) {
				t.Fatalf("equality delete read: err = %v, want an error naming %s", err, file)
			}
		})
	}
}

func TestCorruptPageIsAnErrorNamingTheFileNotAPanic(t *testing.T) {
	body := corruptPageFile(t)

	// Control: parquet-go itself panics while decoding this file's rows.
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		r := parquet.NewReader(bytes.NewReader(body))
		defer r.Close()
		rows := make([]parquet.Row, 64)
		for {
			if _, err := r.ReadRows(rows); err != nil {
				return false
			}
		}
	}()
	if !panicked {
		t.Fatal("control: decoding the corrupt page no longer panics parquet-go; the test file needs replacing")
	}

	fs, client := startFakeFiler(t)
	const bucket, dataPath, file = "b", "ns/t", "data/corrupt-deletes.parquet"
	putInlineFile(t, fs, bucket, dataPath, file, body)
	var err error
	noPanic(t, func() {
		_, err = readPositionDeleteFile(context.Background(), client, bucket, dataPath, file)
	})
	if err == nil || !strings.Contains(err.Error(), file) || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("err = %v, want an error naming %s", err, file)
	}
}

func TestCompactionWithACorruptDeleteFileFailsWithoutCommitting(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{3, "carol"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
		},
		nil,
	)
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	body := corruptPageFile(t)
	fs.putEntry(dataDir, "pd1.parquet", &filer_pb.Entry{Name: "pd1.parquet", Content: body,
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(body))}})
	before, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 256 * 1024 * 1024, MinInputFiles: 2, MaxCommitRetries: 3, ApplyDeletes: true}
	var result string
	noPanic(t, func() {
		result, _, err = handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	})
	if err == nil || !strings.Contains(err.Error(), "pd1.parquet") {
		t.Fatalf("result %q, err = %v: want an error naming the delete file", result, err)
	}
	after, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if after.Metadata.CurrentSnapshot().SnapshotID != before.Metadata.CurrentSnapshot().SnapshotID {
		t.Fatal("a snapshot was committed without the delete file applied")
	}
}

func TestAnUnreadableDataFileSkipsItsBinAndTheOtherBinsCompact(t *testing.T) {
	fs, client := startFakeFiler(t)
	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: iceberg.IdentityTransform{},
	})
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	row := func(id int64, name string) []struct {
		ID   int64
		Name string
	} {
		return []struct {
			ID   int64
			Name string
		}{{id, name}}
	}
	populatePartitionedDataTable(t, fs, setup, partitionSpec, [][]partitionedTestFile{
		{{Name: "us-1.parquet", Partition: map[int]any{1000: "us"}, Rows: row(1, "us")}},
		{{Name: "us-2.parquet", Partition: map[int]any{1000: "us"}, Rows: row(2, "us")}},
		{
			{Name: "eu-1.parquet", Partition: map[int]any{1000: "eu"}, Rows: row(3, "eu")},
			{Name: "eu-2.parquet", Partition: map[int]any{1000: "eu"}, Rows: row(4, "eu")},
		},
	})
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	eu2 := fs.getEntry(dataDir, "eu-2.parquet")
	broken := append([]byte("XXXX"), eu2.Content[4:]...) // wrong magic
	fs.putEntry(dataDir, "eu-2.parquet", &filer_pb.Entry{Name: "eu-2.parquet", Content: broken, Attributes: eu2.Attributes})

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 256 * 1024 * 1024, MinInputFiles: 2, MaxCommitRetries: 3}
	var result string
	var err error
	noPanic(t, func() {
		result, _, err = handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	})
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted 2 files into 1") {
		t.Fatalf("result %q: want the readable bin compacted", result)
	}
	live := liveDataFiles(t, client, setup)
	for _, name := range []string{"eu-1.parquet", "eu-2.parquet"} {
		found := false
		for _, df := range live {
			if strings.HasSuffix(df.FilePath(), name) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("the skipped bin's %s is not in the current snapshot", name)
		}
	}
}

func TestMergeFailsOnACorruptPageNamingTheFile(t *testing.T) {
	fs, client := startFakeFiler(t)
	fs.putEntry("/buckets/test-bucket/ns/tbl/data", "corrupt.parquet", &filer_pb.Entry{
		Name:    "corrupt.parquet",
		Content: corruptPageFile(t),
	})

	// Its footer reads fine, so the panic comes from its pages inside the
	// merge's row decoder.
	var err error
	noPanic(t, func() {
		_, _, err = mergeParquetFilesInto(context.Background(), client, "test-bucket", "ns/tbl",
			[]iceberg.ManifestEntry{schemaFileEntry(t, "data/corrupt.parquet", 1)},
			nil, nil, newTestSchema(), 0, false, nil)
	})
	if err == nil || !strings.Contains(err.Error(), "corrupt.parquet") || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("err = %v, want an error naming corrupt.parquet", err)
	}
}
