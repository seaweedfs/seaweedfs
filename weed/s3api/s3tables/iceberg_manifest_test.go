package s3tables

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// A manifest whose partition union puts the value branch first hides the
// logical type from iceberg-go, which then hands back whatever the Avro decoder
// produced. Rewriting such an entry fails when the writer builds its partition
// summaries, or encodes the wrong number for a time partition, so every logical
// type a partition field can carry has to come back as an Iceberg value.
func TestReadManifestNormalizesForeignPartitions(t *testing.T) {
	day := time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name        string
		sourceType  iceberg.PrimitiveType
		transform   iceberg.Transform
		logicalType string
		// decoded is the value the foreign manifest carries, as the Avro
		// decoder hands it back.
		decoded any
		want    any
	}{
		{
			name:        "day transform on a timestamp column",
			sourceType:  iceberg.PrimitiveTypes.Timestamp,
			transform:   iceberg.DayTransform{},
			logicalType: "date",
			decoded:     day,
			want:        iceberg.Date(20737),
		},
		{
			name:        "identity transform on a date column",
			sourceType:  iceberg.PrimitiveTypes.Date,
			transform:   iceberg.IdentityTransform{},
			logicalType: "date",
			decoded:     day,
			want:        iceberg.Date(20737),
		},
		{
			name:        "identity transform on a timestamp column",
			sourceType:  iceberg.PrimitiveTypes.Timestamp,
			transform:   iceberg.IdentityTransform{},
			logicalType: "timestamp-micros",
			decoded:     day.Add(3 * time.Hour),
			want:        iceberg.Timestamp(day.Add(3 * time.Hour).UnixMicro()),
		},
		{
			name:        "identity transform on a time column",
			sourceType:  iceberg.PrimitiveTypes.Time,
			transform:   iceberg.IdentityTransform{},
			logicalType: "time-micros",
			decoded:     3 * time.Hour,
			want:        iceberg.Time((3 * time.Hour).Microseconds()),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "source", Type: c.sourceType, Required: true},
			)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: c.transform,
			})
			specs := map[int]iceberg.PartitionSpec{spec.ID(): spec}

			dfBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "data/file.parquet",
				iceberg.ParquetFile, map[int]any{1000: c.want}, nil, nil, 1, 1)
			if err != nil {
				t.Fatalf("build data file: %v", err)
			}
			snapshotID := int64(1)
			entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dfBuilder.Build())

			foreignBytes, foreignManifest := s3tablestest.ForeignPartitionManifest(t, schema, spec, entry,
				"metadata/foreign-manifest.avro", c.logicalType, c.decoded)

			// Without the shim the entry still carries the decoder's value, so
			// the fixture reproduces what the foreign writer leaves behind.
			raw, err := iceberg.ReadManifest(foreignManifest, bytes.NewReader(foreignBytes), true)
			if err != nil {
				t.Fatalf("read foreign manifest: %v", err)
			}
			if got := raw[0].DataFile().Partition()[1000]; got != c.decoded {
				t.Fatalf("unnormalized partition value = %#v (%T), want the decoded %T", got, got, c.decoded)
			}

			entries, err := ReadManifest(foreignManifest, foreignBytes, true, specs, schema)
			if err != nil {
				t.Fatalf("read foreign manifest: %v", err)
			}
			if got := entries[0].DataFile().Partition()[1000]; got != c.want {
				t.Fatalf("normalized partition value = %#v (%T), want %#v (%T)", got, got, c.want, c.want)
			}

			// The point of normalizing: the entry can be written out again.
			var buf bytes.Buffer
			if _, err := iceberg.WriteManifest("metadata/manifest.avro", &buf, 2, spec, schema, snapshotID, entries); err != nil {
				t.Fatalf("write normalized manifest: %v", err)
			}
		})
	}
}

// A manifest written under a spec the table metadata no longer records is
// returned as read rather than refused: the caller decides what to do with it.
func TestReadManifestUnknownSpec(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "source", Type: iceberg.PrimitiveTypes.Date, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})
	dfBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "data/file.parquet",
		iceberg.ParquetFile, map[int]any{1000: iceberg.Date(20737)}, nil, nil, 1, 1)
	if err != nil {
		t.Fatalf("build data file: %v", err)
	}
	snapshotID := int64(1)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dfBuilder.Build())
	foreignBytes, foreignManifest := s3tablestest.ForeignPartitionManifest(t, schema, spec, entry,
		"metadata/foreign-manifest.avro", "date", time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC))

	entries, err := ReadManifest(foreignManifest, foreignBytes, true, nil, schema)
	if err != nil {
		t.Fatalf("read foreign manifest: %v", err)
	}
	if _, ok := entries[0].DataFile().Partition()[1000].(time.Time); !ok {
		t.Fatalf("partition value = %T, want it returned as read", entries[0].DataFile().Partition()[1000])
	}
}
