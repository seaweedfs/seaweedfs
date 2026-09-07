package s3tables

import (
	"bytes"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

const secondsPerDay = int64(24 * 60 * 60)

// ReadManifest parses an Iceberg manifest and converts its partition values
// while the manifest's own schema still describes them.
//
// iceberg-go converts what the Avro decoder returned for a logical type - a
// time.Time for a date, a time.Duration for a time - to an Iceberg value
// lazily, on the first Partition() call, using the logical types it read from
// the manifest being parsed. ManifestWriter.addEntry rebinds those to the
// logical types of the manifest it is about to write before it makes that
// call, so an entry that is read and written again without anyone looking at
// its partition converts against the wrong schema. A day partition is where
// the two disagree: iceberg-go's day transform reports an int32 result type,
// so the manifest it writes has no date logical type for that field, nothing
// converts, and the time.Time reaches the encoder as "cannot use time.Time
// with Avro type int".
//
// What survives that is a value iceberg-go never recognized on read either: it
// takes a partition field's logical type from the last branch of its Avro
// union, so a writer that spells an optional partition [<type>, null] rather
// than [null, <type>] hides it, and a time partition is then worse than a
// failed write - time.Duration converts to int64 nanoseconds and silently
// records the wrong value. normalizePartitionValue puts those back.
//
// specs maps partition spec ID to spec the way the table metadata records them.
func ReadManifest(m iceberg.ManifestFile, manifest []byte, discardDeleted bool, specs map[int]iceberg.PartitionSpec, schema *iceberg.Schema) ([]iceberg.ManifestEntry, error) {
	entries, err := iceberg.ReadManifest(m, bytes.NewReader(manifest), discardDeleted)
	if err != nil {
		return nil, err
	}

	var partitionFields []iceberg.NestedField
	var spec iceberg.PartitionSpec
	var specFound bool
	if schema != nil {
		spec, specFound = specs[int(m.PartitionSpecID())]
		if specFound {
			if partitionType := spec.PartitionType(schema); partitionType != nil {
				partitionFields = partitionType.FieldList
			}
		}
	}

	for i, entry := range entries {
		partition := entry.DataFile().Partition()
		changed := false
		for _, field := range partitionFields {
			value, ok := partition[field.ID]
			if !ok {
				continue
			}
			if normalized, ok := normalizePartitionValue(value, field.Type); ok {
				partition[field.ID] = normalized
				changed = true
			}
		}
		if changed {
			// iceberg-go returns a defensive copy from Partition(), so the
			// in-place edits above never reach the underlying DataFile.
			// Rebuild the entry with a DataFile that carries the normalized
			// partition so a subsequent write encodes the Iceberg values.
			entries[i] = rebuildManifestEntry(entry, spec, partition)
		}
	}
	return entries, nil
}

// rebuildManifestEntry returns a copy of entry whose DataFile carries the
// given partition map. It is used to persist partition values normalized
// after a read, since iceberg-go's Partition() getter hands back a clone
// rather than the live map. Only the partition changes; every other DataFile
// field is copied through the builder so manifest round-trips are preserved.
func rebuildManifestEntry(entry iceberg.ManifestEntry, spec iceberg.PartitionSpec, partition map[int]any) iceberg.ManifestEntry {
	df := entry.DataFile()
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		df.ContentType(),
		df.FilePath(),
		df.FileFormat(),
		partition,
		nil, // logical types are derived from the spec at write time
		nil,
		df.Count(),
		df.FileSizeBytes(),
	)
	if err != nil {
		// The original DataFile already validated these fields, so the only
		// way to get here is a nil spec, which rebuildManifestEntry's caller
		// guards against. Fall back to the original entry rather than panic.
		return entry
	}

	builder.BlockSizeInBytes(0) // deprecated in v2; the original is not exposed
	if sizes := df.ColumnSizes(); sizes != nil {
		builder.ColumnSizes(sizes)
	}
	if counts := df.ValueCounts(); counts != nil {
		builder.ValueCounts(counts)
	}
	if counts := df.NullValueCounts(); counts != nil {
		builder.NullValueCounts(counts)
	}
	if counts := df.NaNValueCounts(); counts != nil {
		builder.NaNValueCounts(counts)
	}
	if counts := df.DistinctValueCounts(); counts != nil {
		builder.DistinctValueCounts(counts)
	}
	if bounds := df.LowerBoundValues(); bounds != nil {
		builder.LowerBoundValues(bounds)
	}
	if bounds := df.UpperBoundValues(); bounds != nil {
		builder.UpperBoundValues(bounds)
	}
	if key := df.KeyMetadata(); key != nil {
		builder.KeyMetadata(key)
	}
	if offsets := df.SplitOffsets(); offsets != nil {
		builder.SplitOffsets(offsets)
	}
	if ids := df.EqualityFieldIDs(); ids != nil {
		builder.EqualityFieldIDs(ids)
	}
	if id := df.SortOrderID(); id != nil {
		builder.SortOrderID(*id)
	}
	if id := df.FirstRowID(); id != nil {
		builder.FirstRowID(*id)
	}
	if ref := df.ReferencedDataFile(); ref != nil {
		builder.ReferencedDataFile(*ref)
	}
	if off := df.ContentOffset(); off != nil {
		builder.ContentOffset(*off)
	}
	if size := df.ContentSizeInBytes(); size != nil {
		builder.ContentSizeInBytes(*size)
	}

	newEntry := iceberg.NewManifestEntryBuilder(entry.Status(), ptrInt64(entry.SnapshotID()), builder.Build())
	if seq := entry.SequenceNum(); seq != -1 {
		newEntry.SequenceNum(seq)
	}
	if fileSeq := entry.FileSequenceNum(); fileSeq != nil {
		newEntry.FileSequenceNum(*fileSeq)
	}
	return newEntry.Build()
}

func ptrInt64(v int64) *int64 { return &v }

// normalizePartitionValue converts one value the Avro decoder returned for a
// logical type to the Iceberg representation the manifest writer expects,
// mirroring what iceberg-go does itself for the unions it does recognize.
func normalizePartitionValue(value any, fieldType iceberg.Type) (any, bool) {
	switch v := value.(type) {
	case time.Time:
		utc := v.UTC()
		switch fieldType.(type) {
		case iceberg.DateType, iceberg.Int32Type:
			// A day transform reports an int32 result type and an identity
			// transform on a date column reports date; both hold days.
			midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
			return iceberg.Date(midnight.Unix() / secondsPerDay), true
		case iceberg.TimestampType, iceberg.TimestampTzType:
			return iceberg.Timestamp(utc.UnixMicro()), true
		case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
			return iceberg.TimestampNano(utc.UnixNano()), true
		}
	case time.Duration:
		if _, ok := fieldType.(iceberg.TimeType); ok {
			return iceberg.Time(v.Microseconds()), true
		}
	case [16]byte:
		if _, ok := fieldType.(iceberg.UUIDType); ok {
			return uuid.UUID(v), true
		}
	}
	return nil, false
}
