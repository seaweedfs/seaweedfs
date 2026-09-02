package s3tables

import (
	"bytes"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

const secondsPerDay = int64(24 * 60 * 60)

// ReadManifest parses an Iceberg manifest, restoring partition values that a
// foreign writer's Avro schema hides from iceberg-go.
//
// iceberg-go takes a partition field's logical type from the last branch of its
// Avro union, so a writer that spells an optional day partition [date, null]
// rather than [null, date] leaves the value as the time.Time the Avro decoder
// produced. Rewriting such an entry then fails, either in the manifest writer's
// partition summaries ("expected type iceberg.Date, got time.Time") or in the
// encoder itself ("cannot use time.Time with Avro type int"), and a time
// partition is worse: time.Duration converts to int64 nanoseconds and silently
// records the wrong value.
//
// specs maps partition spec ID to spec the way the table metadata records them.
// Entries from a manifest written under an unknown spec are returned as read.
func ReadManifest(m iceberg.ManifestFile, manifest []byte, discardDeleted bool, specs map[int]iceberg.PartitionSpec, schema *iceberg.Schema) ([]iceberg.ManifestEntry, error) {
	entries, err := iceberg.ReadManifest(m, bytes.NewReader(manifest), discardDeleted)
	if err != nil {
		return nil, err
	}
	spec, found := specs[int(m.PartitionSpecID())]
	if !found || schema == nil {
		return entries, nil
	}

	partitionType := spec.PartitionType(schema)
	if partitionType == nil {
		return entries, nil
	}
	for _, entry := range entries {
		partition := entry.DataFile().Partition()
		for _, field := range partitionType.FieldList {
			value, ok := partition[field.ID]
			if !ok {
				continue
			}
			if normalized, ok := normalizePartitionValue(value, field.Type); ok {
				partition[field.ID] = normalized
			}
		}
	}
	return entries, nil
}

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
