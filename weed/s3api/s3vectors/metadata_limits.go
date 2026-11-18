package s3vectors

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// AWS S3 Vectors limits on a vector's key and metadata, on an index's
// metadata configuration, and on a query's filter, from
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html.
// Where AWS differs by index mode the ENHANCED value applies: this
// implementation prefilters, as ENHANCED indexes do, and takes no indexMode.
const (
	// vectorKeyMaxLength is the PutInputVector key length limit.
	vectorKeyMaxLength = 1024
	// vectorMetadataMaxBytes is the total metadata size per vector, which
	// also bounds a single non-filterable field (100 KB on ENHANCED indexes,
	// 40 KB on CLASSIC).
	vectorMetadataMaxBytes = 100 * 1024
	// vectorFilterableMetadataMaxBytes is the filterable metadata size per vector.
	vectorFilterableMetadataMaxBytes = 2 * 1024
	// vectorMetadataMaxKeys is the number of metadata keys per vector.
	vectorMetadataMaxKeys = 50
	// indexNonFilterableMaxKeys and nonFilterableKeyMaxLength bound
	// MetadataConfiguration.NonFilterableMetadataKeys at CreateIndex.
	indexNonFilterableMaxKeys = 10
	nonFilterableKeyMaxLength = 63
	// queryFilterMaxConstraints is the number of filter constraints per
	// QueryVectors request on an ENHANCED index.
	queryFilterMaxConstraints = 100
)

// filterConstraints counts a filter's constraints: one per comparison on a
// field, so {"a": 1} is one and {"a": {"$gte": 1, "$lt": 5}} is two;
// $and, $or and their nesting add none of their own.
func filterConstraints(filter map[string]interface{}) int {
	n := 0
	for key, cond := range filter {
		if strings.HasPrefix(key, "$") {
			if sub, ok := cond.([]interface{}); ok {
				for _, item := range sub {
					if m, ok := item.(map[string]interface{}); ok {
						n += filterConstraints(m)
					}
				}
			}
			continue
		}
		if ops, ok := cond.(map[string]interface{}); ok && len(ops) > 0 {
			n += len(ops)
		} else {
			n++
		}
	}
	return n
}

// checkFilterConstraints applies queryFilterMaxConstraints.
func checkFilterConstraints(filter map[string]interface{}) error {
	if n := filterConstraints(filter); n > queryFilterMaxConstraints {
		return fmt.Errorf("filter has %d constraints, the maximum is %d", n, queryFilterMaxConstraints)
	}
	return nil
}

// validateMetadataConfiguration applies the CreateIndex limits on the
// non-filterable metadata keys.
func validateMetadataConfiguration(cfg *MetadataConfiguration) error {
	if cfg == nil {
		return nil
	}
	keys := cfg.NonFilterableMetadataKeys
	if len(keys) > indexNonFilterableMaxKeys {
		return fmt.Errorf("nonFilterableMetadataKeys has %d keys, the maximum is %d", len(keys), indexNonFilterableMaxKeys)
	}
	for _, k := range keys {
		if n := utf8.RuneCountInString(k); n < 1 || n > nonFilterableKeyMaxLength {
			return fmt.Errorf("nonFilterableMetadataKeys entries must be 1 to %d characters", nonFilterableKeyMaxLength)
		}
	}
	return nil
}

// checkVectorMetadata applies the per-vector metadata limits. Sizes are the
// JSON encoding of the metadata, the form the request carried it in; the
// filterable size counts only the keys the index does not declare
// non-filterable.
func checkVectorMetadata(key string, metadata map[string]interface{}, cfg *MetadataConfiguration) error {
	if len(metadata) == 0 {
		return nil
	}
	if len(metadata) > vectorMetadataMaxKeys {
		return fmt.Errorf("vector %q has %d metadata keys, the maximum is %d", key, len(metadata), vectorMetadataMaxKeys)
	}
	var nonFilterable []string
	if cfg != nil {
		nonFilterable = cfg.NonFilterableMetadataKeys
	}
	total, filterable := 1, 1 // the braces, less the last member's comma
	for k, v := range metadata {
		size, err := metadataEntrySize(k, v)
		if err != nil {
			return fmt.Errorf("vector %q metadata %q: %v", key, k, err)
		}
		total += size
		if !slices.Contains(nonFilterable, k) {
			filterable += size
		}
	}
	if total > vectorMetadataMaxBytes {
		return fmt.Errorf("vector %q has %d bytes of metadata, the maximum is %d", key, total, vectorMetadataMaxBytes)
	}
	if filterable > vectorFilterableMetadataMaxBytes {
		return fmt.Errorf("vector %q has %d bytes of filterable metadata, the maximum is %d; declare large keys in nonFilterableMetadataKeys",
			key, filterable, vectorFilterableMetadataMaxBytes)
	}
	return nil
}

// metadataEntrySize is the encoded size of one `"key":value,` member.
func metadataEntrySize(k string, v interface{}) (int, error) {
	encodedKey, err := json.Marshal(k)
	if err != nil {
		return 0, err
	}
	encodedValue, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return len(encodedKey) + 1 + len(encodedValue) + 1, nil
}
