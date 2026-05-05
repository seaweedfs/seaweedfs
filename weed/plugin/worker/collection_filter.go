package pluginworker

// CollectionFilterMode controls how collections are interpreted during
// detection. The two recognized sentinels short-circuit regex matching:
//   - CollectionFilterAll: pool every collection together (default).
//   - CollectionFilterEach: run detection separately per collection.
//
// Any other non-empty value is treated as a regex.
type CollectionFilterMode string

const (
	CollectionFilterAll  CollectionFilterMode = "ALL_COLLECTIONS"
	CollectionFilterEach CollectionFilterMode = "EACH_COLLECTION"
)
