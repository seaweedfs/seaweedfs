package iceberg

import (
	"sort"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// compactionOrder names the column by whose bounds a bin's input files are
// ordered before they are merged: the first identity field of the table's
// sort order when it declares one, otherwise the first column every candidate
// file carries bounds for. Rows are copied file by file, so a merged file is
// as ordered as the sequence of its inputs.
type compactionOrder struct {
	fieldID    int
	typ        iceberg.PrimitiveType
	descending bool
	// bestEffort marks an order inferred from column bounds rather than the
	// table's declared sort order; runs it cannot form may still merge by
	// size since contiguous ranges are a preference, not a requirement.
	bestEffort bool
}

func resolveCompactionOrder(meta table.Metadata, entries []iceberg.ManifestEntry) *compactionOrder {
	if meta == nil || meta.CurrentSchema() == nil || len(entries) == 0 {
		return nil
	}
	schema := meta.CurrentSchema()
	if sortOrder := meta.SortOrder(); !sortOrder.IsUnsorted() {
		for _, sortField := range sortOrder.Fields() {
			if _, ok := sortField.Transform.(iceberg.IdentityTransform); !ok {
				continue
			}
			if field, ok := schema.FindFieldByID(sortField.SourceID()); ok {
				if typ, ok := field.Type.(iceberg.PrimitiveType); ok {
					return &compactionOrder{fieldID: field.ID, typ: typ, descending: sortField.Direction == table.SortDESC}
				}
			}
		}
	}
	// A column only orders the merge when every entry in the group carries a
	// bound for it. The check is scoped to the entries handed in — a bin's
	// eligible files — so an unrelated file without bounds (oversized, a
	// different format, or another partition) cannot disable ordering here.
	for _, field := range schema.Fields() {
		typ, ok := field.Type.(iceberg.PrimitiveType)
		if !ok {
			continue
		}
		candidate := &compactionOrder{fieldID: field.ID, typ: typ, bestEffort: true}
		complete := true
		for _, entry := range entries {
			if _, ok := candidate.key(entry); !ok {
				complete = false
				break
			}
		}
		if complete {
			return candidate
		}
	}
	return nil
}

// key is the bound a file is ordered by: its lower bound on the column for an
// ascending order, its upper bound for a descending one.
func (o *compactionOrder) key(entry iceberg.ManifestEntry) (iceberg.Literal, bool) {
	df := entry.DataFile()
	raw := df.LowerBoundValues()[o.fieldID]
	if o.descending {
		raw = df.UpperBoundValues()[o.fieldID]
	}
	if raw == nil {
		return nil, false
	}
	lit, err := iceberg.LiteralFromBytes(o.typ, raw)
	return lit, err == nil
}

// sortEntries orders a bin's files by their key. Files without a key sort
// after the rest, in their original order.
func (o *compactionOrder) sortEntries(entries []iceberg.ManifestEntry) {
	if o == nil || len(entries) < 2 {
		return
	}
	type keyed struct {
		key iceberg.Literal
		ok  bool
	}
	keys := make([]keyed, len(entries))
	idx := make([]int, len(entries))
	for i, entry := range entries {
		k, ok := o.key(entry)
		keys[i], idx[i] = keyed{k, ok}, i
	}
	sort.SliceStable(idx, func(i, j int) bool {
		a, b := keys[idx[i]], keys[idx[j]]
		if a.ok != b.ok {
			return a.ok
		}
		if !a.ok {
			return false
		}
		c, ok := compareLiterals(a.key, b.key)
		if o.descending {
			c = -c
		}
		return ok && c < 0
	})
	sorted := make([]iceberg.ManifestEntry, len(entries))
	for i, j := range idx {
		sorted[i] = entries[j]
	}
	copy(entries, sorted)
}

// splitOrderedBin splits a bin whose files are in bound order into runs of
// consecutive files that stay under targetSize, so every output covers one
// contiguous range of the ordering column. The returned entries are the runs
// too short to reach minFiles: the caller can still merge them by size when
// contiguous ranges are only a preference, or leave them for a later pass
// when order must hold.
func splitOrderedBin(bin compactionBin, targetSize int64, minFiles int) (valid []compactionBin, leftover []iceberg.ManifestEntry) {
	newBin := func() compactionBin {
		return compactionBin{PartitionKey: bin.PartitionKey, Partition: bin.Partition, SpecID: bin.SpecID}
	}
	current := newBin()
	flush := func() {
		if len(current.Entries) >= minFiles {
			valid = append(valid, current)
		} else {
			leftover = append(leftover, current.Entries...)
		}
		current = newBin()
	}
	for _, entry := range bin.Entries {
		size := entry.DataFile().FileSizeBytes()
		if current.TotalSize > 0 && current.TotalSize+size > targetSize {
			flush()
		}
		current.Entries = append(current.Entries, entry)
		current.TotalSize += size
	}
	flush()
	return valid, leftover
}

// compareLiterals orders two literals of the same type; false when they are
// not comparable.
func compareLiterals(a, b iceberg.Literal) (int, bool) {
	switch x := a.(type) {
	case iceberg.TypedLiteral[bool]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[int32]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[int64]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[float32]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[float64]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[iceberg.Date]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[iceberg.Time]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[iceberg.Timestamp]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[iceberg.TimestampNano]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[string]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[[]byte]:
		return compareTyped(x, b)
	case iceberg.TypedLiteral[iceberg.Decimal]:
		return compareTyped(x, b)
	}
	return 0, false
}

func compareTyped[T iceberg.LiteralType](a iceberg.TypedLiteral[T], b iceberg.Literal) (int, bool) {
	bt, ok := b.(iceberg.TypedLiteral[T])
	if !ok {
		return 0, false
	}
	return a.Comparator()(a.Value(), bt.Value()), true
}
