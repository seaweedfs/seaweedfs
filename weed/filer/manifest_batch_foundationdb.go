//go:build foundationdb

package filer

// ManifestBatch is how many data chunks are folded into one manifest chunk. A
// FoundationDB value stops at 100,000 bytes (foundationdb.FDB_VALUE_SIZE_LIMIT)
// and an entry's whole chunk list is one value, which at ~100 bytes per chunk
// record is about 1000 chunks -- so folding at 10000 never ran before the write
// was already too large to store.
//
// A single fold level leaves (chunks/batch) manifest pointers plus up to
// (batch-1) unfolded chunks in the entry, so the reachable chunk count is
// highest when the two terms are near equal. 500 is that optimum for a
// 100,000-byte budget: it keeps an entry inside the limit up to ~250,000
// chunks, ~1 TB at the default -maxMB=4, against ~4 GB before. Larger files
// need nested packing, which no batch size substitutes for.
//
// This is a property of the build rather than of the configured store: the
// FoundationDB image builds one binary for the filer and for every client that
// folds (S3, mount, WebDAV, weed shell, filer.copy), and each of them has to
// agree on a list the filer's store can hold. A binary built with this tag but
// pointed at another store folds earlier than that store requires, which costs
// one manifest blob per 500 chunks and one read to resolve it.
//
// See issue #11158.
const ManifestBatch = 500
