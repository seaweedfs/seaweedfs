//go:build !foundationdb

package filer

// ManifestBatch is how many data chunks are folded into one manifest chunk, so
// that an entry's chunk list stays within the largest value its metadata store
// will accept. Stores reached by this build hold values far larger than the
// list 10000 chunks encode to; see manifest_batch_foundationdb.go for the
// FoundationDB build, whose values stop at 100,000 bytes.
const ManifestBatch = 10000
