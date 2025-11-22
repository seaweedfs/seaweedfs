/*
Package foundationdb provides a FoundationDB-based filer store for SeaweedFS.

FoundationDB is a distributed ACID database with strong consistency guarantees
and excellent scalability characteristics. This filer store leverages FDB's
directory layer for organizing file metadata and its key-value interface for
efficient storage and retrieval.

The referenced "github.com/apple/foundationdb/bindings/go/src/fdb" library
requires FoundationDB client libraries to be installed.
So this is only compiled with "go build -tags foundationdb".
*/
package foundationdb
