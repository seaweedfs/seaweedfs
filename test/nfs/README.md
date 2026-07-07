# SeaweedFS NFS Integration Tests

End-to-end tests that boot a real SeaweedFS cluster (`master` + `volume` +
`filer`) plus the experimental `weed nfs` frontend and drive it through the
NFSv3 wire protocol. The tests talk to the server over TCP using
`github.com/willscott/go-nfs-client`, which means they do **not** need a
kernel NFS mount, privileged ports, or any platform-specific tooling.

## Prerequisites

1. Build the `weed` binary:
   ```bash
   cd ../../weed
   go build -o weed .
   ```
2. Go 1.24 or later.

## Running the tests

```bash
# Build weed and run everything
make test

# Verbose output, keeps the subprocess stdout
make test-verbose

# Skip integration tests — useful when iterating on the framework itself
make test-short

# Run a single test
go test -v -run TestNfsBasicReadWrite ./...
```

Every test starts its own cluster on random loopback ports, so runs are
isolated and can execute in parallel.

## Layout

- `framework.go` — launches `weed master`, `weed volume`, `weed filer`, and
  `weed nfs` as subprocesses, waits for each to accept TCP, and exposes a
  `Mount()` helper that returns an `nfsclient.Target`.
- `basic_test.go` — covers the most common NFS operations:
  - Read/write round-trip (`TestNfsBasicReadWrite`)
  - Mkdir / ReadDirPlus / RmDir (`TestNfsMkdirAndRmdir`)
  - Nested directory + leaf file (`TestNfsNestedDirectories`)
  - Rename preserves content (`TestNfsRenamePreservesContent`)
  - Overwrite shrinks file size (`TestNfsOverwriteShrinksFile`)
  - Large binary file round-trip (`TestNfsLargeFile`)
  - Arbitrary binary and empty files (`TestNfsBinaryAndEmptyFiles`)
  - Symlink + Readlink (`TestNfsSymlinkRoundTrip`)
  - ReadDirPlus ordering sanity (`TestNfsReadDirPlusOrdering`)
  - Remove on missing path errors cleanly (`TestNfsRemoveMissingFailsCleanly`)
  - FSINFO advertises non-zero limits (`TestNfsFSInfoReturnsSaneLimits`)
  - Sequential append writes concatenate (`TestNfsAppendIsSequential`)
  - ReadDir after remove (`TestNfsReadDirAfterRemove`)

## Debugging a failing test

Keep the cluster temp dir for inspection:

```go
config := DefaultTestConfig()
config.SkipCleanup = true
```

Enable subprocess stdout/stderr:

```go
config := DefaultTestConfig()
config.EnableDebug = true
```

Or run with `-v`, which flips `EnableDebug` automatically via `testing.Verbose()`.

## Notes

- The NFS server binds to `127.0.0.1` with `-ip.bind=127.0.0.1` and exports
  `/nfs_export`. The test framework pre-creates that directory via the
  filer's HTTP API before starting the NFS server — the NFS server requires
  its export root to exist in the filer's namespace with a real entry, and
  the filer's synthetic `/` root does not match the `Name=="/"` check the
  NFS server performs during `ensureIndexedEntry`.
- Ports are allocated dynamically. Each test run opens a short-lived
  listener on `127.0.0.1:0`, reads back the assigned port, closes the
  listener, and hands the port to `weed master/volume/filer/nfs`. There is
  a tiny race window between close and reopen that has not been a problem
  in practice but is worth remembering if you see a "bind: address already
  in use" failure.
- All four `weed` components are started with explicit `-port.grpc=...`
  flags. Without them, the default is `-port + 10000`, which overflows
  `65535` whenever the HTTP port lands above `55535` — the kernel's
  ephemeral port range on macOS routinely does.
