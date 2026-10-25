# FUSE volume server failover tests

Integration tests for what happens to FUSE mounts when a volume server goes
away, comes back, or restarts underneath in-flight IO. They automate the manual
matrix reported in
[discussion #10206](https://github.com/seaweedfs/seaweedfs/discussions/10206):
one mount appends to a file while a second mount tails it, and a volume server
is stopped, started or restarted mid-stream.

The cluster is 1 master (`-defaultReplication=001`), 3 volume servers, 1 filer
and 2 mounts, all as local processes. With 001 every chunk has a copy on two of
the three servers, so losing any single server must be invisible to both mounts.

| Reported scenario | Test |
| --- | --- |
| control: append + tail with nothing failing | `TestAppendWithoutChaos` |
| read a file while one volume server is down | `TestReadWithVolumeServerDown` |
| "STOP volumes": append + tail, kill a server mid-stream | `TestAppendWhileVolumeServerStops` |
| "Start volumes": append + tail with a server down, start it mid-stream | `TestAppendWhileVolumeServerStarts` |
| "Re-start volumes": append + tail, restart a server mid-stream | `TestAppendWhileVolumeServerRestarts` |
| part 2: large file copy instead of small appends | `TestLargeWriteWhileVolumeServerStops` |

Volume servers are dropped with SIGKILL, the closest local equivalent of a Swarm
task disappearing from the overlay network: no deregistration, and the address
stops answering.

## Running

```bash
go build -o weed/weed ./weed
WEED_BINARY=$PWD/weed/weed go test -v -count=1 -timeout=30m ./test/fuse_failover/...
```

Needs FUSE and, on Linux, a working `/dev/fuse`. Logs from a failed run are
copied to `/tmp/seaweedfs-fuse-failover-logs/`.
