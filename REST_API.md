# SeaweedFS HTTP REST API

SeaweedFS exposes three HTTP surfaces:

| Service | Default port | Addressing |
|---------|--------------|------------|
| Filer | 8888 | File system paths (`/dir/name`) |
| Master | 9333 | File id assignment and cluster topology |
| Volume server | 8080 | File content by file id (`vid,fid`) |

Most clients only need the filer API (paths) or the S3 API. The master and
volume APIs are the lower-level blob store interface.

Conventions applying to all three:

- Responses are JSON unless noted otherwise. Append `&pretty=y` to pretty-print.
- A file id (`fid`) has the form `volumeId,fileKeyCookie`, e.g. `3,01637037d6`.
  An optional suffix selects a reserved id from a `count` assignment
  (`3,01637037d6_1`, `_2`, ...), and an optional extension
  (`3,01637037d6.jpg`) sets the content type on reads.
- `replication` is a 3-digit replica placement `xyz`: `x` copies in other
  data centers, `y` on other racks in the same data center, `z` on other
  volume servers on the same rack. `000` = no replication, `001` = one copy
  on the same rack, `010` = one copy on a different rack, `100` = one copy in
  another data center, `200` = two copies in two other data centers, `110` =
  one copy in another data center plus one on another rack.
- `ttl` units: `m` minute, `h` hour, `d` day, `w` week, `M` month, `y` year.

## Filer API (port 8888)

The filer presents a POSIX-like namespace over the volume servers.

### Upload a file

```bash
# PUT the raw body to the target path
curl -T /home/chris/myphoto.jpg "http://localhost:8888/dir/myphoto.jpg"

# or POST as multipart form (the part filename becomes the entry name)
curl -F file=@/home/chris/myphoto.jpg "http://localhost:8888/dir/"
```

Response `201 Created`:

```json
{"name":"myphoto.jpg","size":43234,"eTag":"0x6c656...","mtime":"...","chunks":[...]}
```

Query parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `collection` | collection name | empty |
| `replication` | replica placement code | filer default |
| `ttl` | file expiration, e.g. `3d` | never |
| `disk` | disk type to store on | filer default |
| `fsync` | `true` fsyncs on the volume server | false |
| `dataCenter` | preferred data center | empty |
| `rack` | preferred rack | empty |
| `dataNode` | preferred volume server | empty |
| `saveInside` | store small content inside the metadata instead of a volume | false |
| `maxMB` | split the upload into chunks of this many MB | filer `-maxMB` |
| `mode` | unix permission bits, e.g. `0644` | `0664` |
| `op` | `append` appends to an existing file | overwrite |
| `skipCheckParentDir` | `true` skips the parent-directory existence check | false |

### Create a directory

```bash
curl -X POST "http://localhost:8888/dir/newdir/"
```

A POST to a path ending in `/` with no content creates the directory,
including missing parents.

### Read a file

```bash
curl "http://localhost:8888/dir/myphoto.jpg"
```

Supports `Range` requests (`Accept-Ranges: bytes`), `ETag`, and the
`If-None-Match` / `If-Modified-Since` conditional headers. `HEAD` returns
headers only. Entry headers stored as extended attributes are echoed back,
minus internal `Seaweed-` and `xattr-` keys.

Entry metadata instead of content:

```bash
curl "http://localhost:8888/dir/myphoto.jpg?metadata=true"
```

`metadata=true&resolveManifest=true` additionally resolves chunked-manifest
entries into their real chunk list.

### List a directory

```bash
curl -H "Accept: application/json" "http://localhost:8888/dir/?limit=10&lastFileName=a.jpg"
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `limit` | max entries per page | filer `-dirListingLimit` |
| `lastFileName` | resume listing after this entry name | empty |
| `namePattern` | include only names matching the wildcard | empty |
| `namePatternExclude` | exclude names matching the wildcard | empty |

The JSON response carries `Path`, `Entries`, `Limit`, `LastFileName`,
`ShouldDisplayLoadMore`, and `EmptyFolder`. Without the `Accept` header the
filer renders its HTML browser.

### Move and copy

```bash
curl -X POST "http://localhost:8888/dir/newname.jpg?mv.from=/dir/myphoto.jpg"
curl -X POST "http://localhost:8888/dir/copy.jpg?cp.from=/dir/myphoto.jpg"
```

`mv.from` renames or moves the source to the request path (`204 No Content`).
`cp.from` copies it.

### Append

```bash
curl -T chunk2.bin "http://localhost:8888/dir/file.bin?op=append"
```

### Delete

```bash
curl -X DELETE "http://localhost:8888/dir/myphoto.jpg"
curl -X DELETE "http://localhost:8888/dir/?recursive=true"
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `recursive` | delete a non-empty directory tree | false |
| `ignoreRecursiveError` | keep deleting remaining entries after an error | false |
| `skipChunkDeletion` | remove only the metadata, keep volume data | false |

### Tagging

```bash
curl -X PUT "http://localhost:8888/dir/file.jpg?tagging&k1=v1"
curl -X DELETE "http://localhost:8888/dir/file.jpg?tagging=k1,k2"
```

### Read by file id

```bash
curl "http://localhost:8888/?proxyChunkId=3,01637037d6"
```

The filer proxies the chunk read to the right volume server, so only the
filer port needs to be exposed.

### Resumable uploads

When started with `-tusBasePath` the filer serves the [TUS protocol](https://tus.io/)
(`POST`, `PATCH`, `HEAD` on upload URLs) for resumable uploads.

### Health

`GET /healthz` and `GET /readyz` return `200 OK`.

## Master API (port 9333)

Write-affecting endpoints are automatically proxied to the current leader, so
any master in the quorum can serve them.

### Assign a file id

```bash
curl "http://localhost:9333/dir/assign?count=1&replication=001&collection=turbo&dataCenter=dc1&ttl=3d&disk=ssd"
{"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
```

Upload the file content to `http://<url>/<fid>` afterwards. With `count>1`,
use `<fid>_1`, `<fid>_2`, ... for the additional ids.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `count` | file ids to reserve | 1 |
| `collection` | collection name | empty |
| `dataCenter` | preferred data center | empty |
| `rack` | preferred rack | empty |
| `dataNode` | preferred volume server | empty |
| `replication` | replica placement | master `-defaultReplication` |
| `ttl` | file expiration, e.g. `3d` | never |
| `disk` | disk type | empty |
| `dataSize` | expected file size in bytes | 0 |
| `preallocate` | bytes to preallocate for new volumes | master `-volumePreallocate` |
| `writableVolumeCount` | grow this many volumes when none are writable | master default |
| `memoryMapMaxSizeMb` | memory-mapped file size (Windows) | 0 |

### Look up a volume or file id

```bash
curl "http://localhost:9333/dir/lookup?volumeId=3"
{"locations":[{"url":"localhost:8080","publicUrl":"localhost:8080"}]}
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `volumeId` | volume id; a full `vid,fid` is accepted too | required |
| `fileId` | like `volumeId`, but also returns a write JWT when security is on | empty |
| `collection` | speeds up the lookup | empty |
| `read` | `yes` generates a read JWT instead of a write JWT | empty |

### Store a file in one call

```bash
curl -F file=@/home/chris/report.pdf "http://localhost:9333/submit?collection=turbo&replication=001"
{"fileName":"report.pdf","fid":"3,01637037d6","fileUrl":"localhost:8080/3,01637037d6","size":43234,"eTag":"0x6c656..."}
```

`POST /submit` accepts multipart file data plus the `dir/assign` placement
parameters (`count`, `collection`, `dataCenter`, `rack`, `replication`,
`ttl`, `disk`), assigns a file id, uploads to the volume server, and returns
the result.

### Redirect to a file

```bash
curl -v "http://localhost:9333/3,01637037d6"
```

`GET /{fileId}` answers `301 Moved Permanently` to a volume server holding
the file, preserving the query string (e.g. image-resize parameters).

### Cluster status

```bash
curl "http://localhost:9333/dir/status?pretty=y"   # full topology tree
curl "http://localhost:9333/vol/status?pretty=y"   # every volume on every node
curl "http://localhost:9333/collection/info?collection=turbo"
curl "http://localhost:9333/collection/info?collection=turbo&detail=true"
```

`collection/info` returns aggregated `TotalSize`, `FileCount`, `UsedSize`,
`VolumeCount`; `detail=true` splits them per volume layout.

### Grow volumes

```bash
curl "http://localhost:9333/vol/grow?count=4&replication=001&collection=turbo&ttl=5d&disk=ssd&dataCenter=dc1&rack=rack1"
{"count":4}
```

`count` is required; the placement parameters match `dir/assign`. One volume
serves one write at a time, so pre-allocated volumes raise write concurrency.

### Vacuum deleted space

```bash
curl "http://localhost:9333/vol/vacuum?garbageThreshold=0.4"
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `garbageThreshold` | minimum deleted-bytes ratio before a volume is compacted | master `-garbageThreshold` (0.3) |

Vacuuming makes a volume read-only, copies live needles to a new volume, and
swaps it in.

### Delete a collection

```bash
curl "http://localhost:9333/col/delete?collection=benchmark"
```

Deletes all volumes of the collection, including erasure-coded shards.
`204 No Content` on success.

### Health

```bash
curl -I "http://localhost:9333/healthz"     # liveness
curl -I "http://localhost:9333/readyz"      # readiness
curl "http://localhost:9333/"               # web UI
```

## Volume server API (port 8080)

The volume server stores file content by file id. Clients normally get the
volume URL from `dir/assign` or `dir/lookup`.

### Upload

```bash
curl -F file=@/home/chris/myphoto.jpg "http://127.0.0.1:8080/3,01637037d6"
{"name":"myphoto.jpg","size":43234,"eTag":"0x6c656...","mime":"image/jpeg","contentMd5":"..."}
```

PUT or POST the body (or a multipart `file` part) to `/{vid},{fid}`.
`204 No Content` is returned when the content is unchanged. `?ts=<unix>`
sets the stored modification time.

### Read

```bash
curl "http://127.0.0.1:8080/3,01637037d6"
curl "http://127.0.0.1:8080/3,01637037d6.jpg"        # sets Content-Type from the extension
```

Supports `Range` and `HEAD`. Image files can be resized server-side:

| Parameter | Description |
|-----------|-------------|
| `width`, `height` | resize bounds in pixels |
| `mode` | `fit`, `fill`, or `crop` (default `fit`) |
| `crop_x1`, `crop_y1`, `crop_x2`, `crop_y2` | explicit crop rectangle |
| `cm` | `false` returns the chunk-manifest blob instead of resolving it |
| `readDeleted` | `true` reads soft-deleted needles |
| `collection` | passed through redirects for the right volume |

### Delete

```bash
curl -X DELETE "http://127.0.0.1:8080/3,01637037d6"
{"size":43234}
```

`?ts=<unix>` sets the deletion timestamp. Replicated volumes propagate the
delete to every replica.

### Status

```bash
curl "http://localhost:8080/status?pretty=y"  # disk and volume inventory
curl -I "http://localhost:8080/healthz"       # liveness/readiness
```

`OPTIONS` preflights answer CORS headers; a volume server started with
`-publicUrl` serves read-only requests on a separate public port.
