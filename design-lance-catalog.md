# Lance Catalog for SeaweedFS

A second catalog surface next to the Iceberg REST catalog, speaking the Lance Namespace
REST spec, over the same table buckets and the same filer.

## Why

Gravitino 1.1 added a Lance REST service and 1.3 ships it as a standalone server; Lakekeeper
added Lance in the same window by a completely different route. That is the useful signal:
two unrelated catalogs decided independently that Lance had to be first-class, not a niche.
The client side is already there — `lance-spark` (`LanceNamespaceSparkCatalog`
with `impl=rest`), `lance-ray`, and the generated Python/Java/Rust clients all talk the same
OpenAPI. Implementing the spec means those engines work against SeaweedFS with no
SeaweedFS-specific code on the client.

The second reason is that Gravitino's own documentation names the gap it cannot close:
DuckDB, pandas and DataFusion "do not support Lance REST natively yet" and have to fetch a
location from the catalog and then open the dataset directly. Gravitino cannot help there,
because it does not own the storage. SeaweedFS does. That is the whole design opportunity
below.

## Prior art: three families

Upstream lists twelve catalog implementations, and they fall into three shapes. Knowing
which one we are building matters more than any individual API decision.

**1. Storage-native, no service.** The Lance Directory Catalog. V1 is a directory listing
where every `<name>.lance/` child of a prefix is a table; V2 adds a `__manifest` table —
itself a Lance table — holding `object_id`/`object_type`/`location` rows, with nested
namespaces, hash-prefixed table directories, and optional managed versioning. No server, no
credentials, no governance. This is the floor every other implementation has to beat.

**2. Protocol-native server.** Someone implements the Lance Namespace REST OpenAPI and
clients connect with `impl=rest`. Gravitino is the only one of the twelve that does this,
and it is what this design proposes.

**3. Client-side adapters onto an existing catalog.** Nine of the twelve. The Lance client
translates namespace operations into whatever the backing catalog already speaks: Apache
Polaris, Unity Catalog, AWS Glue, Hive Metastore v2 and v3, Google BigLake, Dataproc,
Microsoft OneLake — and Apache Iceberg REST. Two flavors:

- Catalogs with a real non-Iceberg table concept mark the format directly. Polaris uses its
  Generic Table API with `format = lance`; Unity uses an `EXTERNAL` table with
  `table_type=lance` in properties and the path in `storage_location`; Glue uses
  `EXTERNAL_TABLE` plus `table_type=lance` in `Parameters`, path in
  `StorageDescriptor.Location`.
- Catalogs with no such concept fake one. The Iceberg REST adapter registers **a regular
  Iceberg table with a dummy schema — a single nullable string column named `dummy`** —
  carrying the property `table_type=lance`, and treats the Iceberg table location as the
  Lance dataset root.

Every adapter in family 3 lands in the same place: `DeclareTable`/`ListTables`/
`DescribeTable`/`DeregisterTable` only, `DropNamespace` in RESTRICT mode only,
`load_detailed_metadata=false` only, and `managed_versioning=false`. They are a name-to-
location map and nothing more.

Lakekeeper is the instructive outlier. It has the same generic-table concept Polaris has,
but no upstream adapter exists for it — there is no `lance-namespace` reference anywhere in
its repository and no page for it in the supported-catalogs list. So Polaris's generic tables
are reachable from a stock Lance client and Lakekeeper's are not, despite being the same
idea. Shipping the concept is not the same as shipping the integration.

## Gravitino and Lakekeeper: the two opposite bets

Both shipped Lance support in the same window and did not build the same thing.

**Gravitino implements the protocol.** Its `lance/` module serves the Lance Namespace REST
spec on its own port (`:9101/lance`), so stock `lance-spark` and `lance-ray` connect with
`impl=rest` and no vendor-specific client. The cost is governance: storage credentials are
static properties on the catalog (`lance.storage.access_key_id`, `secret_access_key`,
`endpoint`, `region`, `allow_http`), optionally overridden per table, handed to the engine
as-is. No STS, no expiry, no per-table scoping.

**Lakekeeper refuses the protocol and governs the object instead.** There is no
`lance-namespace` anywhere in the repository; Lance arrived in 0.13.0 (2026-06-30, issue
#1673 `Generic Table API with Lance`) as one `format` string on a Lakekeeper-native Generic
Table API:

```
POST/GET/DELETE /lakekeeper/v1/{prefix}/namespaces/{ns}/generic-tables[/{table}]
GET             /lakekeeper/v1/{prefix}/namespaces/{ns}/generic-tables/{table}/credentials
POST            /lakekeeper/v1/{prefix}/generic-tables/rename
```

`format` is opaque, `schema` and `statistics` are stored but never validated, and the
catalog writes no format-specific metadata — engines go straight to the location. In
exchange Lance tables get everything Iceberg tables get: STS-vended prefix-scoped
credentials, OpenFGA per-action permissions (16 actions), soft-delete with undrop, a
protection flag, rename, pagination, and name uniqueness across Iceberg tables, views and
generic tables in one namespace. The price is that no stock Lance client can talk to it —
you need `pylakekeeper`, which exists mainly to translate vended credentials into
`lance_storage_options`.

So: protocol fidelity and weak governance, or strong governance and client lock-in. Both
documented their limit honestly, and it is the same limit. Lakekeeper's capability table
says it outright — "Commit coordination: the catalog does not arbitrate writes — engines
write directly." Gravitino does not claim it either. Neither of them coordinates a Lance
commit, which is exactly the thing a store can do and a control plane cannot.

We do not have to choose. Serve the Lance protocol natively the way Gravitino does, over
the `s3tables` entries that already carry ARNs, policies, tags and maintenance config, and
the governance comes from the layer underneath rather than from a proprietary API on top.
That is only available to us because we are the store, which is also what makes the third
option — arbitrating the commit — available.

## We are probably already a Lance catalog, and that is a problem

The Iceberg REST adapter does not care whose Iceberg catalog it is talking to. It needs
`/v1/config?warehouse=`, `/v1/{prefix}/namespaces`, `/v1/{prefix}/namespaces/{ns}/tables`
and unit-separator (`\x1F`) multi-level namespaces. We serve all of those, and
`parseNamespace` in `weed/s3api/iceberg/utils.go:22` already splits on `\x1F`. So a stock
Lance client pointed at our Iceberg catalog on :8181 with the Iceberg impl should already
create, list, describe and deregister Lance tables today, with no SeaweedFS change at all.

That is worth testing before writing a line of the design above, for two reasons. It is a
free baseline — and possibly a free announcement. And it is a data-loss hazard.

A Lance table registered this way is an Iceberg table whose metadata references no data
files, sitting on top of a Lance dataset that uses `data/` for its fragments — the same
subdirectory name Iceberg uses. The maintenance worker's orphan cleaner walks exactly
`<table>/metadata` and `<table>/data`, and deletes every file not referenced by a snapshot
and older than `orphan_older_than_hours`
(`weed/worker/tasks/iceberg/operations.go:331`, default 72). Against an adapter-registered
Lance table, every fragment is unreferenced by construction. Run maintenance and the
dataset is deleted.

Maintenance is disabled by default (`handler.go:334`), so this is a latent hazard rather
than a live one: it needs an operator to enable Iceberg maintenance on a bucket that also
holds adapter-registered Lance tables. But it costs nothing to close — detection should
skip any table carrying a non-Iceberg format marker (`table_type` property, or
`Format != "ICEBERG"` once the format field is honest), and that guard is worth landing on
its own regardless of whether the rest of this design ever gets built. It is the same
"catalog-only, no maintenance" marker the generic-format question needs.

## Where we differ from Gravitino

Gravitino is a metadata service in front of somebody else's object store:

```
  Spark / Ray                  Spark / Ray / pandas / duckdb
      |                              |
  Lance REST                    Lance REST          (direct S3)
      |                              |                   |
  Gravitino                     SeaweedFS S3 gateway ----+
      |                              |
   S3 keys handed out           SeaweedFS filer + volumes
      |
  somebody else's S3
```

It resolves a name to a location plus `lance.storage.*` credentials, and steps out of the
way. Everything a Lance table actually is — `_versions/`, `data/`, `_indices/` — is opaque
to it.

We are the store. Three things follow that Gravitino cannot do:

1. The catalog and a plain directory listing can be made to agree, so a client with no
   catalog at all still sees the right tables.
2. `_versions/` is a filer directory listing, not an object-store `LIST`. Version history
   is cheap and can back the admin UI.
3. We can offer a genuinely atomic commit reservation. Lance's commit protocol needs
   put-if-not-exists; our S3 layer does not currently provide one (see
   [Commit safety](#commit-safety)). The filer does.

## Placement

The Iceberg catalog is a thin HTTP shell over `s3tables.Manager`; the storage work lives in
`weed/s3api/s3tables`. Table buckets live under `TablesPath = s3_constants.DefaultBucketsPath`,
i.e. the same filer tree the S3 gateway serves, so `s3://bucket/ns/table/` is simultaneously
a catalog entry and an S3 prefix. Catalog entries are filer directories carrying `s3tables.*`
extended attributes. `Table.Format` already exists and is hard-checked against `"ICEBERG"`
in `weed/s3api/s3tables/handler_table.go:48`.

So:

```
weed/s3api/lance/          new: HTTP surface, id codec, error model
weed/s3api/s3tables/       extended: Format "LANCE", lance state xattr, version entries
weed/command/s3.go         new: -port.lance (default 9101), startLanceServer
```

`Format: "LANCE"` on the table entry is the whole storage-model change for phase 1.
Everything else — namespaces, ARNs, policies, tags, ownership — is shared verbatim.

```
                        s3tables.Manager (filer)
                                 |
        +------------------------+------------------------+
        |                                                 |
  weed/s3api/iceberg                              weed/s3api/lance
   Iceberg REST :8181                              Lance REST :9101
        |                                                 |
  Iceberg tables                                   Lance datasets
        \                                                 /
         +-------------------- s3 :8333 -----------------+
                                 |
                         SeaweedFS volumes
```

## Identifier mapping

Lance identifiers are `["ns", ..., "table"]`, encoded in the URL as a single string joined
by a delimiter that defaults to `$`. The delimiter alone means the root namespace, so
`/v1/namespace/$/list` lists the root's children.

Iceberg had to invent a warehouse selector because its identifier is flat and every table
bucket is a separate catalog. Lance does not need that — its identifier is already
hierarchical, and Gravitino uses exactly three levels (`["lance_catalog", "sales", "orders"]`).
That maps onto us without inventing anything:

```
  $                            root      -> list of table buckets
  $analytics                   level 1   -> a table bucket
  $analytics$sales             level 2   -> a namespace in that bucket
  $analytics$sales$orders      table
```

`spark.sql.catalog.lance.parent = analytics` then makes `sales.orders` resolve, which is the
same shape Gravitino's Spark example uses.

Levels 2..N join into one `s3tables` namespace with `.`, matching what the Iceberg catalog
already does with `flattenNamespacePath`. The flattened form is only the directory name —
`namespaceMetadata.Namespace []string` in the xattr keeps the authoritative parts, so the
mapping stays invertible even though `.` is a legal character inside a namespace part.
Reject `$` in any name part with `InvalidInput`; our charsets already exclude it, so no
escaping scheme is needed.

Root-level `ListNamespaces` returning table buckets means an unauthenticated or
broadly-scoped caller can enumerate buckets. Filter it through the same
`s3tables/permissions.go` check `ListTableBuckets` uses, not a separate path.

`CreateNamespace` on a one-part identifier creates a table bucket, and it does so only if
the caller is permitted to — the namespace never creates a bucket as a side effect of
creating something inside it. A table bucket is a tenant resource with its own policy, ARN
and lifecycle, and conjuring one because a client said `CREATE SCHEMA` is a privilege
escalation dressed as a convenience. Lakekeeper draws the same line explicitly: its client
creates tables, not warehouses.

## Storage layout

Lay tables out as:

```
s3://<table-bucket>/<flattened-namespace>/<table>/
    data/
    _versions/
    _indices/
```

**Built without the `.lance` suffix this design originally proposed.** The suffix would have
made every namespace prefix a valid Lance Directory Catalog V1 root, since V1 recognises a
table by exactly that naming. It does not survive contact with the storage layer: the
catalog entry *is* the dataset directory, `validateTableName` excludes `.` from the charset,
and a suffixed entry name would leak into ARNs, policy documents and the S3 Tables API,
where the same table would answer to two different names. Making `GetTablePath` format-aware
instead spreads an "unless it is Lance" branch through code that has no business knowing —
the exact cross-cutting cost this design rejects family 3 for.

So one name, one directory. What survives is direct access by URI, which is the larger half
of the story and needs no naming convention at all:

```python
# with the catalog
spark.sql("SELECT * FROM lance.sales.orders")

# without it, same bytes
lance.dataset("s3://analytics/sales/orders")
```

DuckDB, pandas and DataFusion still reach the data with no catalog running, which is the gap
Gravitino's documentation admits to. What they no longer get for free is *enumeration* — a
directory-catalog client pointed at the namespace prefix will not list these as tables. If
that turns out to matter, the cheapest fix is a repair-style tool that materialises `.lance`
aliases, not a rename of the catalog entry.

Note also that the directory catalog's own V2 mode puts child-namespace tables in
`<hash>_<ns$table>` directories at the root and creates no physical subdirectories for
namespaces, so full directory-catalog fidelity was never on offer anyway. We are a
server-backed catalog; the human-readable prefix layout is worth more than partial V1
lookalike behaviour.

## The table bucket was not a neutral container

This design assumed a table bucket is a place to put a table's files. It is
not: `validateTableBucketObjectPath` runs on every S3 write into one and
validated the path against Iceberg's layout, so a Lance client got 403 on
`data/*.lance`, on `_versions/`, and on `_transactions/` — a directory Lance
writes that neither the spec documentation nor this design anticipated. Nothing
about the catalog worked end to end until that changed.

The layout guard now admits the union of what the supported formats write, and
treats any underscore-prefixed top-level directory as belonging to the format,
checking only that the path stays inside the table. Enumerating Lance's
internal directories by name is exactly the mistake that missed
`_transactions`. Iceberg writes none of them, so it loses nothing.

Found by pointing the real Python client at a running gateway, not by reading
the spec. Worth remembering for the next format: the premise to check first is
whether the storage layer will accept its files at all.

## Table lifecycle

Lance has three table states, and the spec pins them to marker files:

| State | Marker | Created by | Visible in ListTables |
| --- | --- | --- | --- |
| declared | `.lance-reserved` | `DeclareTable` | yes, when `include_declared=true` |
| created | `_versions/` present | client writes, or `CreateTable` | yes |
| deregistered | `.lance-deregistered` | `DeregisterTable` | no; data preserved |

Record the state in an xattr (`s3tables.lanceState`) on the catalog entry *and* write the
marker file into the table directory. The xattr is what the catalog reads; the marker is
what keeps a directory-catalog client honest. Dual-write is the price of the interop claim
above, and it is one extra filer write on three rarely-called operations.

`DeclareTable` is the operation `lance-spark` actually calls on `CREATE TABLE` (it replaced
the legacy `create-empty`), so it is not optional in practice even though the spec marks
only a subset as required.

`DeregisterTable` preserving data is the same shape as our Iceberg rename, where the catalog
entry moves and the data stays put — reuse `TableDataDirFromMetadataLocation`'s idea rather
than re-deriving the data path from the catalog name.

## Commit safety

This is the part I got wrong, and the correction removed a feature rather than adding one.

Lance commits a version by writing `_versions/{v}.manifest` with put-if-not-exists: exactly
one writer is supposed to win, and the loser rebases. In lance 10 that path is not optional
and needs nothing bolted on — `commit_handler_from_url` hands every `s3://` dataset a
`ConditionalPutCommitHandler`, which calls `put_opts` with `PutMode::Create`, which
object_store's S3 backend sends as `If-None-Match: *`.

I originally read our gateway as evaluating that header check-then-act, and designed around
it. That was already out of date. `buildWriteCondition`
(`weed/s3api/s3api_object_routed_write.go`) reduces `If-None-Match: *` to a filer
`WriteCondition{IF_NOT_EXISTS}`, and `putToFiler` routes the create to the object's owner
filer, which evaluates the precondition under its per-path lock; when routing is not
available it falls back to the object write lock, which evaluates it under the lock too.
Either way it is atomic. Sixteen concurrent writers of the same fresh key get one 200 and
fifteen 412s, repeatedly.

So the store already has the primitive Lance needs, cluster-wide, for every conditional-PUT
client and not just this one.

### What that removed

An earlier draft of this design offered the catalog as an **external manifest store**:
`managed_versioning: true` plus `CreateTableVersion` and friends, with the reserve step as a
filer `CreateEntry` with `o_excl`. It was implemented, tested, and shipped behind a default-off
flag — and it should not exist.

- It solves a problem this store does not have. The spec offers that path for stores that
  cannot order commits themselves.
- It moves a table's version history out of the dataset and into the catalog, so a reader
  that does not go through this namespace no longer sees the whole picture. That is a real
  cost paid for nothing.
- lance 10 cannot even use it past the first commit: `NamespaceManifestStore::put_if_not_exists`
  answers "put_if_not_exists is not supported for namespace-backed stores", which is exactly
  what a second `append` needs.

The version operations now answer `Unsupported` alongside the other operations the catalog
does not serve, and `managed_versioning` is answered `false`. The property they were
protecting is covered instead by a test that races eight writers at the manifest key through
S3 and asserts one wins — testing the path Lance actually takes.

## Credential vending

Iceberg needed a header (`X-Iceberg-Access-Delegation: vended-credentials`) and a bespoke
response shape. Lance has it in the spec: `vend_credentials: true` on the request,
`storage_options` on the response, with `expires_at_millis` as the well-known expiry key.

Reuse the existing vendor interface unchanged — `iceberg.CredentialVendor` /
`STSService.AssumeRoleForPrincipal` scoped to the table prefix (#10777) — and map its output
to the storage options Lance passes through to `object_store`:

```
aws_access_key_id, aws_secret_access_key, aws_session_token,
aws_region, aws_endpoint, allow_http, expires_at_millis
```

Those are the names `pylakekeeper` emits as `lance_storage_options`, which is the shape
Lakekeeper's tested S3 path actually feeds to Lance. `object_store` also accepts the
un-prefixed aliases (`endpoint`, `region`) that the directory catalog's `storage.` prefix
strips down to and that Gravitino's `lance.storage.endpoint` resolves to, but the `aws_`
forms are the ones with a tested integration behind them, so emit those. `aws_endpoint`
should come from `deriveS3AdvertisedEndpoint()`, the same source the Iceberg `FileIO` config
uses, and `allow_http` must be set when that endpoint is plain HTTP or every read fails with
a TLS error that looks like a credential problem — Lakekeeper vends both automatically for
exactly this reason, and calls out that there is then no per-vendor branch in client code.

We emit this server-side, in the `storage_options` field the Lance spec already defines,
which is strictly better than Lakekeeper's arrangement: no client library has to translate
anything, so vending works from any stock Lance client rather than only from theirs.

Guard the same way #10777 had to after review: bucket-scoped list grants need an `s3:prefix`
condition, and a location containing `*` or `?` must be refused rather than widened into a
resource pattern.

## Auth and authorization

Authentication reuses `S3Authenticator` and `CredentialValidator` as-is. The Lance spec maps
identity to headers — `api_key` to `x-api-key`, `auth_token` to `Authorization: Bearer` — and
SigV4 keeps working because it is the same authenticator the Iceberg catalog already fronts.

Authorization needs nothing new. A Lance table gets the same ARN shape,
`arn:aws:s3tables:...:bucket/B/table/NS/T`, so every existing table-bucket policy covers
Lance tables with no new policy language and no second permission model. Route it through
`s3tables/permissions.go` and inherit the `DefaultAllow` semantics the Iceberg server already
mirrors from the S3 port.

One spec quirk worth honoring: request context entries prefixed `header.` become request
headers, and every response header comes back as a `header.`-prefixed context entry. Echoing
`x-request-id` through it costs nothing and makes tracing work.

## What to take from Lakekeeper

Rejecting Lakekeeper's API shape does not mean rejecting what it learned building it.

**Deregister is soft-delete, so implement it as one.** Lakekeeper gives generic tables
soft-deletion with undrop and a `protected` flag that makes a drop require `force=true`.
Lance already has the concept — `DeregisterTable` preserves the data and hides the table —
so the `.lance-deregistered` marker is a soft-delete by another name, and a re-register is
an undrop. A protection flag on table-bucket entries is worth having regardless of Lance:
it is a few lines against the existing xattrs and it applies to Iceberg tables too.

**Enforce one identifier space across entry kinds.** Lakekeeper rejects a generic table
whose name collides with an Iceberg table or view in the same namespace. Our catalog entries
already share one filer directory and already carry `s3tables.entryType`, so this is
structurally true — but it has to be enforced deliberately on every path, or a Lance handler
happily loads an Iceberg table's directory and vice versa. That is the same crossover bug
class as the view/table rename authorization fixed in #10776; the `catalogEntryKind` pattern
from that change is the thing to reuse rather than re-derive.

**A re-vend path matters more than it looks.** Lakekeeper exposes `/credentials` separately
from load, because STS credentials expire in the middle of long jobs and re-loading the
whole table to refresh them is wasteful. In Lance the spec's answer is another
`DescribeTable` with `vend_credentials: true`, which is fine — but it means `DescribeTable`
must stay cheap when `load_detailed_metadata` is false, which is another reason not to open
the dataset on that path.

**Generic tables are a cheap orthogonal win.** Lakekeeper's real insight is that Delta,
Parquet, CSV, Vortex and Paimon all get governance for free once the catalog stops caring
what the format is. Our `Table.Format` field already exists and the only thing stopping it
is the hard `"ICEBERG"` check in `handler_table.go:48`. Loosening that and letting the S3
Tables API register a table with an arbitrary format and a location — no metadata, no
commits — is a small change that makes every format cataloguable. It is independent of this
design and probably worth doing first, since `Format: "LANCE"` is then just a value rather
than a special case.

**Skip remote signing.** It is Lakekeeper's fallback for S3-compatible stores with no STS,
and their own documentation notes that Lance will not use it — format libraries with their
own S3 client expect static credentials and do not implement the Iceberg signer protocol. We
have STS, so vended credentials are the path, and the signer is not worth building for a
client that cannot consume it.

## Errors

Lance uses `{code, error, detail, instance}` with numeric codes, not Iceberg's exception-type
strings. The mapping is mechanical:

| HTTP | code | when |
| --- | --- | --- |
| 400 | 13 InvalidInput | charset violations, malformed id, route/body mismatch |
| 401 | 16 Unauthenticated | |
| 403 | 15 PermissionDenied | |
| 404 | 1 NamespaceNotFound, 4 TableNotFound, 11 TableVersionNotFound | |
| 409 | 2/5 AlreadyExists, 3 NamespaceNotEmpty, 14 ConcurrentModification | |
| 501 | 0 Unsupported | every phase-3 data operation |

Route/body mismatch is a spec requirement, not a nicety: when the identifier appears in both
the path and the body and they disagree, the server must return 400. Cheap to get right at
the decode step, annoying to retrofit.

## Route surface

Phase 0 is not in this table: point a stock Lance client at the existing Iceberg catalog
with the Iceberg impl, see how far it gets, and land the maintenance guard either way. That
tells us what the native server actually has to beat.

Phase 1, the whole `lance-spark` and `lance-ray` contract:

```
POST /v1/namespace/{id}/create         CreateNamespace       mode: Create|ExistOk|Overwrite
GET  /v1/namespace/{id}/list           ListNamespaces
POST /v1/namespace/{id}/describe       DescribeNamespace
POST /v1/namespace/{id}/drop           DropNamespace         mode: Fail|Skip, behavior: Restrict|Cascade
POST /v1/namespace/{id}/exists         NamespaceExists
GET  /v1/namespace/{id}/table/list     ListTables            ?include_declared, ?page_token, ?limit
GET  /v1/table                         ListAllTables
POST /v1/table/{id}/declare            DeclareTable
POST /v1/table/{id}/describe           DescribeTable         ?with_table_uri, ?load_detailed_metadata, ?check_declared
POST /v1/table/{id}/exists             TableExists
POST /v1/table/{id}/register           RegisterTable         mode: Create|Overwrite
POST /v1/table/{id}/deregister         DeregisterTable
POST /v1/table/{id}/drop               DropTable
POST /v1/table/{id}/rename             RenameTable
```

`DescribeTable` with `load_detailed_metadata=false` needs only `location`, which is the
common case and which we can answer from xattrs alone. With `load_detailed_metadata=true`
the spec wants `version`, `schema` and `stats`, which means reading the Lance manifest. For
phase 1, return the fields we can derive from the filer — `version` from the highest entry in
`_versions/`, given V2 naming is `{u64::MAX - version:020}.manifest` and V1 is
`{version}.manifest` — and omit `schema`/`stats` rather than fabricating them. The spec
tolerates a partial response here; it does not tolerate a wrong one.

Phase 2 was the five version operations plus `managed_versioning`; it was built and then
removed, for the reasons under Commit safety.

Phase 3 is the data plane: `CreateTable`, `InsertIntoTable`, `MergeInsertIntoTable`,
`UpdateTable`, `DeleteFromTable`, `QueryTable`, `CountTableRows`, and the index and tag
operations. These exchange Arrow IPC, and more to the point they require reading and writing
the Lance file format, for which no Go implementation exists. Return `Unsupported` (code 0)
and say so in the docs. `arrow-go/v18` is already an indirect dependency, so Arrow framing is
not the blocker — Lance is.

## Does a Lance table need maintenance?

Yes, and one part of it has no Iceberg equivalent. The client exposes three jobs:

- `optimize.compact_files()` — Lance writes a fragment per write batch, so a table fed by
  small appends accumulates small files exactly the way an Iceberg table does.
- `optimize.optimize_indices()` — **rows written after an index was built are not covered by
  it.** A vector search against a stale index silently misses recent data. That is a
  correctness-shaped failure, not a slow query, and it is specific to what people use Lance
  for.
- `cleanup_old_versions()` — every version is retained until something removes it. Lance can
  do this itself: `optimize.enable_auto_cleanup()` sets it on the dataset, so this one need
  not be an external job at all.

None of it can run in the Go worker. All three read and rewrite Lance files, which needs
Lance format code that does not exist in Go, and there is no useful subset either: deciding
which fragments an old version still references means parsing Lance manifests.

So the maintenance worker must not touch a Lance table, and it declines by reading the format
the catalog recorded rather than by failing to parse Iceberg metadata.

## The worker can be Rust, and it is not a sidecar

The Go worker is not the only worker. `weed/pb/plugin.proto` defines `PluginControlService`,
a language-agnostic gRPC stream that external maintenance workers connect on: the worker
opens `WorkerStream`, sends `WorkerHello` with the job types it can `detect` and `execute`,
answers `RequestConfigSchema` with a `JobTypeDescriptor`, replies to `RunDetectionRequest`
with `JobProposal`s and to `ExecuteJobRequest` with `JobProgressUpdate`s and `JobCompleted`.
`weed worker -admin=host:23646` is the Go reference implementation of exactly that contract,
from outside the admin process.

Nothing in it is Go-specific, and the Rust toolchain is already in the tree.
`seaweed-volume/build.rs` compiles protos straight out of `../weed/pb/` with `tonic_build`,
including `filer.proto`, on tonic 0.12 and prost 0.13. A Lance worker is that same build
with `plugin.proto` added and the `lance` crate as a dependency — the real one, no FFI and
no Python.

Three job types, one per real maintenance operation:

| Job type | Calls | Detected from |
| --- | --- | --- |
| `lance_compact` | `optimize.compact_files` | fragment count and sizes |
| `lance_optimize_indices` | `optimize.optimize_indices` | rows an index does not cover |
| `lance_cleanup_versions` | `cleanup_old_versions` | version count and age |

What the existing machinery then supplies for free is the part worth noticing. Scheduling,
retries, dedupe by `dedupe_key`, progress reporting, per-job concurrency limits and the
admin settings page all come from the protocol: a worker that answers `RequestConfigSchema`
with a descriptor gets its configuration form rendered in the admin UI without a line of Go
or templ. A Rust worker is a first-class maintenance worker, not an appendage.

The remaining wiring is small and mostly decided already. `RunDetectionRequest` carries a
`ClusterContext` with filer and S3 addresses plus a free-form `metadata` map, which is where
the Lance namespace URL goes; the worker lists Lance tables from the namespace, which is the
catalog of record and already filters by format. It gets at the data by asking
`DescribeTable` for `storage_options` with `vend_credentials`, so the worker is just another
client of the STS path rather than a component with its own credentials. And when it commits
a compaction it goes through `CreateTableVersion` like any other writer, which is what
managed versioning was for.

## The worker is also the only thing that can describe the table

Admin can render an Iceberg table because it can read Iceberg metadata. It cannot read
Lance: it knows the dataset's location and its format string, and that is the whole of it.
The details page showed a location and two empty panels, which is an honest answer and a
useless one.

The worker already knows. Detection opens every dataset to decide whether it needs
compacting, so at that moment it holds the schema, the row count, the fragment count and
the version count. It just had no way to say so — every message on the stream was about
work.

So `WorkerObservations` is a body on `WorkerToAdminMessage`: a repeated `ObjectObservation`
of `object_id`, `object_kind`, `format`, and a `ConfigValue` map the worker fills with
whatever it can cheaply say. Admin keeps the last observation per object and serves it back
with the time it was taken and the worker that took it. Nothing schedules from it, and it is
not authoritative — it is a cache with its staleness on the label, which is why the page
badges it rather than presenting it as metadata it read itself.

The keys are the worker's to choose, which keeps the protocol out of the business of knowing
what a Lance table is. A worker for any other format admin cannot parse describes itself the
same way.

## A bucket declares its format

Format was recorded per table, which is enough for the storage layer and not enough for
anything that has to answer a question about a bucket. The admin UI printed one Iceberg
endpoint for every bucket, including the ones holding Lance datasets, where that endpoint
serves nothing; an empty bucket had no format at all.

So `CreateTableBucket` takes an optional `format`, stored with the rest of the bucket
metadata. Empty means `ICEBERG` - what AWS S3 Tables serves, and therefore what an SDK
that has never heard of the field means. `CreateTable` refuses another format, and
`CreateView` refuses outright outside an Iceberg bucket, a view being Iceberg metadata.
The Lance namespace declares `LANCE` for the buckets it creates.

**Enforced rather than defaulted**, because the point of showing a format at all is the
endpoint that follows from it, and that endpoint is only truthful if the bucket holds one
format. **Buckets that already exist stay undeclared** and keep taking anything: nothing is
migrated, and the UI shows "unset" as a fact about the bucket's age rather than a fault.
That state is also the only way to hold both formats at once, which is what the
Iceberg-REST adapter path produces.

## Sample rows are fetched, not cached

The same asymmetry has a second half. Admin renders an Iceberg table's rows by
reading its Parquet files directly; for Lance it has nothing to read with, so the data
page offered a Browse Data button that led to an empty grid.

`RequestObjectPreview` / `ObjectPreviewResponse` mirror the config-schema round trip
already on the stream: admin asks, the worker scans the dataset and hands back rows it
has already rendered as text, because it is the only side that knows the types. Admin
picks the worker from the observation store, so the one that last described a table is
the one asked to read it.

The rows are deliberately not cached, and that is the line between the two channels. An
observation describes an object, so a copy with a timestamp on it is useful. Rows are the
object's contents: a copy held in admin would be stale, larger, and nobody's business.
The page fetches on load, bounded, or says why it cannot.

## The sidecar question

The data plane is a different problem, and this design previously conflated the two.
Maintenance rides the worker protocol; `QueryTable` and `InsertIntoTable` do not, because
they are synchronous REST operations on the namespace's own surface. Serving those means a
Rust process that answers HTTP, either behind the Go namespace as a proxy target or in front
of it. It would make SeaweedFS a store you can run vector search *in* rather than one you
read vectors *out of*, which is the larger prize and the reason to keep the option open.

Neither should gate phase 1. Phases 1 and 2 are pure Go over the filer and are worth
shipping on their own — they are what makes Spark and Ray work.

## Testing

Mirror the Iceberg package: `httptest` plus a fake filer client for the handler tests, in
`weed/s3api/lance`. Then an integration suite under `test/s3tables/catalog/` next to the
existing `pyiceberg_test.go`, driving the generated Python `lance-namespace` client against
a live gateway. Three things that suite must cover and unit tests cannot:

- the storage-options key names actually work, i.e. a client that gets `storage_options` from
  `DescribeTable` can open the dataset;
- a table created through the catalog is visible to `lance.dataset()` by URI and to a V1
  directory-catalog client rooted at the namespace prefix;
- concurrent writers do not lose a commit, which is the phase-2 acceptance test and the
  thing that justifies the external manifest store.

Phase 1 is validated: `lance_namespace` 0.11.1 with `impl=rest` drives the namespace,
`lance.write_dataset` writes to the vended location with the vended `storage_options`, and
the rows read back. Note that this client version drops `check_declared` and
`include_declared` on the wire, so `is_only_declared` reads null through it however the
server behaves.

The commit path is validated at both levels. The mechanism: eight writers race the same
manifest key through S3 with `If-None-Match: *`, and exactly one wins. The property that
actually matters, which single-winner exclusivity does not by itself establish: eight
writers append to one dataset concurrently through lance, and afterwards every batch is
still there — the losers saw the conflict, rebased, and committed again. That second test
is also the sequence managed versioning could not complete at all, since its store answers
"put_if_not_exists is not supported" to the second commit.

One more that belongs in the Iceberg suite, not this one: a Lance dataset registered through
the Iceberg adapter must survive a full maintenance pass. Reading the code, that test should
fail today; it has not been run.

## Open questions

- Root-level `ListNamespaces` enumerating table buckets is convenient and is a listing
  surface we do not have on the Iceberg side. Decide whether it is gated behind a flag.
- Whether the `.lance` directory suffix is worth the divergence from the Iceberg layout. I
  think yes — it is what makes the catalog optional — but it means the two catalogs' tables
  do not look alike on disk, and the admin UI has to know that.
- Names: our charsets are lowercase-only and Lance identifiers are arbitrary strings. Reject
  and document, as Iceberg does, or case-fold. Rejecting is right, but see #10734 for how
  case handling bites when only one side normalizes.
- Whether to land generic-format registration first. Dropping the `"ICEBERG"` check and
  letting a table carry an arbitrary format plus a location is smaller than this whole
  design, gets Delta and Parquet catalogued as a side effect, and turns `Format: "LANCE"`
  into an ordinary value. The argument against is that it invites tables the maintenance
  worker cannot service, so it needs a "catalog-only, no maintenance" marker to be honest.
