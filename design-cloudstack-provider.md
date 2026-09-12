# SeaweedFS as an Apache CloudStack Object Storage Provider

A CloudStack ObjectStore plugin that makes SeaweedFS a first-class object storage
backend inside Apache CloudStack, alongside the existing MinIO and Ceph RGW
providers. This is a collaboration with proIO (Swen), who builds private clouds on
CloudStack and wants SeaweedFS as a storage option.

## The request

> We can only add MinIO and Ceph as object storage [in CloudStack] today. I want
> to get SeaweedFS into this project... What we need is to build a provider which
> does the communication between Cloudstack and SeaweedFS.

This is **not** a SeaweedFS-side feature. The work lives in the Apache CloudStack
repo (Java): a new plugin under `plugins/storage/object/seaweedfs/` that implements
CloudStack's ObjectStore plugin framework and talks to SeaweedFS over its S3 and
IAM APIs. SeaweedFS itself needs no changes for the core to work — its S3 API
already covers every bucket operation CloudStack requires, and its IAM API covers
user/credential management.

## How the CloudStack ObjectStore framework works

CloudStack 4.18+ introduced an Object Storage framework. An admin registers an
object storage pool via `addObjectStoragePool` (URL + provider + credentials);
tenants then create and manage buckets on it through CloudStack APIs. CloudStack
manages pool and bucket lifecycle; the underlying provider handles the actual
object protocol.

A provider is a plugin module implementing three interfaces:

### 1. `ObjectStoreProvider` — registration

`MinIOObjectStoreProviderImpl` is the reference. It is a Spring `@Component` that:
- Returns a provider name (`"MinIO"`)
- Returns `DataStoreProviderType.OBJECT`
- In `configure()`, injects the lifecycle and driver implementations and calls
  `storeMgr.registerDriver(name, driver)`

### 2. `ObjectStoreLifeCycle` — pool add/remove

`MinIOObjectStoreLifeCycleImpl.initialize()` reads the URL, name, and
`accesskey`/`secretkey` details from the `addObjectStoragePool` call, tests the
connection by listing buckets, and persists an `ObjectStoreVO` via
`ObjectStoreHelper`. The other methods (attachCluster/Host/Zone, maintain,
deleteDataStore) are no-ops for object storage.

### 3. `ObjectStoreDriver` — bucket + user operations

`ObjectStoreDriver` (in `engine/storage/.../object/ObjectStoreDriver.java`) extends
`DataStoreDriver` and defines the bucket/user contract. Every provider must
implement:

| Method | Purpose |
| --- | --- |
| `createBucket(Bucket, boolean objectLock)` | Create a bucket |
| `listBuckets(long storeId)` | List all buckets |
| `deleteBucket(BucketTO, long storeId)` | Delete a bucket |
| `createUser(long accountId, long storeId)` | Provision a user + credentials for a CloudStack account |
| `setBucketPolicy` / `getBucketPolicy` / `deleteBucketPolicy` | Bucket policy CRUD |
| `setBucketEncryption` / `deleteBucketEncryption` | SSE config |
| `setBucketVersioning` / `deleteBucketVersioning` | Versioning enable/suspend |
| `setBucketQuota(BucketTO, long storeId, long size)` | Per-bucket quota |
| `getAllBucketsUsage(long storeId)` | Usage map for billing/accounting |
| `getBucketAcl` / `setBucketAcl` | ACLs (MinIO/Ceph return null / no-op) |

`BaseObjectStoreDriverImpl` provides no-op defaults for the `DataStoreDriver`
methods (`createAsync`, `deleteAsync`, `copyAsync`, `canCopy`, `resize`,
`getTO`, `getStoreTO`), so object-store providers only implement the bucket/user
methods above.

## How the four existing providers differ (and where SeaweedFS lands)

CloudStack ships four object-store providers. Three are relevant; the simulator
is a test stub.

| Concern | MinIO | Ceph RGW | Cloudian HyperStore | SeaweedFS |
| --- | --- | --- | --- | --- |
| Bucket CRUD | `MinioClient` (S3) | `AmazonS3` (AWS SDK v1) | `AmazonS3` (AWS SDK v1) | `AmazonS3` (AWS SDK v1) |
| Bucket policy | `MinioClient` | `AmazonS3` | `AmazonS3` | `AmazonS3` |
| Versioning | `MinioClient` | `AmazonS3` | `AmazonS3` | `AmazonS3` |
| Encryption | `MinioClient` | not implemented | `AmazonS3` | `AmazonS3` |
| **User creation** | `MinioAdminClient` | `RgwAdmin` | **`AmazonIdentityManagement`** | **`AmazonIdentityManagement`** |
| **Per-bucket quota** | `MinioAdminClient` | `RgwAdmin` | **not supported** (throws) | **S3 extension** (`PUT /{bucket}?seaweedfs-quota`, SigV4, `s3:PutBucketQuota`) |
| **Usage reporting** | `MinioAdminClient` | `RgwAdmin` | Cloudian admin API | S3 `ListObjectsV2` (MVP); Prometheus / SOSAPI `capacity.xml` (recommended) |

**Cloudian HyperStore is the direct precedent.** It is an S3-compatible store
that, like SeaweedFS, manages users via the **standard AWS IAM API** using the
AWS IAM Java SDK (`com.amazonaws.services.identitymanagement`). Its driver
(`CloudianHyperStoreObjectStoreDriverImpl`) and util
(`CloudianHyperStoreUtil`) are the template this design follows almost line for
line. Cloudian even validates the quota limitation the same way this design
proposes for the MVP: `setBucketQuota` throws for any non-zero size and only
accepts `0` (no quota).

The SeaweedFS plugin is therefore a **simpler Cloudian** — same AWS S3 + IAM SDK
clients, same store-details keys (`s3Url`, `iamUrl`, `accesskey`, `secretkey`),
same IAM-user-with-restricted-policy pattern, but with no proprietary admin
client at all (Cloudian has its own `CloudianClient` for its admin API; SeaweedFS
needs only S3 + IAM). For quota, the plugin uses a narrow SeaweedFS S3 extension
(see below); for usage reporting, it falls back to S3 `ListObjectsV2` in the MVP
and recommends Prometheus or SOSAPI `capacity.xml` for production scale.

### Quota via the S3 `?seaweedfs-quota` extension

SeaweedFS supports bucket quota natively (server-side enforcement via a
read-only flag when usage exceeds the limit). Rather than exposing the broad
admin REST API (which would require a global bearer token and grant cluster-wide
admin access), the integration uses a **narrow, scoped S3 subresource**:

- `PUT /{bucket}?seaweedfs-quota` — set bucket quota (IAM permission `s3:PutBucketQuota`)
- `GET /{bucket}?seaweedfs-quota` — get bucket quota (IAM permission `s3:GetBucketQuota`)

The request/response body is JSON:
```json
{"quota_size": 100, "quota_unit": "GB", "quota_enabled": true}
```

Quota is stored on the bucket's filer entry (positive = enabled, negative =
disabled but retained, zero = no quota), matching the existing admin REST API
behavior. When quota is cleared, the bucket's read-only flag is also lifted.

**Authentication** uses the existing S3 SigV4 flow — no new global secret is
needed. The CloudStack service credential (the `accesskey`/`secretkey` on the
object store) is granted only `s3:PutBucketQuota` and `s3:GetBucketQuota` via an
IAM policy, so it cannot delete buckets, manage users, or change cluster
topology. This is the principle of least privilege applied to the integration
boundary.

The plugin's `setBucketQuota` signs and sends the `PUT /{bucket}?seaweedfs-quota`
request using the AWS SDK v1 `S3Signer` for SigV4 signing, then sends the signed
request via `java.net.http.HttpClient` (the AWS S3 SDK doesn't natively support
custom subresources, so we sign manually and send the request ourselves).

### Usage reporting

`getAllBucketsUsage` must return a `Map<String, Long>` of bucket name → size.
MinIO uses `MinioAdminClient.getDataUsageInfo`; Ceph uses
`RgwAdmin.listBucketInfo`. SeaweedFS has no admin rollup endpoint, so the MVP
plugin computes it by listing buckets and summing object sizes via S3
`ListObjectsV2` — expensive for large stores.

For production scale, SeaweedFS already exposes per-bucket size in:
- **Prometheus metrics** (`bucket_size_bytes` gauge, refreshed every minute)
- **SOSAPI `capacity.xml`** (reports capacity, available space, and usage
  through the S3 endpoint)

Operators should consume one of those instead of S3 list-based aggregation for
large deployments. The MVP's list-based approach is correct but slow; flag it as
a known limitation.

## SeaweedFS API surface (what the plugin relies on)

SeaweedFS exposes two relevant APIs, both AWS-compatible:

### S3 API (`weed s3`)
Full S3-compatible surface. Confirmed against the SeaweedFS S3 wiki and code:
- `CreateBucket`, `HeadBucket`, `ListBuckets`, `DeleteBucket`
- `PutBucketPolicy`, `GetBucketPolicy`, `DeleteBucketPolicy`
- `PutBucketVersioning` (Enabled / Suspended), `GetBucketVersioning`
- `PutBucketEncryption`, `GetBucketEncryption`, `DeleteBucketEncryption`
- `PutBucketAcl`, `GetBucketAcl`
- `ListObjectsV2`, `HeadObject`, `GetObject`, `PutObject`, `DeleteObject`
- Bucket quota via extended attributes / `s3.bucket.quota` (enforced server-side,
  surfaced as a read-only state when exceeded — see PR #10224)

### IAM API (`weed iam` / `iamapi`)
AWS IAM-compatible REST endpoints, implemented in `weed/iamapi/`. Confirmed by
the test suite which uses the **AWS IAM SDK** (`aws-sdk-go/service/iam`) against
the same handlers CloudStack would call:
- `CreateUser`, `DeleteUser`, `ListUsers`, `GetUser`
- `CreateAccessKey`, `DeleteAccessKey`, `ListAccessKeys`
- `PutUserPolicy`, `GetUserPolicy`, `DeleteUserPolicy`
- `AttachUserPolicy`, `ListAttachedUserPolicies`

This means the CloudStack plugin can manage SeaweedFS users with the **AWS IAM
Java SDK** (`com.amazonaws.services.identitymanagement.AmazonIdentityManagement`),
exactly the way the AWS IAM Go SDK is used in SeaweedFS's own tests. No proprietary
admin client is needed. **Cloudian HyperStore already does exactly this** in the
CloudStack tree — the SeaweedFS plugin follows the same pattern.

## Design

### Module layout

New CloudStack plugin module, mirroring `plugins/storage/object/cloudian/`
(the closest precedent — same AWS S3 + IAM SDK approach):

```
plugins/storage/object/seaweedfs/
  pom.xml
  src/main/java/org/apache/cloudstack/storage/datastore/
    driver/SeaweedFSObjectStoreDriverImpl.java
    lifecycle/SeaweedFSObjectStoreLifeCycleImpl.java
    provider/SeaweedFSObjectStoreProviderImpl.java
    util/SeaweedFSObjectStoreUtil.java
  src/test/java/org/apache/cloudstack/storage/datastore/
    driver/SeaweedFSObjectStoreDriverImplTest.java
    provider/SeaweedFSObjectStoreProviderImplTest.java
  src/main/resources/META-INF/cloudstack/storage-object-seaweedfs/
    module.properties
    spring-storage-object-seaweedfs-context.xml
```

### `SeaweedFSObjectStoreProviderImpl`

Direct copy of `MinIOObjectStoreProviderImpl` with `providerName = "SeaweedFS"`,
injecting the SeaweedFS lifecycle and driver. Registers via
`storeMgr.registerDriver`.

### `SeaweedFSObjectStoreLifeCycleImpl`

Copy of `MinIOObjectStoreLifeCycleImpl`. `initialize()` reads `url`, `name`,
`accesskey`, `secretkey` from the `addObjectStoragePool` details map, tests the
connection by calling `AmazonS3.listBuckets()` against the SeaweedFS S3 endpoint,
and persists the `ObjectStoreVO`. No proprietary client needed — the AWS S3 SDK
is enough for the health check.

### `SeaweedFSObjectStoreDriverImpl`

The substantive class. Uses two AWS SDK v1 clients (same dependency Ceph already
pulls in, so no new CloudStack dependency):

- `AmazonS3` for bucket operations (path-style, endpoint-pinned, `us-east-1`
  region placeholder — same as Ceph's `getS3Client`)
- `AmazonIdentityManagement` for user/credential operations, pointed at the
  SeaweedFS IAM endpoint

#### Bucket operations — straightforward S3

| Interface method | Implementation |
| --- | --- |
| `createBucket` | `s3.createBucket(name)`; reject if `doesBucketExistV2`; persist access/secret key + URL on `BucketVO` (same as Ceph) |
| `listBuckets` | `s3.listBuckets()` → wrap as `BucketObject` (same as Ceph) |
| `deleteBucket` | `s3.deleteBucket(name)` (same as Ceph) |
| `setBucketPolicy` | `s3.setBucketPolicy(...)` with the same public/private JSON the MinIO/Ceph drivers build |
| `getBucketPolicy` / `deleteBucketPolicy` | `s3.getBucketPolicy` / `s3.deleteBucketPolicy` |
| `setBucketVersioning` | `s3.setBucketVersioningConfiguration(Enabled)` |
| `deleteBucketVersioning` | `s3.setBucketVersioningConfiguration(Suspended)` |
| `setBucketEncryption` | `s3.setBucketEncryptionConfiguration(SSE-S3 rule)` |
| `deleteBucketEncryption` | `s3.deleteBucketEncryptionConfiguration` |
| `getBucketAcl` / `setBucketAcl` | no-op / null (same as MinIO and Ceph) |

#### User creation — the key difference

MinIO calls `MinioAdminClient.addUser`; Ceph calls `RgwAdmin.createUser`. SeaweedFS
exposes the standard AWS IAM API, so the plugin calls:

```java
AmazonIdentityManagement iam = getIamClient(storeId);
String userName = "acs-" + account.getUuid();

// CreateUser (idempotent — check GetUser first, like Ceph does)
iam.createUser(new CreateUserRequest(userName));

// CreateAccessKey → returns the access key + secret key to persist
CreateAccessKeyResult result = iam.createAccessKey(
    new CreateAccessKeyRequest().withUserName(userName));
AccessKey key = result.getAccessKey();

// Persist per-account, same pattern as Ceph's CEPH_ACCESS_KEY/CEPH_SECRET_KEY
details.put(SEAWEEDFS_ACCESS_KEY, key.getAccessKeyId());
details.put(SEAWEEDFS_SECRET_KEY, key.getSecretAccessKey());
_accountDetailsDao.persist(accountId, details);
```

This is the cleanest mapping of the three providers: no proprietary admin client,
just the AWS IAM SDK that CloudStack already has access to. The IAM endpoint URL
is provided as `iamUrl` in the store details. If `iamUrl` is omitted, the driver
defaults it to `s3Url` — SeaweedFS registers its embedded IAM API at `POST /` on
the same S3 endpoint (`UnifiedPostHandler` in `s3api_server.go`), so the IAM
endpoint is the same as the S3 endpoint unless the deployment runs a separate
`weed iam` server.

#### Bucket quota — S3 `?seaweedfs-quota` extension

This is the one genuine gap. MinIO and Ceph both have an admin API to set a
per-bucket quota that the backend enforces. SeaweedFS enforces bucket quota
server-side, but the configuration path was **not exposed over a standard S3 or
IAM API** — it was only set via the admin REST API or shell commands.

The integration adds a **narrow S3 subresource** to SeaweedFS:
- `PUT /{bucket}?seaweedfs-quota` — set bucket quota (IAM permission `s3:PutBucketQuota`)
- `GET /{bucket}?seaweedfs-quota` — get bucket quota (IAM permission `s3:GetBucketQuota`)

This is implemented in SeaweedFS PR #11279. It uses SigV4 authentication and
dedicated IAM permissions, so the CloudStack service credential can be scoped
to quota management only — no global admin token, no cluster-wide admin access.
The enforcement already exists (PR #10224); this PR only adds the HTTP
configuration surface.

An earlier approach (PR #11278, closed) added bearer-token auth to the broad
admin REST API. After review, that was unnecessary for this integration —
static S3 config plus standard S3 APIs plus one scoped quota mutation API is
sufficient and far safer.

> **Note on AWS tools compatibility.** `?seaweedfs-quota` is a SeaweedFS-specific
> S3 subresource, not part of the AWS S3 API. Standard AWS tools (`aws s3api`,
> `s3cmd`, `rclone`) cannot call it directly. This is the same limitation MinIO
> and Ceph have — MinIO quota lives behind a separate admin API (`mc admin
> bucket quota`), and Ceph quota lives behind the Admin Ops API
> (`radosgw-admin quota set`). Neither is callable via `aws s3api` either.
> SeaweedFS's approach is the closest to standard S3 because it uses the same
> endpoint and same SigV4 credentials, just with a custom query parameter.
> Interactive quota management remains available via `weed shell`; the S3
> extension exists for programmatic integration (CloudStack) where the
> integrator can sign SigV4 requests but cannot run shell commands.

#### Usage reporting

`getAllBucketsUsage` must return a `Map<String, Long>` of bucket name → size.
MinIO uses `MinioAdminClient.getDataUsageInfo`; Ceph uses
`RgwAdmin.listBucketInfo`. SeaweedFS has no admin rollup endpoint, so the MVP
plugin computes it by listing buckets and summing object sizes via S3
`ListObjectsV2` — expensive for large stores. Better options exist in
SeaweedFS already:
- **Prometheus metrics** (`bucket_size_bytes` gauge, refreshed every minute)
- **SOSAPI `capacity.xml`** (reports capacity, available space, and usage
  through the S3 endpoint — note: the current "return zero on backend error"
  behavior should be validated before using it for billing)

For the MVP, `listBuckets` + per-bucket size via the S3 API is correct but slow;
flag it as a known limitation. Operators should consume Prometheus or SOSAPI
for production-scale usage reporting.

### Spring wiring

`spring-storage-object-seaweedfs-context.xml` registers the provider bean,
identical to the MinIO one. `module.properties` sets
`name=storage-object-seaweedfs`, `parent=storage`.

### `pom.xml`

Depends on `aws-java-sdk-s3` and `aws-java-sdk-iam` — both already in the
CloudStack dependency tree (Ceph uses the S3 SDK; the IAM SDK is the standard AWS
bundle). No new third-party dependency, unlike MinIO which pulls in the MinIO
Java client.

## What changes on the SeaweedFS side

**One narrow S3 extension is required for quota management.** SeaweedFS PR #11279
adds the `?seaweedfs-quota` S3 subresource:

- `PUT /{bucket}?seaweedfs-quota` — set bucket quota (IAM permission `s3:PutBucketQuota`)
- `GET /{bucket}?seaweedfs-quota` — get bucket quota (IAM permission `s3:GetBucketQuota`)

This is authenticated via standard S3 SigV4 and authorized via dedicated IAM
permissions, so no global admin token is needed. The enforcement already exists
(PR #10224); this PR only adds the HTTP configuration surface.

One follow-up improvement on the SeaweedFS side would close the usage reporting
gap:

1. **Validate SOSAPI `capacity.xml` usage calculation** — the current "return
   zero on backend error" behavior should be validated before using it for
   billing. If reliable, CloudStack can consume it directly instead of
   list-based aggregation.

## Open questions for proIO / Swen

1. **IAM endpoint path.** ~~Where does `weed iam` listen relative to the S3
   endpoint in a typical proIO deployment?~~ **Resolved.** SeaweedFS registers
   its embedded IAM API at `POST /` on the same S3 endpoint
   (`UnifiedPostHandler`), so the driver defaults `iamUrl` to `s3Url`. A
   separate `iamUrl` is only needed if the deployment runs a standalone
   `weed iam` server on a different host/port.
2. **Quota requirements.** Do proIO's customers need server-enforced per-bucket
   quotas, or is CloudStack-side accounting sufficient for the first release?
   The `?seaweedfs-quota` S3 extension (PR #11279) provides server-enforced
   quotas via a scoped credential; this is the recommended path.
3. **Object Lock.** `createBucket` takes an `objectLock` boolean. MinIO supports
   it; Ceph ignores it. SeaweedFS has Object Lock support. Should the plugin pass
   it through?
4. **Contribution model.** Does proIO want to submit the PR to
   `apache/cloudstack` themselves (with SeaweedFS maintainers as reviewers), or
   the reverse? Apache CloudStack requires an ICLA for non-trivial contributions.

## Files

All in the `apache/cloudstack` repo (new module):

| File | Purpose |
| --- | --- |
| `plugins/storage/object/seaweedfs/pom.xml` | Maven module |
| `.../datastore/util/SeaweedFSObjectStoreUtil.java` | S3 + IAM client builders, constants, URL validators |
| `.../datastore/provider/SeaweedFSObjectStoreProviderImpl.java` | Spring provider registration |
| `.../datastore/lifecycle/SeaweedFSObjectStoreLifeCycleImpl.java` | Pool add/health-check |
| `.../datastore/driver/SeaweedFSObjectStoreDriverImpl.java` | Bucket + user ops via S3 + IAM SDK |
| `.../resources/META-INF/cloudstack/storage-object-seaweedfs/module.properties` | Module name |
| `.../resources/META-INF/cloudstack/storage-object-seaweedfs/spring-storage-object-seaweedfs-context.xml` | Spring bean |
| `plugins/pom.xml` | Register `storage/object/seaweedfs` module |

No files in `seaweedfs/seaweedfs` for the MVP.

### SeaweedFS-side changes (PR #11279)

| File | Purpose |
| --- | --- |
| `weed/s3api/s3_constants/s3_action_strings.go` | Add `S3_ACTION_PUT_BUCKET_QUOTA` and `S3_ACTION_GET_BUCKET_QUOTA` |
| `weed/s3api/s3_constants/s3_actions.go` | Add coarse-grained `ACTION_PUT_BUCKET_QUOTA` and `ACTION_GET_BUCKET_QUOTA` |
| `weed/s3api/s3_action_resolver.go` | Map `seaweedfs-quota` query param to fine-grained s3: actions |
| `weed/s3api/s3api_bucket_quota_handlers.go` | New — `PutBucketQuotaHandler` and `GetBucketQuotaHandler` |
| `weed/s3api/s3api_bucket_quota_handlers_test.go` | New — tests for unit conversion, validation, and error paths |
| `weed/s3api/s3api_server.go` | Register the two routes in the bucket subrouter |
