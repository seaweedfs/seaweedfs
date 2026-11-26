# Yandex Disk as a remote storage

SeaweedFS can mount a Yandex Disk and mirror buckets to it, the same way it uses
S3, Google Cloud Storage, or Azure Blob Storage. The storage type is `yandex`.

Yandex Disk has no buckets: a **top-level folder under `yandex.root` plays the
bucket**, and the folder holds the objects. A file's modification time and
content encoding, which the Disk API cannot store natively, travel as custom
resource properties (`seaweedfs_modified`, `seaweedfs_content_encoding`). The
client also reads `rclone_modified`, so objects uploaded by rclone keep their
mtime.

## Limitations to know first

- The public Disk API has **no multipart upload**: an object is one PUT. Very
  large objects are limited by the plan's maximum file size (see `GET /` quota)
  and need a long HTTP timeout.
- Writes are expensive: one upload is an upload-link request, the PUT, a
  metadata PATCH, and a stat.
- Yandex applies rate limits; the client retries 423, 429, 5xx and 509 with a
  backoff, the sync retries a failed upload from a fresh reader.
- The Disk API lists one folder per request, so `remote.cache` and
  `remote.copy.local`, which walk a whole tree, cost one request per folder.
- S3 keys that contain characters Yandex rejects in a path fail; there is no
  key-encoding layer. S3 keys are not folders: a prefix that only appears as
  part of object names is created on demand.
- This is a consumer product: it fits backups and small mirrors, not millions
  of objects or a high request rate.

## 1. Register an OAuth app

1. Open <https://oauth.yandex.ru/client/new> and create an app.
2. Pick the **Web services** platform.
3. Set the redirect URI to `https://oauth.yandex.ru/verification_code`. That
   page shows the authorization code (or the token) for the operator to copy,
   so no browser is needed on the server.
4. Grant the app the permissions you need:
   - `cloud_api:disk.write` — write anywhere on the disk (needed for buckets
     and for `-yandex.root` outside the app folder),
   - `cloud_api:disk.read` — read the disk,
   - `cloud_api:disk.info` — read the quota,
   - `cloud_api:disk.app_folder` — app folder only; then the configuration
     needs `-yandex.app_folder=true`.
5. Copy the **ClientID** (and the **Client secret** if you want a refreshable
   token).

A Yandex Mail account is required for a working Disk subscription; a token
issued for an account without one fails with
`403 DiskUnsupportedUserAccountTypeError`.

## 2. Get a token

### Refreshable token (recommended)

```bash
# 1. print the authorization url and keep the app credentials
weed shell -master=... <<< "remote.configure -name=disk -type=yandex \
  -yandex.client_id=<client id> -yandex.client_secret=<client secret> -yandex.oauth_url"

# 2. open the printed url, allow the app, copy the code from the page
# 3. store the token
weed shell -master=... <<< "remote.configure -name=disk -yandex.code=<code>"
```

The stored value is a token JSON document
(`{"access_token","refresh_token","token_type","expiry"}`, the same shape
rclone stores), so a token can also be copied from an existing rclone
configuration.

### Debug token (one year, no refresh)

Open
`https://oauth.yandex.ru/authorize?response_type=token&client_id=<client id>`
in a browser, allow the app, and copy the token. Then:

```bash
weed shell -master=... <<< "remote.configure -name=disk -type=yandex -yandex.token=<token>"
```

To try the backend without registering an app, the Disk API polygon
(<https://yandex.ru/dev/disk/poligon/>) issues a one-year token for its own
OAuth app: sign in, click "Получить OAuth-токен", allow the access, and copy
`access_token` from the address the page lands on. That token cannot be
refreshed; use the code flow above for anything long-lived.

## 3. Configure

```bash
remote.configure -name=disk -type=yandex -yandex.token=<token> [-yandex.root=/seaweedfs]
```

| flag | meaning |
| --- | --- |
| `-yandex.token` | token JSON or a bare access token |
| `-yandex.client_id`, `-yandex.client_secret` | OAuth app credentials, needed to refresh the token and for `-yandex.code` |
| `-yandex.root` | folder that holds the buckets, default `/` |
| `-yandex.app_folder` | the token only reaches the app folder: paths are addressed under `app:` |
| `-yandex.trash` | move deleted files to the trash instead of deleting them permanently (the trash keeps occupying quota) |
| `-yandex.endpoint` | override the API endpoint, e.g. for a proxy (default `https://cloud-api.yandex.net/v1/disk`); it must be `https`, plain `http` is accepted on a loopback host only, since every request carries the OAuth token |

A dedicated root (`-yandex.root=/seaweedfs`) is recommended: with the default
`/`, `remote.mount.buckets` treats every top-level folder of the disk,
personal folders included, as a bucket.

## 4. Mount or mirror

```bash
# mount one folder as a local directory
remote.mount -dir=/buckets/photos -remote=disk/photos

# mount every folder under the root as a bucket and pull its metadata
remote.mount.buckets -remote=disk -apply

# pull remote changes into the local metadata later
remote.meta.sync -dir=/buckets/photos

# mirror local buckets to the disk
weed filer.remote.gateway -filer=<filer host>:<filer port> -createBucketAt=disk
```

`weed filer.remote.sync -filer=... -dir=/buckets` uploads changes made under
the mounted directories.

Deleting a local bucket deletes its remote folder only when that folder is
empty, the same rule S3 and GCS apply to a bucket: a Disk API delete is
recursive, and the folder may hold files SeaweedFS never saw.

## Token lifetime and refresh

An access token and its refresh token both live about a year. The client
refreshes the access token when it expires or when the API answers 401, and
uses the rotated refresh token Yandex returns. The refreshed token is kept in
memory only: a client cannot write the configuration back to the filer. A
restart within the token's lifetime keeps working with the stored refresh
token; once Yandex rejects it (`invalid_grant`), issue a new one with
`-yandex.oauth_url` and `-yandex.code` above. Yandex recommends refreshing a
long-lived token every three months.

## Not covered

- `weed filer.backup` and the legacy `weed/replication/sink` sinks have no
  Yandex backend; use `filer.remote.sync` / `filer.remote.gateway`.
- The Rust volume server (`seaweed-volume`) supports only S3-compatible remote
  storage; a Yandex remote cannot back a tiered volume there.

## Troubleshooting

- `403 DiskUnsupportedUserAccountTypeError` — the account has no Yandex Mail /
  Disk subscription.
- `401` on every request — the token is wrong or revoked; reissue it.
- `invalid_grant` while refreshing — the refresh token expired or was rotated
  elsewhere; reissue it.
- `500` right after an upload — Yandex finalizes an upload asynchronously; the
  client retries the stat, and a retry of the whole upload succeeds.
