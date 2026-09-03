# Docker

## Compose V2 
SeaweedFS now uses the `v2` syntax `docker compose`

If you rely on using Docker Compose as docker-compose (with a hyphen), you can set up Compose V2 to act as a drop-in replacement of the previous docker-compose. Refer to the [Installing Compose](https://docs.docker.com/compose/install/) section for detailed instructions on upgrading.

Confirm your system has docker compose v2 with a version check
```bash
$ docker compose version
Docker Compose version v2.10.2
```

## Try it out

```bash

wget https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/docker/seaweedfs-compose.yml

docker compose -f seaweedfs-compose.yml -p seaweedfs up

```

## Try latest tip

```bash

wget https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/docker/seaweedfs-dev-compose.yml

docker compose -f seaweedfs-dev-compose.yml -p seaweedfs up

```

## Verify an image signature

Every image CI pushes to `chrislusf/seaweedfs` and `ghcr.io/chrislusf/seaweedfs` is signed with [cosign](https://docs.sigstore.dev/cosign/verifying/verify/), keyless, by the GitHub Actions workflow that built it, so there is no key to fetch or pin. The signature is attached to the image digest and covers the multi-arch index and each platform image in it; `latest` is the release image under another tag and verifies the same way. Images published before September 2026 predate signing.

```bash
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/seaweedfs/seaweedfs/\.github/workflows/container_release_unified\.yml@' \
  chrislusf/seaweedfs:latest
```

The identity ends in the git ref the workflow ran for: `refs/tags/<version>` for a release, `refs/heads/master` when a variant was republished by hand. The other images name the workflow that built them. `dev` is `container_dev.yml`, a `latest` rebuilt by hand is `container_latest.yml`, the `_large_disk_foundationdb` release image is `container_release_foundationdb.yml`, and the per-version FoundationDB and RocksDB builds are `container_foundationdb_version.yml` and `container_rocksdb_version.yml`.

The same check as a Kyverno policy:

```yaml
verifyImages:
  - imageReferences:
      - "chrislusf/seaweedfs:*"
      - "ghcr.io/chrislusf/seaweedfs:*"
    attestors:
      - entries:
          - keyless:
              issuer: https://token.actions.githubusercontent.com
              subject: https://github.com/seaweedfs/seaweedfs/.github/workflows/container_release_unified.yml@refs/tags/*
```

## Local Development

```bash
cd $GOPATH/src/github.com/seaweedfs/seaweedfs/docker
make
```

### S3 cmd

list
```
s3cmd --no-ssl --host=127.0.0.1:8333 ls s3://
```

## Build and push a multiarch build

Make sure that `docker buildx` is supported (might be an experimental docker feature)
```bash
BUILDER=$(docker buildx create --driver docker-container --use)
docker buildx build --pull --push --platform linux/386,linux/amd64,linux/arm64,linux/arm/v7,linux/arm/v6 . -t chrislusf/seaweedfs
docker buildx stop $BUILDER
```

## Minio debugging
```
mc config host add local http://127.0.0.1:9000 some_access_key1 some_secret_key1
mc admin trace --all --verbose local
```
