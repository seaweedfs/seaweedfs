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

cosign prints the digest it verified. Deploy by that digest, or let an admission controller resolve the tag, so what runs is what was checked.

The identity is `https://github.com/seaweedfs/seaweedfs/.github/workflows/<workflow>@<ref>`. The ref is `refs/tags/<version>` for a release and `refs/heads/master` when a variant was republished by hand. The workflow is `container_release_unified.yml` for the release images, `container_dev.yml` for `dev`, `container_latest.yml` for a `latest` rebuilt by hand, `container_release_foundationdb.yml` for the `_large_disk_foundationdb` release image, and `container_foundationdb_version.yml` or `container_rocksdb_version.yml` for the per-version builds. The regexp above accepts release images only; `container_[a-z_]+\.yml@` accepts everything this repository publishes, `dev` included.

The same check as a Kyverno policy, release images only:

```yaml
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: verify-seaweedfs-images
spec:
  validationFailureAction: Enforce
  webhookTimeoutSeconds: 30
  rules:
    - name: signed-by-the-release-workflow
      match:
        any:
          - resources:
              kinds:
                - Pod
      verifyImages:
        - imageReferences:
            - "docker.io/chrislusf/seaweedfs:*"
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
