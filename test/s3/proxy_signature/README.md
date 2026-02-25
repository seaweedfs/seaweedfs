# S3 Proxy Signature Verification Test

Integration test that verifies S3 signature verification works correctly when
SeaweedFS is deployed behind a reverse proxy (nginx).

## What it tests

- S3 operations (create bucket, put/get/head/list/delete) through an nginx
  reverse proxy with `X-Forwarded-Host`, `X-Forwarded-Port`, and
  `X-Forwarded-Proto` headers
- SeaweedFS configured with `-s3.externalUrl=http://localhost:9000` so the
  signature verification uses the client-facing host instead of the internal
  backend address

## Architecture

```
AWS CLI (signs with Host: localhost:9000)
    |
    v
nginx (:9000)
    |  proxy_pass → seaweedfs:8333
    |  Sets: X-Forwarded-Host: localhost
    |         X-Forwarded-Port: 9000
    |         X-Forwarded-Proto: http
    v
SeaweedFS S3 (:8333, -s3.externalUrl=http://localhost:9000)
    |  externalHost = "localhost:9000" (parsed at startup)
    |  extractHostHeader() returns "localhost:9000"
    |  Matches what AWS CLI signed with
    v
Signature verification succeeds
```

**Note:** When `-s3.externalUrl` is configured, direct access to the backend
port (8333) will fail signature verification because the client signs with a
different Host header than what `externalUrl` specifies. This is expected —
all S3 traffic should go through the proxy.

## Prerequisites

- Docker and Docker Compose
- AWS CLI v2 (on host or via Docker, see below)

## Running

```bash
# Build the weed binary first (from repo root):
cd /path/to/seaweedfs
go build -o test/s3/proxy_signature/weed ./weed
cd test/s3/proxy_signature

# Start services
docker compose up -d --build

# Option A: Run test with aws CLI installed locally
./test.sh

# Option B: Run test without aws CLI (uses Docker container)
docker run --rm --network host --entrypoint "" amazon/aws-cli:latest \
    bash < test.sh

# Tear down
docker compose down

# Clean up the weed binary
rm -f weed
```

## Troubleshooting

If signature verification fails through the proxy, check:

1. nginx is setting `X-Forwarded-Host` and `X-Forwarded-Port` correctly
2. SeaweedFS is started with `-s3.externalUrl` matching the client endpoint
3. The AWS CLI endpoint URL matches the proxy address

You can also set the `S3_EXTERNAL_URL` environment variable instead of the
`-s3.externalUrl` flag.
