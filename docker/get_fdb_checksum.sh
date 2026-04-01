#!/bin/bash
# Helper script to get SHA256 checksum for FoundationDB client package
# Usage: ./get_fdb_checksum.sh <version> [arch]
# Example: ./get_fdb_checksum.sh 7.4.5 amd64
# Example: ./get_fdb_checksum.sh 7.4.5 arm64

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: $0 <fdb_version> [arch]" >&2
    echo "Example: $0 7.4.5" >&2
    echo "Example: $0 7.4.5 arm64" >&2
    exit 1
fi

FDB_VERSION="$1"
FDB_ARCH="${2:-amd64}"

case "$FDB_ARCH" in
    "amd64")
        CANONICAL_ARCH="amd64"
        PACKAGE_ARCH="amd64"
        ;;
    "arm64"|"aarch64")
        CANONICAL_ARCH="arm64"
        PACKAGE_ARCH="aarch64"
        ;;
    *)
        echo "Error: Architecture must be 'amd64', 'arm64', or 'aarch64'" >&2
        exit 1
        ;;
esac

PACKAGE="foundationdb-clients_${FDB_VERSION}-1_${PACKAGE_ARCH}.deb"
URL="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${PACKAGE}"

echo "Downloading FoundationDB ${FDB_VERSION} client package for ${FDB_ARCH}..."
echo "URL: ${URL}"
echo ""

# Download to temp directory
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

cd "${TEMP_DIR}"
if wget --timeout=30 --tries=3 -q "${URL}"; then
    CHECKSUM=$(sha256sum "${PACKAGE}" | awk '{print $1}')
    echo "✓ Download successful"
    echo ""
    echo "SHA256 Checksum:"
    echo "${CHECKSUM}"
    echo ""
    echo "Add this to Dockerfile.foundationdb_large:"
    echo "    \"${FDB_VERSION}_${CANONICAL_ARCH}\") \\"
    echo "        EXPECTED_SHA256=\"${CHECKSUM}\" ;; \\"
else
    echo "✗ Failed to download package from ${URL}" >&2
    echo "Please verify the version number, architecture, and URL" >&2
    exit 1
fi

