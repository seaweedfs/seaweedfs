#!/bin/bash
# Helper script to get SHA256 checksum for FoundationDB client package
# Usage: ./get_fdb_checksum.sh <version>
# Example: ./get_fdb_checksum.sh 7.4.5

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <fdb_version>" >&2
    echo "Example: $0 7.4.5" >&2
    exit 1
fi

FDB_VERSION="$1"
PACKAGE="foundationdb-clients_${FDB_VERSION}-1_amd64.deb"
URL="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${PACKAGE}"

echo "Downloading FoundationDB ${FDB_VERSION} client package..."
echo "URL: ${URL}"
echo ""

# Download to temp directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf ${TEMP_DIR}" EXIT

cd "${TEMP_DIR}"
if wget -q "${URL}"; then
    CHECKSUM=$(sha256sum "${PACKAGE}" | awk '{print $1}')
    echo "✓ Download successful"
    echo ""
    echo "SHA256 Checksum:"
    echo "${CHECKSUM}"
    echo ""
    echo "Add this to Dockerfile.foundationdb_large:"
    echo "    \"${FDB_VERSION}\") \\"
    echo "        EXPECTED_SHA256=\"${CHECKSUM}\" ;; \\"
else
    echo "✗ Failed to download package from ${URL}" >&2
    echo "Please verify the version number and URL" >&2
    exit 1
fi

