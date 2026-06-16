// Package all registers every tiered-storage backend factory. Binaries that
// serve or manage tiered volumes blank-import it from their composition root;
// keeping the registrations out of weed/storage lets library consumers avoid
// pulling the backend SDKs into their dependency graph.
package all

import (
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/rclone_backend"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/s3_backend"
)
