package command

// Register the tiered-storage backends here, at the binary's composition root,
// so library consumers of weed/storage don't pull the backend SDKs.
import _ "github.com/seaweedfs/seaweedfs/weed/storage/backend/all"
