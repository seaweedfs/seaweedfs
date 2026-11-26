package operation

import "net/url"

const (
	// ReplicateSourceParam names the query parameter that says who sent a
	// ?type=replicate write.
	ReplicateSourceParam = "src"
	// ReplicateSourceClient marks a write a gateway fans out to every holder
	// itself. A write without it is a volume server forwarding to its replicas.
	ReplicateSourceClient = "client"
)

// IsReplicaForward reports whether the request is a volume server's own
// primary-to-replica forward, as opposed to a gateway's fan-out write.
func IsReplicaForward(q url.Values) bool {
	return q.Get("type") == "replicate" && q.Get(ReplicateSourceParam) != ReplicateSourceClient
}
