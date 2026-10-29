package s3

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

// The default credential chain applies when an access key or a secret is
// missing, for every S3-compatible type, and only then: the servers hash the
// shared credentials file into the client cache key in that case alone.
func TestUsesDefaultCredentialChain(t *testing.T) {
	for _, tc := range []struct {
		name string
		conf *remote_pb.RemoteConf
		want bool
	}{
		{"s3 with both keys", &remote_pb.RemoteConf{Type: "s3", S3AccessKey: "a", S3SecretKey: "b"}, false},
		{"s3 with a key, no secret", &remote_pb.RemoteConf{Type: "s3", S3AccessKey: "a"}, true},
		{"s3 with no keys", &remote_pb.RemoteConf{Type: "s3"}, true},
		{"b2 with both keys (S3AccessKey unset)", &remote_pb.RemoteConf{Type: "b2", BackblazeKeyId: "a", BackblazeApplicationKey: "b", BackblazeEndpoint: "https://s3.example"}, false},
		{"b2 with no secret", &remote_pb.RemoteConf{Type: "b2", BackblazeKeyId: "a", BackblazeEndpoint: "https://s3.example"}, true},
		{"not S3-compatible", &remote_pb.RemoteConf{Type: "azure"}, false},
	} {
		if got := UsesDefaultCredentialChain(tc.conf); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
