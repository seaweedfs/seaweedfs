package s3

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

// TestS3CompatibleEndpointCoversAllProviders locks the type-to-endpoint mapping
// the volume server's SSRF guard relies on. Every S3-SDK-backed provider dials
// a caller-supplied endpoint, so each must surface it here; a new sibling that
// forgets to would silently bypass the guard.
func TestS3CompatibleEndpointCoversAllProviders(t *testing.T) {
	cases := []struct {
		conf *remote_pb.RemoteConf
		want string
	}{
		{&remote_pb.RemoteConf{Type: "s3", S3Endpoint: "http://s3.internal"}, "http://s3.internal"},
		{&remote_pb.RemoteConf{Type: "wasabi", WasabiEndpoint: "http://wasabi.internal"}, "http://wasabi.internal"},
		{&remote_pb.RemoteConf{Type: "b2", BackblazeEndpoint: "http://b2.internal"}, "http://b2.internal"},
		{&remote_pb.RemoteConf{Type: "aliyun", AliyunEndpoint: "http://aliyun.internal"}, "http://aliyun.internal"},
		{&remote_pb.RemoteConf{Type: "tencent", TencentEndpoint: "http://tencent.internal"}, "http://tencent.internal"},
		{&remote_pb.RemoteConf{Type: "baidu", BaiduEndpoint: "http://baidu.internal"}, "http://baidu.internal"},
		{&remote_pb.RemoteConf{Type: "filebase", FilebaseEndpoint: "http://filebase.internal"}, "http://filebase.internal"},
		{&remote_pb.RemoteConf{Type: "storj", StorjEndpoint: "http://storj.internal"}, "http://storj.internal"},
		{&remote_pb.RemoteConf{Type: "contabo", ContaboEndpoint: "http://contabo.internal"}, "http://contabo.internal"},
	}
	for _, tc := range cases {
		endpoint, ok := S3CompatibleEndpoint(tc.conf)
		if !ok {
			t.Errorf("type %q: expected an S3-compatible endpoint, got ok=false", tc.conf.Type)
			continue
		}
		if endpoint != tc.want {
			t.Errorf("type %q: endpoint = %q, want %q", tc.conf.Type, endpoint, tc.want)
		}
	}

	// Every registered maker must be an S3-compatible type; the guard treats
	// anything else (gcs, azure, ...) as not dialing a caller-supplied URL.
	if _, ok := S3CompatibleEndpoint(&remote_pb.RemoteConf{Type: "gcs"}); ok {
		t.Error("gcs must not be reported as an S3-compatible endpoint")
	}
}
