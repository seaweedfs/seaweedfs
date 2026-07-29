package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestAdvertisedHost(t *testing.T) {
	detected := util.DetectedHostAddress()

	for _, tc := range []struct {
		name   string
		ip     string
		bindIp string
		want   string
	}{
		{"advertised ip wins over wildcard bind", "localhost", "0.0.0.0", "localhost"},
		{"advertised ip wins over specific bind", "s3.example.com", "10.0.0.5", "s3.example.com"},
		{"bind ip used when no advertised ip", "", "10.0.0.5", "10.0.0.5"},
		{"wildcard bind falls back to detected", "", "0.0.0.0", detected},
		{"empty falls back to detected", "", "", detected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			option := &S3ApiServerOption{Ip: tc.ip, BindIp: tc.bindIp}
			if got := option.advertisedHost(); got != tc.want {
				t.Errorf("advertisedHost() = %q, want %q", got, tc.want)
			}
		})
	}
}
