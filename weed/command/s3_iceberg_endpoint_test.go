package command

import (
	"testing"
)

func strp(s string) *string { return &s }
func intp(i int) *int       { return &i }

func TestDeriveS3AdvertisedEndpoint(t *testing.T) {
	tests := []struct {
		name string
		opt  S3Options
		want string
	}{
		{
			name: "wildcard bind IP with no externalUrl does not advertise an endpoint",
			opt: S3Options{
				bindIp:        strp("0.0.0.0"),
				port:          intp(8333),
				portHttps:     intp(0),
				tlsPrivateKey: strp(""),
				externalUrl:   strp(""),
			},
			want: "",
		},
		{
			name: "empty bind IP with no externalUrl does not advertise an endpoint",
			opt: S3Options{
				bindIp:        strp(""),
				port:          intp(8333),
				portHttps:     intp(0),
				tlsPrivateKey: strp(""),
				externalUrl:   strp(""),
			},
			want: "",
		},
		{
			name: "explicit bind IP is kept",
			opt: S3Options{
				bindIp:        strp("10.0.0.5"),
				port:          intp(8333),
				portHttps:     intp(0),
				tlsPrivateKey: strp(""),
				externalUrl:   strp(""),
			},
			want: "http://10.0.0.5:8333",
		},
		{
			name: "IPv6 literals are bracketed",
			opt: S3Options{
				bindIp:        strp("fe80::1"),
				port:          intp(8333),
				portHttps:     intp(0),
				tlsPrivateKey: strp(""),
				externalUrl:   strp(""),
			},
			want: "http://[fe80::1]:8333",
		},
		{
			name: "TLS key + https port switches scheme and port",
			opt: S3Options{
				bindIp:        strp("10.0.0.5"),
				port:          intp(8333),
				portHttps:     intp(8443),
				tlsPrivateKey: strp("/etc/seaweed/tls.key"),
				externalUrl:   strp(""),
			},
			want: "https://10.0.0.5:8443",
		},
		{
			name: "TLS-only (no separate https port) still uses https on the HTTP port",
			opt: S3Options{
				bindIp:        strp("10.0.0.5"),
				port:          intp(8333),
				portHttps:     intp(0),
				tlsPrivateKey: strp("/etc/seaweed/tls.key"),
				externalUrl:   strp(""),
			},
			want: "https://10.0.0.5:8333",
		},
		{
			name: "externalUrl wins and trailing slash is stripped",
			opt: S3Options{
				bindIp:        strp("10.0.0.5"),
				port:          intp(8333),
				portHttps:     intp(0),
				tlsPrivateKey: strp(""),
				externalUrl:   strp("https://s3.example.com/"),
			},
			want: "https://s3.example.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.opt.deriveS3AdvertisedEndpoint()
			if got != tc.want {
				t.Fatalf("deriveS3AdvertisedEndpoint() = %q, want %q", got, tc.want)
			}
		})
	}
}
