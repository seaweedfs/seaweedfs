package weed_server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWebDavStatWithFilerAuthentication(t *testing.T) {
	for _, require := range []bool{false, true} {
		for _, sender := range []struct{ name, key string }{
			{"matching", scopeTestKey},
			{"missing", ""},
			{"different", "webdav-disposable-unrelated-test-key"},
		} {
			t.Run(fmt.Sprintf("require_%v/%s", require, sender.name), func(t *testing.T) {
				t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", sender.key)
				t.Setenv("WEED_JWT_FILER_SIGNING_KEY", "")
				h := newScopeHarness(t)
				h.scope.cfg.requireToken = func() bool { return require }
				h.fake.put("/file", &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 7}})
				host, portText, err := net.SplitHostPort(h.addr)
				if err != nil {
					t.Fatal(err)
				}
				port, err := strconv.Atoi(portText)
				if err != nil {
					t.Fatal(err)
				}
				fs := &WebDavFileSystem{option: &WebDavOption{
					Filer:          pb.NewServerAddress(host, 1, port),
					GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
				}}
				info, err := fs.Stat(context.Background(), "/file")
				accepted := sender.name == "matching" || (!require && sender.name == "missing")
				if !accepted {
					if err == nil || len(h.fake.received("LookupDirectoryEntry")) != 0 {
						t.Fatal("unauthorized WebDAV stat reached the filer handler")
					}
					return
				}
				if err != nil {
					t.Fatalf("stat: %v", err)
				}
				if info.Name() != "file" || info.Size() != 7 || len(h.fake.received("LookupDirectoryEntry")) != 1 {
					t.Fatal("WebDAV stat did not return the stored file metadata")
				}
			})
		}
	}
}
