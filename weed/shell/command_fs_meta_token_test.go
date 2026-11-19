package shell

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/metadata"
)

func TestFsMetaToken(t *testing.T) {
	v := util.GetViper()
	defer v.Set("jwt.filer_signing.subscribe.key", "")
	c := &commandFsMetaToken{}
	var out bytes.Buffer
	if err := c.Do([]string{"-prefix=/a"}, nil, &out); err == nil {
		t.Fatal("minted without a subscribe key")
	}

	v.Set("jwt.filer_signing.subscribe.key", "sub-key")
	if err := c.Do([]string{"-prefix=a"}, nil, &out); err == nil {
		t.Fatal("minted for a relative prefix")
	}
	if err := c.Do([]string{"-prefix=/buckets/a, /buckets/b/", "-expire=1h"}, nil, &out); err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+strings.TrimSpace(out.String())))
	scope, err := security.CheckGrpcMetadataSubscription(ctx, security.SigningKey("sub-key"), nil, security.MethodSubscribeMetadata)
	if err != nil || strings.Join(scope, ",") != "/buckets/a,/buckets/b" {
		t.Fatalf("scope=%v err=%v", scope, err)
	}
}
