package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGrpcScopeDedicatedPreviousKeyVerifiesTokensAndProofs(t *testing.T) {
	const (
		httpCurrent  = "http-current-key"
		httpPrevious = "http-previous-key"
		grpcCurrent  = "grpc-current-key"
		grpcPrevious = "grpc-previous-key"
		holder       = "scope-holder"
		fid          = "5,0c00000055"
	)

	fs := &FilerServer{filerGuard: security.NewGuard(nil, httpCurrent, 10, "", 0, httpPrevious, "")}
	config := util.NewViperProxy(viper.New())
	config.Set("jwt.filer_signing.grpc_key", grpcCurrent)
	config.Set("jwt.filer_signing.grpc_previous_key", grpcPrevious)
	config.Set("jwt.filer_signing.previous_key", httpPrevious)
	fs.loadGrpcScopeConfig(config)

	grpcScope := newFilerGrpcScope(scopeConfig{verificationKeys: fs.grpcScopeVerificationKeys}, newScopeFakeFiler())
	previousToken := security.GenJwtForFilerScope(security.SigningKey(grpcPrevious), time.Hour, holder, []string{aliceWS}, nil)
	previousProof := security.GenJwtForAssign(security.SigningKey(grpcPrevious), time.Hour, fid, holder, 1, "")
	previousContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(previousToken)))

	resolved, err := grpcScope.resolve(previousContext)
	if err != nil || resolved == nil {
		t.Fatalf("token signed with grpc_previous_key was refused: scope=%v err=%v", resolved, err)
	}
	if !grpcScope.proven(resolved, map[string]string{fid: string(previousProof)}, fid, "") {
		t.Fatal("proof signed with grpc_previous_key was refused")
	}

	httpPreviousToken := security.GenJwtForFilerScope(security.SigningKey(httpPrevious), time.Hour, holder, []string{aliceWS}, nil)
	httpPreviousContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(httpPreviousToken)))
	resolved, err = grpcScope.resolve(httpPreviousContext)
	if resolved != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("HTTP previous_key authenticated a scoped gRPC token: scope=%v err=%v", resolved, err)
	}
	httpPreviousProof := security.GenJwtForAssign(security.SigningKey(httpPrevious), time.Hour, fid, holder, 1, "")
	if grpcScope.proven(&scope{id: holder}, map[string]string{fid: string(httpPreviousProof)}, fid, "") {
		t.Fatal("HTTP previous_key authenticated a gRPC assignment proof")
	}

	// A reload reads the fresh file values. Removing grpc_previous_key revokes
	// both credentials immediately, without retaining the previous setting.
	reloaded := util.NewViperProxy(viper.New())
	reloaded.Set("jwt.filer_signing.grpc_key", grpcCurrent)
	fs.loadGrpcScopeConfig(reloaded)
	resolved, err = grpcScope.resolve(previousContext)
	if resolved != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("removed grpc_previous_key still authenticated a scoped token: scope=%v err=%v", resolved, err)
	}
	if grpcScope.proven(&scope{id: holder}, map[string]string{fid: string(previousProof)}, fid, "") {
		t.Fatal("removed grpc_previous_key still authenticated an assignment proof")
	}
}
