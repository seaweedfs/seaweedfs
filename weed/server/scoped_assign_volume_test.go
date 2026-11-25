package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestScopeAssignVolumeBindsProofToHolderAndAllocatedRange(t *testing.T) {
	h := newScopeHarness(t)
	request := &filer_pb.AssignVolumeRequest{Path: aliceWS + "/new", Count: 3}
	key := security.SigningKey(scopeTestKey)
	const fid = "5,0c00000055"
	volumeAuth := security.GenJwtForVolumeServer(key, 60, fid)
	result, err := h.scope.unary(scopedRetentionContext(), request, &grpc.UnaryServerInfo{
		FullMethod: scopeFilerMethodPrefix + "AssignVolume",
	}, func(ctx context.Context, req any) (any, error) {
		if req != request || !scopeKeepOnly(ctx) {
			t.Fatal("assignment lost the request or scoped context")
		}
		return &filer_pb.AssignVolumeResponse{FileId: fid, Count: 3, Auth: string(volumeAuth), Collection: "resolved-collection"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	out := result.(*filer_pb.AssignVolumeResponse)
	if out.Auth != string(volumeAuth) || out.AssignAuth == "" || out.AssignAuth == out.Auth {
		t.Fatal("assignment proof missing or volume upload authorization changed")
	}
	for _, tc := range []struct {
		fid, holder string
		want        bool
	}{
		{fid, "retention", true}, {fid + "_1", "retention", true}, {fid + "_2", "retention", true},
		{fid + "_3", "retention", false}, {fid + "_01", "retention", false}, {fid, "other", false},
	} {
		if got := security.VerifyAssignJwt(key, nil, security.EncodedJwt(out.AssignAuth), tc.fid, tc.holder, 0, "resolved-collection"); got != tc.want {
			t.Errorf("proof fid=%s holder=%s valid=%v, want %v", tc.fid, tc.holder, got, tc.want)
		}
	}
	if security.VerifyAssignJwt(key, nil, security.EncodedJwt(out.AssignAuth), fid, "retention", 0, "other-collection") {
		t.Fatal("assignment proof accepted outside the handler's resolved collection")
	}
}

func TestScopeAssignVolumeRefusesPathsBeforeAllocating(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	for _, p := range []string{"", bobWS + "/new", aliceWS + "/../outside", aliceWS + "/file/"} {
		expectScopeDenied(t, h, "AssignVolume", p, func() error {
			_, err := c.AssignVolume(scopeCtx(t), &filer_pb.AssignVolumeRequest{Path: p, Count: 1})
			return err
		})
	}
	readOnly := h.client(scopedToken("reader", time.Hour, nil, []string{aliceWS}))
	expectScopeDenied(t, h, "AssignVolume", "negative allocation count", func() error {
		_, err := c.AssignVolume(scopeCtx(t), &filer_pb.AssignVolumeRequest{Path: aliceWS + "/new", Count: -1})
		return err
	})
	expectScopeDenied(t, h, "AssignVolume", "read-only grant", func() error {
		_, err := readOnly.AssignVolume(scopeCtx(t), &filer_pb.AssignVolumeRequest{Path: aliceWS + "/new", Count: 1})
		return err
	})
}

func TestScopeAssignVolumeDefaultLifetimeAlwaysExpires(t *testing.T) {
	for _, lifetime := range []time.Duration{0, -time.Second, time.Minute} {
		t.Run(lifetime.String(), func(t *testing.T) {
			h := newScopeHarness(t)
			h.scope.cfg.assignLifetime = func() time.Duration { return lifetime }
			before := time.Now()
			result, err := h.scope.unary(scopedRetentionContext(), &filer_pb.AssignVolumeRequest{Path: aliceWS + "/new"}, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + "AssignVolume",
			}, func(context.Context, any) (any, error) {
				return &filer_pb.AssignVolumeResponse{FileId: "5,0c00000055", Count: 1}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			claims := &security.SeaweedAssignClaims{}
			_, err = security.DecodeJwtWith(security.AssignSigningKey(security.SigningKey(scopeTestKey)), nil,
				security.EncodedJwt(result.(*filer_pb.AssignVolumeResponse).AssignAuth), claims, 0)
			want := lifetime
			if want <= 0 {
				want = security.DefaultAssignTokenLifetime
			}
			if err != nil || claims.ExpiresAt == nil {
				t.Fatalf("proof must expire: %v", err)
			}
			if claims.ExpiresAt.Time.Before(before.Add(want-time.Second)) || claims.ExpiresAt.Time.After(time.Now().Add(want)) {
				t.Fatal("assignment lifetime does not match configured/default duration")
			}
		})
	}
}

func TestGrpcScopeSeparateKeyAuthenticatesScopeAndPreservesOtherServices(t *testing.T) {
	fs := &FilerServer{filerGuard: security.NewGuard(nil, "http-test-key", 10, "", 0, "http-previous-test-key", "")}
	v := util.NewViperProxy(viper.New())
	v.Set("jwt.filer_signing.grpc_key", "grpc-test-key")
	v.Set("jwt.filer_signing.previous_key", "http-previous-test-key")
	fs.loadGrpcScopeConfig(v)
	s := newFilerGrpcScope(scopeConfig{verificationKeys: fs.grpcScopeVerificationKeys}, newScopeFakeFiler())
	good := security.GenJwtForFilerScope(security.SigningKey("grpc-test-key"), time.Hour, "writer", []string{aliceWS}, nil)
	badCurrent := security.GenJwtForFilerScope(security.SigningKey("http-test-key"), time.Hour, "writer", []string{aliceWS}, nil)
	badPrevious := security.GenJwtForFilerScope(security.SigningKey("http-previous-test-key"), time.Hour, "writer", []string{aliceWS}, nil)
	badAdminCurrent := security.GenJwtForFilerAdmin(security.SigningKey("http-test-key"), 60)
	badAdminPrevious := security.GenJwtForFilerAdmin(security.SigningKey("http-previous-test-key"), 60)
	for _, tc := range []struct {
		token   security.EncodedJwt
		method  string
		allowed bool
	}{
		{good, scopeFilerMethodPrefix + "Ping", true},
		{badCurrent, scopeFilerMethodPrefix + "Ping", false},
		{badCurrent, "/iam_pb.SeaweedIdentityAccessManagement/GetUser", true},
		{badPrevious, scopeFilerMethodPrefix + "Ping", false},
		{badPrevious, "/iam_pb.SeaweedIdentityAccessManagement/GetUser", true},
		{badAdminCurrent, scopeFilerMethodPrefix + "Ping", false},
		{badAdminCurrent, "/iam_pb.SeaweedIdentityAccessManagement/GetUser", true},
		{badAdminPrevious, scopeFilerMethodPrefix + "Ping", false},
		{badAdminPrevious, "/iam_pb.SeaweedIdentityAccessManagement/GetUser", true},
	} {
		called := false
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(tc.token)))
		_, err := s.unary(ctx, &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
			called = true
			return &filer_pb.PingResponse{}, nil
		})
		if called != tc.allowed || (err == nil) != tc.allowed {
			t.Fatalf("method=%s called=%v err=%v", tc.method, called, err)
		}
	}
}

func TestScopeAssignVolumeDoesNotSignFailedAllocations(t *testing.T) {
	h := newScopeHarness(t)
	for _, tc := range []struct {
		name     string
		response *filer_pb.AssignVolumeResponse
		err      error
	}{
		{"transport", &filer_pb.AssignVolumeResponse{FileId: "5,0c00000055", Count: 1}, status.Error(codes.Unavailable, "master unavailable")},
		{"application", &filer_pb.AssignVolumeResponse{FileId: "5,0c00000055", Count: 1, Error: "allocation refused"}, nil},
		{"error code", &filer_pb.AssignVolumeResponse{FileId: "5,0c00000055", Count: 1, ErrorCode: filer_pb.FilerError_READ_ONLY}, nil},
		{"empty fid", &filer_pb.AssignVolumeResponse{Count: 1}, nil},
		{"empty count", &filer_pb.AssignVolumeResponse{FileId: "5,0c00000055"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.scope.unary(scopedRetentionContext(), &filer_pb.AssignVolumeRequest{Path: aliceWS + "/new"}, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + "AssignVolume",
			}, func(context.Context, any) (any, error) { return tc.response, tc.err })
			if err != tc.err || tc.response.AssignAuth != "" {
				t.Fatal("failed allocation changed its error or received a proof")
			}
		})
	}
}

func TestScopeAssignVolumeWormRefusedBeforeMasterCall(t *testing.T) {
	fs, scope, store := newScopedWormServer(t)
	insertScopedWormEntry(t, store, "retained", time.Now())
	err := scopedWormUnary(t, scope, "AssignVolume", &filer_pb.AssignVolumeRequest{Path: aliceWS + "/retained", Count: 1}, func(ctx context.Context, req any) (any, error) {
		return fs.AssignVolume(ctx, req.(*filer_pb.AssignVolumeRequest))
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("assignment to WORM entry: %v", err)
	}
}

func TestGrpcScopeAssignmentConfigAndKeyRotation(t *testing.T) {
	for _, seconds := range []int{0, -1, 90} {
		t.Run(time.Duration(seconds).String(), func(t *testing.T) {
			v := util.NewViperProxy(viper.New())
			v.Set("jwt.filer_signing.assign_expires_after_seconds", seconds)
			fs := &FilerServer{}
			fs.loadGrpcScopeConfig(v)
			want := security.DefaultAssignTokenLifetime
			if seconds > 0 {
				want = time.Duration(seconds) * time.Second
			}
			if got := fs.grpcScopeSettings.Load().assignLifetime; got != want {
				t.Fatalf("lifetime=%v, want %v", got, want)
			}
		})
	}
	fs := &FilerServer{filerGuard: security.NewGuard(nil, "http-test-key", 10, "", 0, "http-previous-test-key", "")}
	v := util.NewViperProxy(viper.New())
	fs.loadGrpcScopeConfig(v)
	if fs.grpcScopeSettings.Load().assignLifetime != security.DefaultAssignTokenLifetime {
		t.Fatal("missing lifetime did not use the expiring default")
	}
	key, previous := fs.grpcScopeVerificationKeys()
	if string(key) != "http-test-key" {
		t.Fatal("gRPC did not fall back to HTTP key")
	}
	if string(previous) != "http-previous-test-key" {
		t.Fatal("shared HTTP previous key was not retained for gRPC rotation")
	}
	shared := newFilerGrpcScope(scopeConfig{verificationKeys: fs.grpcScopeVerificationKeys}, newScopeFakeFiler())
	for name, token := range map[string]security.EncodedJwt{
		"current shared key":  security.GenJwtForFilerScope(security.SigningKey("http-test-key"), time.Hour, "writer", []string{aliceWS}, nil),
		"previous shared key": security.GenJwtForFilerScope(security.SigningKey("http-previous-test-key"), time.Hour, "writer", []string{aliceWS}, nil),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(token)))
			called := false
			_, err := shared.unary(ctx, &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "Ping"}, func(context.Context, any) (any, error) {
				called = true
				return &filer_pb.PingResponse{}, nil
			})
			if err != nil || !called {
				t.Fatalf("shared rotation token was refused: called=%v err=%v", called, err)
			}
		})
	}
	v.Set("jwt.filer_signing.grpc_key", "grpc-test-key")
	v.Set("jwt.filer_signing.previous_key", "old-grpc-test-key")
	fs.loadGrpcScopeConfig(v)
	key, previous = fs.grpcScopeVerificationKeys()
	if string(key) != "grpc-test-key" || len(previous) != 0 || string(fs.filerGuard.SigningKey()) != "http-test-key" {
		t.Fatal("gRPC override imported HTTP rotation state or changed HTTP key")
	}
	// Removing the override must revoke it and restore the shared key pair,
	// just as starting with the same configuration would.
	fs.loadGrpcScopeConfig(util.NewViperProxy(viper.New()))
	key, previous = fs.grpcScopeVerificationKeys()
	if string(key) != "http-test-key" || string(previous) != "http-previous-test-key" {
		t.Error("reload did not restore the shared verification keys")
	}
	for _, tc := range []struct {
		name    string
		key     string
		allowed bool
	}{
		{"removed dedicated key", "grpc-test-key", false},
		{"shared current key", "http-test-key", true},
		{"shared previous key", "http-previous-test-key", true},
	} {
		t.Run("reload/"+tc.name, func(t *testing.T) {
			token := security.GenJwtForFilerScope(security.SigningKey(tc.key), time.Hour, "writer", []string{aliceWS}, nil)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(token)))
			called := false
			_, err := shared.unary(ctx, &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "Ping"}, func(context.Context, any) (any, error) {
				called = true
				return &filer_pb.PingResponse{}, nil
			})
			if called != tc.allowed || (err == nil) != tc.allowed {
				t.Fatalf("called=%v err=%v, want allowed=%v", called, err, tc.allowed)
			}
			if !tc.allowed && status.Code(err) != codes.Unauthenticated {
				t.Fatalf("removed key must fail authentication: %v", err)
			}
		})
	}
}

func TestGrpcScopeKeylessReloadMatchesStartup(t *testing.T) {
	for _, mode := range []string{"startup", "reload removed key", "reload empty key"} {
		t.Run(mode, func(t *testing.T) {
			fs := &FilerServer{filerGuard: security.NewGuard(nil, "", 0, "", 0, "", "")}
			if mode != "startup" {
				v := util.NewViperProxy(viper.New())
				v.Set("jwt.filer_signing.grpc_key", "grpc-test-key")
				fs.loadGrpcScopeConfig(v)
			}
			v := util.NewViperProxy(viper.New())
			if mode == "reload empty key" {
				v.Set("jwt.filer_signing.grpc_key", "")
			}
			fs.loadGrpcScopeConfig(v)
			s := newFilerGrpcScope(scopeConfig{verificationKeys: fs.grpcScopeVerificationKeys}, newScopeFakeFiler())
			for name, token := range map[string]security.EncodedJwt{
				"tokenless": "",
				"scoped":    security.GenJwtForFilerScope(security.SigningKey("grpc-test-key"), time.Hour, "writer", []string{aliceWS}, nil),
				"expired":   security.GenJwtForFilerScope(security.SigningKey("grpc-test-key"), -time.Hour, "writer", []string{aliceWS}, nil),
				"other key": security.GenJwtForFilerAdmin(security.SigningKey("other-test-key"), 60),
			} {
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					if token != "" {
						ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+string(token)))
					}
					called := false
					_, err := s.unary(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: bobWS, Name: "file"}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "LookupDirectoryEntry"}, func(ctx context.Context, _ any) (any, error) {
						called = true
						if scopeKeepOnly(ctx) {
							t.Error("keyless call acquired scope enforcement")
						}
						return &filer_pb.LookupDirectoryEntryResponse{}, nil
					})
					if err != nil || !called {
						t.Fatalf("keyless call was refused: called=%v err=%v", called, err)
					}
				})
			}
		})
	}
}
