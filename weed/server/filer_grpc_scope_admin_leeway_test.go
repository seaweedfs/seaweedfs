package weed_server

import (
	"context"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	adminLeewayGrpcKey = "dedicated-grpc-key"
	adminLeewayHttpKey = "shared-http-admin-key"
)

func signAdminLeewayToken(t *testing.T, claims security.SeaweedFilerAdminClaims) security.EncodedJwt {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(adminLeewayHttpKey))
	if err != nil {
		t.Fatalf("sign admin token: %v", err)
	}
	return security.EncodedJwt(token)
}

func adminLeewayContext(token security.EncodedJwt) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(token)))
}

func adminLeewayScope(leeway time.Duration) *filerGrpcScope {
	fs := &FilerServer{}
	config := util.NewViperProxy(viper.New())
	config.Set("jwt.filer_signing.clock_leeway_seconds", int(leeway/time.Second))
	fs.loadGrpcScopeConfig(config)
	return newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(adminLeewayGrpcKey), nil
		},
		adminKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(adminLeewayHttpKey), nil
		},
		leeway: func() time.Duration { return fs.grpcScopeSettings.Load().leeway },
	}, newScopeFakeFiler())
}

func TestFilerGrpcAdminFallbackHonorsClockLeeway(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name    string
		claims  security.SeaweedFilerAdminClaims
		leeway  time.Duration
		wantErr codes.Code
	}{
		{
			name: "expired within leeway",
			claims: security.SeaweedFilerAdminClaims{RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(now.Add(-5 * time.Second)),
			}},
			leeway:  10 * time.Second,
			wantErr: codes.OK,
		},
		{
			name: "expired with zero leeway",
			claims: security.SeaweedFilerAdminClaims{RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(now.Add(-5 * time.Second)),
			}},
			wantErr: codes.Unauthenticated,
		},
		{
			name: "expired beyond leeway",
			claims: security.SeaweedFilerAdminClaims{RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(now.Add(-30 * time.Second)),
			}},
			leeway:  10 * time.Second,
			wantErr: codes.Unauthenticated,
		},
		{
			name: "not before within leeway",
			claims: security.SeaweedFilerAdminClaims{RegisteredClaims: jwt.RegisteredClaims{
				NotBefore: jwt.NewNumericDate(now.Add(5 * time.Second)),
				ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
			}},
			leeway:  10 * time.Second,
			wantErr: codes.OK,
		},
		{
			name: "not before beyond leeway",
			claims: security.SeaweedFilerAdminClaims{RegisteredClaims: jwt.RegisteredClaims{
				NotBefore: jwt.NewNumericDate(now.Add(30 * time.Second)),
				ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
			}},
			leeway:  10 * time.Second,
			wantErr: codes.Unauthenticated,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, requireToken := range []bool{false, true} {
				scope := adminLeewayScope(tc.leeway)
				scope.cfg.requireToken = func() bool { return requireToken }
				_, err := scope.resolve(adminLeewayContext(signAdminLeewayToken(t, tc.claims)))
				if got := status.Code(err); got != tc.wantErr {
					t.Fatalf("require=%v: resolve code=%v, want %v (err=%v)", requireToken, got, tc.wantErr, err)
				}
			}
		})
	}
}

func TestFilerGrpcAdminFallbackRejectsScopedAndSessionClaims(t *testing.T) {
	now := time.Now().Add(time.Minute)
	for _, tc := range []struct {
		name   string
		claims security.SeaweedFilerAdminClaims
	}{
		{
			name: "session",
			claims: security.SeaweedFilerAdminClaims{
				SessionId: "session-id",
				RegisteredClaims: jwt.RegisteredClaims{
					ExpiresAt: jwt.NewNumericDate(now),
				},
			},
		},
		{
			name: "allowed prefixes",
			claims: security.SeaweedFilerAdminClaims{
				AllowedPrefixes: []string{"/buckets/test"},
				RegisteredClaims: jwt.RegisteredClaims{
					ExpiresAt: jwt.NewNumericDate(now),
				},
			},
		},
		{
			name: "allowed methods",
			claims: security.SeaweedFilerAdminClaims{
				AllowedMethods: []string{"LookupDirectoryEntry"},
				RegisteredClaims: jwt.RegisteredClaims{
					ExpiresAt: jwt.NewNumericDate(now),
				},
			},
		},
		{
			name: "read prefixes",
			claims: security.SeaweedFilerAdminClaims{
				ReadPrefixes: []string{"/buckets/test"},
				RegisteredClaims: jwt.RegisteredClaims{
					ExpiresAt: jwt.NewNumericDate(now),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := adminLeewayScope(time.Minute).resolve(adminLeewayContext(signAdminLeewayToken(t, tc.claims)))
			if got := status.Code(err); got != codes.Unauthenticated {
				t.Fatalf("resolve code=%v, want Unauthenticated (err=%v)", got, err)
			}
		})
	}
}
