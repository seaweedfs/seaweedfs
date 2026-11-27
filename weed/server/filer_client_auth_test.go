package weed_server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exercise the actual admission interceptors over gRPC. In optional mode a
// missing bearer is accepted, but presenting an unrelated key is a refusal.
func TestFilerClientAuthReceiverMatrix(t *testing.T) {
	newKey := func() security.SigningKey {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			t.Fatal("generate disposable signing key")
		}
		return security.SigningKey(hex.EncodeToString(b))
	}
	httpKey, grpcKey, otherKey := newKey(), newKey(), newKey()
	for _, receiver := range []struct {
		name       string
		http, grpc security.SigningKey
	}{
		{"no_key", nil, nil},
		{"http_only", httpKey, nil},
		{"grpc_only", nil, grpcKey},
		{"both", httpKey, grpcKey},
	} {
		for _, require := range []bool{false, true} {
			for _, sender := range []struct {
				name string
				key  security.SigningKey
			}{
				{"no_key", nil},
				{"http_key", httpKey},
				{"grpc_key", grpcKey},
				{"different_key", otherKey},
			} {
				t.Run(fmt.Sprintf("%s/require_%v/%s", receiver.name, require, sender.name), func(t *testing.T) {
					h := newScopeHarness(t)
					verificationKey := receiver.grpc
					if len(verificationKey) == 0 {
						verificationKey = receiver.http
					}
					h.scope.cfg.verificationKeys = func() (security.SigningKey, security.SigningKey) { return verificationKey, nil }
					h.scope.cfg.adminKeys = func() (security.SigningKey, security.SigningKey) { return receiver.http, nil }
					h.scope.cfg.requireToken = func() bool { return require }
					source := func(context.Context) (security.EncodedJwt, error) {
						return security.GenJwtForFilerAdmin(sender.key, 10), nil
					}
					client := filer_pb.NewSeaweedFilerClient(security.WithFilerClientAuth(h.conn(""), source))
					accepted := (!require && (len(verificationKey) == 0 || len(sender.key) == 0)) ||
						(len(sender.key) != 0 && (string(sender.key) == string(verificationKey) || string(sender.key) == string(receiver.http)))
					want := codes.Unauthenticated
					if accepted {
						want = codes.OK
					}
					_, err := client.Ping(scopeCtx(t), &filer_pb.PingRequest{})
					if status.Code(err) != want {
						t.Fatalf("unary code=%v, want %v", status.Code(err), want)
					}
					if !accepted && len(sender.key) != 0 && len(verificationKey) != 0 && !strings.Contains(status.Convert(err).Message(), "key does not match") {
						t.Fatal("signature refusal does not explain the key mismatch")
					}
					if got := len(h.fake.received("Ping")); got != map[bool]int{true: 1, false: 0}[accepted] {
						t.Fatalf("unary handler calls=%d, accepted=%v", got, accepted)
					}
					stream, err := client.ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: "/"})
					if err == nil {
						_, err = stream.Recv()
					}
					if err == io.EOF {
						err = nil
					}
					if status.Code(err) != want {
						t.Fatalf("stream code=%v, want %v", status.Code(err), want)
					}
					if got := len(h.fake.received("ListEntries")); got != map[bool]int{true: 1, false: 0}[accepted] {
						t.Fatalf("stream handler calls=%d, accepted=%v", got, accepted)
					}
				})
			}
		}
	}
}

func TestFilerClientAuthReceiverAcceptsPreviousSigningKeys(t *testing.T) {
	newKey := func() security.SigningKey {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			t.Fatal("generate disposable signing key")
		}
		return security.SigningKey(hex.EncodeToString(b))
	}
	dedicatedCurrent, dedicatedPrevious := newKey(), newKey()
	httpCurrent, httpPrevious := newKey(), newKey()
	for _, tc := range []struct {
		name      string
		tokenKey  security.SigningKey
		configure func(*scopeHarness)
	}{
		{
			name:     "dedicated previous key",
			tokenKey: dedicatedPrevious,
			configure: func(h *scopeHarness) {
				h.scope.cfg.verificationKeys = func() (security.SigningKey, security.SigningKey) {
					return dedicatedCurrent, dedicatedPrevious
				}
			},
		},
		{
			name:     "HTTP previous key through admin fallback",
			tokenKey: httpPrevious,
			configure: func(h *scopeHarness) {
				h.scope.cfg.verificationKeys = func() (security.SigningKey, security.SigningKey) {
					return dedicatedCurrent, nil
				}
				h.scope.cfg.adminKeys = func() (security.SigningKey, security.SigningKey) {
					return httpCurrent, httpPrevious
				}
			},
		},
	} {
		for _, expired := range []bool{false, true} {
			name := tc.name
			if expired {
				name += " expired within clock leeway"
			}
			t.Run(name, func(t *testing.T) {
				h := newScopeHarness(t)
				h.scope.cfg.requireToken = func() bool { return true }
				if expired {
					h.scope.cfg.leeway = func() time.Duration { return 30 * time.Second }
				}
				tc.configure(h)
				sourceCalls := 0
				source := func(context.Context) (security.EncodedJwt, error) {
					sourceCalls++
					token := security.GenJwtForFilerAdmin(tc.tokenKey, 10)
					if expired {
						claims := security.SeaweedFilerAdminClaims{RegisteredClaims: jwt.RegisteredClaims{
							ExpiresAt: jwt.NewNumericDate(time.Now().Add(-10 * time.Second)),
						}}
						signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(tc.tokenKey))
						if err != nil {
							t.Fatal("sign expired disposable token")
						}
						token = security.EncodedJwt(signed)
					}
					return token, nil
				}
				client := filer_pb.NewSeaweedFilerClient(security.WithFilerClientAuth(h.conn(""), source))
				if _, err := client.Ping(scopeCtx(t), &filer_pb.PingRequest{}); status.Code(err) != codes.OK {
					t.Fatal("unary call signed with an accepted previous key was refused")
				}
				stream, err := client.ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: "/"})
				if err != nil {
					t.Fatal("stream call signed with an accepted previous key was refused")
				}
				if _, err := stream.Recv(); err != io.EOF {
					t.Fatal("accepted stream did not finish normally")
				}
				if sourceCalls != 2 {
					t.Fatalf("per-call source resolved %d times, want two", sourceCalls)
				}
				if got := len(h.fake.received("Ping")); got != 1 {
					t.Fatalf("unary handler calls=%d, want one", got)
				}
				if got := len(h.fake.received("ListEntries")); got != 1 {
					t.Fatalf("stream handler calls=%d, want one", got)
				}
			})
		}
	}
}
