package security

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAssignJwt(t *testing.T) {
	key, other := SigningKey("filer-key"), SigningKey("other-key")
	// A count-3 assignment covers the base id and base_1, base_2 only.
	token := GenJwtForAssign(key, 0, "3,0100000011", "lease-1", 3)
	single := GenJwtForAssign(key, 0, "3,0100000011", "lease-1", 1)
	if token == "" || single == "" {
		t.Fatal("no token")
	}
	cases := map[string]struct {
		key, previous SigningKey
		token         EncodedJwt
		fid, holder   string
		leeway        time.Duration
		want          bool
	}{
		"valid":                       {key, nil, token, "3,0100000011", "lease-1", 0, true},
		"multi-count id in range":     {key, nil, token, "3,0100000011_2", "lease-1", 0, true},
		"multi-count id out of range": {key, nil, token, "3,0100000011_3", "lease-1", 0, false},
		"single-count rejects suffix": {key, nil, single, "3,0100000011_1", "lease-1", 0, false},
		"single-count base ok":        {key, nil, single, "3,0100000011", "lease-1", 0, true},
		"non-digit suffix":            {key, nil, token, "3,0100000011_x", "lease-1", 0, false},
		"empty suffix":                {key, nil, token, "3,0100000011_", "lease-1", 0, false},
		"zero suffix":                 {key, nil, token, "3,0100000011_0", "lease-1", 0, false},
		"other fid":                   {key, nil, token, "3,0100000012", "lease-1", 0, false},
		"other holder":                {key, nil, token, "3,0100000011", "lease-2", 0, false},
		"other key":                   {other, nil, token, "3,0100000011", "lease-1", 0, false},
		"rotated with previous key":   {other, key, token, "3,0100000011", "lease-1", 0, true},
		"empty token":                 {key, nil, "", "3,0100000011", "lease-1", 0, false},
		"filer token as assignment":   {key, nil, GenJwtForFilerServer(key, 60), "3,0100000011", "lease-1", 0, false},
	}
	for label, tc := range cases {
		if got := VerifyAssignJwt(tc.key, tc.previous, tc.token, tc.fid, tc.holder, tc.leeway); got != tc.want {
			t.Errorf("%s: got %v want %v", label, got, tc.want)
		}
	}
	bounded := GenJwtForAssign(key, time.Second, "3,0100000011", "lease-1", 1)
	if !VerifyAssignJwt(key, nil, bounded, "3,0100000011", "lease-1", 0) {
		t.Error("a bounded token verifies before its expiry")
	}
	time.Sleep(1100 * time.Millisecond)
	if VerifyAssignJwt(key, nil, bounded, "3,0100000011", "lease-1", 0) {
		t.Error("a bounded token expires")
	}
	if !VerifyAssignJwt(key, nil, bounded, "3,0100000011", "lease-1", time.Minute) {
		t.Error("the leeway covers a just-expired token")
	}
	if GenJwtForAssign(nil, 0, "3,1", "l", 1) != "" || GenJwtForAssign(key, 0, "", "l", 1) != "" || GenJwtForAssign(key, 0, "3,1", "", 1) != "" {
		t.Error("no token without a key, a file id or a holder")
	}
}

// A scoped filer token or an assignment token is not an admin credential,
// although both are signed under jwt.filer_signing.key.
func TestCheckGrpcAdminPeerRefusesScopedAndAssignmentTokens(t *testing.T) {
	key := SigningKey("test-signing-key-0123456789abcdef")
	remote := &net.TCPAddr{IP: net.ParseIP("203.0.113.9"), Port: 5000}
	for label, token := range map[string]EncodedJwt{
		"scoped":     GenJwtForFilerScope(key, time.Hour, "lease-1", []string{"/buckets/a"}, nil),
		"read-only":  GenJwtForFilerScope(key, time.Hour, "lease-1", nil, []string{"/buckets/a"}),
		"assignment": GenJwtForAssign(key, 0, "3,1", "lease-1", 1),
	} {
		ctx := metadata.NewIncomingContext(peerCtx(remote), metadata.Pairs("authorization", "Bearer "+string(token)))
		if err := CheckGrpcAdminPeer(ctx, key); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s token accepted on the admin plane: %v", label, err)
		}
	}
	admin := metadata.NewIncomingContext(peerCtx(remote), metadata.Pairs("authorization", "Bearer "+string(GenJwtForFilerAdmin(key, 60))))
	if err := CheckGrpcAdminPeer(admin, key); err != nil {
		t.Errorf("admin token refused: %v", err)
	}
}
