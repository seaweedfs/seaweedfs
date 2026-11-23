package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestScopeTokenlessCallsKeepExistingBehavior(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client("")

	expectScopeForwarded(t, h, "LookupDirectoryEntry", "tokenless lookup", func() error {
		_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: bobWS, Name: "file"})
		return err
	})
	expectScopeForwarded(t, h, "CreateEntry", "tokenless metadata create", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: bobWS, Entry: entryWith("file")})
		return err
	})
	if _, err := c.MountRegister(scopeCtx(t), &filer_pb.MountRegisterRequest{PeerAddr: "127.0.0.1:1"}); err != nil {
		t.Fatalf("tokenless MountRegister: %v", err)
	}
	if got := len(h.fake.received("MountRegister")); got != 1 {
		t.Fatalf("tokenless MountRegister reached the handler %d times, want 1", got)
	}
}

func TestScopeUnscopedFilerTokenKeepsExistingBehavior(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(unscopedToken())
	if _, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: "/outside", Name: "file"}); err != nil {
		t.Fatalf("unscoped lookup: %v", err)
	}
	if got := len(h.fake.received("LookupDirectoryEntry")); got != 1 {
		t.Fatalf("unscoped lookup reached the handler %d times, want 1", got)
	}
	if _, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: "/outside", Entry: entryWith("new")}); err != nil {
		t.Fatalf("unscoped create: %v", err)
	}

	// An unrestricted filer token is minted explicitly by the server helper;
	// the scoped helper requires at least one path grant.
	serverToken := string(security.GenJwtForFilerServer(security.SigningKey(scopeTestKey), 3600))
	if _, err := h.client(serverToken).Ping(scopeCtx(t), &filer_pb.PingRequest{}); err != nil {
		t.Fatalf("unscoped filer token: %v", err)
	}
}

func TestScopeMissingSigningKeyLeavesCallsUnscoped(t *testing.T) {
	h := newScopeHarness(t)
	h.key.Store("")
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	if _, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: bobWS, Name: "outside"}); err != nil {
		t.Fatalf("missing-key lookup: %v", err)
	}
	if got := len(h.fake.received("LookupDirectoryEntry")); got != 1 {
		t.Fatalf("missing-key lookup reached the handler %d times, want 1", got)
	}
}
