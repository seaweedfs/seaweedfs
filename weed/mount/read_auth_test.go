package mount

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// makeUnsignedJwtWithExp builds a token in header.payload.signature shape whose
// payload carries the given exp claim, so jwtExpiry can parse it without a key.
func makeUnsignedJwtWithExp(t *testing.T, exp int64) string {
	t.Helper()
	payload, err := json.Marshal(struct {
		Exp int64 `json:"exp"`
	}{Exp: exp})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." + enc.EncodeToString(payload) + ".sig"
}

func newTestReadAuth() *ReadAuth {
	return newReadAuth(&WFS{option: &Option{}})
}

// TestReadAuthCachedHonorsRefreshMargin checks that a token with comfortable
// life is served from the cache, while one inside the refresh margin is treated
// as stale so Token triggers a refresh.
func TestReadAuthCachedHonorsRefreshMargin(t *testing.T) {
	ra := newTestReadAuth()
	ra.tokens["fresh"] = readAuthToken{token: "fresh-token", expires: time.Now().Add(time.Minute)}
	ra.tokens["stale"] = readAuthToken{token: "stale-token", expires: time.Now().Add(readAuthRefreshMargin / 2)}

	if token, ok := ra.cached("fresh"); !ok || token != "fresh-token" {
		t.Fatalf("fresh token: got (%q, %v), want (fresh-token, true)", token, ok)
	}
	if token, ok := ra.cached("stale"); ok || token != "" {
		t.Fatalf("within-margin token must be stale: got (%q, %v)", token, ok)
	}
}

// TestReadAuthUnexpiredTokenKeepsValidButShortToken is the core of the
// short-lifetime fix: a token inside the refresh margin but not yet expired is
// still usable, so a read JWT lifetime at or below the margin is not discarded
// the instant it is stored. An expired token is dropped.
func TestReadAuthUnexpiredTokenKeepsValidButShortToken(t *testing.T) {
	ra := newTestReadAuth()
	ra.tokens["short"] = readAuthToken{token: "short-token", expires: time.Now().Add(time.Second)}
	ra.tokens["gone"] = readAuthToken{token: "gone-token", expires: time.Now().Add(-time.Second)}

	if token := ra.unexpiredToken("short"); token != "short-token" {
		t.Fatalf("unexpired short token: got %q, want short-token", token)
	}
	if token := ra.unexpiredToken("gone"); token != "" {
		t.Fatalf("expired token must be dropped: got %q", token)
	}
	if token := ra.unexpiredToken("missing"); token != "" {
		t.Fatalf("unknown fid must return empty: got %q", token)
	}
}

// TestReadAuthTokenFallsBackToUnexpiredWhenRefreshCannotImprove drives Token
// end to end: the fid has a path and a still-valid but short-lived token, and
// the refresh cannot produce a fresher one (the test WFS has no filer). Token
// must return the unexpired token rather than fail the read.
func TestReadAuthTokenFallsBackToUnexpiredWhenRefreshCannotImprove(t *testing.T) {
	ra := newTestReadAuth()
	fid := "3,01637037d6"
	ra.paths[fid] = util.FullPath("/bucket/object")
	ra.tokens[fid] = readAuthToken{token: "short-token", expires: time.Now().Add(time.Second)}

	if token := ra.Token(context.Background(), fid); token != "short-token" {
		t.Fatalf("Token should fall back to the unexpired token: got %q", token)
	}
}

// TestReadAuthTokenEmptyWhenPathUnknown confirms the fail-closed property: with
// no path learned for a fid and no cached token, Token returns no credential
// rather than guessing one.
func TestReadAuthTokenEmptyWhenPathUnknown(t *testing.T) {
	ra := newTestReadAuth()
	if token := ra.Token(context.Background(), "3,deadbeef"); token != "" {
		t.Fatalf("unknown fid must yield no token: got %q", token)
	}
}

// TestReadAuthTrackIndexesChunksByPath checks the fid->path index and its
// no-op guards.
func TestReadAuthTrackIndexesChunksByPath(t *testing.T) {
	ra := newTestReadAuth()
	path := util.FullPath("/bucket/object")
	ra.Track(path, []*filer_pb.FileChunk{{FileId: "3,aaa"}, {FileId: "3,bbb"}, {FileId: ""}})

	if ra.paths["3,aaa"] != path || ra.paths["3,bbb"] != path {
		t.Fatalf("chunks not indexed to path: %v", ra.paths)
	}
	if _, ok := ra.paths[""]; ok {
		t.Fatalf("empty fid must not be indexed")
	}

	// Empty path and nil receiver must be no-ops rather than panic.
	ra.Track("", []*filer_pb.FileChunk{{FileId: "3,ccc"}})
	if _, ok := ra.paths["3,ccc"]; ok {
		t.Fatalf("empty path must not index chunks")
	}
	var nilRA *ReadAuth
	nilRA.Track(path, []*filer_pb.FileChunk{{FileId: "3,ddd"}})
}

// TestReadAuthRefreshDoesNotClobberNewerTrackedPath reproduces the rename race:
// a refresh begins for the old path, a rename re-tracks the fid to the new path
// while the lookup is in flight, and the delayed lookup response then lands. The
// newer path must survive (so a later refresh looks up the live entry, not the
// vacated one) while the fresh token is still recorded. A fid the lookup
// discovers that was not tracked at all is still filled in under the looked-up
// path.
func TestReadAuthRefreshDoesNotClobberNewerTrackedPath(t *testing.T) {
	ra := newTestReadAuth()
	movedFid := "3,01637037d6"
	discoveredFid := "7,0a0b0c0d0e" // a data chunk behind a manifest, never Tracked
	chunk := &filer_pb.FileChunk{FileId: movedFid}

	// The handle learns the fid under the old path (a refresh of it is now in
	// flight), then a rename re-tracks the same fid under the new path.
	ra.Track(util.FullPath("/dir/a"), []*filer_pb.FileChunk{chunk})
	ra.Track(util.FullPath("/dir/b"), []*filer_pb.FileChunk{chunk})

	// The delayed lookup of the old path returns fresh tokens for both the moved
	// fid and a data fid Track never saw.
	exp := time.Now().Add(time.Hour).Unix()
	ra.installRefreshResult(util.FullPath("/dir/a"), map[string]string{
		movedFid:      makeUnsignedJwtWithExp(t, exp),
		discoveredFid: makeUnsignedJwtWithExp(t, exp),
	})

	ra.mu.Lock()
	defer ra.mu.Unlock()
	if ra.paths[movedFid] != util.FullPath("/dir/b") {
		t.Fatalf("rename-tracked path was clobbered: got %q, want /dir/b", ra.paths[movedFid])
	}
	if _, ok := ra.tokens[movedFid]; !ok {
		t.Fatalf("refresh must still record the token for the moved fid")
	}
	if ra.paths[discoveredFid] != util.FullPath("/dir/a") {
		t.Fatalf("a newly discovered fid must be filled under the looked-up path: got %q", ra.paths[discoveredFid])
	}
}

func TestReadAuthRefreshKeepsLongestLivedToken(t *testing.T) {
	for _, tt := range []struct {
		name         string
		heldLifetime time.Duration
		lateLifetime time.Duration
		wantHeld     bool
	}{
		{name: "older response", heldLifetime: 2 * time.Minute, lateLifetime: time.Minute, wantHeld: true},
		{name: "expired response", heldLifetime: time.Minute, lateLifetime: -time.Minute, wantHeld: true},
		{name: "newer response", heldLifetime: time.Minute, lateLifetime: 2 * time.Minute},
		{name: "same expiry", heldLifetime: time.Minute, lateLifetime: time.Minute},
		{name: "expired cache", heldLifetime: -time.Minute, lateLifetime: time.Minute},
		{name: "empty cache", lateLifetime: time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ra := newTestReadAuth()
			fid := "3,01637037d6"
			chunks := []*filer_pb.FileChunk{{FileId: fid}}
			oldPath := util.FullPath("/dir/a")
			newPath := util.FullPath("/dir/b")
			now := time.Now()
			held := makeUnsignedJwtWithExp(t, now.Add(tt.heldLifetime).Unix()) + "-held"
			late := makeUnsignedJwtWithExp(t, now.Add(tt.lateLifetime).Unix()) + "-late"

			// A lookup starts under the old path, then a rename allows another
			// lookup to finish first. Install the delayed response last.
			ra.Track(oldPath, chunks)
			ra.Track(newPath, chunks)
			if tt.heldLifetime != 0 {
				ra.installRefreshResult(newPath, map[string]string{fid: held})
			}
			ra.installRefreshResult(oldPath, map[string]string{fid: late})

			want := late
			if tt.wantHeld {
				want = held
			}
			if ra.Token(context.Background(), fid) != want {
				t.Fatal("Token did not retain the usable token with the latest expiry")
			}
			if ra.paths[fid] != newPath {
				t.Fatalf("late response restored stale path: got %q", ra.paths[fid])
			}
		})
	}
}

func TestReadAuthRefreshPreservesResponseTokensAcrossCapacityReset(t *testing.T) {
	ra := newTestReadAuth()
	ra.tokens = make(map[string]readAuthToken, readAuthMaxFids+3)
	for i := 0; i <= readAuthMaxFids; i++ {
		ra.tokens[strconv.Itoa(i)] = readAuthToken{}
	}
	now := time.Now()
	held := makeUnsignedJwtWithExp(t, now.Add(time.Minute).Unix())
	fresher := makeUnsignedJwtWithExp(t, now.Add(2*time.Minute).Unix())
	expired := makeUnsignedJwtWithExp(t, now.Add(-time.Minute).Unix())
	ra.tokens["retained"] = readAuthToken{token: held, expires: jwtExpiry(held)}
	ra.tokens["renewed"] = readAuthToken{token: held, expires: jwtExpiry(held)}

	path := util.FullPath("/dir/file")
	ra.installRefreshResult(path, map[string]string{
		"retained":   expired,
		"renewed":    fresher,
		"discovered": fresher,
	})

	for fid, want := range map[string]string{"retained": held, "renewed": fresher, "discovered": fresher} {
		if ra.Token(context.Background(), fid) != want {
			t.Errorf("wrong token after capacity reset for %s", fid)
		}
		if ra.paths[fid] != path {
			t.Errorf("response fid %s was not indexed to its path", fid)
		}
	}
	if len(ra.tokens) != 3 {
		t.Fatalf("capacity reset retained unrelated entries: got %d tokens, want 3", len(ra.tokens))
	}
}

// TestJwtExpiryReadsExpClaim checks the unverified exp parse and its fallback.
func TestJwtExpiryReadsExpClaim(t *testing.T) {
	exp := time.Now().Add(42 * time.Second).Unix()
	token := makeUnsignedJwtWithExp(t, exp)
	if got := jwtExpiry(token); got.Unix() != exp {
		t.Fatalf("jwtExpiry: got %d, want %d", got.Unix(), exp)
	}
	// A malformed token falls back to a near-future expiry rather than the zero
	// time, so a token the mount cannot parse is still retried, not trusted
	// forever.
	if got := jwtExpiry("not-a-jwt"); !got.After(time.Now()) {
		t.Fatalf("malformed token must fall back to a future expiry, got %v", got)
	}
}
