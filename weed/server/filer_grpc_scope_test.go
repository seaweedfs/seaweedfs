package weed_server

// Acceptance tests of scoped filer tokens on the filesystem gRPC service:
// authentication modes, the method table, the path rule, the chunk and
// hard-link rule, streams, the fixed answers and the token lifecycle. Every
// case checks the client's answer and what reached the fake filer: a refused
// call must reach nothing.

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ---- authentication modes ---------------------------------------------------

func TestScopeNoTokenServedUnlessRequired(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client("")
	if _, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: bobWS, Name: "b"}); status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied {
		t.Fatalf("without require, a call without a token must be served: %v", err)
	}
	if len(h.fake.received("LookupDirectoryEntry")) != 1 {
		t.Fatal("the call did not reach the filer")
	}

	h.require = true
	_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: bobWS, Name: "b"})
	expectCode(t, "lookup without a token in require mode", err, codes.Unauthenticated)
	ls, err := c.ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: bobWS})
	if err == nil {
		_, err = ls.Recv()
	}
	expectCode(t, "list without a token in require mode", err, codes.Unauthenticated)
	if n := h.fake.total(); n != 1 {
		t.Fatalf("refused calls reached the filer: %d calls in total", n)
	}

	// An unscoped token (what in-tree clients mint from the key) has full access.
	u := h.client(unscopedToken())
	if _, err := u.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: bobWS, Name: "b"}); status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied {
		t.Fatalf("an unscoped token must be served: %v", err)
	}
	if len(h.fake.received("LookupDirectoryEntry")) != 2 {
		t.Fatal("the unscoped call did not reach the filer")
	}
}

func TestScopeTokenValidation(t *testing.T) {
	h := newScopeHarness(t)
	cases := map[string]struct {
		token string
		want  codes.Code
	}{
		"garbage":            {"not.a.jwt", codes.Unauthenticated},
		"wrong key":          {string(securityGenScoped("other-key", "alice", time.Hour, []string{aliceWS})), codes.Unauthenticated},
		"expired":            {expiredScopedToken(), codes.Unauthenticated},
		"no id":              {scopedToken("", time.Hour, []string{aliceWS}, nil), codes.PermissionDenied},
		"unclean prefix":     {scopedToken("alice", time.Hour, []string{aliceWS + "/"}, nil), codes.PermissionDenied},
		"relative prefix":    {scopedToken("alice", time.Hour, []string{"buckets/t1"}, nil), codes.PermissionDenied},
		"dot-dot prefix":     {scopedToken("alice", time.Hour, []string{aliceWS + "/../bob"}, nil), codes.PermissionDenied},
		"sts session token":  {stsToken(), codes.Unauthenticated},
		"scoped, valid":      {aliceToken(), codes.OK},
		"read-only, reading": {scopedToken("ro", time.Hour, nil, []string{aliceWS}), codes.OK},
	}
	for label, tc := range cases {
		c := h.client(tc.token)
		before := len(h.fake.received("LookupDirectoryEntry"))
		_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "a"})
		got := status.Code(err)
		if tc.want == codes.OK {
			if got == codes.Unauthenticated || got == codes.PermissionDenied {
				t.Errorf("%s: want served, got %v", label, err)
			}
			if len(h.fake.received("LookupDirectoryEntry")) != before+1 {
				t.Errorf("%s: served but did not reach the filer", label)
			}
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %v (%v), want %v", label, got, err, tc.want)
		}
		if len(h.fake.received("LookupDirectoryEntry")) != before {
			t.Errorf("%s: refused but reached the filer", label)
		}
	}
}

// ---- method table -------------------------------------------------------------

// scopeMethods is the table of methods a scoped token may use; every other
// method of the filer service is refused before it reaches the handler.
var scopeMethods = map[string]bool{
	"LookupDirectoryEntry": true, "LookupDirectoryEntries": true, "ListEntries": true,
	"CreateEntry": true, "UpdateEntry": true, "TouchAccessTime": true, "AppendToEntry": true,
	"DeleteEntry": true, "PosixLock": true, "AtomicRenameEntry": true, "StreamRenameEntry": true,
	"StreamMutateEntry": true, "AssignVolume": true, "LookupVolume": true, "Statistics": true,
	"Ping": true, "GetFilerConfiguration": true, "SubscribeMetadata": true,
	"SubscribeLocalMetadata": true,
	// MountRegister is intentionally NOT listed: a scoped token must not write
	// the shared mount peer registry, so it is refused as unavailable.
}

// invoke calls one method of the filer service with an empty message.
func invoke(t *testing.T, conn *grpc.ClientConn, name string, stream *grpc.StreamDesc) error {
	t.Helper()
	ctx := scopeCtx(t)
	full := "/" + filer_pb.SeaweedFiler_ServiceDesc.ServiceName + "/" + name
	if stream == nil {
		return conn.Invoke(ctx, full, &emptypb.Empty{}, &emptypb.Empty{})
	}
	st, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: stream.ServerStreams, ClientStreams: stream.ClientStreams}, full)
	if err != nil {
		return err
	}
	_ = st.SendMsg(&emptypb.Empty{})
	_ = st.CloseSend()
	err = st.RecvMsg(&emptypb.Empty{})
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func isNotAvailable(err error) bool {
	return status.Code(err) == codes.PermissionDenied && strings.Contains(status.Convert(err).Message(), "not available to a scoped token")
}

func TestScopeMethodTable(t *testing.T) {
	h := newScopeHarness(t)
	conn := h.conn(aliceToken())
	desc := filer_pb.SeaweedFiler_ServiceDesc
	seen := map[string]bool{}
	check := func(name string, stream *grpc.StreamDesc) {
		seen[name] = true
		before := h.fake.total()
		err := invoke(t, conn, name, stream)
		if scopeMethods[name] {
			if isNotAvailable(err) {
				t.Errorf("%s is in the table but is refused as unavailable", name)
			}
			return
		}
		if !isNotAvailable(err) {
			t.Errorf("%s is not in the table: got %v, want refused as unavailable", name, err)
		}
		if h.fake.total() != before {
			t.Errorf("%s reached the filer although a scoped token may not use it", name)
		}
	}
	for _, m := range desc.Methods {
		check(m.MethodName, nil)
	}
	for i := range desc.Streams {
		check(desc.Streams[i].StreamName, &desc.Streams[i])
	}
	for name := range scopeMethods {
		if !seen[name] {
			t.Errorf("the table lists %s, which the filer service no longer has", name)
		}
	}
}

// TestScopeMountRegisterDenied proves a scoped token cannot write the shared
// mount peer registry (other mounts read it via MountList for HRW seeding),
// while an unscoped/full token is unaffected.
func TestScopeMountRegisterDenied(t *testing.T) {
	h := newScopeHarness(t)
	_, err := h.client(aliceToken()).MountRegister(scopeCtx(t),
		&filer_pb.MountRegisterRequest{PeerAddr: "10.0.0.9:1", DataCenter: "dc-evil"})
	if status.Code(err) != codes.PermissionDenied || !isNotAvailable(err) {
		t.Errorf("scoped MountRegister: got %v, want PermissionDenied not-available", err)
	}
	if n := len(h.fake.received("MountRegister")); n != 0 {
		t.Errorf("scoped MountRegister reached the filer (%d times)", n)
	}
	// An unscoped token (what in-tree clients mint from the key) is unaffected.
	if _, err := h.client(unscopedToken()).MountRegister(scopeCtx(t),
		&filer_pb.MountRegisterRequest{PeerAddr: "10.0.0.9:1"}); err != nil {
		t.Fatalf("unscoped MountRegister must be served: %v", err)
	}
	if n := len(h.fake.received("MountRegister")); n != 1 {
		t.Errorf("unscoped MountRegister did not reach the filer (got %d)", n)
	}
}

// ---- path rule ----------------------------------------------------------------

type pathOp struct {
	name, method string
	write        bool
	call         func(c filer_pb.SeaweedFilerClient, dir, name string) error
}

var pathOps = []pathOp{
	{"lookup", "LookupDirectoryEntry", false, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.LookupDirectoryEntry(ctxBG(), &filer_pb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
		return err
	}},
	{"batch lookup", "LookupDirectoryEntries", false, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.LookupDirectoryEntries(ctxBG(), &filer_pb.LookupDirectoryEntriesRequest{
			Requests: []*filer_pb.LookupDirectoryEntryRequest{{Directory: aliceWS, Name: "ok"}, {Directory: dir, Name: name}}})
		return err
	}},
	{"touch", "TouchAccessTime", false, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.TouchAccessTime(ctxBG(), &filer_pb.TouchAccessTimeRequest{Directory: dir, Name: name})
		return err
	}},
	{"create", "CreateEntry", true, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: dir, Entry: &filer_pb.Entry{Name: name}})
		return err
	}},
	{"update", "UpdateEntry", true, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.UpdateEntry(ctxBG(), &filer_pb.UpdateEntryRequest{Directory: dir, Entry: &filer_pb.Entry{Name: name}})
		return err
	}},
	{"append", "AppendToEntry", true, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.AppendToEntry(ctxBG(), &filer_pb.AppendToEntryRequest{Directory: dir, EntryName: name})
		return err
	}},
	{"delete", "DeleteEntry", true, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.DeleteEntry(ctxBG(), &filer_pb.DeleteEntryRequest{Directory: dir, Name: name})
		return err
	}},
	{"assign", "AssignVolume", true, func(c filer_pb.SeaweedFilerClient, dir, name string) error {
		_, err := c.AssignVolume(ctxBG(), &filer_pb.AssignVolumeRequest{Path: dir + "/" + name})
		return err
	}},
}

func TestScopePathRule(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()
	cases := []struct {
		label       string
		dir, name   string
		read, write bool
	}{
		{"own file", aliceWS, "a.txt", true, true},
		{"own nested file", aliceWS + "/sub/deeper", "b", true, true},
		{"own grant root", "/buckets/t1/users/alice", "workspace", true, true},
		{"read-only group file", groupG1, "g.txt", true, false},
		{"sibling with the same prefix", "/buckets/t1/users/alice", "workspace2", false, false},
		{"parent directory", "/buckets/t1/users", "alice", false, false},
		{"filesystem root", "/", "buckets", false, false},
		{"another user's workspace", bobWS, "b.txt", false, false},
		{"dot-dot in directory", aliceWS + "/../../bob/workspace", "x", false, false},
		{"double slash", "/buckets//t1/users/alice/workspace", "x", false, false},
		{"trailing slash", aliceWS + "/", "x", false, false},
		{"relative directory", "buckets/t1/users/alice/workspace", "x", false, false},
		{"slash in name", aliceWS, "sub/x", false, false},
		{"dot-dot name", aliceWS, "..", false, false},
		{"empty directory", "", "x", false, false},
	}
	for _, tc := range cases {
		for _, op := range pathOps {
			if op.name == "assign" && (tc.dir == "" || tc.name == ".." || strings.Contains(tc.name, "/")) {
				continue // an assign path is one string: a slash in the name is just a deeper own path
			}
			want := tc.read
			if op.write {
				want = tc.write
			}
			before := len(h.fake.received(op.method))
			err := op.call(c, tc.dir, tc.name)
			forwarded := len(h.fake.received(op.method)) > before
			denied := status.Code(err) == codes.PermissionDenied
			if want && (denied || !forwarded) {
				t.Errorf("%s / %s: want allowed, got %v (forwarded=%v)", tc.label, op.name, err, forwarded)
			}
			if !want && (!denied || forwarded) {
				t.Errorf("%s / %s: want refused and not forwarded, got %v (forwarded=%v)", tc.label, op.name, err, forwarded)
			}
		}
	}
}

func drainList(stream interface {
	Recv() (*filer_pb.ListEntriesResponse, error)
}) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func TestScopeListRule(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()
	cases := map[string]bool{
		aliceWS: true, aliceWS + "/sub": true, groupG1: true,
		"/buckets/t1/users/alice": false, "/buckets/t1/users/alice/workspace2": false,
		bobWS: false, "/": false, "/buckets": false, aliceWS + "/../../bob/workspace": false,
	}
	for dir, want := range cases {
		before := len(h.fake.received("ListEntries"))
		stream, err := c.ListEntries(ctxBG(), &filer_pb.ListEntriesRequest{Directory: dir})
		if err == nil {
			err = drainList(stream)
		}
		forwarded := len(h.fake.received("ListEntries")) > before
		if want && (err != nil || !forwarded) {
			t.Errorf("list %s: want allowed, got %v (forwarded=%v)", dir, err, forwarded)
		}
		if !want && (status.Code(err) != codes.PermissionDenied || forwarded) {
			t.Errorf("list %s: want refused, got %v (forwarded=%v)", dir, err, forwarded)
		}
	}
}

func TestScopeRenameAndLocks(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()
	renames := []struct {
		label                            string
		oldDir, oldName, newDir, newName string
		want                             bool
	}{
		{"within own workspace", aliceWS, "a", aliceWS + "/sub", "a", true},
		{"own into another user's", aliceWS, "a", bobWS, "a", false},
		{"another user's into own", bobWS, "b", aliceWS, "b", false},
		{"own into read-only group", aliceWS, "a", groupG1, "a", false},
		{"read-only group into own", groupG1, "g", aliceWS, "g", false},
	}
	for _, r := range renames {
		before := len(h.fake.received("AtomicRenameEntry"))
		_, err := c.AtomicRenameEntry(ctxBG(), &filer_pb.AtomicRenameEntryRequest{OldDirectory: r.oldDir, OldName: r.oldName, NewDirectory: r.newDir, NewName: r.newName})
		forwarded := len(h.fake.received("AtomicRenameEntry")) > before
		if r.want {
			if err != nil || !forwarded {
				t.Errorf("atomic rename %s: want allowed and dispatched, got err=%v forwarded=%v", r.label, err, forwarded)
			}
		} else if status.Code(err) != codes.PermissionDenied || forwarded {
			t.Errorf("atomic rename %s: want denied and not dispatched, got err=%v forwarded=%v", r.label, err, forwarded)
		}
		before = len(h.fake.received("StreamRenameEntry"))
		st, err := c.StreamRenameEntry(ctxBG(), &filer_pb.StreamRenameEntryRequest{OldDirectory: r.oldDir, OldName: r.oldName, NewDirectory: r.newDir, NewName: r.newName})
		if err == nil {
			_, err = st.Recv()
			if errors.Is(err, io.EOF) {
				err = nil
			}
		}
		forwarded = len(h.fake.received("StreamRenameEntry")) > before
		if r.want {
			if err != nil || !forwarded {
				t.Errorf("stream rename %s: want allowed and dispatched, got err=%v forwarded=%v", r.label, err, forwarded)
			}
		} else if status.Code(err) != codes.PermissionDenied || forwarded {
			t.Errorf("stream rename %s: want denied and not dispatched, got err=%v forwarded=%v", r.label, err, forwarded)
		}
	}

	locks := map[string]bool{
		scopePosixLockKeyPrefix + aliceWS + "/a":         true,
		scopePosixLockKeyPrefix + groupG1 + "/g":         true, // a lock needs read access only
		scopePosixLockKeyPrefix + bobWS + "/b":           false,
		"other.prefix:" + aliceWS + "/a":                 false,
		scopePosixLockKeyPrefix + "hl:" + "00112233aabb": false, // link id unknown to the holder
		scopePosixLockKeyPrefix + "hl:" + "not-hex":      false,
	}
	for key, want := range locks {
		before := len(h.fake.received("PosixLock"))
		_, err := c.PosixLock(ctxBG(), &filer_pb.PosixLockRequest{Key: key})
		forwarded := len(h.fake.received("PosixLock")) > before
		if want {
			if err != nil || !forwarded {
				t.Errorf("lock %q: want allowed and dispatched, got err=%v forwarded=%v", key, err, forwarded)
			}
		} else if status.Code(err) != codes.PermissionDenied || forwarded {
			t.Errorf("lock %q: want denied and not dispatched, got err=%v forwarded=%v", key, err, forwarded)
		}
	}
	before := len(h.fake.received("PosixLock"))
	_, err := c.PosixLock(ctxBG(), &filer_pb.PosixLockRequest{Key: scopePosixLockKeyPrefix + aliceWS + "/a", IsMoved: true})
	if status.Code(err) != codes.PermissionDenied || len(h.fake.received("PosixLock")) != before {
		t.Errorf("a forwarded (is_moved) lock request must be refused, got %v", err)
	}
}

// ---- chunk and hard-link rule ---------------------------------------------------

func TestScopeChunkRule(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()
	create, update := creatorAuth(c), updaterAuth(c)

	// A chunk is stored with the assignment token the filer returned for it.
	fid, token := assign(t, c, aliceWS+"/new")
	expectRefused(t, h, "CreateEntry", "create with an assigned chunk but no token", create(aliceWS, entryWith("new", fid), nil))
	expectForwarded(t, h, "CreateEntry", "create with the assignment token", create(aliceWS, entryWith("new", fid), auth(fid, token)))
	multi, multiToken := assignN(t, c, aliceWS+"/multi", 2)
	expectForwarded(t, h, "CreateEntry", "the base token covers an in-range multi-count id", create(aliceWS, entryWith("multi", multi+"_1"), auth(multi+"_1", multiToken)))
	expectRefused(t, h, "CreateEntry", "the base token does not cover an out-of-range multi-count id", create(aliceWS, entryWith("multiX", multi+"_2"), auth(multi+"_2", multiToken)))

	// A chunk that exists only under another user's path is never accepted.
	const bobsChunk = "7,0a00000077"
	h.fake.put(bobWS+"/secret", entryWith("secret", bobsChunk))
	expectRefused(t, h, "CreateEntry", "create referencing another user's chunk", create(aliceWS, entryWith("x", bobsChunk), nil))
	expectRefused(t, h, "UpdateEntry", "update referencing another user's chunk", update(aliceWS, entryWith("x", bobsChunk), nil))
	expectRefused(t, h, "AppendToEntry", "append of another user's chunk", func() error {
		_, err := c.AppendToEntry(ctxBG(), &filer_pb.AppendToEntryRequest{Directory: aliceWS, EntryName: "x", Chunks: []*filer_pb.FileChunk{chunk(bobsChunk)}})
		return err
	})
	expectRefused(t, h, "CreateEntry", "another user's chunk under alice's token", create(aliceWS, entryWith("x", bobsChunk), auth(bobsChunk, token)))

	// A chunk already stored at the target path needs no token (re-save,
	// truncate, the mount's periodic flush); the same chunk at another path
	// does.
	h.fake.put(aliceWS+"/kept", entryWith("kept", "5,0c00000055"))
	expectForwarded(t, h, "UpdateEntry", "re-saving the chunk stored at the path", update(aliceWS, entryWith("kept", "5,0c00000055"), nil))
	expectRefused(t, h, "CreateEntry", "the stored chunk at a different own path", create(aliceWS, entryWith("other", "5,0c00000055"), nil))
	expectForwarded(t, h, "AppendToEntry", "append with the token", func() error {
		f, tok := assign(t, c, aliceWS+"/kept")
		_, err := c.AppendToEntry(ctxBG(), &filer_pb.AppendToEntryRequest{Directory: aliceWS, EntryName: "kept", Chunks: []*filer_pb.FileChunk{chunk(f)}, ChunkAuth: auth(f, tok)})
		return err
	})

	// The refusal names the chunks without a proof, apart from a path refusal.
	f2, t2 := assign(t, c, aliceWS+"/two")
	err := create(aliceWS, entryWith("two", f2, bobsChunk), auth(f2, t2))()
	if msg := status.Convert(err).Message(); status.Code(err) != codes.PermissionDenied || !strings.Contains(msg, ScopeChunkProofRefused+" "+bobsChunk) || strings.Contains(msg, f2) {
		t.Fatalf("the reason must name only the unproven chunk, got %v", err)
	}
	err = create(bobWS, entryWith("two", f2), auth(f2, t2))()
	if msg := status.Convert(err).Message(); strings.Contains(msg, ScopeChunkProofRefused) || !strings.Contains(msg, "outside the token's scope") {
		t.Fatalf("a path refusal is not a proof refusal, got %v", err)
	}

	// Constructs never accepted, even with a token.
	withSource := entryWith("src", fid)
	withSource.Chunks[0].SourceFileId = fid
	expectRefused(t, h, "CreateEntry", "chunk with a source file id", create(aliceWS, withSource, auth(fid, token)))
	mf, mtok := assign(t, c, aliceWS+"/m")
	manifest := entryWith("m", mf)
	manifest.Chunks[0].IsChunkManifest = true
	expectRefused(t, h, "CreateEntry", "manifest chunk even with a proof", create(aliceWS, manifest, auth(mf, mtok)))
	expectRefused(t, h, "CreateEntry", "remote entry", create(aliceWS, &filer_pb.Entry{Name: "r", RemoteEntry: &filer_pb.RemoteEntry{StorageName: "s3"}}, nil))
	expectRefused(t, h, "CreateEntry", "entry from another cluster", func() error {
		_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("o", fid), ChunkAuth: auth(fid, token), IsFromOtherCluster: true})
		return err
	})
	expectRefused(t, h, "CreateEntry", "forwarded (is_moved) create", func() error {
		_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("o", fid), ChunkAuth: auth(fid, token), IsMoved: true})
		return err
	})

	// A read-only grant refuses stores even with a token.
	expectRefused(t, h, "CreateEntry", "store in a read-only grant", create(groupG1, entryWith("g", fid), auth(fid, token)))

	// Another holder cannot use alice's token.
	expectRefused(t, h, "CreateEntry", "bob storing alice's chunk with her token", creatorAuth(h.bob())(bobWS, entryWith("y", fid), auth(fid, token)))

	// An unscoped caller gets no assignment token and needs none.
	u := h.client(unscopedToken())
	resp, err := u.AssignVolume(ctxBG(), &filer_pb.AssignVolumeRequest{Path: aliceWS + "/u"})
	if err != nil || resp.GetAssignAuth() != "" {
		t.Fatalf("unscoped assign: %v %v", resp, err)
	}
	expectForwarded(t, h, "CreateEntry", "unscoped create", creatorAuth(u)(aliceWS, entryWith("u", resp.GetFileId()), nil))
}

// TestScopeAssignmentTokensAcrossReplicas: a chunk assigned through one filer
// is stored through another that shares the key and the store, with nothing
// but the token; the token survives a key rotation while previous_key is set,
// and expires with leeway when a lifetime is configured.
func TestScopeAssignmentTokensAcrossReplicas(t *testing.T) {
	fake := newScopeFakeFiler()
	a, b := newScopeHarnessWith(t, fake), newScopeHarnessWith(t, fake)
	fid, token := assign(t, a.alice(), aliceWS+"/f")
	expectForwarded(t, b, "CreateEntry", "store on replica b of a chunk assigned by replica a", creatorAuth(b.alice())(aliceWS, entryWith("f", fid), auth(fid, token)))

	// The token is bound to the holder, not to the token that obtained it.
	renewed := b.client(scopedToken("alice", 2*time.Hour, []string{aliceWS}, nil))
	expectForwarded(t, b, "CreateEntry", "renewed token of the same holder", creatorAuth(renewed)(aliceWS, entryWith("g", fid), auth(fid, token)))
	other := b.client(scopedToken("alice-2", time.Hour, []string{aliceWS}, nil))
	expectRefused(t, b, "CreateEntry", "another holder id", creatorAuth(other)(aliceWS, entryWith("h", fid), auth(fid, token)))
	narrowed := b.client(scopedToken("alice", 3*time.Hour, nil, []string{aliceWS}))
	expectRefused(t, b, "CreateEntry", "write after the renewal made the scope read-only", creatorAuth(narrowed)(aliceWS, entryWith("i", fid), auth(fid, token)))

	// Key rotation: refused on the rotated replica until previous_key names
	// the old key; a scoped token minted under the old key is treated alike.
	b.key = "rotated-key"
	bearerOld := b.client(aliceToken())
	if _, err := bearerOld.Ping(ctxBG(), &filer_pb.PingRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("a token under the old key must fail after rotation, got %v", err)
	}
	b.previousKey = scopeTestKey
	expectForwarded(t, b, "CreateEntry", "old assignment token under previous_key", creatorAuth(bearerOld)(aliceWS, entryWith("j", fid), auth(fid, token)))
	b.previousKey = ""
	rotatedBearer := b.client(string(securityGenScoped("rotated-key", "alice", time.Hour, []string{aliceWS})))
	expectRefused(t, b, "CreateEntry", "old assignment token without previous_key", creatorAuth(rotatedBearer)(aliceWS, entryWith("k", fid), auth(fid, token)))
	fid2, token2 := assign(t, rotatedBearer, aliceWS+"/l")
	expectRefused(t, a, "CreateEntry", "token under the new key on the replica that still has the old one", creatorAuth(a.alice())(aliceWS, entryWith("l", fid2), auth(fid2, token2)))

	// Lifetime and leeway.
	advance := fakeClock(t)
	a.assignLifetime = time.Minute
	fid3, token3 := assign(t, a.alice(), aliceWS+"/m")
	if !security.VerifyAssignJwt(security.SigningKey(scopeTestKey), nil, security.EncodedJwt(token3), fid3, "alice", 0) {
		t.Fatal("a fresh bounded token verifies")
	}
	expired := expiredAssignToken(fid3, "alice")
	expectRefused(t, a, "CreateEntry", "expired assignment token", creatorAuth(a.alice())(aliceWS, entryWith("m", fid3), auth(fid3, expired)))
	a.leeway = 2 * time.Minute
	expectForwarded(t, a, "CreateEntry", "expired within the leeway", creatorAuth(a.alice())(aliceWS, entryWith("m", fid3), auth(fid3, expired)))
	_ = advance
}

// TestScopeAssignTokenIsNotACredential: an assignment token presented as a
// bearer is refused: it is signed with a derived key.
func TestScopeAssignTokenIsNotACredential(t *testing.T) {
	h := newScopeHarness(t)
	_, token := assign(t, h.alice(), aliceWS+"/f")
	_, err := h.client(token).LookupDirectoryEntry(ctxBG(), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "f"})
	expectCode(t, "assignment token as a bearer", err, codes.Unauthenticated)
	if n := len(h.fake.received("LookupDirectoryEntry")); n != 0 {
		t.Fatalf("the call reached the filer %d times", n)
	}
}

func newLinkID(b byte) []byte {
	id := make([]byte, 17)
	for i := 0; i < 16; i++ {
		id[i] = b + byte(i)
	}
	id[16] = scopeHardLinkMarker
	return id
}

func TestScopeHardLinkRule(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()
	create, update := creator(c), updater(c)
	link := func(name string, id []byte, fids ...string) *filer_pb.Entry {
		e := entryWith(name, fids...)
		e.HardLinkId = id
		return e
	}
	linkOf := func(dir string, e *filer_pb.Entry, source string) func() error {
		return func() error {
			_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: dir, Entry: e, LinkSource: source})
			return err
		}
	}
	h.fake.put(aliceWS+"/plain", entryWith("plain", "1,0100000011"))
	id := newLinkID(0x10)

	// Each condition alone refuses a new hard-link id.
	expectRefused(t, h, "CreateEntry", "create introducing a new link id", create(aliceWS, link("h", id)))
	expectRefused(t, h, "UpdateEntry", "id without the marker byte", update(aliceWS, link("plain", id[:16], "1,0100000011")))
	taken := newLinkID(0x20)
	h.fake.mu.Lock()
	h.fake.kv[string(taken)] = []byte("registered elsewhere")
	h.fake.mu.Unlock()
	expectRefused(t, h, "UpdateEntry", "id the filer already has", update(aliceWS, link("plain", taken, "1,0100000011")))
	h.fake.put(aliceWS+"/linked", link("linked", newLinkID(0x30)))
	expectRefused(t, h, "UpdateEntry", "entry that already has a link", update(aliceWS, link("linked", newLinkID(0x40))))
	expectRefused(t, h, "UpdateEntry", "link in a read-only grant", update(groupG1, link("g", newLinkID(0x50))))
	expectRefused(t, h, "UpdateEntry", "link on an entry that does not exist", update(aliceWS, link("absent", newLinkID(0x60))))

	// All conditions met: the mount's first link of a file is accepted
	// (its chunks are those stored at the path) and the registry learns it.
	expectForwarded(t, h, "UpdateEntry", "first link of an own file", update(aliceWS, link("plain", id, "1,0100000011")))
	h.fake.put(aliceWS+"/plain", link("plain", id, "1,0100000011"))
	h.fake.mu.Lock()
	h.fake.kv[string(id)] = []byte("plain's link entry")
	h.fake.mu.Unlock()

	// A second name needs the source: an entry stored with the id at a
	// writable path. Its chunks come from the source, with no token.
	expectRefused(t, h, "CreateEntry", "second name without a source", create(aliceWS, link("plain-2", id, "1,0100000011")))
	expectRefused(t, h, "CreateEntry", "second name with a wrong source", linkOf(aliceWS, link("plain-2", id, "1,0100000011"), aliceWS+"/linked"))
	expectRefused(t, h, "CreateEntry", "source without a link id", linkOf(aliceWS, entryWith("plain-3", "1,0100000011"), aliceWS+"/plain"))
	expectForwarded(t, h, "CreateEntry", "second name naming its source", linkOf(aliceWS, link("plain-2", id, "1,0100000011"), aliceWS+"/plain"))
	expectForwarded(t, h, "UpdateEntry", "update of a linked entry keeps its id", update(aliceWS, link("plain", id, "1,0100000011")))
	expectForwarded(t, h, "PosixLock", "lock on a registered link", func() error {
		_, err := c.PosixLock(ctxBG(), &filer_pb.PosixLockRequest{Key: scopePosixLockKeyPrefix + "hl:" + hex.EncodeToString(id)})
		return err
	})

	// Another holder cannot attach alice's id, with or without her path as source.
	expectRefused(t, h, "CreateEntry", "bob using alice's link id", creator(h.bob())(bobWS, link("z", id)))
	expectRefused(t, h, "CreateEntry", "bob naming alice's file as source", func() error {
		_, err := h.bob().CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: bobWS, Entry: link("z", id, "1,0100000011"), LinkSource: aliceWS + "/plain"})
		return err
	})
}

// ---- streams --------------------------------------------------------------------

func ev(dir string, n *filer_pb.EventNotification) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{Directory: dir, EventNotification: n}
}

func TestScopeSubscription(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()

	refused := []*filer_pb.SubscribeMetadataRequest{
		{PathPrefix: "/buckets/t1/users/"},
		{PathPrefix: "/"},
		{PathPrefixes: []string{bobWS + "/"}},
		{Directories: []string{bobWS}},
		{PathPrefix: aliceWS + "/", Directories: []string{"/buckets"}},
		{PathPrefix: "/buckets/t1/users/alice/workspace2/"},
		{},
	}
	for _, req := range refused {
		st, err := c.SubscribeMetadata(scopeCtx(t), req)
		if err == nil {
			_, err = st.Recv()
		}
		expectCode(t, "subscription "+req.String(), err, codes.PermissionDenied)
	}
	if n := len(h.fake.received("SubscribeMetadata")); n != 0 {
		t.Fatalf("refused subscriptions reached the filer %d times", n)
	}

	st, err := c.SubscribeMetadata(scopeCtx(t), &filer_pb.SubscribeMetadataRequest{
		PathPrefix: aliceWS + "/", PathPrefixes: []string{"/etc/seaweedfs/filer.conf"}, ClientSupportsMetadataChunks: true})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitFor(t, "the subscription to reach the handler", func() bool { return len(h.fake.received("SubscribeMetadata")) == 1 })
	fwd := h.fake.received("SubscribeMetadata")[0].(*filer_pb.SubscribeMetadataRequest)
	if fwd.GetClientSupportsMetadataChunks() {
		t.Error("metadata chunks must be switched off in the handled subscription")
	}
	if len(fwd.GetPathPrefixes()) != 0 {
		t.Errorf("the filer.conf prefix must be dropped, handler got %v", fwd.GetPathPrefixes())
	}

	inside := ev(aliceWS, &filer_pb.EventNotification{NewEntry: entryWith("in.txt", "8,0d00000088")})
	outside := ev(bobWS, &filer_pb.EventNotification{NewEntry: entryWith("out.txt", "9,0e00000099")})
	sibling := ev("/buckets/t1/users/alice/workspace2", &filer_pb.EventNotification{NewEntry: entryWith("s.txt")})
	renameOut := ev(aliceWS, &filer_pb.EventNotification{OldEntry: entryWith("a"), NewEntry: entryWith("a"), NewParentPath: bobWS})
	renameIn := ev(bobWS, &filer_pb.EventNotification{OldEntry: entryWith("b"), NewEntry: entryWith("b"), NewParentPath: aliceWS})
	bareOut := ev(bobWS, &filer_pb.EventNotification{})
	bareIn := ev(aliceWS, &filer_pb.EventNotification{})
	batch := &filer_pb.SubscribeMetadataResponse{Events: []*filer_pb.SubscribeMetadataResponse{inside, outside}}
	for _, e := range []*filer_pb.SubscribeMetadataResponse{inside, outside, sibling, renameOut, renameIn, bareOut, bareIn, batch} {
		h.fake.events <- e
	}

	want := []struct {
		label string
		ok    func(r *filer_pb.SubscribeMetadataResponse) bool
	}{
		{"event inside the scope", func(r *filer_pb.SubscribeMetadataResponse) bool {
			return r.GetEventNotification().GetNewEntry().GetName() == "in.txt"
		}},
		{"rename out arrives as a delete", func(r *filer_pb.SubscribeMetadataResponse) bool {
			n := r.GetEventNotification()
			return n.GetOldEntry().GetName() == "a" && n.GetNewEntry() == nil && n.GetNewParentPath() == ""
		}},
		{"rename in arrives as a create", func(r *filer_pb.SubscribeMetadataResponse) bool {
			n := r.GetEventNotification()
			return n.GetNewEntry().GetName() == "b" && n.GetOldEntry() == nil
		}},
		{"entry-less event inside", func(r *filer_pb.SubscribeMetadataResponse) bool {
			n := r.GetEventNotification()
			return r.GetDirectory() == aliceWS && n != nil && n.GetNewEntry() == nil && n.GetOldEntry() == nil
		}},
		{"batch filtered to the inside event", func(r *filer_pb.SubscribeMetadataResponse) bool {
			return len(r.GetEvents()) == 1 && r.GetEvents()[0].GetDirectory() == aliceWS
		}},
	}
	for _, w := range want {
		got, err := st.Recv()
		if err != nil {
			t.Fatalf("waiting for %q: %v", w.label, err)
		}
		if !w.ok(got) {
			t.Fatalf("expected %q, got %v", w.label, got)
		}
	}
	if h.fake.subscribers() == 0 {
		t.Fatal("the subscription should still be open")
	}

	// A response with log-file references (metadata chunks) ends the stream.
	h.fake.events <- &filer_pb.SubscribeMetadataResponse{LogFileRefs: []*filer_pb.LogFileChunkRef{{FilerId: "f1"}}}
	_, err = st.Recv()
	expectCode(t, "log file references", err, codes.PermissionDenied)
}

func TestScopeStreamMutate(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()
	st, err := c.StreamMutateEntry(scopeCtx(t))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	send := func(id uint64, req *filer_pb.StreamMutateEntryRequest) {
		req.RequestId = id
		if err := st.Send(req); err != nil {
			t.Fatalf("send %d: %v", id, err)
		}
	}
	recv := func(wantID uint64) *filer_pb.StreamMutateEntryResponse {
		resp, err := st.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", wantID, err)
		}
		if resp.GetRequestId() != wantID {
			t.Fatalf("got response for %d, want %d", resp.GetRequestId(), wantID)
		}
		return resp
	}

	send(1, &filer_pb.StreamMutateEntryRequest{Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
		CreateRequest: &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("ok")}}})
	if r := recv(1); r.GetError() != "" {
		t.Fatalf("allowed create answered with error %q", r.GetError())
	}
	send(2, &filer_pb.StreamMutateEntryRequest{Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
		CreateRequest: &filer_pb.CreateEntryRequest{Directory: bobWS, Entry: entryWith("no")}}})
	if r := recv(2); r.GetErrno() != scopeErrnoEACCES || r.GetError() == "" || !r.GetIsLast() {
		t.Fatalf("refused create must answer errno 13 with a reason, got %v", r)
	}
	send(3, &filer_pb.StreamMutateEntryRequest{Request: &filer_pb.StreamMutateEntryRequest_RenameRequest{
		RenameRequest: &filer_pb.StreamRenameEntryRequest{OldDirectory: aliceWS, OldName: "ok", NewDirectory: bobWS, NewName: "ok"}}})
	if r := recv(3); r.GetErrno() != scopeErrnoEACCES {
		t.Fatalf("refused rename must answer errno 13, got %v", r)
	}
	send(4, &filer_pb.StreamMutateEntryRequest{Request: &filer_pb.StreamMutateEntryRequest_DeleteRequest{
		DeleteRequest: &filer_pb.DeleteEntryRequest{Directory: groupG1, Name: "g"}}})
	if r := recv(4); r.GetErrno() != scopeErrnoEACCES {
		t.Fatalf("delete in a read-only grant must answer errno 13, got %v", r)
	}
	send(5, &filer_pb.StreamMutateEntryRequest{Request: &filer_pb.StreamMutateEntryRequest_DeleteRequest{
		DeleteRequest: &filer_pb.DeleteEntryRequest{Directory: aliceWS, Name: "ok"}}})
	if r := recv(5); r.GetError() != "" {
		t.Fatalf("the stream must keep working after a refusal, got %v", r)
	}
	if err := st.CloseSend(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := st.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF after close, got %v", err)
	}

	got := h.fake.received("StreamMutateEntry")
	if len(got) != 2 {
		t.Fatalf("handler received %d mutate messages, want the 2 allowed ones", len(got))
	}
	for i, id := range []uint64{1, 5} {
		if got[i].(*filer_pb.StreamMutateEntryRequest).GetRequestId() != id {
			t.Fatalf("handled message %d has request id %d, want %d", i, got[i].(*filer_pb.StreamMutateEntryRequest).GetRequestId(), id)
		}
	}
}

func TestScopeFixedAnswers(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()

	_, err := c.LookupDirectoryEntry(ctxBG(), &filer_pb.LookupDirectoryEntryRequest{Directory: "/etc/seaweedfs", Name: "filer.conf"})
	if err == nil || status.Convert(err).Message() != filer_pb.ErrNotFound.Error() {
		t.Fatalf("filer.conf lookup must answer the filer's not-found, got %v", err)
	}
	if n := len(h.fake.received("LookupDirectoryEntry")); n != 0 {
		t.Fatalf("filer.conf lookup reached the handler %d times", n)
	}
	_, err = c.LookupDirectoryEntry(ctxBG(), &filer_pb.LookupDirectoryEntryRequest{Directory: "/etc/seaweedfs", Name: "other"})
	expectCode(t, "another file under /etc/seaweedfs", err, codes.PermissionDenied)

	if _, err := c.Ping(ctxBG(), &filer_pb.PingRequest{Target: "10.0.0.9:8888", TargetType: "filer"}); err != nil {
		t.Fatalf("ping: %v", err)
	}
	got := h.fake.received("Ping")
	if len(got) != 1 || got[0].(*filer_pb.PingRequest).GetTarget() != "" || got[0].(*filer_pb.PingRequest).GetTargetType() != "" {
		t.Fatalf("ping must reach the handler with its target cleared, got %v", got)
	}
}

// filer.conf is hidden from a scoped token on every read path, not only the
// single LookupDirectoryEntry, even when the token's grant covers /etc/seaweedfs.
func TestScopeFilerConfHiddenOnBatchLookupAndList(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("etc", time.Hour, []string{scopeFilerConfDir}, nil))
	h.fake.put(scopeFilerConfDir+"/"+scopeFilerConfName, entryWith(scopeFilerConfName))
	h.fake.put(scopeFilerConfDir+"/other.json", entryWith("other.json"))

	// batch lookup: filer.conf is dropped before the handler; other.json reaches it.
	resp, err := c.LookupDirectoryEntries(ctxBG(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests: []*filer_pb.LookupDirectoryEntryRequest{
			{Directory: scopeFilerConfDir, Name: scopeFilerConfName},
			{Directory: scopeFilerConfDir, Name: "other.json"},
		},
	})
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	// Results stay positional: the filer.conf slot is a miss, other.json keeps
	// its slot (a shortened response would misalign a position-matching caller).
	if n := len(resp.GetResults()); n != 2 {
		t.Fatalf("batch lookup: want 2 results in order, got %d", n)
	}
	if resp.GetResults()[0].GetFound() || resp.GetResults()[0].GetEntry() != nil {
		t.Error("batch lookup: the filer.conf slot must be a miss")
	}
	if resp.GetResults()[1].GetEntry().GetName() != "other.json" {
		t.Errorf("batch lookup: slot 1 want other.json, got %q", resp.GetResults()[1].GetEntry().GetName())
	}
	for _, got := range h.fake.received("LookupDirectoryEntries") {
		for _, lk := range got.(*filer_pb.LookupDirectoryEntriesRequest).GetRequests() {
			if lk.GetName() == scopeFilerConfName {
				t.Fatal("filer.conf reached the batch handler")
			}
		}
	}

	// An all-filer.conf batch is answered all-miss without dispatching an empty
	// batch to the handler (the handler rejects an empty forwarded batch).
	before := len(h.fake.received("LookupDirectoryEntries"))
	allConf, err := c.LookupDirectoryEntries(ctxBG(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests: []*filer_pb.LookupDirectoryEntryRequest{
			{Directory: scopeFilerConfDir, Name: scopeFilerConfName},
			{Directory: scopeFilerConfDir, Name: scopeFilerConfName},
		},
	})
	if err != nil {
		t.Fatalf("all-filer.conf batch: %v", err)
	}
	if n := len(allConf.GetResults()); n != 2 {
		t.Fatalf("all-filer.conf batch: want 2 miss results, got %d", n)
	}
	for i, res := range allConf.GetResults() {
		if res.GetFound() || res.GetEntry() != nil {
			t.Errorf("all-filer.conf batch: slot %d must be a miss", i)
		}
	}
	if len(h.fake.received("LookupDirectoryEntries")) != before {
		t.Error("all-filer.conf batch must not dispatch to the handler")
	}

	// list: filer.conf is omitted from the directory listing; other.json is kept.
	stream, err := c.ListEntries(ctxBG(), &filer_pb.ListEntriesRequest{Directory: scopeFilerConfDir})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	names := map[string]bool{}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("list recv: %v", err)
		}
		names[m.GetEntry().GetName()] = true
	}
	if names[scopeFilerConfName] {
		t.Fatal("list returned filer.conf")
	}
	if !names["other.json"] {
		t.Fatalf("list dropped a non-filer.conf entry; got %v", names)
	}
}

// filer.conf events are dropped from a scoped subscription even when the grant
// covers /etc/seaweedfs (the checkSubscribe prefix rewrite only drops the
// prefix weed mount adds, not the delivered events).
func TestScopeFilterEventDropsFilerConf(t *testing.T) {
	grants, _ := scopeGrantsFromClaims([]string{scopeFilerConfDir}, nil)
	sc := &scope{grants: grants}
	if sc.filterEvent(ev(scopeFilerConfDir, &filer_pb.EventNotification{NewEntry: entryWith(scopeFilerConfName)})) != nil {
		t.Error("a filer.conf create/update event must be dropped")
	}
	if sc.filterEvent(ev(scopeFilerConfDir, &filer_pb.EventNotification{OldEntry: entryWith(scopeFilerConfName)})) != nil {
		t.Error("a filer.conf delete event must be dropped")
	}
	renameIn := &filer_pb.SubscribeMetadataResponse{Directory: "/somewhere", EventNotification: &filer_pb.EventNotification{NewEntry: entryWith(scopeFilerConfName), NewParentPath: scopeFilerConfDir}}
	if sc.filterEvent(renameIn) != nil {
		t.Error("a rename that creates filer.conf with no visible old side must be dropped")
	}
	// A rename of a visible entry INTO filer.conf hides only the filer.conf (new)
	// side; the old side is still delivered as a deletion so the subscriber does
	// not keep a stale copy of the entry that moved.
	renameVisibleIntoConf := &filer_pb.SubscribeMetadataResponse{
		Directory: scopeFilerConfDir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      entryWith("was-visible"),
			NewEntry:      entryWith(scopeFilerConfName),
			NewParentPath: scopeFilerConfDir,
		},
	}
	out := sc.filterEvent(renameVisibleIntoConf)
	if out == nil {
		t.Fatal("a rename of a visible entry into filer.conf must still deliver the old-side deletion")
	}
	if out.GetEventNotification().GetNewEntry() != nil {
		t.Error("the filer.conf (new) side must be hidden")
	}
	if out.GetEventNotification().GetOldEntry().GetName() != "was-visible" {
		t.Errorf("the visible old side must be delivered as a deletion, got %+v", out.GetEventNotification())
	}
	// Renaming filer.conf OUT to a visible name must never expose its content.
	renameConfOut := &filer_pb.SubscribeMetadataResponse{
		Directory: scopeFilerConfDir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      entryWith(scopeFilerConfName),
			NewEntry:      entryWith("leaked.json"),
			NewParentPath: scopeFilerConfDir,
		},
	}
	if sc.filterEvent(renameConfOut) != nil {
		t.Error("a rename of filer.conf out to a visible name must be dropped (its content must not leak)")
	}
	if sc.filterEvent(ev(scopeFilerConfDir, &filer_pb.EventNotification{NewEntry: entryWith("other.json")})) == nil {
		t.Error("a non-filer.conf event under /etc/seaweedfs must still be delivered")
	}
}

// ---- token lifecycle -------------------------------------------------------------

func TestScopeExpiryEndsStreamsAndRefusesCalls(t *testing.T) {
	h := newScopeHarness(t)
	advance := fakeClock(t)
	c := h.client(scopedToken("alice", 10*time.Minute, []string{aliceWS}, nil))
	st, err := c.SubscribeMetadata(scopeCtx(t), &filer_pb.SubscribeMetadataRequest{PathPrefix: aliceWS + "/"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitFor(t, "the subscription", func() bool { return h.fake.subscribers() == 1 })
	h.fake.events <- ev(aliceWS, &filer_pb.EventNotification{NewEntry: entryWith("before")})
	if got, err := st.Recv(); err != nil || got.GetEventNotification().GetNewEntry().GetName() != "before" {
		t.Fatalf("event before expiry: %v %v", got, err)
	}

	advance(11 * time.Minute)
	h.fake.events <- ev(aliceWS, &filer_pb.EventNotification{NewEntry: entryWith("after")})
	_, err = st.Recv()
	expectCode(t, "stream after expiry", err, codes.Unauthenticated)
	waitFor(t, "the subscription to close", func() bool { return h.fake.subscribers() == 0 })

	before := h.fake.total()
	_, err = c.Ping(ctxBG(), &filer_pb.PingRequest{})
	expectCode(t, "unary call after expiry", err, codes.Unauthenticated)
	if h.fake.total() != before {
		t.Fatal("an expired token's call reached the handler")
	}
}

// TestScopeStreamsKeepTheirToken: a stream keeps the grants of the token it
// was opened with; a renewal reaches it when the client reconnects, and the
// stream still ends at its own token's expiry (TestScopeExpiryEndsStreams).
func TestScopeStreamsKeepTheirToken(t *testing.T) {
	h := newScopeHarness(t)
	rw := scopedToken("alice", time.Hour, []string{aliceWS}, nil)
	st, err := h.client(rw).StreamMutateEntry(scopeCtx(t))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	mutate := func(id uint64, name string) *filer_pb.StreamMutateEntryResponse {
		if err := st.Send(&filer_pb.StreamMutateEntryRequest{RequestId: id, Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
			CreateRequest: &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith(name)}}}); err != nil {
			t.Fatalf("send %d: %v", id, err)
		}
		resp, err := st.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", id, err)
		}
		return resp
	}
	if r := mutate(1, "a"); r.GetError() != "" {
		t.Fatalf("write under the read-write token: %v", r)
	}
	ro := h.client(scopedToken("alice", time.Hour, nil, []string{aliceWS}))
	if _, err := ro.Ping(ctxBG(), &filer_pb.PingRequest{}); err != nil {
		t.Fatalf("ping with the read-only token: %v", err)
	}
	if r := mutate(2, "b"); r.GetError() != "" {
		t.Fatalf("the open stream keeps its own token's grants, got %v", r)
	}
	roStream, err := ro.StreamMutateEntry(scopeCtx(t))
	if err != nil {
		t.Fatalf("open read-only stream: %v", err)
	}
	if err := roStream.Send(&filer_pb.StreamMutateEntryRequest{RequestId: 3, Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
		CreateRequest: &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("c")}}}); err != nil {
		t.Fatalf("send on the read-only stream: %v", err)
	}
	if r, err := roStream.Recv(); err != nil || r.GetErrno() != scopeErrnoEACCES {
		t.Fatalf("a stream opened with the read-only token refuses writes, got %v %v", r, err)
	}
	if n := len(h.fake.received("StreamMutateEntry")); n != 2 {
		t.Fatalf("handler received %d mutate messages, want 2", n)
	}
}

// A metadata-subscription token signed with jwt.filer_signing.subscribe.key does
// not verify under the scope key; the handler authorizes it
// (CheckGrpcMetadataSubscription). The scope stream interceptor must defer to the
// handler for the two subscription methods rather than refuse the call before it
// runs -- 0007 regressed this by verifying every token under the scope key first.
func TestScopeSubscribeKeyTokenDeferredToHandler(t *testing.T) {
	const subKey = "subscribe-only-key"
	subToken := string(security.GenJwtForMetadataSubscription(security.SigningKey(subKey), 60, []string{aliceWS}))
	if subToken == "" {
		t.Fatal("failed to mint a subscribe-key token")
	}

	// Accepted (deferred to the handler) on both subscription methods, with
	// require mode off and on. The token never verifies under the scope key, so
	// reaching the handler at all proves the interceptor deferred rather than
	// refused.
	for _, method := range []string{"SubscribeMetadata", "SubscribeLocalMetadata"} {
		for _, require := range []bool{false, true} {
			t.Run(fmt.Sprintf("accepted/%s/require=%v", method, require), func(t *testing.T) {
				h := newScopeHarness(t)
				h.subscribeKey = subKey
				h.require = require
				c := h.client(subToken)
				req := &filer_pb.SubscribeMetadataRequest{PathPrefix: aliceWS + "/"}
				var err error
				if method == "SubscribeMetadata" {
					_, err = c.SubscribeMetadata(scopeCtx(t), req)
				} else {
					_, err = c.SubscribeLocalMetadata(scopeCtx(t), req)
				}
				if err != nil {
					t.Fatalf("open subscription: %v", err)
				}
				waitFor(t, "the subscribe-key subscription to reach the handler", func() bool {
					return len(h.fake.received(method)) == 1
				})
			})
		}
	}

	// A token invalid under both the scope key and the subscribe key is refused,
	// so require mode stays fail-closed on subscriptions.
	t.Run("foreign-key token refused", func(t *testing.T) {
		h := newScopeHarness(t)
		h.subscribeKey = subKey
		h.require = true
		bad := string(security.GenJwtForMetadataSubscription(security.SigningKey("neither-key"), 60, []string{aliceWS}))
		st, err := h.client(bad).SubscribeMetadata(scopeCtx(t), &filer_pb.SubscribeMetadataRequest{PathPrefix: aliceWS + "/"})
		if err == nil {
			_, err = st.Recv()
		}
		expectCode(t, "foreign-key subscription", err, codes.Unauthenticated)
		if n := len(h.fake.received("SubscribeMetadata")); n != 0 {
			t.Fatalf("refused subscription reached the handler %d times", n)
		}
	})

	// A subscribe-key token is accepted ONLY on the two subscription methods: an
	// ordinary (unary) method still refuses it, even in require mode.
	t.Run("not accepted on a non-subscription method", func(t *testing.T) {
		h := newScopeHarness(t)
		h.subscribeKey = subKey
		h.require = true
		_, err := h.client(subToken).CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory: aliceWS, Entry: &filer_pb.Entry{Name: "x"}})
		expectCode(t, "subscribe-key token on CreateEntry", err, codes.Unauthenticated)
		if n := len(h.fake.received("CreateEntry")); n != 0 {
			t.Fatalf("subscribe-key CreateEntry reached the handler %d times", n)
		}
	})
}
