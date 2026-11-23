package weed_server

import (
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestScopeEntryShapeRejectsUnadmittedChunkForms(t *testing.T) {
	cases := []struct {
		name  string
		entry *filer_pb.Entry
	}{
		{name: "remote entry", entry: &filer_pb.Entry{Name: "remote", RemoteEntry: &filer_pb.RemoteEntry{StorageName: "remote"}}},
		{name: "source file id", entry: &filer_pb.Entry{Name: "source", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", SourceFileId: "1,a"}}}},
		{name: "source fid", entry: &filer_pb.Entry{Name: "source", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", SourceFid: &filer_pb.FileId{VolumeId: 1}}}}},
		{name: "missing file id", entry: &filer_pb.Entry{Name: "missing", Chunks: []*filer_pb.FileChunk{{Size: 1}}}},
		{name: "nil file chunk", entry: &filer_pb.Entry{Name: "nil", Chunks: []*filer_pb.FileChunk{nil}}},
		{name: "manifest chunk", entry: &filer_pb.Entry{Name: "manifest", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", IsChunkManifest: true}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := scopeCheckEntryShape(tc.entry); err == nil {
				t.Fatalf("scopeCheckEntryShape accepted %s", tc.name)
			}
		})
	}
	if err := scopeCheckEntryShape(entryWith("plain", "1,a")); err != nil {
		t.Fatalf("plain stored chunk rejected: %v", err)
	}
	if err := scopeCheckEntryShape(&filer_pb.Entry{Name: "plain"}); err != nil {
		t.Fatalf("metadata-only entry rejected: %v", err)
	}
}

func TestScopeKeepOnlyEntryWrites(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	const keptFid = "5,0c00000055"
	h.fake.put(aliceWS+"/kept", entryWith("kept", keptFid))
	h.fake.putDir(aliceWS)

	expectScopeForwarded(t, h, "UpdateEntry", "update reusing the target's stored chunk", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: entryWith("kept", keptFid)})
		return err
	})
	expectScopeForwarded(t, h, "AppendToEntry", "append reusing a chunk already stored at the target", func() error {
		_, err := c.AppendToEntry(scopeCtx(t), &filer_pb.AppendToEntryRequest{Directory: aliceWS, EntryName: "kept", Chunks: []*filer_pb.FileChunk{{FileId: keptFid, Size: 10}}})
		return err
	})
	expectScopeDenied(t, h, "CreateEntry", "new file id", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("new", "9,deadbeef")})
		return err
	})
	expectScopeDenied(t, h, "CreateEntry", "stored chunk at a different target", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("other", keptFid)})
		return err
	})
	expectScopeDenied(t, h, "UpdateEntry", "chunk at a missing target", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: entryWith("missing", keptFid)})
		return err
	})

	shapes := []struct {
		name  string
		entry *filer_pb.Entry
	}{
		{name: "manifest", entry: &filer_pb.Entry{Name: "manifest", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", IsChunkManifest: true}}}},
		{name: "remote", entry: &filer_pb.Entry{Name: "remote", RemoteEntry: &filer_pb.RemoteEntry{StorageName: "remote"}}},
		{name: "source", entry: &filer_pb.Entry{Name: "source", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", SourceFid: &filer_pb.FileId{VolumeId: 1}}}}},
	}
	for _, tc := range shapes {
		t.Run(tc.name, func(t *testing.T) {
			expectScopeDenied(t, h, "CreateEntry", tc.name, func() error {
				_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: tc.entry})
				return err
			})
		})
	}

	expectScopeDenied(t, h, "CreateEntry", "missing entry", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: aliceWS})
		return err
	})
}

func TestScopeInlineContentWithinWritablePrefix(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	h.fake.put(aliceWS+"/inline", &filer_pb.Entry{Name: "inline", Content: []byte("old")})

	expectScopeForwarded(t, h, "UpdateEntry", "existing inline content", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "inline", Content: []byte("old")},
		})
		return err
	})
	expectScopeForwarded(t, h, "UpdateEntry", "changed inline content", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "inline", Content: []byte("new")},
		})
		return err
	})
	expectScopeForwarded(t, h, "CreateEntry", "new inline content", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory:                aliceWS,
			Entry:                    &filer_pb.Entry{Name: "new-inline", Content: []byte("new")},
			SkipCheckParentDirectory: true,
		})
		return err
	})
	expectScopeForwarded(t, h, "UpdateEntry", "clear inline content", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "inline"},
		})
		return err
	})
}

func TestScopeKeepOnlyRejectsBackendReadErrors(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	h.fake.setFindErr(errors.New("backend unavailable"))

	expectScopeDenied(t, h, "UpdateEntry", "stored target read error", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     entryWith("stale", "5,0c00000055"),
		})
		return err
	})
}

func TestScopeKeepOnlyHardLinkRules(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	knownID := []byte("known-hard-link")
	h.fake.put(aliceWS+"/known", &filer_pb.Entry{Name: "known", HardLinkId: knownID})
	expectScopeForwarded(t, h, "UpdateEntry", "retain the target hard link", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "known", HardLinkId: knownID},
		})
		return err
	})

	newID := append([]byte("0123456789abcdef"), scopeHardLinkMarker)
	h.fake.put(aliceWS+"/unlinked", entryWith("unlinked"))
	beforeLookups := h.fake.kvLookups()
	expectScopeForwarded(t, h, "UpdateEntry", "attach a fresh hard link to an existing unlinked target", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "unlinked", HardLinkId: newID},
		})
		return err
	})
	if got := h.fake.kvLookups(); got != beforeLookups+1 {
		t.Fatalf("fresh hard-link registry lookups=%d, want %d", got, beforeLookups+1)
	}

	registeredID := append([]byte("fedcba9876543210"), scopeHardLinkMarker)
	h.fake.put(aliceWS+"/registered", entryWith("registered"))
	h.fake.putKV(registeredID, []byte("/somewhere-else"))
	beforeLookups = h.fake.kvLookups()
	expectScopeDenied(t, h, "UpdateEntry", "attach a registered hard link", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "registered", HardLinkId: registeredID},
		})
		return err
	})
	if got := h.fake.kvLookups(); got != beforeLookups+1 {
		t.Fatalf("registered hard-link registry lookups=%d, want %d", got, beforeLookups+1)
	}
}

func TestScopeKeepOnlyRejectsConflictingFileIDs(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	const storedFileID = "5,0c00000055"
	h.fake.put(aliceWS+"/authoritative", &filer_pb.Entry{Name: "authoritative", Chunks: []*filer_pb.FileChunk{{FileId: storedFileID}}})
	expectScopeDenied(t, h, "UpdateEntry", "retained FileId cannot hide a different Fid", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry: &filer_pb.Entry{Name: "authoritative", Chunks: []*filer_pb.FileChunk{
				{FileId: storedFileID, Fid: &filer_pb.FileId{VolumeId: 99, FileKey: 99}},
			}},
		})
		return err
	})

	h.fake.put(aliceWS+"/invalid-legacy", &filer_pb.Entry{Name: "invalid-legacy", Chunks: []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 8, Cookie: 9}}}})
	expectScopeDenied(t, h, "UpdateEntry", "invalid legacy FileId does not fall back to Fid", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry: &filer_pb.Entry{Name: "invalid-legacy", Chunks: []*filer_pb.FileChunk{
				{FileId: "not-a-real-file-id", Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 8, Cookie: 9}},
			}},
		})
		return err
	})
}
