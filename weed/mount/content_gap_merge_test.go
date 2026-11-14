//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

// These caller controls measure published bytes; none imports #12608 internals.
func masterAssertPeerMergeOutcome(t *testing.T, periodic bool) {
	for _, scenario := range []string{"peer-append", "peer-replaced-file", "peer-truncate"} {
		t.Run(scenario, func(t *testing.T) {
			base := attributeAppendEntry(1)
			stored := attributeAppendEntry(2)
			pending := attributeAppendEntry(2)
			pending.Chunks[1].Fid.FileKey = 3
			pending.Chunks[1].Offset = 512
			pending.Attributes.FileSize = 768
			blobs := map[string][]byte{}
			for i, c := range stored.Chunks {
				blobs[c.GetFileIdString()] = bytes.Repeat([]byte{[]byte{'O', 'P'}[i]}, 256)
			}
			blobs[pending.Chunks[1].GetFileIdString()] = bytes.Repeat([]byte{'C'}, 256)
			switch scenario {
			case "peer-replaced-file":
				stored = attributeAppendEntry(1)
				stored.Attributes.Inode = 99
				stored.Chunks[0].Fid.FileKey = 900
				blobs[stored.Chunks[0].GetFileIdString()] = bytes.Repeat([]byte{'R'}, 256)
			case "peer-truncate":
				base = attributeAppendEntry(2)
				stored = attributeAppendEntry(1)
				pending = attributeAppendEntry(3)
				blobs[pending.Chunks[2].GetFileIdString()] = bytes.Repeat([]byte{'C'}, 256)
				blobs[base.Chunks[1].GetFileIdString()] = bytes.Repeat([]byte{'A'}, 256)
			}
			stored.Attributes.FileMode, stored.CommitTsNs = 0600, 20000
			original := proto.Clone(stored).(*filer_pb.Entry)
			peer := &g6ConditionalPeer{entry: proto.Clone(stored).(*filer_pb.Entry), blobs: blobs}
			w := newInvalidateTestWFS(t)
			startFakeFiler(t, w, peer)
			w.mapPbIdFromFilerToLocal(base)
			w.mapPbIdFromFilerToLocal(pending)
			fh := newFileHandle(w, 1, 42, pending)
			t.Cleanup(fh.dirtyPages.Destroy)
			fh.RememberPath("/dir/file")
			fh.baseEntry.Store(base)
			fh.dirtyMetadata = true
			code := fuse.OK
			if periodic {
				if err := w.flushFileMetadata(fh); err != nil {
					code = fuse.EIO
					t.Logf("periodic error: %v", err)
				}
			} else {
				code = w.doFlush(context.Background(), fh, 1000, 20, false, 0)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			actual := g6StoredBytes(t, peer)
			t.Logf("scenario=%s status=%v size=%d rejected=%d gap[256,512)-zero=%v", scenario, code, len(actual), peer.rejected, len(actual) >= 512 && bytes.Equal(actual[256:512], make([]byte, 256)))
			if scenario == "peer-replaced-file" {
				if code != fuse.EIO || !g6EntryEqual(peer.entry, original) || !bytes.Equal(actual, bytes.Repeat([]byte{'R'}, 256)) {
					t.Fatal("replacement must be refused without publishing stale content")
				}
				return
			}
			if scenario == "peer-truncate" && code == fuse.EIO {
				if !g6EntryEqual(peer.entry, original) || !bytes.Equal(actual, bytes.Repeat([]byte{'O'}, 256)) {
					t.Fatal("refusal changed peer truncate")
				}
				return
			}
			want := bytes.Repeat([]byte{'O'}, 256)
			if scenario == "peer-append" {
				want = append(want, bytes.Repeat([]byte{'P'}, 256)...)
			}
			want = append(want, bytes.Repeat([]byte{'C'}, 256)...)
			if code != fuse.OK || peer.entry.Attributes.FileMode != 0600 || peer.entry.Attributes.FileSize != uint64(len(want)) || !bytes.Equal(actual, want) {
				t.Fatalf("unsafe merge: status=%v size=%d want-size=%d exact-bytes=%v; peer/local bytes must survive without a gap", code, len(actual), len(want), bytes.Equal(actual, want))
			}
		})
	}
}

func TestMasterCommittedEOFAppendHasNoGap(t *testing.T) {
	for _, appendOpen := range []bool{true, false} {
		name := "append"
		if !appendOpen {
			name = "intentional-sparse-pwrite"
		}
		t.Run(name, func(t *testing.T) {
			base := attributeAppendEntry(1)
			peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
			w, h := g6AppendHandle(t, peer, base, 2100)
			if !appendOpen {
				out := &fuse.OpenOut{}
				if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY}, out); code != fuse.OK {
					t.Fatal(code)
				}
				h = out.Fh
			}
			if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h, Offset: 512, Size: 256}, bytes.Repeat([]byte{'B'}, 256)); n != 256 || code != fuse.OK {
				t.Fatal(n, code)
			}
			code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h})
			peer.mu.Lock()
			defer peer.mu.Unlock()
			actual := g6StoredBytes(t, peer)
			if appendOpen {
				if code != fuse.EIO || peer.entry.Attributes.FileSize != 256 || !bytes.Equal(actual, bytes.Repeat([]byte{'O'}, 256)) {
					t.Fatalf("stale append frontier published a gap: status=%v size=%d interval[256,512)-zero=%v", code, len(actual), len(actual) >= 512 && bytes.Equal(actual[256:512], make([]byte, 256)))
				}
			} else {
				want := append(bytes.Repeat([]byte{'O'}, 256), make([]byte, 256)...)
				want = append(want, bytes.Repeat([]byte{'B'}, 256)...)
				if code != fuse.OK || !bytes.Equal(actual, want) || peer.entry.Attributes.FileSize != 768 {
					t.Fatal("intentional sparse pwrite must remain allowed", code, len(actual))
				}
			}
		})
	}
}

func TestMasterConflictThenAppendCannotPublishGap(t *testing.T) {
	for _, mode := range []struct {
		name                 string
		writeback, coalesced bool
	}{{"normal", false, false}, {"writeback", true, false}, {"writeback-coalesced", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			base := attributeAppendEntry(1)
			peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
			w, h := g6AppendHandle(t, peer, base, 2200, mode.writeback)
			if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h, Offset: 256, Size: 256}, bytes.Repeat([]byte{'A'}, 256)); n != 256 || code != fuse.OK {
				t.Fatal(n, code)
			}
			// Model the native compactor's output, including independently uploaded
			// bytes and the rewrite marker: a single chunk containing original O+A.
			// No product implementation is supplied or changed by this fixture.
			fh := w.GetHandle(FileHandleId(h))
			if err := fh.dirtyPages.FlushData(); err != nil {
				t.Fatal(err)
			}
			compacted := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 8000, Cookie: 1}, Size: 512, ModifiedTsNs: 100}
			peer.mu.Lock()
			peer.blobs[compacted.GetFileIdString()] = append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
			peer.mu.Unlock()
			fh.UpdateEntry(func(e *filer_pb.Entry) {
				e.Chunks = []*filer_pb.FileChunk{proto.Clone(compacted).(*filer_pb.FileChunk)}
				e.Attributes.FileSize = 512
			})
			fh.noteRewrite()
			replacement := attributeAppendEntry(1)
			replacement.Chunks[0].Fid.FileKey = 900
			replacement.Attributes.FileMode, replacement.CommitTsNs = 0600, 20000
			peer.mu.Lock()
			peer.entry = proto.Clone(replacement).(*filer_pb.Entry)
			peer.clock = 20000
			peer.blobs[replacement.Chunks[0].GetFileIdString()] = bytes.Repeat([]byte{'P'}, 256)
			peer.mu.Unlock()
			if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h}); code != fuse.EIO {
				t.Fatal("setup: native real rewrite conflict must be refused", code)
			}
			w.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: h, ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK})
			if _, kept := w.fhMap.FindFileHandle(42); kept {
				t.Fatal("failed synchronous close retained handle")
			}
			peer.mu.Lock()
			unchanged := g6EntryEqual(peer.entry, replacement)
			peer.mu.Unlock()
			if !unchanged {
				t.Fatal("refused close published over peer")
			}
			if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", proto.Clone(replacement).(*filer_pb.Entry)), 20000); err != nil {
				t.Fatal(err)
			}
			out := &fuse.OpenOut{}
			if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY | syscall.O_APPEND}, out); code != fuse.OK {
				t.Fatal(code)
			}
			g6AppendUploader(t, w.GetHandle(FileHandleId(out.Fh)), 2300, peer)
			offset := uint64(512)
			payload := bytes.Repeat([]byte{'B'}, 256)
			if mode.coalesced {
				offset = 0
				payload = append(bytes.Repeat([]byte{'P'}, 256), make([]byte, 256)...)
				payload = append(payload, bytes.Repeat([]byte{'B'}, 256)...)
			}
			if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh, Offset: offset, Size: uint32(len(payload))}, payload); n != uint32(len(payload)) || code != fuse.OK {
				t.Fatal(n, code)
			}
			code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh})
			peer.mu.Lock()
			defer peer.mu.Unlock()
			actual := g6StoredBytes(t, peer)
			if code != fuse.EIO || peer.entry.Attributes.FileSize != 256 || !bytes.Equal(actual, bytes.Repeat([]byte{'P'}, 256)) {
				t.Fatalf("terminal conflict followed by false success/gap: status=%v size=%d interval[256,512)-zero=%v", code, len(actual), len(actual) >= 512 && bytes.Equal(actual[256:512], make([]byte, 256)))
			}
		})
	}
}

func TestContentGapPeerMerge(t *testing.T)         { masterAssertPeerMergeOutcome(t, false) }
func TestContentGapPeriodicPeerMerge(t *testing.T) { masterAssertPeerMergeOutcome(t, true) }
