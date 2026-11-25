package weed_server

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func sseInlineMetadata() map[string][]byte {
	return map[string][]byte{
		s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256),
		s3_constants.SeaweedFSSSES3Key:       []byte("test-sse-metadata"),
	}
}

func sseInlineStoredEntry(name, content string) *filer.Entry {
	entry := newFileEntry(scopeJoin(aliceWS, name), 1)
	entry.Content = []byte(content)
	entry.FileSize = uint64(len(entry.Content))
	entry.Extended = sseInlineMetadata()
	return entry
}

func sseChunkStoredEntry(name string) *filer.Entry {
	path := scopeJoin(aliceWS, name)
	if name != "" && name[0] == '/' {
		path = name
	}
	entry := newFileEntry(path, 1)
	entry.Chunks = []*filer_pb.FileChunk{{
		FileId:      "5,0c00000090",
		Size:        10,
		SseType:     filer_pb.SSEType_SSE_S3,
		SseMetadata: []byte("test-sse-chunk"),
	}}
	entry.FileSize = 10
	entry.Extended = sseInlineMetadata()
	return entry
}

func TestScopedSSEInlineReplacementRefusedAtAdmissionAndUnderLock(t *testing.T) {
	for _, method := range []string{"UpdateEntry", "CreateEntry"} {
		t.Run(method+" admission", func(t *testing.T) {
			const name = "sse-inline-admission"
			initial := sseInlineStoredEntry(name, "old inline")
			expected := scopedEntryClone(initial)
			requested := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			requested.Content = []byte("new inline")

			var run scopedEntryRun
			if method == "UpdateEntry" {
				run = runScopedUpdateState(t, initial, requested, nil)
			} else {
				run = runScopedCreateState(t, initial, requested, nil)
			}
			if status.Code(run.err) != codes.PermissionDenied || run.called {
				t.Fatalf("%s SSE inline replacement err=%v called=%v, want admission PermissionDenied", method, run.err, run.called)
			}
			assertScopedEntryUnchanged(t, run, expected)
		})

		t.Run(method+" under lock", func(t *testing.T) {
			const name = "sse-inline-race"
			initial := sseInlineStoredEntry(name, "old inline")
			requested := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			expected := scopedEntryClone(initial)
			expected.Content = []byte("new inline")

			var run scopedEntryRun
			late := func(entry *filer.Entry) {
				entry.Content = []byte("new inline")
			}
			if method == "UpdateEntry" {
				run = runScopedUpdateState(t, initial, requested, late)
			} else {
				run = runScopedCreateState(t, initial, requested, late)
			}
			if status.Code(run.err) != codes.PermissionDenied || !run.called {
				t.Fatalf("%s late SSE inline replacement err=%v called=%v, want locked PermissionDenied", method, run.err, run.called)
			}
			assertScopedEntryUnchanged(t, run, expected)
		})
	}
}

func TestScopedSSEIdenticalInlineAndKeysAllowModeChange(t *testing.T) {
	for _, method := range []string{"UpdateEntry", "CreateEntry"} {
		t.Run(method, func(t *testing.T) {
			const name = "sse-inline-mode"
			initial := sseInlineStoredEntry(name, "same inline")
			requested := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			requested.Attributes.FileMode = 0600

			var run scopedEntryRun
			if method == "UpdateEntry" {
				run = runScopedUpdateState(t, initial, requested, nil)
			} else {
				run = runScopedCreateState(t, initial, requested, nil)
			}
			if run.err != nil || !run.called {
				t.Fatalf("%s identical SSE inline update err=%v called=%v, want admitted write", method, run.err, run.called)
			}
			got := run.got.ToProtoEntry()
			if got.GetAttributes().GetFileMode() != 0600 || !bytes.Equal(got.GetContent(), []byte("same inline")) {
				t.Fatalf("%s stored inline mode/content = %o/%q, want 0600/same inline", method, got.GetAttributes().GetFileMode(), got.GetContent())
			}
			if !bytes.Equal(got.GetExtended()[s3_constants.AmzServerSideEncryption], requested.GetExtended()[s3_constants.AmzServerSideEncryption]) ||
				!bytes.Equal(got.GetExtended()[s3_constants.SeaweedFSSSES3Key], requested.GetExtended()[s3_constants.SeaweedFSSSES3Key]) {
				t.Fatalf("%s SSE metadata changed: got=%v want=%v", method, got.GetExtended(), requested.GetExtended())
			}
		})
	}
}

func TestScopedSSEIdenticalMarkedChunksAllowModeChange(t *testing.T) {
	for _, method := range []string{"UpdateEntry", "CreateEntry"} {
		t.Run(method, func(t *testing.T) {
			const name = "sse-chunk-mode"
			initial := sseChunkStoredEntry(name)
			requested := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			requested.Attributes.FileMode = 0600

			var run scopedEntryRun
			if method == "UpdateEntry" {
				run = runScopedUpdateState(t, initial, requested, nil)
			} else {
				run = runScopedCreateState(t, initial, requested, nil)
			}
			if run.err != nil || !run.called {
				t.Fatalf("%s identical SSE chunk update err=%v called=%v, want admitted write", method, run.err, run.called)
			}
			got := run.got.ToProtoEntry()
			if got.GetAttributes().GetFileMode() != 0600 || len(got.GetChunks()) != 1 {
				t.Fatalf("%s stored mode/chunks = %o/%d, want 0600/1", method, got.GetAttributes().GetFileMode(), len(got.GetChunks()))
			}
			gotChunk := proto.Clone(got.GetChunks()[0]).(*filer_pb.FileChunk)
			wantChunk := proto.Clone(requested.GetChunks()[0]).(*filer_pb.FileChunk)
			gotChunk.FileId, gotChunk.Fid = gotChunk.GetFileIdString(), nil
			wantChunk.FileId, wantChunk.Fid = wantChunk.GetFileIdString(), nil
			if !proto.Equal(gotChunk, wantChunk) {
				t.Fatalf("%s stored SSE chunk = %v, want %v", method, gotChunk, wantChunk)
			}
		})
	}
}

func TestScopedSSEAppendSameStoredChunksRefused(t *testing.T) {
	initial := sseChunkStoredEntry(appendLockTestPath)
	before := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
	requested := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry).GetChunks()
	err, got, _ := runScopedAppendState(t, initial, requested, nil)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("identical SSE chunk append err=%v, want PermissionDenied", err)
	}
	if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(before)) {
		t.Fatalf("stored SSE entry changed after refused identical append: got=%v want=%v", got.ToProtoEntry(), before)
	}
}

func TestScopedSSEChunklessAppendAllowed(t *testing.T) {
	initial := sseChunkStoredEntry(appendLockTestPath)
	before := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
	err, got, _ := runScopedAppendState(t, initial, nil, nil)
	if err != nil {
		t.Fatalf("chunkless append to stored SSE entry: %v", err)
	}
	if len(got.GetChunks()) != len(before.GetChunks()) || !bytes.Equal(got.Extended[s3_constants.AmzServerSideEncryption], before.GetExtended()[s3_constants.AmzServerSideEncryption]) ||
		!bytes.Equal(got.Extended[s3_constants.SeaweedFSSSES3Key], before.GetExtended()[s3_constants.SeaweedFSSSES3Key]) {
		t.Fatalf("chunkless SSE append changed stored state: got=%v want=%v", got.ToProtoEntry(), before)
	}
}
