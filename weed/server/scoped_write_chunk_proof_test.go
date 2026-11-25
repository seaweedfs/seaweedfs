package weed_server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func scopedChunkProof(t *testing.T, holder, fid string, lifetime time.Duration) string {
	t.Helper()
	proof := security.GenJwtForAssign(security.SigningKey(scopeTestKey), lifetime, fid, holder, 1, "")
	if proof == "" {
		t.Fatalf("mint assignment proof for %s", fid)
	}
	return string(proof)
}

func expiredScopedChunkProof(t *testing.T, holder, fid string) string {
	t.Helper()
	collection := ""
	claims := security.SeaweedAssignClaims{
		Fid:        fid,
		Count:      1,
		Collection: &collection,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        holder,
			IssuedAt:  jwt.NewNumericDate(time.Unix(0, 0)),
			ExpiresAt: jwt.NewNumericDate(time.Unix(1, 0)),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(security.AssignSigningKey(security.SigningKey(scopeTestKey))))
	if err != nil {
		t.Fatalf("sign expired assignment proof: %v", err)
	}
	return token
}

func TestScopedChunkProofWritesPersistThroughUnaryHandlers(t *testing.T) {
	for _, tc := range []struct {
		method     string
		structured bool
		dual       bool
		suffix     bool
	}{
		{"CreateEntry", false, false, false}, {"UpdateEntry", false, false, false},
		{"CreateEntry", true, false, false}, {"UpdateEntry", true, false, false},
		{"CreateEntry", true, true, false}, {"UpdateEntry", true, true, false},
		{"CreateEntry", false, false, true}, {"UpdateEntry", false, false, true},
	} {
		method := tc.method
		name := method + "/string"
		if tc.structured {
			name = method + "/structured"
		}
		if tc.dual {
			name = method + "/consistent dual identity"
		}
		if tc.suffix {
			name = method + "/suffixed string"
		}
		t.Run(name, func(t *testing.T) {
			fid := "5,0d00000055"
			if tc.suffix {
				fid = "5,0c00000055_1"
			}
			store := newRenameTestStore()
			store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
			fs, scope := newScopeRetentionServer(t, store)
			requested := &filer_pb.Entry{
				Name:       "chunk-" + method,
				Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
				Chunks:     []*filer_pb.FileChunk{{FileId: fid, Size: 10}},
			}
			if method == "UpdateEntry" {
				initial := &filer_pb.Entry{
					Name:       requested.Name,
					Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
					Chunks:     []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}},
				}
				if err := store.InsertEntry(context.Background(), filer.FromPbEntry(aliceWS, initial)); err != nil {
					t.Fatalf("seed %s: %v", method, err)
				}
			}

			proof := scopedChunkProof(t, "retention", fid, time.Hour)
			if tc.structured {
				requested.Chunks[0].FileId = ""
				requested.Chunks[0].Fid = &filer_pb.FileId{VolumeId: 5, FileKey: 13, Cookie: 85}
				proof = string(security.GenJwtForAssign(security.SigningKey(scopeTestKey), time.Hour, "5,0c00000055", "retention", 2, ""))
			}
			if tc.dual {
				requested.Chunks[0].FileId = fid
			}
			if tc.suffix {
				proof = string(security.GenJwtForAssign(security.SigningKey(scopeTestKey), time.Hour, "5,0c00000055", "retention", 2, ""))
			}
			var req any
			if method == "CreateEntry" {
				req = &filer_pb.CreateEntryRequest{
					Directory:                aliceWS,
					Entry:                    requested,
					SkipCheckParentDirectory: true,
					ChunkAuth:                map[string]string{fid: proof},
				}
			} else {
				req = &filer_pb.UpdateEntryRequest{
					Directory: aliceWS,
					Entry:     requested,
					ChunkAuth: map[string]string{fid: proof},
				}
			}

			resp, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + method,
			}, func(handlerCtx context.Context, handlerReq any) (any, error) {
				switch method {
				case "CreateEntry":
					return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
				case "UpdateEntry":
					return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
				default:
					t.Fatalf("unexpected method %q", method)
					return nil, nil
				}
			})
			if err != nil {
				t.Fatalf("scoped %s with a valid chunk proof: %v", method, err)
			}
			if method == "CreateEntry" {
				createResp, ok := resp.(*filer_pb.CreateEntryResponse)
				if !ok || createResp == nil || createResp.GetError() != "" || createResp.GetErrorCode() != filer_pb.FilerError_OK {
					t.Fatalf("scoped CreateEntry response = %#v, want empty error and FilerError_OK", resp)
				}
			}

			stored, findErr := store.FindEntry(context.Background(), filer.FromPbEntry(aliceWS, requested).FullPath)
			if findErr != nil {
				t.Fatalf("find persisted %s: %v", method, findErr)
			}
			if len(stored.GetChunks()) != 1 || stored.GetChunks()[0].GetFileIdString() != fid {
				t.Fatalf("persisted %s chunks = %v, want %s", method, stored.GetChunks(), fid)
			}
		})
	}
}

func TestScopedChunkProofRejectsConflictingChunkIdentities(t *testing.T) {
	const base = "5,0c00000055"
	for _, fileID := range []string{base, base + "_1"} {
		for _, field := range []string{"volume", "key", "cookie"} {
			for _, afterAdmission := range []bool{false, true} {
				stage := "admission"
				if afterAdmission {
					stage = "locked handler"
				}
				t.Run(fileID+"/"+field+"/"+stage, func(t *testing.T) {
					store := newRenameTestStore()
					store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
					fs, scope := newScopeRetentionServer(t, store)
					chunk := &filer_pb.FileChunk{FileId: fileID, Size: 10}
					conflict := &filer_pb.FileId{VolumeId: 5, FileKey: 12, Cookie: 85}
					if fileID != base {
						conflict.FileKey++
					}
					switch field {
					case "volume":
						conflict.VolumeId++
					case "key":
						conflict.FileKey++
					case "cookie":
						conflict.Cookie++
					}
					if !afterAdmission {
						chunk.Fid = conflict
					}
					proof := security.GenJwtForAssign(security.SigningKey(scopeTestKey), time.Hour, base, "retention", 2, "")
					req := &filer_pb.CreateEntryRequest{
						Directory: aliceWS, SkipCheckParentDirectory: true,
						Entry:     &filer_pb.Entry{Name: "conflict", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Chunks: []*filer_pb.FileChunk{chunk}},
						ChunkAuth: map[string]string{fileID: string(proof)},
					}
					called := false
					_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "CreateEntry"}, func(ctx context.Context, request any) (any, error) {
						called = true
						chunk.Fid = conflict
						return fs.CreateEntry(ctx, request.(*filer_pb.CreateEntryRequest))
					})
					if status.Code(err) != codes.PermissionDenied || called != afterAdmission {
						t.Fatalf("conflicting identity err=%v called=%v, want PermissionDenied at %s", err, called, stage)
					}
					if _, err := store.FindEntry(context.Background(), util.NewFullPath(aliceWS, "conflict")); err != filer_pb.ErrNotFound {
						t.Fatalf("conflicting chunk persisted: find error=%v", err)
					}
				})
			}
		}
	}
}

func TestScopedChunkProofAppendPersistsThroughUnaryHandler(t *testing.T) {
	fs, store := newAppendLockServer(t)
	// The append-lock fixture pauses its first lookup so the lock tests can
	// control the critical section. This test exercises the real handler to
	// completion, so release that pause before invoking it.
	close(store.release)

	const fid = "5,0c0000005c"
	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, fs)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization",
		"Bearer "+scopedToken("append", time.Hour, []string{"/data"}, nil),
	))
	_, err := scope.unary(ctx, &filer_pb.AppendToEntryRequest{
		Directory: "/data",
		EntryName: "app.log",
		Chunks:    []*filer_pb.FileChunk{{FileId: fid, Size: 10}},
		ChunkAuth: map[string]string{fid: scopedChunkProof(t, "append", fid, time.Hour)},
	}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AppendToEntry"}, func(handlerCtx context.Context, req any) (any, error) {
		return fs.AppendToEntry(handlerCtx, req.(*filer_pb.AppendToEntryRequest))
	})
	if err != nil {
		t.Fatalf("scoped append with a valid chunk proof: %v", err)
	}

	stored, err := store.FindEntry(context.Background(), appendLockTestPath)
	if err != nil {
		t.Fatalf("find persisted append: %v", err)
	}
	if len(stored.GetChunks()) != 1 || stored.GetChunks()[0].GetFileIdString() != fid {
		t.Fatalf("persisted append chunks = %v, want %s", stored.GetChunks(), fid)
	}
}

func TestScopedChunkProofAdmissionRejectsUnprovenWritesAndManifests(t *testing.T) {
	h := newScopeHarness(t)
	h.fake.putDir(aliceWS)
	h.fake.put(aliceWS+"/existing", entryWith("existing", "5,0c00000055"))
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	const fid = "5,0c00000056"

	expectScopeDenied(t, h, "CreateEntry", "unproven new chunk on create", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory:                aliceWS,
			SkipCheckParentDirectory: true,
			Entry:                    entryWith("new", fid),
		})
		return err
	})
	expectScopeDenied(t, h, "UpdateEntry", "unproven new chunk on update", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     entryWith("existing", fid),
		})
		return err
	})
	expectScopeDenied(t, h, "AppendToEntry", "unproven new chunk on append", func() error {
		_, err := c.AppendToEntry(scopeCtx(t), &filer_pb.AppendToEntryRequest{
			Directory: aliceWS,
			EntryName: "new-append",
			Chunks:    []*filer_pb.FileChunk{{FileId: fid, Size: 10}},
		})
		return err
	})

	manifestFid := "5,0c00000057"
	expectScopeDenied(t, h, "CreateEntry", "manifest with a valid proof", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory:                aliceWS,
			SkipCheckParentDirectory: true,
			Entry: &filer_pb.Entry{Name: "manifest", Chunks: []*filer_pb.FileChunk{{
				FileId: manifestFid, Size: 10, IsChunkManifest: true,
			}}},
			ChunkAuth: map[string]string{manifestFid: scopedChunkProof(t, "alice", manifestFid, time.Hour)},
		})
		return err
	})
}

func TestScopedChunkProofAdmissionForAppend(t *testing.T) {
	const fid = "5,0c00000058"
	cases := []struct {
		name       string
		proof      string
		wantDenied bool
	}{
		{name: "valid holder-bound proof", proof: "valid"},
		{name: "foreign holder proof", proof: "foreign", wantDenied: true},
		{name: "expired proof", proof: "expired", wantDenied: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newScopeHarness(t)
			h.fake.putDir(aliceWS)
			var proof string
			switch tc.proof {
			case "valid":
				proof = scopedChunkProof(t, "alice", fid, time.Hour)
			case "foreign":
				proof = scopedChunkProof(t, "bob", fid, time.Hour)
			case "expired":
				proof = expiredScopedChunkProof(t, "alice", fid)
			}
			c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
			call := func() error {
				_, err := c.AppendToEntry(scopeCtx(t), &filer_pb.AppendToEntryRequest{
					Directory: aliceWS,
					EntryName: "append-" + tc.proof,
					Chunks:    []*filer_pb.FileChunk{{FileId: fid, Size: 10}},
					ChunkAuth: map[string]string{fid: proof},
				})
				return err
			}
			before := len(h.fake.received("AppendToEntry"))
			err := call()
			if tc.wantDenied {
				expectScopeCode(t, tc.name, err, codes.PermissionDenied)
				if got := len(h.fake.received("AppendToEntry")); got != before {
					t.Fatalf("%s reached the handler: %d calls before=%d", tc.name, got, before)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			calls := h.fake.received("AppendToEntry")
			if len(calls) != before+1 {
				t.Fatalf("%s handler calls = %d, want %d", tc.name, len(calls), before+1)
			}
			got := calls[len(calls)-1].(*filer_pb.AppendToEntryRequest)
			if got.GetChunkAuth()[fid] != proof {
				t.Fatalf("%s proof at handler = %q, want %q", tc.name, got.GetChunkAuth()[fid], proof)
			}
		})
	}
}

func TestScopedChunkProofAdmissionForStreamMutations(t *testing.T) {
	const fid = "5,0c00000059"
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	stream, err := c.StreamMutateEntry(scopeCtx(t))
	if err != nil {
		t.Fatalf("open mutate stream: %v", err)
	}

	valid := scopedChunkProof(t, "alice", fid, time.Hour)
	if err := stream.Send(&filer_pb.StreamMutateEntryRequest{
		RequestId: 1,
		Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{CreateRequest: &filer_pb.CreateEntryRequest{
			Directory:                aliceWS,
			SkipCheckParentDirectory: true,
			Entry:                    entryWith("stream-valid", fid),
			ChunkAuth:                map[string]string{fid: valid},
		}},
	}); err != nil {
		t.Fatalf("send valid stream create: %v", err)
	}
	response, err := stream.Recv()
	if err != nil || response.GetError() != "" {
		t.Fatalf("valid stream create response=%v err=%v, want success", response, err)
	}

	foreign := scopedChunkProof(t, "bob", fid, time.Hour)
	if err := stream.Send(&filer_pb.StreamMutateEntryRequest{
		RequestId: 2,
		Request: &filer_pb.StreamMutateEntryRequest_UpdateRequest{UpdateRequest: &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     entryWith("stream-foreign", fid),
			ChunkAuth: map[string]string{fid: foreign},
		}},
	}); err != nil {
		t.Fatalf("send foreign stream update: %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("receive foreign stream refusal: %v", err)
	}
	if response.GetRequestId() != 2 || response.GetErrno() != scopeErrnoEACCES || response.GetError() == "" || !response.GetIsLast() {
		t.Fatalf("foreign stream response=%v, want EACCES refusal", response)
	}
	if got := len(h.fake.received("StreamMutateEntry")); got != 1 {
		t.Fatalf("stream handler calls = %d, want only the valid request", got)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close mutate stream: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("mutate stream after close = %v, want EOF", err)
	}
}

func TestScopedChunkProofDoesNotBypassWorm(t *testing.T) {
	fs, scope, store := newScopedWormServer(t)
	path := insertScopedWormEntry(t, store, "proven", time.Unix(1700000000, 0))
	const fid = "5,0c0000005a"
	called := false
	_, err := scope.unary(scopedRetentionContext(), &filer_pb.UpdateEntryRequest{
		Directory: aliceWS,
		Entry:     entryWith("proven", fid),
		ChunkAuth: map[string]string{fid: scopedChunkProof(t, "retention", fid, time.Hour)},
	}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "UpdateEntry"}, func(ctx context.Context, req any) (any, error) {
		called = true
		return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
	})
	if status.Code(err) != codes.PermissionDenied || !called {
		t.Fatalf("proven WORM update err=%v called=%v, want handler and PermissionDenied", err, called)
	}
	got, findErr := store.FindEntry(context.Background(), path)
	if findErr != nil || string(got.Content) != "stored" || len(got.GetChunks()) != 0 {
		t.Fatalf("proven WORM update changed entry: got=%v err=%v", got, findErr)
	}
}

func TestScopedChunkProofDoesNotBypassSharedHardLink(t *testing.T) {
	const fid = "5,0c0000005b"
	h := newScopeHarness(t)
	h.fake.put(aliceWS+"/shared", &filer_pb.Entry{
		Name:            "shared",
		HardLinkId:      []byte("shared-hard-link"),
		HardLinkCounter: 2,
		Chunks:          []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}},
	})
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	expectScopeDenied(t, h, "UpdateEntry", "valid proof through shared hard link", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     entryWith("shared", fid),
			ChunkAuth: map[string]string{fid: scopedChunkProof(t, "alice", fid, time.Hour)},
		})
		return err
	})
}
