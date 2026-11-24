package weed_server

// Scoped filer tokens on the filesystem gRPC service. Tokenless calls retain
// their existing behavior. A verified scoped token is confined to its path and
// method grants. New chunk references require holder-bound assignment proofs.
// Unruled methods are refused. Streams recheck expiry on every message.

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	scopePosixLockKeyPrefix = "s3.fuse.lock:"
	scopeErrnoEACCES        = 13

	// scopeFilerConfDir and scopeFilerConfName locate the filer's per-path
	// storage rules (collection, replication, ttl, read-only), which weed
	// mount looks up once at start. A scoped token is answered "not found"
	// without a lookup: the mount then uses its defaults, the filer applies
	// the same rules itself in AssignVolume (detectStorageOption), and the
	// rule list, which names every path on the filer, stays with the filer.
	scopeFilerConfDir  = "/etc/seaweedfs"
	scopeFilerConfName = "filer.conf"
)

// scopeBackend is what the rules need from the filer: the entry currently
// stored at a path, and the hard-link registry. The FilerServer implements it;
// tests use a fake.
type scopeBackend interface {
	// scopeFindEntry returns the entry at the canonical path p, or nil when
	// there is none.
	scopeFindEntry(ctx context.Context, p string) (*filer_pb.Entry, error)
	// scopeKvGet returns the value stored under key, or nil when there is none.
	scopeKvGet(ctx context.Context, key []byte) ([]byte, error)
	// scopeCollection resolves the path's collection using the assignment rules.
	scopeCollection(ctx context.Context, p string) (string, error)
}

// scopeConfig is the jwt.filer_signing configuration the scope reads, as
// functions so that a reload is seen at once.
type scopeConfig struct {
	signingKey        func() security.SigningKey
	verificationKeys  func() (security.SigningKey, security.SigningKey)
	subscriptionScope func(context.Context, string) (*security.MetadataSubscriptionScope, error)
	leeway            func() time.Duration
	assignLifetime    func() time.Duration
}

// filerGrpcScope holds the interceptors. It keeps no state across calls.
type filerGrpcScope struct {
	cfg     scopeConfig
	backend scopeBackend
}

func newFilerGrpcScope(cfg scopeConfig, backend scopeBackend) *filerGrpcScope {
	return &filerGrpcScope{cfg: cfg, backend: backend}
}

// ServerOptions returns the gRPC server options that install the scope on a
// server. They chain behind whatever interceptors the server already has.
func (s *filerGrpcScope) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.unary),
		grpc.ChainStreamInterceptor(s.stream),
	}
}

// scope is the resolved authority of one call: the token's holder, expiry and
// grants. A nil *scope means the call is unscoped.
type scope struct {
	id     string
	exp    time.Time
	grants []scopeGrant
	// methods, when non-empty, are the gRPC method short names this token may
	// call (allowed_methods). A lookup-only token cannot mutate entries under
	// its prefixes. An empty list means no additional method restriction.
	methods []string
	peer    string
	leeway  time.Duration
}

// methodAllowed reports whether the token may call fullMethod. A token without
// allowed_methods may call any method the per-method rules admit; one with
// allowed_methods may call only those, matched by the gRPC method's short name
// (the part after the last "/").
func (sc *scope) methodAllowed(fullMethod string) bool {
	if len(sc.methods) == 0 {
		return true
	}
	short := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	for _, m := range sc.methods {
		if m == short {
			return true
		}
	}
	return false
}

func (sc *scope) expired() bool { return !time.Now().Before(sc.exp.Add(sc.leeway)) }

// scopeExpiredError ends a call or stream whose token passed its expiry while
// in use; the holder reconnects with a renewed token.
func scopeExpiredError(sc *scope) error {
	glog.V(1).Infof("filer grpc: token %s (%s) expired", sc.id, sc.peer)
	return status.Errorf(codes.Unauthenticated, "filer: token %s expired", sc.id)
}

// bearerFromMetadata distinguishes absent credentials from malformed or
// ambiguous credentials. Only the former retains tokenless compatibility.
func bearerFromMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 {
		return "", status.Error(codes.Unauthenticated, "filer: exactly one authorization value is required")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", status.Error(codes.Unauthenticated, "filer: malformed bearer authorization")
	}
	return parts[1], nil
}

func scopePeer(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

func (s *filerGrpcScope) verificationKeys() (security.SigningKey, security.SigningKey) {
	if s.cfg.verificationKeys != nil {
		return s.cfg.verificationKeys()
	}
	if s.cfg.signingKey != nil {
		return s.cfg.signingKey(), nil
	}
	return nil, nil
}

func (s *filerGrpcScope) proven(sc *scope, chunkAuth map[string]string, fid, collection string) bool {
	raw := chunkAuth[fid]
	if raw == "" {
		return false
	}
	key, previous := s.verificationKeys()
	return security.VerifyAssignJwt(key, previous, security.EncodedJwt(raw), fid, sc.id, sc.leeway, collection)
}

// Resolve only when a new chunk needs a proof, once per check. The handler
// creates a fresh verifier under its path lock so changes since admission are
// seen there. Retained chunks and chunkless mount creates need no resolution.
func (s *filerGrpcScope) proofVerifier(ctx context.Context, sc *scope, chunkAuth map[string]string, p string) func(string) bool {
	var collection string
	var err error
	resolved := false
	return func(fid string) bool {
		if !resolved {
			collection, err = s.backend.scopeCollection(ctx, p)
			resolved = true
		}
		return err == nil && s.proven(sc, chunkAuth, fid, collection)
	}
}

// resolve authenticates the call. It returns (nil, nil) for an unscoped call,
// a *scope for a scoped token, and an error that refuses the call.
func (s *filerGrpcScope) resolve(ctx context.Context) (*scope, error) {
	key, previous := s.verificationKeys()
	if len(key) == 0 {
		return nil, nil
	}
	raw, authErr := bearerFromMetadata(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if raw == "" {
		return nil, nil
	}
	leeway := time.Duration(0)
	if s.cfg.leeway != nil {
		leeway = s.cfg.leeway()
		if leeway < 0 {
			leeway = 0
		}
	}
	claims := &security.SeaweedFilerClaims{}
	token, err := security.DecodeJwtWith(key, previous, security.EncodedJwt(raw), claims, leeway)
	if errors.Is(err, jwt.ErrTokenExpired) {
		// The claims are parsed before validation, so the id names the holder.
		glog.V(1).Infof("filer grpc: token %s (%s) expired", claims.ID, scopePeer(ctx))
		return nil, status.Errorf(codes.Unauthenticated, "filer: token %s expired", claims.ID)
	}
	if err != nil || token == nil || !token.Valid {
		glog.V(1).Infof("filer grpc: refused call from %s: invalid token: %v", scopePeer(ctx), err)
		return nil, status.Error(codes.Unauthenticated, "filer: invalid bearer token")
	}
	if claims.SessionId != "" {
		return nil, status.Error(codes.Unauthenticated, "filer: an STS session token is not a filer credential")
	}
	// A token with neither prefixes nor allowed_methods has full access.
	// allowed_methods alone also scopes the token (so a method restriction is
	// enforced even without a path grant); IsScoped stays prefix-only so the
	// HTTP path authorization of a method-only token is unchanged.
	if !claims.IsScoped() && len(claims.AllowedMethods) == 0 {
		return nil, nil
	}
	if claims.ID == "" {
		return nil, status.Error(codes.PermissionDenied, "filer: a scoped token must carry an id (jti)")
	}
	if claims.ExpiresAt == nil {
		return nil, status.Error(codes.PermissionDenied, "filer: a scoped token must expire")
	}
	grants, err := scopeGrantsFromClaims(claims.AllowedPrefixes, claims.ReadPrefixes)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "filer: scoped token: %v", err)
	}
	return &scope{id: claims.ID, exp: claims.ExpiresAt.Time, grants: grants, methods: claims.AllowedMethods, peer: scopePeer(ctx), leeway: leeway}, nil
}

// deny logs and builds the refusal of a scoped call.
func (sc *scope) deny(method, reason string) error {
	glog.V(1).Infof("filer grpc: scoped token %s (%s) refused %s: %s", sc.id, sc.peer, method, reason)
	return status.Errorf(codes.PermissionDenied, "filer: %s: %s", method, reason)
}

// checkPath resolves directory+name and applies the path rule.
func (sc *scope) checkPath(method, dir, name string, write bool) (string, error) {
	p, err := scopeFullPath(dir, name)
	if err != nil {
		return "", sc.deny(method, err.Error())
	}
	if !scopePathAllowed(sc.grants, p, write) {
		mode := "read"
		if write {
			mode = "write"
		}
		return "", sc.deny(method, fmt.Sprintf("%s of %s is outside the token's scope", mode, p))
	}
	return p, nil
}

func (sc *scope) checkProtectedPath(method, p string, descendants bool) error {
	if scopeProtectedPath(p, descendants) {
		return sc.deny(method, "filer.conf is protected from scoped mutations")
	}
	return nil
}

// checkClientEntry admits retained chunks or chunks with valid assignment proofs.
// A hard-link id may be retained there, or a fresh id may be introduced by an
// update of an existing unlinked entry. Attaching another path's id is refused.
// Client signatures stay allowed so mounts can filter their own events.
func (s *filerGrpcScope) checkClientEntry(ctx context.Context, sc *scope, method, p string, e *filer_pb.Entry, chunkAuth map[string]string, allowNewHardLink bool) error {
	if err := scopeCheckEntryShape(e); err != nil {
		return sc.deny(method, err.Error())
	}
	if err := sc.checkProtectedPath(method, p, false); err != nil {
		return err
	}
	var stored *filer_pb.Entry
	loaded := false
	load := func() error {
		if loaded {
			return nil
		}
		loaded = true
		var err error
		stored, err = s.backend.scopeFindEntry(ctx, p)
		if err != nil {
			return sc.deny(method, fmt.Sprintf("the entry stored at %s could not be read: %v", p, err))
		}
		if err := scopeWritesSharedHardLink(stored, e); err != nil {
			return sc.deny(method, err.Error())
		}
		return nil
	}
	// Keep ordinary mount creates lookup-free at admission. The handlers
	// recheck stored data and SSE state under the path lock, including SSE-key
	// removal by a chunkless request that carries no SSE state of its own.
	if len(e.GetContent()) > 0 || len(e.GetChunks()) > 0 || scopeHasSSE(e) {
		if err := load(); err != nil {
			return err
		}
		if err := scopeCheckSSEState(stored, e, method == "AppendToEntry"); err != nil {
			return sc.deny(method, err.Error())
		}
		if err := scopeRetainsData(stored, e, s.proofVerifier(ctx, sc, chunkAuth, p)); err != nil {
			return sc.deny(method, err.Error())
		}
		if method == "AppendToEntry" {
			if err := scopeCheckAppendState(stored, e.GetChunks()); err != nil {
				return sc.deny(method, err.Error())
			}
		}
	}

	if len(e.HardLinkId) == 0 {
		return nil
	}
	if err := load(); err != nil {
		return err
	}
	if stored != nil && bytes.Equal(stored.HardLinkId, e.HardLinkId) {
		return nil
	}
	if !allowNewHardLink || stored == nil || len(stored.HardLinkId) > 0 || !scopeLooksLikeNewHardLinkId(e.HardLinkId) {
		return sc.deny(method, fmt.Sprintf("hard link %x cannot be attached to %s", e.HardLinkId, p))
	}
	value, kvErr := s.backend.scopeKvGet(ctx, e.HardLinkId)
	if kvErr != nil {
		return sc.deny(method, fmt.Sprintf("hard link registry unavailable: %v", kvErr))
	}
	if len(value) > 0 {
		return sc.deny(method, fmt.Sprintf("hard link %s already exists elsewhere", hex.EncodeToString(e.HardLinkId)))
	}
	return nil
}

func (s *filerGrpcScope) checkCreate(ctx context.Context, sc *scope, m string, req *filer_pb.CreateEntryRequest) error {
	if req.GetEntry() == nil || req.GetEntry().GetName() == "" {
		return sc.deny(m, "a named entry is required")
	}
	if req.GetIsFromOtherCluster() || req.GetIsMoved() {
		return sc.deny(m, "is_from_other_cluster and is_moved are not allowed")
	}
	p, err := sc.checkPath(m, req.GetDirectory(), req.GetEntry().GetName(), true)
	if err != nil {
		return err
	}
	if !req.GetSkipCheckParentDirectory() {
		if err := s.checkAncestors(ctx, sc, m, p); err != nil {
			return err
		}
	}
	return s.checkClientEntry(ctx, sc, m, p, req.GetEntry(), req.GetChunkAuth(), false)
}

func (s *filerGrpcScope) checkUpdate(ctx context.Context, sc *scope, m string, req *filer_pb.UpdateEntryRequest) error {
	if req.GetEntry() == nil || req.GetEntry().GetName() == "" {
		return sc.deny(m, "a named entry is required")
	}
	if req.GetIsFromOtherCluster() || req.GetIsMoved() {
		return sc.deny(m, "is_from_other_cluster and is_moved are not allowed")
	}
	p, err := sc.checkPath(m, req.GetDirectory(), req.GetEntry().GetName(), true)
	if err != nil {
		return err
	}
	return s.checkClientEntry(ctx, sc, m, p, req.GetEntry(), req.GetChunkAuth(), true)
}

func (s *filerGrpcScope) checkAppend(ctx context.Context, sc *scope, m string, req *filer_pb.AppendToEntryRequest) error {
	p, err := sc.checkPath(m, req.GetDirectory(), req.GetEntryName(), true)
	if err != nil {
		return err
	}
	if err := s.checkAncestors(ctx, sc, m, p); err != nil {
		return err
	}
	return s.checkClientEntry(ctx, sc, m, p, &filer_pb.Entry{Name: req.GetEntryName(), Chunks: req.GetChunks()}, req.GetChunkAuth(), false)
}

// checkAncestors refuses a write at p that would make an ancestor outside the
// write grants: the filer creates the missing ancestors of p up to the first
// one it finds, and turns that one into a directory if it is a file.
func (s *filerGrpcScope) checkAncestors(ctx context.Context, sc *scope, m, p string) error {
	for dir, _ := scopeSplitPath(p); dir != "/"; dir, _ = scopeSplitPath(dir) {
		entry, err := s.backend.scopeFindEntry(ctx, dir)
		if err != nil {
			return sc.deny(m, fmt.Sprintf("the entry stored at %s could not be read: %v", dir, err))
		}
		if entry != nil && entry.GetIsDirectory() {
			return nil
		}
		if !scopePathAllowed(sc.grants, dir, true) {
			return sc.deny(m, fmt.Sprintf("%s is outside the token's scope and is not a directory", dir))
		}
		if entry != nil {
			return nil
		}
	}
	return nil
}

func (sc *scope) checkDelete(m string, req *filer_pb.DeleteEntryRequest) error {
	if req.GetIsFromOtherCluster() || req.GetIsMoved() {
		return sc.deny(m, "is_from_other_cluster and is_moved are not allowed")
	}
	p, err := sc.checkPath(m, req.GetDirectory(), req.GetName(), true)
	if err != nil {
		return err
	}
	return sc.checkProtectedPath(m, p, true)
}

func (s *filerGrpcScope) checkRename(ctx context.Context, sc *scope, m, oldDir, oldName, newDir, newName string) error {
	oldPath, err := sc.checkPath(m, oldDir, oldName, true)
	if err != nil {
		return err
	}
	if err := sc.checkProtectedPath(m, oldPath, true); err != nil {
		return err
	}
	newPath, err := sc.checkPath(m, newDir, newName, true)
	if err != nil {
		return err
	}
	if err := sc.checkProtectedPath(m, newPath, true); err != nil {
		return err
	}
	return s.checkAncestors(ctx, sc, m, newPath)
}

// checkPosixLock admits locks on paths inside the scope and on hard links
// the registry knows (their ids are random, so a holder only has those it
// read from an entry; a lock serializes, it reads nothing).
func (s *filerGrpcScope) checkPosixLock(ctx context.Context, sc *scope, m string, req *filer_pb.PosixLockRequest) error {
	if req.GetIsMoved() || req.GetCoolingProbe() {
		return sc.deny(m, "internal lock flags are not allowed")
	}
	// Only GET_LK is a read: the sid is client-supplied, so a read grant must
	// not release or renew another session's locks.
	write := req.GetOp() != filer_pb.PosixLockOp_GET_LK
	key := req.GetKey()
	if !strings.HasPrefix(key, scopePosixLockKeyPrefix) {
		return sc.deny(m, fmt.Sprintf("lock key %q has no %q prefix", key, scopePosixLockKeyPrefix))
	}
	rest := strings.TrimPrefix(key, scopePosixLockKeyPrefix)
	if strings.HasPrefix(rest, "hl:") {
		if write {
			hasWriteGrant := false
			for _, grant := range sc.grants {
				if grant.readWrite {
					hasWriteGrant = true
					break
				}
			}
			if !hasWriteGrant {
				return sc.deny(m, "write lock on a hard link requires a read-write scope")
			}
		}
		id, decErr := hex.DecodeString(strings.TrimPrefix(rest, "hl:"))
		if decErr != nil || len(id) == 0 {
			return sc.deny(m, fmt.Sprintf("lock on hard link %q: malformed id", rest))
		}
		value, kvErr := s.backend.scopeKvGet(ctx, id)
		if kvErr != nil {
			return sc.deny(m, fmt.Sprintf("hard link registry unavailable: %v", kvErr))
		}
		if len(value) == 0 {
			return sc.deny(m, fmt.Sprintf("lock on hard link %q: no such link", rest))
		}
		return nil
	}
	dir, name := scopeSplitPath(rest)
	_, err := sc.checkPath(m, dir, name, write)
	return err
}

// scopeFilerMethodPrefix is the gRPC path prefix of the filesystem service.
// Other services on the same server retain their own authorization.
const scopeFilerMethodPrefix = "/filer_pb.SeaweedFiler/"

// unary is the unary server interceptor.
func (s *filerGrpcScope) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !strings.HasPrefix(info.FullMethod, scopeFilerMethodPrefix) {
		return handler(ctx, req)
	}
	sc, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if sc == nil {
		return handler(ctx, req)
	}
	if sc.expired() {
		return nil, scopeExpiredError(sc)
	}
	if !sc.methodAllowed(info.FullMethod) {
		return nil, sc.deny(info.FullMethod, "method is not in the token's allowed_methods")
	}
	method := strings.TrimPrefix(info.FullMethod, scopeFilerMethodPrefix)
	expected, ok := scopeUnaryMethods[method]
	if !ok || reflect.TypeOf(req) != expected {
		return nil, sc.deny(info.FullMethod, "not available to a scoped token")
	}
	return s.unaryScoped(s.withWriteAuthority(ctx, sc), sc, req, handler)
}

// isScopeFilerConf reports whether (dir, name) is the filer's per-path config
// file, which is hidden from a scoped token on every read path: single and
// batch lookup and directory listing.
func isScopeFilerConf(dir, name string) bool {
	p, err := scopeFullPath(dir, name)
	return err == nil && p == scopeFilerConfDir+"/"+scopeFilerConfName
}

// unaryScoped applies the per-method path rule before calling the handler.
func (s *filerGrpcScope) unaryScoped(ctx context.Context, sc *scope, req any, handler grpc.UnaryHandler) (any, error) {
	switch r := req.(type) {
	case *filer_pb.LookupDirectoryEntryRequest:
		const m = "LookupDirectoryEntry"
		if isScopeFilerConf(r.GetDirectory(), r.GetName()) {
			return nil, filer_pb.ErrNotFound
		}
		if _, err := sc.checkPath(m, r.GetDirectory(), r.GetName(), false); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.LookupDirectoryEntriesRequest:
		const m = "LookupDirectoryEntries"
		if err := validateFilerBatchLookupRequest(r); err != nil {
			return nil, err
		}
		reqs := r.GetRequests()
		kept := make([]*filer_pb.LookupDirectoryEntryRequest, 0, len(reqs))
		hidden := make([]bool, len(reqs))
		for i, lookup := range reqs {
			// filer.conf is hidden from a scoped token on every read path, as in
			// the single LookupDirectoryEntry above. Results are positional and
			// carry no request key, so a hidden slot must still produce one miss
			// result in its place rather than shortening the response.
			if isScopeFilerConf(lookup.GetDirectory(), lookup.GetName()) {
				hidden[i] = true
				continue
			}
			if _, err := sc.checkPath(m, lookup.GetDirectory(), lookup.GetName(), false); err != nil {
				return nil, err
			}
			kept = append(kept, lookup)
		}
		if len(kept) == len(reqs) {
			return handler(ctx, req)
		}
		// Every request hidden: an empty forwarded batch is rejected, so answer
		// all-miss without calling the handler.
		if len(kept) == 0 {
			results := make([]*filer_pb.LookupDirectoryEntryResult, len(reqs))
			for i := range results {
				results[i] = &filer_pb.LookupDirectoryEntryResult{Found: false}
			}
			return &filer_pb.LookupDirectoryEntriesResponse{Results: results}, nil
		}
		resp, err := handler(ctx, &filer_pb.LookupDirectoryEntriesRequest{
			Requests:                kept,
			UnavailableVolumeIsMiss: r.GetUnavailableVolumeIsMiss(),
		})
		if err != nil {
			return nil, err
		}
		out, ok := resp.(*filer_pb.LookupDirectoryEntriesResponse)
		if !ok {
			return resp, nil
		}
		// Splice a miss into each hidden slot so the results line up with the
		// original request order; the handler returns one result per kept request.
		spliced := make([]*filer_pb.LookupDirectoryEntryResult, 0, len(reqs))
		j := 0
		for i := range reqs {
			if !hidden[i] && j < len(out.GetResults()) {
				spliced = append(spliced, out.GetResults()[j])
				j++
				continue
			}
			spliced = append(spliced, &filer_pb.LookupDirectoryEntryResult{Found: false})
		}
		out.Results = spliced
		return out, nil

	case *filer_pb.TouchAccessTimeRequest:
		p, err := sc.checkPath("TouchAccessTime", r.GetDirectory(), r.GetName(), false)
		if err != nil {
			return nil, err
		}
		if err := sc.checkProtectedPath("TouchAccessTime", p, false); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.CreateEntryRequest:
		if err := s.checkCreate(ctx, sc, "CreateEntry", r); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.UpdateEntryRequest:
		if err := s.checkUpdate(ctx, sc, "UpdateEntry", r); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.AppendToEntryRequest:
		if err := s.checkAppend(ctx, sc, "AppendToEntry", r); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.DeleteEntryRequest:
		if err := sc.checkDelete("DeleteEntry", r); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.AtomicRenameEntryRequest:
		if err := s.checkRename(ctx, sc, "AtomicRenameEntry", r.GetOldDirectory(), r.GetOldName(), r.GetNewDirectory(), r.GetNewName()); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.AssignVolumeRequest:
		const m = "AssignVolume"
		if r.GetCount() < 0 {
			return nil, sc.deny(m, "count must not be negative")
		}
		if r.GetPath() == "" {
			return nil, sc.deny(m, "path is required")
		}
		dir, name := scopeSplitPath(r.GetPath())
		p, err := sc.checkPath(m, dir, name, true)
		if err != nil {
			return nil, err
		}
		if p != r.GetPath() {
			return nil, sc.deny(m, "path is not canonical")
		}
		if err := sc.checkProtectedPath(m, p, false); err != nil {
			return nil, err
		}
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, err
		}
		out, ok := resp.(*filer_pb.AssignVolumeResponse)
		if !ok || out == nil || out.GetError() != "" || out.GetErrorCode() != filer_pb.FilerError_OK || out.GetFileId() == "" || out.GetCount() <= 0 {
			return resp, nil
		}
		key, _ := s.verificationKeys()
		lifetime := security.DefaultAssignTokenLifetime
		if s.cfg.assignLifetime != nil {
			if configured := s.cfg.assignLifetime(); configured > 0 {
				lifetime = configured
			}
		}
		out.AssignAuth = string(security.GenJwtForAssign(key, lifetime, out.GetFileId(), sc.id, out.GetCount(), out.GetCollection()))
		if out.AssignAuth == "" {
			return nil, status.Error(codes.Internal, "filer: assignment proof could not be signed")
		}
		return out, nil

	case *filer_pb.PosixLockRequest:
		if err := s.checkPosixLock(ctx, sc, "PosixLock", r); err != nil {
			return nil, err
		}
		return handler(ctx, req)

	case *filer_pb.PingRequest:
		// The filer answers for itself; a scoped client cannot make it probe an
		// address it names.
		r.Target, r.TargetType = "", ""
		return handler(ctx, req)

	case *filer_pb.LookupVolumeRequest, *filer_pb.GetFilerConfigurationRequest, *filer_pb.StatisticsRequest:
		// No file paths or chunk ids. MountRegister is deliberately absent: a
		// scoped token must not write the shared mount peer registry (other
		// mounts read it via MountList for HRW seeding), so it falls to the
		// default-deny below, exactly as MountList does.
		return handler(ctx, req)
	}
	return nil, sc.deny(fmt.Sprintf("%T", req), "not available to a scoped token")
}

// scopeCollection uses the same path rules and bucket defaults as AssignVolume.
func (fs *FilerServer) scopeCollection(ctx context.Context, p string) (string, error) {
	so, err := fs.resolveAssignStorageOption(ctx, &filer_pb.AssignVolumeRequest{Path: p})
	if err != nil {
		return "", err
	}
	return so.Collection, nil
}

// scopeFindEntry implements scopeBackend on the filer.
func (fs *FilerServer) scopeFindEntry(ctx context.Context, p string) (*filer_pb.Entry, error) {
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(p))
	if err == filer_pb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return entry.ToProtoEntry(), nil
}

// scopeKvGet implements scopeBackend on the filer.
func (fs *FilerServer) scopeKvGet(ctx context.Context, key []byte) ([]byte, error) {
	value, err := fs.filer.Store.KvGet(ctx, key)
	if err == filer.ErrKvNotFound {
		return nil, nil
	}
	return value, err
}

// GrpcScopeServerOptions returns the server options that enforce scoped
// tokens on the filer's filesystem gRPC service.
func (fs *FilerServer) GrpcScopeServerOptions() []grpc.ServerOption {
	return fs.grpcScope.ServerOptions()
}
