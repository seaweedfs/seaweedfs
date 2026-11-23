package mount

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/sync/singleflight"
)

const (
	// readAuthRefreshMargin renews a token this long before it expires, so a
	// read never starts on a token the volume server rejects mid-transfer.
	readAuthRefreshMargin = 5 * time.Second
	// readAuthMaxFids caps the fid indexes. Past it they are cleared and
	// relearned: every open handle re-tracks its chunks on its next SetEntry,
	// AddChunks, or truncate, and until then a read whose fid is not yet known
	// fails closed and is retried rather than served without a token.
	readAuthMaxFids = 1 << 20
)

type readAuthToken struct {
	token   string
	expires time.Time
}

// ReadAuth holds the exact-fid volume read tokens this mount was granted. It
// learns which entry a fid belongs to from the file handles that read it, and
// on a miss asks the filer for that entry with LookupDirectoryEntries, whose
// read_auth carries one short-lived token per chunk. The mount never holds a
// signing key, so it can read only chunks of entries the filer (or a guard in
// front of it) let it look up. A nil *ReadAuth is disabled.
type ReadAuth struct {
	wfs    *WFS
	mu     sync.Mutex
	paths  map[string]util.FullPath
	tokens map[string]readAuthToken
	flight singleflight.Group
}

func newReadAuth(wfs *WFS) *ReadAuth {
	return &ReadAuth{wfs: wfs, paths: map[string]util.FullPath{}, tokens: map[string]readAuthToken{}}
}

// Track records that chunks belong to the entry at path.
func (ra *ReadAuth) Track(path util.FullPath, chunks []*filer_pb.FileChunk) {
	if ra == nil || path == "" {
		return
	}
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if len(ra.paths) > readAuthMaxFids {
		ra.paths = map[string]util.FullPath{}
	}
	for _, chunk := range chunks {
		if fid := chunk.GetFileIdString(); fid != "" {
			ra.paths[fid] = path
		}
	}
}

// TrackHandle records that the chunks fh holds now belong to the entry at
// path, for a handle whose name moved without a new SetEntry.
func (ra *ReadAuth) TrackHandle(path util.FullPath, fh *FileHandle) {
	if ra == nil {
		return
	}
	le := fh.GetEntry()
	if le == nil {
		return
	}
	le.RLock()
	defer le.RUnlock()
	if le.Entry != nil {
		ra.Track(path, le.Entry.Chunks)
	}
}

// Token is the security.VolumeReadTokenProvider of this mount.
func (ra *ReadAuth) Token(ctx context.Context, fileId string) string {
	if token, ok := ra.cached(fileId); ok {
		return token
	}
	ra.mu.Lock()
	path := ra.paths[fileId]
	ra.mu.Unlock()
	if path == "" {
		glog.V(1).Infof("read auth: no entry known for chunk %s", fileId)
		return ""
	}
	// Other reads of this entry wait on the same lookup, so the caller that
	// started it must not cancel it for them.
	flight := ra.flight.DoChan(string(path), func() (interface{}, error) {
		return nil, ra.refresh(context.WithoutCancel(ctx), path)
	})
	select {
	case <-flight:
	case <-ctx.Done():
		return ""
	}
	if token, ok := ra.cached(fileId); ok {
		return token
	}
	// The refresh produced no token with more than the refresh margin left. A
	// token that has not expired is still better than none: a read JWT lifetime
	// at or below the margin (jwt.signing.read.expires_after_seconds small)
	// would otherwise be discarded the instant it is stored, and every read of
	// this chunk would go out without a credential the volume server accepts.
	return ra.unexpiredToken(fileId)
}

func (ra *ReadAuth) cached(fileId string) (string, bool) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	t, ok := ra.tokens[fileId]
	if !ok || time.Until(t.expires) < readAuthRefreshMargin {
		return "", false
	}
	return t.token, true
}

// unexpiredToken returns a stored token that is still valid even if it is
// within the refresh margin, so a refresh that could not mint a fresher one
// does not drop a credential the volume server would still accept.
func (ra *ReadAuth) unexpiredToken(fileId string) string {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if t, ok := ra.tokens[fileId]; ok && time.Until(t.expires) > 0 {
		return t.token
	}
	return ""
}

func (ra *ReadAuth) refresh(ctx context.Context, path util.FullPath) error {
	dir, name := path.DirAndName()
	var resp *filer_pb.LookupDirectoryEntriesResponse
	// Token reads reach refresh with context.Background() (JwtForVolumeServer),
	// so bound each attempt with filerRPCWait; a filer that accepts the lookup
	// and then stalls must not block the read forever, nor the concurrent reads
	// that singleflight holds behind this one.
	err := ra.wfs.withFilerClient(ctx, false, ra.wfs.filerRPCWait(), func(callCtx context.Context, client filer_pb.SeaweedFilerClient) error {
		var lookupErr error
		resp, lookupErr = client.LookupDirectoryEntries(callCtx, &filer_pb.LookupDirectoryEntriesRequest{
			Requests: []*filer_pb.LookupDirectoryEntryRequest{{Directory: dir, Name: name}},
		})
		return lookupErr
	})
	if err != nil {
		glog.V(0).Infof("read auth: lookup %s: %v", path, err)
		return err
	}
	ra.installRefreshResult(path, resp.GetReadAuth())
	return nil
}

// installRefreshResult records the tokens a lookup of path returned. It
// keeps each fid's longest-lived usable token, but fills a path only for a fid
// that is not already mapped. Track owns path changes: a fid re-tracked to a newer path
// (a rename) while this lookup was in flight must keep it, not have the
// vacated path it was looked up under restored. The fids a lookup discovers
// that Track has not seen -- the data chunks behind a manifest -- are the
// ones still filled in here.
func (ra *ReadAuth) installRefreshResult(path util.FullPath, readAuth map[string]string) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	previous := ra.tokens
	if len(ra.tokens) > readAuthMaxFids {
		ra.tokens = map[string]readAuthToken{}
	}
	for fid, token := range readAuth {
		updated := readAuthToken{token: token, expires: jwtExpiry(token)}
		// Compare against the old map even when capacity eviction reset it.
		if held, ok := previous[fid]; ok && time.Until(held.expires) > 0 && updated.expires.Before(held.expires) {
			updated = held
		}
		ra.tokens[fid] = updated
		if _, known := ra.paths[fid]; !known {
			ra.paths[fid] = path
		}
	}
}

// jwtExpiry reads the exp claim without verifying the token: the volume server
// verifies it; the mount only needs to know when to ask again.
func jwtExpiry(token string) time.Time {
	fallback := time.Now().Add(readAuthRefreshMargin * 2)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fallback
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fallback
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Exp == 0 {
		return fallback
	}
	return time.Unix(claims.Exp, 0)
}

func (wfs *WFS) installReadAuth() {
	if !wfs.option.VolumeReadAuth {
		return
	}
	wfs.readAuth = newReadAuth(wfs)
	security.SetVolumeReadTokenProvider(wfs.readAuth.Token)
	glog.V(0).Infof("volume reads use filer-issued exact-fid tokens")
}

func (wfs *WFS) metadataLogLookupFn() wdclient.LookupFileIdFunctionType {
	// Log chunks have no entry path from which to request a read token.
	// Let the filer decode and stream metadata events for keyless mounts.
	if wfs.option.VolumeReadAuth {
		return nil
	}
	return wfs.LookupFn()
}
