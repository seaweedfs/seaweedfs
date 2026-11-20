package weed_server

// Rules applied to a scoped filer token on the filesystem gRPC service: which
// paths the token may use, and which chunk file ids and hard-link ids an entry
// it sends may reference. See filer_grpc_scope.go for how they are applied.

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// scopeGrant is one path prefix a token may use, read-only or read-write.
type scopeGrant struct {
	path      string
	readWrite bool
}

// scopeGrantsFromClaims turns the token's prefix lists into grants. Every
// prefix must be a clean absolute path: the filer stores the directory string
// it receives without cleaning, so an unclean grant could never match a stored
// path exactly and would only invite confusion about what it covers.
func scopeGrantsFromClaims(allowedPrefixes, readPrefixes []string) ([]scopeGrant, error) {
	grants := make([]scopeGrant, 0, len(allowedPrefixes)+len(readPrefixes))
	add := func(p string, readWrite bool) error {
		if p == "" || p[0] != '/' || path.Clean(p) != p {
			return fmt.Errorf("prefix %q is not a clean absolute path", p)
		}
		grants = append(grants, scopeGrant{path: p, readWrite: readWrite})
		return nil
	}
	for _, p := range allowedPrefixes {
		if err := add(p, true); err != nil {
			return nil, err
		}
	}
	for _, p := range readPrefixes {
		if err := add(p, false); err != nil {
			return nil, err
		}
	}
	return grants, nil
}

var errScopePathNotCanonical = errors.New("path is not canonical")

// scopeFullPath joins a filer directory and entry name as the rules check
// them. The directory must be absolute and already clean, the name one clean
// segment or empty (the directory itself). Anything else is refused rather
// than normalised, because the filer would store it as received.
func scopeFullPath(dir, name string) (string, error) {
	if dir == "" || dir[0] != '/' || path.Clean(dir) != dir {
		return "", fmt.Errorf("%w: directory %q", errScopePathNotCanonical, dir)
	}
	switch {
	case name == "":
		return dir, nil
	case name == "." || name == ".." || strings.Contains(name, "/"):
		return "", fmt.Errorf("%w: name %q", errScopePathNotCanonical, name)
	}
	if dir == "/" {
		return "/" + name, nil
	}
	return dir + "/" + name, nil
}

// scopePathAllowed reports whether the canonical path p equals a grant or lies
// under it; a mutation needs a read-write grant covering p.
func scopePathAllowed(grants []scopeGrant, p string, write bool) bool {
	if p == "" || p[0] != '/' {
		return false
	}
	for _, g := range grants {
		under := g.path == "/" || p == g.path || strings.HasPrefix(p, g.path+"/")
		if !under {
			continue
		}
		if write && !g.readWrite {
			continue
		}
		return true
	}
	return false
}

// scopePrefixAllowed applies the path rule to a metadata-subscription prefix.
// weed mount subscribes to its root with a trailing slash ("<root>/", matching
// only paths under the root), so one trailing slash is accepted; otherwise the
// prefix must be a canonical path inside a grant.
func scopePrefixAllowed(grants []scopeGrant, prefix string) bool {
	p := prefix
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		p = strings.TrimSuffix(p, "/")
	}
	if _, err := scopeFullPath(p, ""); err != nil {
		return false
	}
	return scopePathAllowed(grants, p, false)
}

func scopeSplitPath(p string) (dir, name string) {
	i := strings.LastIndex(p, "/")
	if i <= 0 {
		return "/", strings.TrimPrefix(p, "/")
	}
	return p[:i], p[i+1:]
}

// scopeCheckEntryShape refuses constructs a scoped token may never store:
// remote entries, chunks that copy from a source file id, chunks without a
// file id, and chunk manifests. A chunk manifest holds a blob that lists more
// file ids, which the filer expands on read and queues for deletion on an
// IsDeleteData delete; those inner ids are not covered by the entry's chunk
// proofs, so a scoped holder could otherwise reference or delete chunks it
// never proved. Until a scoped caller can prove the ids inside a manifest, it
// stores large files as a flat, fully proven chunk list instead.
func scopeCheckEntryShape(e *filer_pb.Entry) error {
	if e == nil {
		return nil
	}
	if e.RemoteEntry != nil {
		return errors.New("remote entries are not allowed")
	}
	for _, c := range e.GetChunks() {
		if c.GetSourceFileId() != "" || c.GetSourceFid() != nil {
			return fmt.Errorf("chunk %s: source file ids are not allowed", c.GetFileIdString())
		}
		if c.GetFileIdString() == "" {
			return errors.New("chunk without a file id")
		}
		if c.GetIsChunkManifest() {
			return fmt.Errorf("chunk %s: manifest chunks are not allowed", c.GetFileIdString())
		}
	}
	return nil
}

// scopeHardLinkMarker terminates every id filer.NewHardLinkId produces (16
// random bytes plus the marker), the only shape a client may introduce as a
// new id.
const scopeHardLinkMarker = '\x01'

func scopeLooksLikeNewHardLinkId(id []byte) bool {
	return len(id) == 17 && id[16] == scopeHardLinkMarker
}
