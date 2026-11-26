package weed_server

// Rules applied to a scoped filer token on the filesystem gRPC service: which
// paths the token may use, and which chunk file ids and hard-link ids an entry
// it sends may reference. See filer_grpc_scope.go for how they are applied.

import (
	"errors"
	"fmt"
	"math"
	"path"
	"reflect"
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

func scopeSplitPath(p string) (dir, name string) {
	i := strings.LastIndex(p, "/")
	if i <= 0 {
		return "/", strings.TrimPrefix(p, "/")
	}
	return p[:i], p[i+1:]
}

// scopeProtectedPath reports whether a mutation can change the filer's shared
// storage configuration. Entry writes cannot replace or promote filer.conf to
// a directory; a rename or delete also cannot move or remove an ancestor.
func scopeProtectedPath(p string, descendants bool) bool {
	config := scopeFilerConfDir + "/" + scopeFilerConfName
	return p == config || strings.HasPrefix(p, config+"/") ||
		(descendants && (p == "/" || strings.HasPrefix(config, p+"/")))
}

// scopeCheckEntryShape refuses indirect or missing chunk identities. Manifests
// contain additional file ids that cannot be checked against the target entry.
func scopeCheckEntryShape(e *filer_pb.Entry) error {
	if e == nil {
		return errors.New("an entry is required")
	}
	if e.RemoteEntry != nil {
		return errors.New("remote entries are not allowed")
	}
	if len(e.GetContent()) > 0 && len(e.GetChunks()) > 0 {
		return errors.New("inline content and chunks cannot be combined")
	}
	if len(e.GetChunks()) > 0 && e.GetAttributes().GetFileSize() > math.MaxInt64 {
		return errors.New("file size must fit a nonnegative signed offset")
	}
	if err := scopeCheckChunkRanges(e.GetChunks()); err != nil {
		return err
	}
	for _, c := range e.GetChunks() {
		if c == nil {
			return errors.New("nil chunks are not allowed")
		}
		// Some readers use Fid directly. A suffixed FileId is not converted
		// during serialization, so an unchecked Fid would survive alongside it.
		if c.FileId != "" && c.Fid != nil {
			structured := (&filer_pb.FileChunk{Fid: c.Fid}).GetFileIdString()
			if c.FileId != structured {
				return errors.New("chunk string and structured file ids must match")
			}
		}
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

// scopeUnaryMethods binds each admitted RPC name to its request type. The path
// rules in unaryScoped are selected by that type; a new RPC reusing a request
// type cannot inherit permission accidentally. All absent methods are refused.
var scopeUnaryMethods = map[string]reflect.Type{
	"AssignVolume":           reflect.TypeOf((*filer_pb.AssignVolumeRequest)(nil)),
	"LookupDirectoryEntry":   reflect.TypeOf((*filer_pb.LookupDirectoryEntryRequest)(nil)),
	"LookupDirectoryEntries": reflect.TypeOf((*filer_pb.LookupDirectoryEntriesRequest)(nil)),
	"TouchAccessTime":        reflect.TypeOf((*filer_pb.TouchAccessTimeRequest)(nil)),
	"CreateEntry":            reflect.TypeOf((*filer_pb.CreateEntryRequest)(nil)),
	"UpdateEntry":            reflect.TypeOf((*filer_pb.UpdateEntryRequest)(nil)),
	"AppendToEntry":          reflect.TypeOf((*filer_pb.AppendToEntryRequest)(nil)),
	"DeleteEntry":            reflect.TypeOf((*filer_pb.DeleteEntryRequest)(nil)),
	"AtomicRenameEntry":      reflect.TypeOf((*filer_pb.AtomicRenameEntryRequest)(nil)),
	"PosixLock":              reflect.TypeOf((*filer_pb.PosixLockRequest)(nil)),
	"Ping":                   reflect.TypeOf((*filer_pb.PingRequest)(nil)),
	"LookupVolume":           reflect.TypeOf((*filer_pb.LookupVolumeRequest)(nil)),
	"GetFilerConfiguration":  reflect.TypeOf((*filer_pb.GetFilerConfigurationRequest)(nil)),
	"Statistics":             reflect.TypeOf((*filer_pb.StatisticsRequest)(nil)),
}

// Each admitted stream has a request/response rule in the stream interceptor.
var scopeStreamMethods = map[string]bool{
	"ListEntries":            true,
	"StreamRenameEntry":      true,
	"StreamMutateEntry":      true,
	"SubscribeMetadata":      true,
	"SubscribeLocalMetadata": true,
}
