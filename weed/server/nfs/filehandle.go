package nfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

const (
	fileHandleVersion = 1
	fileHandleLength  = 28
)

var (
	ErrInvalidHandle        = errors.New("invalid nfs filehandle")
	ErrHandleExportMismatch = errors.New("nfs filehandle export mismatch")
	ErrStaleHandle          = errors.New("stale nfs filehandle")
)

type FileHandleKind uint8

const (
	FileHandleKindUnknown   FileHandleKind = 0
	FileHandleKindFile      FileHandleKind = 1
	FileHandleKindDirectory FileHandleKind = 2
)

type FileHandle struct {
	Kind       FileHandleKind
	ExportID   uint32
	Inode      uint64
	Generation uint64
}

type filerResolverClient interface {
	KvGet(ctx context.Context, in *filer_pb.KvGetRequest, opts ...grpc.CallOption) (*filer_pb.KvGetResponse, error)
	LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error)
}

type Resolver struct {
	exportRoot util.FullPath
	exportID   uint32
	client     filerResolverClient
}

type ResolvedHandle struct {
	Handle FileHandle
	Path   util.FullPath
	Entry  *filer_pb.Entry
}

func NewFileHandle(exportID uint32, kind FileHandleKind, inode, generation uint64) FileHandle {
	if generation == 0 {
		generation = filer.InodeIndexInitialGeneration
	}
	return FileHandle{
		Kind:       kind,
		ExportID:   exportID,
		Inode:      inode,
		Generation: generation,
	}
}

func (h FileHandle) Encode() []byte {
	buf := make([]byte, fileHandleLength)
	buf[0] = fileHandleVersion
	buf[1] = byte(h.Kind)
	binary.BigEndian.PutUint32(buf[4:8], h.ExportID)
	binary.BigEndian.PutUint64(buf[8:16], h.Inode)
	binary.BigEndian.PutUint64(buf[16:24], h.Generation)
	binary.BigEndian.PutUint32(buf[24:28], crc32.ChecksumIEEE(buf[:24]))
	return buf
}

func DecodeFileHandle(raw []byte) (FileHandle, error) {
	if len(raw) != fileHandleLength {
		return FileHandle{}, fmt.Errorf("%w: unexpected length %d", ErrInvalidHandle, len(raw))
	}
	if raw[0] != fileHandleVersion {
		return FileHandle{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidHandle, raw[0])
	}

	wantChecksum := binary.BigEndian.Uint32(raw[24:28])
	gotChecksum := crc32.ChecksumIEEE(raw[:24])
	if wantChecksum != gotChecksum {
		return FileHandle{}, fmt.Errorf("%w: checksum mismatch", ErrInvalidHandle)
	}

	handle := FileHandle{
		Kind:       FileHandleKind(raw[1]),
		ExportID:   binary.BigEndian.Uint32(raw[4:8]),
		Inode:      binary.BigEndian.Uint64(raw[8:16]),
		Generation: binary.BigEndian.Uint64(raw[16:24]),
	}
	if handle.Generation == 0 {
		return FileHandle{}, fmt.Errorf("%w: empty generation", ErrInvalidHandle)
	}
	return handle, nil
}

func NewResolver(exportRoot util.FullPath, client filerResolverClient) *Resolver {
	root := normalizeExportRoot(exportRoot)
	return &Resolver{
		exportRoot: root,
		exportID:   exportIDForRoot(root),
		client:     client,
	}
}

func (r *Resolver) ExportID() uint32 {
	if r == nil {
		return 0
	}
	return r.exportID
}

func (r *Resolver) ResolveHandle(ctx context.Context, raw []byte) (*ResolvedHandle, error) {
	if r == nil || r.client == nil {
		return nil, errors.New("nfs resolver is not configured")
	}

	handle, err := DecodeFileHandle(raw)
	if err != nil {
		return nil, err
	}
	if handle.ExportID != r.exportID {
		return nil, ErrHandleExportMismatch
	}

	kvResp, err := r.client.KvGet(ctx, &filer_pb.KvGetRequest{Key: filer.InodeIndexKey(handle.Inode)})
	if err != nil {
		return nil, err
	}
	if kvResp.GetError() != "" {
		return nil, errors.New(kvResp.GetError())
	}
	if len(kvResp.GetValue()) == 0 {
		return nil, ErrStaleHandle
	}

	record, err := filer.DecodeInodeIndexRecord(kvResp.GetValue())
	if err != nil {
		return nil, err
	}
	if record.Generation != handle.Generation {
		return nil, ErrStaleHandle
	}

	for _, path := range record.FullPaths() {
		if !pathVisibleFromExport(path, r.exportRoot) {
			continue
		}

		dir, name := path.DirAndName()
		lookupResp, lookupErr := r.client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if isLookupNotFound(lookupErr) || lookupResp == nil || lookupResp.Entry == nil {
			continue
		}
		if lookupErr != nil {
			return nil, lookupErr
		}
		if attrs := lookupResp.Entry.Attributes; attrs != nil && attrs.Inode != 0 && attrs.Inode != handle.Inode {
			continue
		}
		if handle.Kind == FileHandleKindDirectory && !lookupResp.Entry.IsDirectory {
			continue
		}
		if handle.Kind == FileHandleKindFile && lookupResp.Entry.IsDirectory {
			continue
		}

		return &ResolvedHandle{
			Handle: handle,
			Path:   path,
			Entry:  lookupResp.Entry,
		}, nil
	}

	return nil, ErrStaleHandle
}

func normalizeExportRoot(root util.FullPath) util.FullPath {
	if normalized := util.NormalizePath(string(root)); normalized != "" {
		return normalized
	}
	return "/"
}

func exportIDForRoot(root util.FullPath) uint32 {
	return crc32.ChecksumIEEE([]byte(normalizeExportRoot(root)))
}

func pathVisibleFromExport(path, exportRoot util.FullPath) bool {
	return path == exportRoot || path.IsUnder(exportRoot)
}

func isLookupNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err == filer_pb.ErrNotFound || strings.Contains(err.Error(), filer_pb.ErrNotFound.Error())
}
