package nfs

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeResolverClient struct {
	kv      map[string][]byte
	entries map[util.FullPath]*filer_pb.Entry
}

func (f *fakeResolverClient) KvGet(_ context.Context, in *filer_pb.KvGetRequest, _ ...grpc.CallOption) (*filer_pb.KvGetResponse, error) {
	if value, found := f.kv[string(in.Key)]; found {
		return &filer_pb.KvGetResponse{Value: value}, nil
	}
	return &filer_pb.KvGetResponse{}, nil
}

func (f *fakeResolverClient) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	fullPath := util.NewFullPath(in.Directory, in.Name)
	if entry, found := f.entries[fullPath]; found {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func TestFileHandleEncodeDecodeRoundTrip(t *testing.T) {
	handle := NewFileHandle(1234, FileHandleKindDirectory, 5678, 9)

	raw := handle.Encode()
	decoded, err := DecodeFileHandle(raw)
	require.NoError(t, err)
	assert.Equal(t, handle, decoded)

	raw[len(raw)-1] ^= 0xff
	_, err = DecodeFileHandle(raw)
	require.ErrorIs(t, err, ErrInvalidHandle)
}

func TestResolverUsesPathVisibleFromExportRoot(t *testing.T) {
	client := &fakeResolverClient{
		kv:      make(map[string][]byte),
		entries: make(map[util.FullPath]*filer_pb.Entry),
	}
	resolver := NewResolver("/exports", client)

	record := &filer.InodeIndexRecord{
		Generation: 7,
		Paths:      []string{"/a/other.txt", "/exports/demo/link.txt"},
	}
	value, err := record.Encode()
	require.NoError(t, err)
	client.kv[string(filer.InodeIndexKey(101))] = value
	client.entries["/exports/demo/link.txt"] = &filer_pb.Entry{
		Name: "link.txt",
		Attributes: &filer_pb.FuseAttributes{
			Inode: 101,
		},
	}

	handle := NewFileHandle(resolver.ExportID(), FileHandleKindFile, 101, 7)
	resolved, err := resolver.ResolveHandle(context.Background(), handle.Encode())
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/exports/demo/link.txt"), resolved.Path)
	require.NotNil(t, resolved.Entry)
	assert.Equal(t, uint64(101), resolved.Entry.Attributes.Inode)
}

func TestResolverRejectsGenerationMismatch(t *testing.T) {
	client := &fakeResolverClient{
		kv:      make(map[string][]byte),
		entries: make(map[util.FullPath]*filer_pb.Entry),
	}
	resolver := NewResolver("/", client)

	record := &filer.InodeIndexRecord{
		Generation: 3,
		Paths:      []string{"/data/file.txt"},
	}
	value, err := record.Encode()
	require.NoError(t, err)
	client.kv[string(filer.InodeIndexKey(44))] = value
	client.entries["/data/file.txt"] = &filer_pb.Entry{
		Name: "file.txt",
		Attributes: &filer_pb.FuseAttributes{
			Inode: 44,
		},
	}

	handle := NewFileHandle(resolver.ExportID(), FileHandleKindFile, 44, 4)
	_, err = resolver.ResolveHandle(context.Background(), handle.Encode())
	require.ErrorIs(t, err, ErrStaleHandle)
}

func TestResolverKeepsHandleValidAcrossRename(t *testing.T) {
	client := &fakeResolverClient{
		kv:      make(map[string][]byte),
		entries: make(map[util.FullPath]*filer_pb.Entry),
	}
	resolver := NewResolver("/exports", client)

	record := &filer.InodeIndexRecord{
		Generation: 5,
		Paths:      []string{"/exports/new-name.txt"},
	}
	value, err := record.Encode()
	require.NoError(t, err)
	client.kv[string(filer.InodeIndexKey(88))] = value
	client.entries["/exports/new-name.txt"] = &filer_pb.Entry{
		Name: "new-name.txt",
		Attributes: &filer_pb.FuseAttributes{
			Inode: 88,
		},
	}

	handle := NewFileHandle(resolver.ExportID(), FileHandleKindFile, 88, 5)
	resolved, err := resolver.ResolveHandle(context.Background(), handle.Encode())
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/exports/new-name.txt"), resolved.Path)
	require.NotNil(t, resolved.Entry)
	assert.Equal(t, uint64(88), resolved.Entry.Attributes.Inode)
}

func TestResolverRejectsHandleAfterDeleteRecreateWithNewInode(t *testing.T) {
	client := &fakeResolverClient{
		kv:      make(map[string][]byte),
		entries: make(map[util.FullPath]*filer_pb.Entry),
	}
	resolver := NewResolver("/exports", client)

	client.entries["/exports/file.txt"] = &filer_pb.Entry{
		Name: "file.txt",
		Attributes: &filer_pb.FuseAttributes{
			Inode: 999,
		},
	}

	record := &filer.InodeIndexRecord{
		Generation: 4,
		Paths:      []string{"/exports/file.txt"},
	}
	value, err := record.Encode()
	require.NoError(t, err)
	client.kv[string(filer.InodeIndexKey(77))] = value

	handle := NewFileHandle(resolver.ExportID(), FileHandleKindFile, 77, 4)
	_, err = resolver.ResolveHandle(context.Background(), handle.Encode())
	require.ErrorIs(t, err, ErrStaleHandle)
}

func TestResolverSupportsSyntheticRootHandle(t *testing.T) {
	client := &fakeResolverClient{
		kv:      make(map[string][]byte),
		entries: make(map[util.FullPath]*filer_pb.Entry),
	}
	resolver := NewResolver("/", client)

	handle := NewFileHandle(resolver.ExportID(), FileHandleKindDirectory, 0, filer.InodeIndexInitialGeneration)
	resolved, err := resolver.ResolveHandle(context.Background(), handle.Encode())
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/"), resolved.Path)
	require.NotNil(t, resolved.Entry)
	assert.True(t, resolved.Entry.IsDirectory)
}

func TestNewServerNormalizesExportRootAndExportID(t *testing.T) {
	server, err := NewServer(&Option{
		FilerRootPath: "/export/path/",
		Port:          2049,
	})
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/export/path"), server.exportRoot)
	assert.Equal(t, exportIDForRoot("/export/path"), server.exportID)
}
