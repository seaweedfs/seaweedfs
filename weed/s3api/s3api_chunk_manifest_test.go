package s3api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/require"
)

type manifestVolumeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	volumeServer string
}

func (f *manifestVolumeFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	locations := make(map[string]*filer_pb.Locations, len(req.VolumeIds))
	for _, volumeID := range req.VolumeIds {
		locations[volumeID] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.volumeServer}}}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locations}, nil
}

func TestDetectPrimarySSETypeFromManifestedEntry(t *testing.T) {
	s3a := &S3ApiServer{}
	manifest := &filer_pb.FileChunk{IsChunkManifest: true}
	for _, test := range []struct {
		name     string
		extended map[string][]byte
		want     string
	}{
		{
			name: "SSE-C",
			extended: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte(s3_constants.SSEAlgorithmAES256),
			},
			want: s3_constants.SSETypeC,
		},
		{
			name: "SSE-KMS",
			extended: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmKMS),
			},
			want: s3_constants.SSETypeKMS,
		},
		{
			name: "SSE-S3",
			extended: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256),
			},
			want: s3_constants.SSETypeS3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			entry := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{manifest}, Extended: test.extended}
			require.Equal(t, test.want, s3a.detectPrimarySSEType(entry))
		})
	}
}

func TestSSECReadsResolveChunkManifests(t *testing.T) {
	keyPair := GenerateTestSSECKey(9)
	customerKey := &SSECustomerKey{Algorithm: s3_constants.SSEAlgorithmAES256, Key: keyPair.Key, KeyMD5: keyPair.KeyMD5}
	parts := [][]byte{[]byte("first encrypted part"), []byte("second encrypted part")}
	objects := make(map[string][]byte)
	chunks := make([]*filer_pb.FileChunk, 0, len(parts))
	var plaintext []byte
	var firstIV []byte
	var offset int64
	for i, part := range parts {
		encrypted, iv, err := CreateSSECEncryptedReader(bytes.NewReader(part), customerKey)
		require.NoError(t, err)
		ciphertext, err := io.ReadAll(encrypted)
		require.NoError(t, err)
		metadata, err := SerializeSSECMetadata(iv, keyPair.KeyMD5, 0)
		require.NoError(t, err)

		chunk := &filer_pb.FileChunk{
			Fid:         &filer_pb.FileId{VolumeId: uint32(8 + i), FileKey: 1, Cookie: 1},
			Offset:      offset,
			Size:        uint64(len(part)),
			SseType:     filer_pb.SSEType_SSE_C,
			SseMetadata: metadata,
		}
		objects[chunk.GetFileIdString()] = ciphertext
		chunks = append(chunks, chunk)
		plaintext = append(plaintext, part...)
		offset += int64(len(part))
		if i == 0 {
			firstIV = iv
		}
	}

	manifests := make([]*filer_pb.FileChunk, 0, len(chunks))
	for i, chunk := range chunks {
		serializedChunks := []*filer_pb.FileChunk{proto.Clone(chunk).(*filer_pb.FileChunk)}
		filer_pb.BeforeEntrySerialization(serializedChunks)
		manifestData, err := proto.Marshal(&filer_pb.FileChunkManifest{Chunks: serializedChunks})
		require.NoError(t, err)
		manifest := &filer_pb.FileChunk{
			Fid:             &filer_pb.FileId{VolumeId: uint32(17 + i), FileKey: 1, Cookie: 1},
			Offset:          chunk.Offset,
			Size:            chunk.Size,
			IsChunkManifest: true,
		}
		objects[manifest.GetFileIdString()] = manifestData
		manifests = append(manifests, manifest)
	}

	var failFirstManifest atomic.Bool
	volumeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileID := strings.TrimPrefix(r.URL.Path, "/")
		data, found := objects[fileID]
		if fileID == manifests[0].GetFileIdString() && failFirstManifest.Load() {
			found = false
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(volumeServer.Close)
	filerAddress := startFakeFiler(t, &manifestVolumeFiler{volumeServer: strings.TrimPrefix(volumeServer.URL, "http://")})
	filerClient := wdclient.NewFilerClient(
		[]pb.ServerAddress{filerAddress},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"",
	)
	t.Cleanup(filerClient.Close)
	s3a := &S3ApiServer{option: &S3ApiServerOption{}, filerClient: filerClient}

	newEntry := func() *filer_pb.Entry {
		entryManifests := make([]*filer_pb.FileChunk, len(manifests))
		for i, manifest := range manifests {
			entryManifests[i] = proto.Clone(manifest).(*filer_pb.FileChunk)
		}
		return &filer_pb.Entry{
			Name:       "object",
			Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(plaintext))},
			Chunks:     entryManifests,
			Extended: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte(s3_constants.SSEAlgorithmAES256),
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5:    []byte(keyPair.KeyMD5),
				s3_constants.SeaweedFSSSEIV:                           firstIV,
			},
		}
	}
	newRequest := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
		SetupTestSSECHeaders(r, keyPair)
		return r
	}

	t.Run("full object", func(t *testing.T) {
		entry := newEntry()
		sseType := s3a.detectPrimarySSEType(entry)
		w := httptest.NewRecorder()
		err := s3a.streamFromVolumeServersWithSSE(w, newRequest(), entry, sseType, "bucket", "object", "")
		require.NoError(t, err)
		require.Equal(t, plaintext, w.Body.Bytes())
	})

	t.Run("range", func(t *testing.T) {
		failFirstManifest.Store(true)
		entry := newEntry()
		sseType := s3a.detectPrimarySSEType(entry)
		r := newRequest()
		start, end := len(parts[0])+2, len(parts[0])+8
		r.Header.Set("Range", "bytes="+strconv.Itoa(start)+"-"+strconv.Itoa(end))
		w := httptest.NewRecorder()
		err := s3a.streamFromVolumeServersWithSSE(w, r, entry, sseType, "bucket", "object", "")
		require.NoError(t, err)
		require.Equal(t, plaintext[start:end+1], w.Body.Bytes())
	})

	t.Run("invalid range", func(t *testing.T) {
		entry := newEntry()
		r := newRequest()
		r.Header.Set("Range", "bytes="+strconv.Itoa(len(plaintext))+"-")
		w := httptest.NewRecorder()
		err := s3a.streamFromVolumeServersWithSSE(w, r, entry, s3a.detectPrimarySSEType(entry), "bucket", "object", "")
		require.Error(t, err)
		require.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)
	})

	t.Run("wrong key", func(t *testing.T) {
		entry := newEntry()
		r := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
		SetupTestSSECHeaders(r, GenerateTestSSECKey(10))
		w := httptest.NewRecorder()
		err := s3a.streamFromVolumeServersWithSSE(w, r, entry, s3a.detectPrimarySSEType(entry), "bucket", "object", "")
		require.Error(t, err)
		require.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestPartRange(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{FileId: "1,a", Offset: 0, Size: 8},
		{FileId: "1,b", Offset: 8, Size: 8},
		{FileId: "1,c", Offset: 16, Size: 24},
	}

	start, end, ok := partRange(&PartBoundaryInfo{StartChunk: 40, EndChunk: 80, StartOffset: 16, EndOffset: 40}, chunks)
	if !ok || start != 16 || end != 39 {
		t.Errorf("offset boundary: got [%d,%d] ok=%v, want [16,39]", start, end, ok)
	}

	start, end, ok = partRange(&PartBoundaryInfo{StartChunk: 1, EndChunk: 3}, chunks)
	if !ok || start != 8 || end != 39 {
		t.Errorf("legacy boundary: got [%d,%d] ok=%v, want [8,39]", start, end, ok)
	}

	for _, b := range []*PartBoundaryInfo{
		{StartChunk: 2, EndChunk: 9},
		{StartChunk: -1, EndChunk: 2},
		{StartChunk: 2, EndChunk: 2},
	} {
		if _, _, ok := partRange(b, chunks); ok {
			t.Errorf("boundary %+v should not resolve", b)
		}
	}
}
