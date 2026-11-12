package filer

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type assignMaster struct {
	collectionDeleteMaster
	volumeUrl string
	requests  chan *master_pb.AssignRequest
}

func (m *assignMaster) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {
	m.requests <- req
	location := &master_pb.Location{Url: m.volumeUrl, PublicUrl: m.volumeUrl}
	return &master_pb.AssignResponse{Fid: "3,01637037d6", Count: 1, Location: location}, nil
}

type receivedPiece struct {
	contentEncoding string
	body            []byte
}

func uploadMetaLogPiece(t *testing.T, data []byte) (*master_pb.AssignRequest, receivedPiece, *operation.UploadResult) {
	t.Helper()
	received := make(chan receivedPiece, 1)
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		part, err := r.MultipartReader()
		require.NoError(t, err)
		p, err := part.NextPart()
		require.NoError(t, err)
		body, err := io.ReadAll(p)
		require.NoError(t, err)
		received <- receivedPiece{contentEncoding: p.Header.Get("Content-Encoding"), body: body}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(operation.UploadResult{Size: uint32(len(body))})
	}))
	t.Cleanup(volume.Close)

	master := &assignMaster{
		collectionDeleteMaster: collectionDeleteMaster{calls: make(chan collectionDeleteCall, 1)},
		volumeUrl:              strings.TrimPrefix(volume.URL, "http://"),
		requests:               make(chan *master_pb.AssignRequest, 1),
	}
	f, _ := newFilerWithMaster(t, master)
	f.GrpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	_, uploadResult, err := f.assignAndUpload(context.Background(), string(appendTestLog), data)
	require.NoError(t, err)
	return <-master.requests, <-received, uploadResult
}

func metaLogRecords(t *testing.T, n int) []byte {
	t.Helper()
	chunks := make([]*filer_pb.FileChunk, 200)
	for i := range chunks {
		md5 := make([]byte, 16)
		_, err := rand.Read(md5)
		require.NoError(t, err)
		chunks[i] = &filer_pb.FileChunk{
			FileId: fmt.Sprintf("%d,%x%08x", i%7+1, i+1, binary.BigEndian.Uint32(md5)),
			Offset: int64(i) * 256,
			Size:   256,
			ETag:   base64.StdEncoding.EncodeToString(md5),
		}
	}
	var buf []byte
	for i := 0; i < n; i++ {
		event, err := proto.Marshal(&filer_pb.SubscribeMetadataResponse{
			Directory: "/soak",
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "journal.log", Chunks: chunks[:len(chunks)-1]},
				NewEntry: &filer_pb.Entry{Name: "journal.log", Chunks: chunks},
			},
			TsNs: int64(i + 1),
		})
		require.NoError(t, err)
		record, err := proto.Marshal(&filer_pb.LogEntry{TsNs: int64(i + 1), Data: event})
		require.NoError(t, err)
		size := make([]byte, 4)
		util.Uint32toBytes(size, uint32(len(record)))
		buf = append(buf, size...)
		buf = append(buf, record...)
	}
	return buf
}

func TestMetaLogPieceUploadsCompressed(t *testing.T) {
	data := metaLogRecords(t, 50)

	assign, piece, uploadResult := uploadMetaLogPiece(t, data)

	require.Equal(t, "gzip", piece.contentEncoding)
	decompressed, err := util.DecompressData(piece.body)
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
	assert.Less(t, len(piece.body)*5, len(data), "repeated chunk lists should compress well")
	assert.Equal(t, uint64(len(piece.body)), assign.GetExpectedDataSize(), "assign sizes the bytes actually stored")
	assert.Equal(t, uint32(1), uploadResult.Gzip)
	assert.Equal(t, uint32(len(data)), uploadResult.Size, "chunk size and offsets stay in log bytes")
}

func TestMetaLogPieceUploadsIncompressibleAsIs(t *testing.T) {
	data := make([]byte, 64*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)

	_, piece, uploadResult := uploadMetaLogPiece(t, data)

	assert.Empty(t, piece.contentEncoding)
	assert.Equal(t, data, piece.body)
	assert.Zero(t, uploadResult.Gzip)
	assert.Equal(t, uint32(len(data)), uploadResult.Size)
}
