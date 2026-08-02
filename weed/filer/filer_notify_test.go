package filer

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/protobuf/proto"
)

func TestProtoMarshal(t *testing.T) {

	oldEntry := &Entry{
		FullPath: util.FullPath("/this/path/to"),
		Attr: Attr{
			Mtime:  time.Now(),
			Mode:   0644,
			Uid:    1,
			Mime:   "text/json",
			TtlSec: 25,
		},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId:       "234,2423423422",
				Offset:       234234,
				Size:         234,
				ModifiedTsNs: 12312423,
				ETag:         "2342342354",
				SourceFileId: "23234,2342342342",
			},
		},
	}

	notification := &filer_pb.EventNotification{
		OldEntry:     oldEntry.ToProtoEntry(),
		NewEntry:     nil,
		DeleteChunks: true,
	}

	text, _ := proto.Marshal(notification)

	notification2 := &filer_pb.EventNotification{}
	proto.Unmarshal(text, notification2)

	if notification2.OldEntry.GetChunks()[0].SourceFileId != notification.OldEntry.GetChunks()[0].SourceFileId {
		t.Fatalf("marshal/unmarshal error: %s", text)
	}

	println(string(text))

}

// buildLogBuffer lays out records exactly as LogBuffer.AddDataToBuffer does:
// a 4-byte size prefix in front of each marshaled LogEntry.
func buildLogBuffer(t *testing.T, payloadSizes []int) (buf []byte, count int) {
	t.Helper()
	sizeBuf := make([]byte, 4)
	for i, payloadSize := range payloadSizes {
		data, err := proto.Marshal(&filer_pb.LogEntry{
			TsNs: int64(i + 1),
			Data: make([]byte, payloadSize),
		})
		if err != nil {
			t.Fatalf("marshal log entry: %v", err)
		}
		util.Uint32toBytes(sizeBuf, uint32(len(data)))
		buf = append(buf, sizeBuf...)
		buf = append(buf, data...)
	}
	return buf, len(payloadSizes)
}

// splitLogBuffer drains a buffer the way logFlushFunc does, one piece at a
// time, so the tests exercise the same walk.
func splitLogBuffer(buf []byte, maxSize int) [][]byte {
	var pieces [][]byte
	for len(buf) > 0 {
		piece := nextLogPiece(buf, maxSize)
		pieces = append(pieces, piece)
		buf = buf[len(piece):]
	}
	return pieces
}

func TestNextLogPiece(t *testing.T) {
	const maxSize = 1024

	testCases := []struct {
		name         string
		payloadSizes []int
	}{
		{"fits in one piece", []int{10, 20, 30}},
		{"many small records", []int{300, 300, 300, 300, 300, 300, 300}},
		{"one record far over the limit", []int{5000}},
		{"oversized record between small ones", []int{100, 5000, 100}},
		{"record exactly at the limit", []int{maxSize - 4 - 6}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, count := buildLogBuffer(t, tc.payloadSizes)

			var rejoined []byte
			for i, piece := range splitLogBuffer(buf, maxSize) {
				if len(piece) > maxSize {
					t.Errorf("piece %d is %d bytes, over the %d limit", i, len(piece), maxSize)
				}
				if len(piece) == 0 {
					t.Errorf("piece %d is empty", i)
				}
				rejoined = append(rejoined, piece...)
			}
			if !bytes.Equal(rejoined, buf) {
				t.Errorf("rejoined pieces differ from the original buffer")
			}

			// The pieces still stream back as the same records in order.
			decoded, _, err := decodeLogRecords(rejoined)
			if err != nil {
				t.Fatalf("decode rejoined buffer: %v", err)
			}
			if len(decoded) != count {
				t.Errorf("decoded %d records, want %d", len(decoded), count)
			}
		})
	}
}

// A buffer of ordinary records splits on record boundaries, so every piece
// decodes on its own and the readers keep the per-chunk cache path.
func TestNextLogPieceKeepsRecordBoundaries(t *testing.T) {
	buf, count := buildLogBuffer(t, []int{300, 300, 300, 300, 300, 300, 300})

	var decodedCount int
	for i, piece := range splitLogBuffer(buf, 1024) {
		entries, cacheable, err := decodeLogRecords(piece)
		if err != nil {
			t.Fatalf("piece %d does not decode standalone: %v", i, err)
		}
		if !cacheable {
			t.Errorf("piece %d is not cacheable", i)
		}
		decodedCount += len(entries)
	}
	if decodedCount != count {
		t.Errorf("decoded %d records across pieces, want %d", decodedCount, count)
	}
}

// A truncated tail must not be dropped or duplicated, only cut by size.
func TestNextLogPieceTruncatedTail(t *testing.T) {
	buf, _ := buildLogBuffer(t, []int{300, 300})
	buf = append(buf, 0xff, 0xff, 0xff)

	var rejoined []byte
	for i, piece := range splitLogBuffer(buf, 320) {
		if len(piece) > 320 {
			t.Errorf("piece %d is %d bytes, over the 320 limit", i, len(piece))
		}
		rejoined = append(rejoined, piece...)
	}
	if !bytes.Equal(rejoined, buf) {
		t.Errorf("rejoined pieces differ from the original buffer")
	}
}

// A cluster whose fileSizeLimitMB is under metadataLogUploadLimit says so in
// the rejection, and the flush has to take that limit up rather than retry an
// unwritable piece forever.
func TestVolumeFileSizeLimit(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want int
	}{
		{
			"volume server size rejection",
			fmt.Errorf("upload data http://127.0.0.1:8180/1,16ef8dca8a: unmarshalled error http://127.0.0.1:8180/1,16ef8dca8a: file over the limited 16777216 bytes"),
			16777216,
		},
		{"default limit", errors.New("file over the limited 268435456 bytes"), 268435456},
		{"unrelated failure", errors.New("connection refused"), 0},
		{"no byte count", errors.New("file over the limited bytes"), 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := volumeFileSizeLimit(tc.err); got != tc.want {
				t.Errorf("volumeFileSizeLimit = %d, want %d", got, tc.want)
			}
		})
	}
}

// Once the reported limit is adopted, every piece fits it.
func TestNextLogPieceHonorsLoweredLimit(t *testing.T) {
	buf, _ := buildLogBuffer(t, []int{20 << 20})

	const lowered = 1 << 20
	var rejoined []byte
	for i, piece := range splitLogBuffer(buf, lowered) {
		if len(piece) > lowered {
			t.Errorf("piece %d is %d bytes, over the lowered %d limit", i, len(piece), lowered)
		}
		rejoined = append(rejoined, piece...)
	}
	if !bytes.Equal(rejoined, buf) {
		t.Errorf("rejoined pieces differ from the original buffer")
	}
}
