package log_buffer

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// readTsNs must return exactly what a full proto.Unmarshal would put in TsNs,
// for every field combination, without decoding data/key.
func TestReadTsNsMatchesUnmarshal(t *testing.T) {
	cases := []*filer_pb.LogEntry{
		{},                              // all defaults: ts_ns absent on the wire => 0
		{TsNs: 1},                       // minimal
		{TsNs: 1783396413597780505},     // realistic nanosecond timestamp
		{TsNs: math.MaxInt64},           // 9-byte varint
		{TsNs: -1},                      // negative int64 => 10-byte varint
		{TsNs: math.MinInt64},           // extreme negative
		{TsNs: 42, PartitionKeyHash: 7}, // ts followed by another varint field
		{TsNs: 12345, Data: []byte("some-metadata-event-payload"), Key: []byte("/buckets/pvc-x/1.log")},
		{TsNs: 99, Data: make([]byte, 64*1024), Key: []byte("k"), Offset: 555}, // large data, all fields
		{Data: []byte("no-ts"), Key: []byte("k"), Offset: 9},                   // ts_ns == 0 with other fields present
	}
	for i, e := range cases {
		data, err := proto.Marshal(e)
		if err != nil {
			t.Fatalf("case %d marshal: %v", i, err)
		}
		got, err := readTsNs(data)
		if err != nil {
			t.Fatalf("case %d readTsNs: %v", i, err)
		}
		if got != e.TsNs {
			t.Errorf("case %d: readTsNs=%d want %d", i, got, e.TsNs)
		}
	}
}

// Fuzz against proto.Unmarshal with randomized entries.
func TestReadTsNsFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		e := &filer_pb.LogEntry{
			TsNs:             r.Int63() - r.Int63(),
			PartitionKeyHash: r.Int31(),
			Offset:           r.Int63(),
		}
		if r.Intn(2) == 0 {
			e.Data = make([]byte, r.Intn(300))
			r.Read(e.Data)
		}
		if r.Intn(2) == 0 {
			e.Key = make([]byte, r.Intn(64))
			r.Read(e.Key)
		}
		data, err := proto.Marshal(e)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var want filer_pb.LogEntry
		if err := proto.Unmarshal(data, &want); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		got, err := readTsNs(data)
		if err != nil {
			t.Fatalf("readTsNs iter %d: %v", i, err)
		}
		if got != want.TsNs {
			t.Fatalf("iter %d: readTsNs=%d want %d", i, got, want.TsNs)
		}
	}
}

// readTsNs must not panic and must report an error on truncated/garbage input.
func TestReadTsNsCorrupted(t *testing.T) {
	inputs := [][]byte{
		{0x08},                   // ts_ns tag with no value
		{0x1a, 0xff, 0xff, 0xff}, // data field, length overruns buffer
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, // overlong tag
	}
	for i, in := range inputs {
		if _, err := readTsNs(in); err == nil {
			t.Errorf("input %d: expected error, got nil", i)
		}
	}
}

// unmarshalLogEntryAliased must produce the same fields as proto.Unmarshal.
func TestUnmarshalLogEntryAliasedMatchesUnmarshal(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 5000; i++ {
		e := &filer_pb.LogEntry{
			TsNs:             r.Int63() - r.Int63(),
			PartitionKeyHash: r.Int31() - r.Int31(),
			Offset:           r.Int63() - r.Int63(),
		}
		if r.Intn(2) == 0 {
			e.Data = make([]byte, r.Intn(4096))
			r.Read(e.Data)
		}
		if r.Intn(2) == 0 {
			e.Key = make([]byte, r.Intn(128))
			r.Read(e.Key)
		}
		data, err := proto.Marshal(e)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var want, got filer_pb.LogEntry
		if err := proto.Unmarshal(data, &want); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if err := unmarshalLogEntryAliased(data, &got); err != nil {
			t.Fatalf("aliased iter %d: %v", i, err)
		}
		if got.TsNs != want.TsNs || got.PartitionKeyHash != want.PartitionKeyHash ||
			got.Offset != want.Offset || !bytes.Equal(got.Data, want.Data) || !bytes.Equal(got.Key, want.Key) {
			t.Fatalf("iter %d mismatch:\n got=%+v\nwant=%+v", i, &got, &want)
		}
	}
}

// The decoded data/key must point into the source buffer, not a copy.
func TestUnmarshalLogEntryAliasedAliases(t *testing.T) {
	data, err := proto.Marshal(&filer_pb.LogEntry{TsNs: 5, Data: []byte("payload"), Key: []byte("path")})
	if err != nil {
		t.Fatal(err)
	}
	var out filer_pb.LogEntry
	if err := unmarshalLogEntryAliased(data, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "payload" || string(out.Key) != "path" {
		t.Fatalf("decoded data=%q key=%q", out.Data, out.Key)
	}
	for i := range data { // mutate source; an alias must observe it, a copy must not
		data[i] ^= 0xFF
	}
	if string(out.Data) == "payload" || string(out.Key) == "path" {
		t.Fatal("data/key were copied, expected an alias into the source buffer")
	}
}

// sampleEntryData returns a marshaled LogEntry whose Data is a realistic
// marshaled metadata event, as delivered on the subscribe path.
func sampleEntryData(tb testing.TB) []byte {
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/pvc-bf458866-0413-4cca-99c6-3b7386a80855",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "object-000123",
				Attributes: &filer_pb.FuseAttributes{FileSize: 4096, Mtime: 1783396413},
				Chunks: []*filer_pb.FileChunk{
					{FileId: "3,01637037d6", Size: 4096, Fid: &filer_pb.FileId{VolumeId: 3, FileKey: 23283, Cookie: 2015}},
				},
			},
		},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		tb.Fatal(err)
	}
	data, err := proto.Marshal(&filer_pb.LogEntry{
		TsNs: 1783396413597780505, PartitionKeyHash: 12345, Offset: 2013258,
		Key: []byte("/buckets/pvc-bf458866/object-000123"), Data: payload,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

// Per-entry decode cost on the delivery path: the old full unmarshal copies the
// data/key byte fields; the aliased decode does not. The delta is what the fix
// saves for every entry a subscriber reads (and, for non-matching entries a
// subscriber filters out, the whole saving with nothing decoded downstream).
func BenchmarkLogEntryDecodeCopy(b *testing.B) {
	data := sampleEntryData(b)
	var out filer_pb.LogEntry
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := proto.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogEntryDecodeAlias(b *testing.B) {
	data := sampleEntryData(b)
	var out filer_pb.LogEntry
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := unmarshalLogEntryAliased(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadTs(b *testing.B) {
	e := &filer_pb.LogEntry{
		TsNs:             1783396413597780505,
		PartitionKeyHash: 12345,
		Data:             make([]byte, 4*1024), // typical metadata event payload
		Key:              []byte("/buckets/pvc-bf458866/1.log"),
		Offset:           2013258,
	}
	data, err := proto.Marshal(e)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 4+len(data))
	util.Uint32toBytes(buf, uint32(len(data)))
	copy(buf[4:], data)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := readTs(buf, 0); err != nil {
			b.Fatal(err)
		}
	}
}
