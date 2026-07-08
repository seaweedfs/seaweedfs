package log_buffer

import (
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
