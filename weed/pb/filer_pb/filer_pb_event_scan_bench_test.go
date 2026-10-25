package filer_pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func benchmarkEventBytes(b *testing.B, chunkCount int) []byte {
	b.Helper()
	chunks := make([]*FileChunk, chunkCount)
	for i := range chunks {
		chunks[i] = &FileChunk{FileId: "3,1234567890ab", Offset: int64(i) * 4096, Size: 4096, ModifiedTsNs: int64(i)}
	}
	data, err := proto.Marshal(&SubscribeMetadataResponse{
		Directory: "/data/pvc-42",
		TsNs:      123456789,
		EventNotification: &EventNotification{
			OldEntry: &Entry{Name: "0.log", Chunks: chunks},
			NewEntry: &Entry{Name: "0.log", Chunks: chunks},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return data
}

// the non-matching subscriber case, which is what a per-path fleet hits for
// almost every entry

func BenchmarkSubscriptionMatchFullDecode(b *testing.B) {
	data := benchmarkEventBytes(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := &SubscribeMetadataResponse{}
		if err := proto.Unmarshal(data, event); err != nil {
			b.Fatal(err)
		}
		if MetadataEventMatchesSubscription(event, "/data/pvc-7/", nil, nil) {
			b.Fatal("unexpected match")
		}
	}
}

func BenchmarkSubscriptionMatchSkeleton(b *testing.B) {
	data := benchmarkEventBytes(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		skeleton, ok := ScanMetadataEventSkeleton(data)
		if !ok {
			b.Fatal("scan failed")
		}
		if MetadataEventMatchesSubscription(skeleton, "/data/pvc-7/", nil, nil) {
			b.Fatal("unexpected match")
		}
	}
}
