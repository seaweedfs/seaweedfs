package dash

import (
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
)

func TestSumFilterStatsPerHourAcrossEngines(t *testing.T) {
	got, truncated := sumFilterStats([]*vector_pb.FilterStatsResponse{
		{WindowSeconds: 3600, Keys: []*vector_pb.FilterKeyStats{
			{Key: "author", Queries: 100, Seconds: 2, StringOperand: true},
			{Key: "year", Queries: 10, Seconds: 0.5, NumberOperand: true},
		}},
		// An engine up for half an hour counts double per hour.
		{WindowSeconds: 1800, Keys: []*vector_pb.FilterKeyStats{
			{Key: "author", Queries: 50, Seconds: 1, StringOperand: true},
		}},
		nil,
		{WindowSeconds: 0, Keys: []*vector_pb.FilterKeyStats{{Key: "ignored", Queries: 5}}},
	})
	author := got["author"]
	if author == nil || math.Abs(author.perHour-200) > 1e-9 || author.queries != 150 || math.Abs(author.seconds-3) > 1e-9 || !author.strings {
		t.Fatalf("author: %+v", author)
	}
	if year := got["year"]; year == nil || year.strings || !year.numbers || year.perHour != 10 {
		t.Fatalf("year: %+v", year)
	}
	if _, ok := got["ignored"]; ok {
		t.Fatal("an engine with no window must not count")
	}
	if truncated {
		t.Fatal("no engine stopped counting keys")
	}
}

func TestSumFilterStatsKeepsTruncated(t *testing.T) {
	_, truncated := sumFilterStats([]*vector_pb.FilterStatsResponse{
		{WindowSeconds: 3600, Keys: []*vector_pb.FilterKeyStats{{Key: "author", Queries: 1}}},
		{WindowSeconds: 3600, Truncated: true},
	})
	if !truncated {
		t.Fatal("one engine's truncated stats make the sum truncated")
	}
}
