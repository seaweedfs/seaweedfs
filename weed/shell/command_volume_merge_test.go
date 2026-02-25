package shell

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type sliceNeedleStream struct {
	needles []*needle.Needle
	index   int
}

func (s *sliceNeedleStream) Next() (*needle.Needle, bool) {
	if s.index >= len(s.needles) {
		return nil, false
	}
	n := s.needles[s.index]
	s.index++
	return n, true
}

func (s *sliceNeedleStream) Err() error {
	return nil
}

func TestMergeNeedleStreamsOrdersByTimestamp(t *testing.T) {
	streamA := &sliceNeedleStream{needles: []*needle.Needle{
		{Id: 1, AppendAtNs: 10_000_000_100},
		{Id: 2, AppendAtNs: 10_000_000_400},
	}}
	streamB := &sliceNeedleStream{needles: []*needle.Needle{
		{Id: 3, AppendAtNs: 10_000_000_200},
		{Id: 4, AppendAtNs: 10_000_000_300},
	}}
	streamC := &sliceNeedleStream{needles: []*needle.Needle{
		{Id: 5, LastModified: 1},
	}}

	var got []uint64
	err := mergeNeedleStreams([]needleStream{streamA, streamB, streamC}, func(_ int, n *needle.Needle) error {
		got = append(got, uint64(n.Id))
		return nil
	})
	if err != nil {
		t.Fatalf("mergeNeedleStreams error: %v", err)
	}

	want := []uint64{5, 1, 3, 4, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected merge order: got %v want %v", got, want)
	}
}

func TestMergeNeedleStreamsSkipsCrossStreamDuplicates(t *testing.T) {
	streamA := &sliceNeedleStream{needles: []*needle.Needle{
		{Id: 10, AppendAtNs: 10_000_000_100},
		{Id: 10, AppendAtNs: 10_000_000_300},
	}}
	streamB := &sliceNeedleStream{needles: []*needle.Needle{
		{Id: 10, AppendAtNs: 10_000_000_100},
		{Id: 11, AppendAtNs: 10_000_000_200},
	}}

	type seenNeedle struct {
		id uint64
		ts uint64
	}
	var got []seenNeedle
	err := mergeNeedleStreams([]needleStream{streamA, streamB}, func(_ int, n *needle.Needle) error {
		got = append(got, seenNeedle{id: uint64(n.Id), ts: needleTimestamp(n)})
		return nil
	})
	if err != nil {
		t.Fatalf("mergeNeedleStreams error: %v", err)
	}

	want := []seenNeedle{
		{id: 10, ts: 10_000_000_100},
		{id: 11, ts: 10_000_000_200},
		{id: 10, ts: 10_000_000_300},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected merge output: got %v want %v", got, want)
	}
}
