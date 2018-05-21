package filer2

import (
	"testing"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"log"
)

func TestIntervalMerging(t *testing.T) {

	testcases := []struct {
		Chunks   []*filer_pb.FileChunk
		Expected []*visibleInterval
	}{
		// case 0: normal
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 100, Size: 100, FileId: "asdf", Mtime: 134},
				{Offset: 200, Size: 100, FileId: "fsad", Mtime: 353},
			},
			Expected: []*visibleInterval{
				{start: 0, stop: 100, fileId: "abc"},
				{start: 100, stop: 200, fileId: "asdf"},
				{start: 200, stop: 300, fileId: "fsad"},
			},
		},
		// case 1: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
			},
			Expected: []*visibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
			},
		},
		// case 2: updates overwrite part of previous chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 50, FileId: "asdf", Mtime: 134},
			},
			Expected: []*visibleInterval{
				{start: 0, stop: 50, fileId: "asdf"},
				{start: 50, stop: 100, fileId: "abc"},
			},
		},
		// case 3: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 50, Size: 250, FileId: "xxxx", Mtime: 154},
			},
			Expected: []*visibleInterval{
				{start: 0, stop: 50, fileId: "asdf"},
				{start: 50, stop: 300, fileId: "xxxx"},
			},
		},
		// case 4: updates far away from prev chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 250, Size: 250, FileId: "xxxx", Mtime: 154},
			},
			Expected: []*visibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
				{start: 250, stop: 500, fileId: "xxxx"},
			},
		},
		// case 5: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 184},
				{Offset: 70, Size: 150, FileId: "abc", Mtime: 143},
				{Offset: 80, Size: 100, FileId: "xxxx", Mtime: 134},
			},
			Expected: []*visibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
				{start: 200, stop: 220, fileId: "abc"},
			},
		},
	}

	for i, testcase := range testcases {
		log.Printf("++++++++++ merged test case %d ++++++++++++++++++++", i)
		intervals := nonOverlappingVisibleIntervals(testcase.Chunks)
		for x, interval := range intervals {
			log.Printf("test case %d, interval %d, start=%d, stop=%d, fileId=%s",
				i, x, interval.start, interval.stop, interval.fileId)
			if interval.start != testcase.Expected[x].start {
				t.Fatalf("failed on test case %d, interval %d, start %d, expect %d",
					i, x, interval.start, testcase.Expected[x].start)
			}
			if interval.stop != testcase.Expected[x].stop {
				t.Fatalf("failed on test case %d, interval %d, stop %d, expect %d",
					i, x, interval.stop, testcase.Expected[x].stop)
			}
			if interval.fileId != testcase.Expected[x].fileId {
				t.Fatalf("failed on test case %d, interval %d, chunkId %s, expect %s",
					i, x, interval.fileId, testcase.Expected[x].fileId)
			}
		}
		if len(intervals) != len(testcase.Expected) {
			t.Fatalf("failed to compact test case %d, len %d expected %d", i, len(intervals), len(testcase.Expected))
		}
	}

}
