package shell

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestPlanUnrouteTTL_RemovesDayTTLEntries(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bk/logs/", Collection: "bk", Ttl: "7d",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bk/data/", Collection: "bk", Ttl: "30d",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	removed, changed := planUnrouteTTL(fc, "bk", "/buckets")
	if !changed {
		t.Fatalf("changed=false, want true")
	}
	if len(removed) != 2 {
		t.Fatalf("removed=%v, want 2 entries", removed)
	}
	for _, line := range removed {
		if !strings.Contains(line, "unroute /buckets/bk/") {
			t.Errorf("unexpected line: %q", line)
		}
	}
	if _, ok := fc.GetLocationConf("/buckets/bk/logs/"); ok {
		t.Errorf("/buckets/bk/logs/ should be removed")
	}
	if _, ok := fc.GetLocationConf("/buckets/bk/data/"); ok {
		t.Errorf("/buckets/bk/data/ should be removed")
	}
}

func TestPlanUnrouteTTL_LeavesForeignBucketsAlone(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bk/logs/", Collection: "bk", Ttl: "7d",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/other/logs/", Collection: "other", Ttl: "7d",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, changed := planUnrouteTTL(fc, "bk", "/buckets")
	if !changed {
		t.Fatalf("changed=false, want true (bk's entry should be removed)")
	}
	if _, ok := fc.GetLocationConf("/buckets/other/logs/"); !ok {
		t.Errorf("/buckets/other/logs/ must not be touched when unrouting bk")
	}
}

func TestPlanUnrouteTTL_SkipsNonDayTTLEntries(t *testing.T) {
	// Operators may have manually configured non-lifecycle entries
	// under the bucket (e.g. fsync, readonly); those don't carry a
	// "Nd" TTL and must survive.
	fc := filer.NewFilerConf()
	if err := fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/bk/manual/", Collection: "bk", Fsync: true,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, changed := planUnrouteTTL(fc, "bk", "/buckets")
	if changed {
		t.Fatalf("changed=true; non-day-TTL operator config must not be touched")
	}
	if _, ok := fc.GetLocationConf("/buckets/bk/manual/"); !ok {
		t.Errorf("/buckets/bk/manual/ must survive an unroute call")
	}
}

func TestPlanUnrouteTTL_NoEntriesIsNoop(t *testing.T) {
	fc := filer.NewFilerConf()
	removed, changed := planUnrouteTTL(fc, "bk", "/buckets")
	if changed {
		t.Fatalf("changed=true on empty FilerConf")
	}
	if len(removed) != 0 {
		t.Fatalf("removed=%v, want empty", removed)
	}
}
