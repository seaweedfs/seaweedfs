package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The metadata log's collection resolution must let operators redirect the
// internal /topics/.system/log chunks into their own collection without
// touching where user data goes, and must be strictly backward compatible
// when the override is unset.

func TestMetaLogCollectionResolution(t *testing.T) {
	tests := []struct {
		name           string
		target         string // filer.options.metaLog.collection override
		filerDefault   string // -collection flag value
		ruleCollection string // storage rule matched on the log path
		want           string
	}{
		{
			name:           "no override: filer default wins (today's behaviour)",
			target:         "",
			filerDefault:   "mydata",
			ruleCollection: "",
			want:           "mydata",
		},
		{
			name:           "no override anywhere: still empty (default collection)",
			target:         "",
			filerDefault:   "",
			ruleCollection: "",
			want:           "",
		},
		{
			name:           "override wins over filer default",
			target:         "filer-meta",
			filerDefault:   "mydata",
			ruleCollection: "",
			want:           "filer-meta",
		},
		{
			name:           "override wins over matched rule",
			target:         "filer-meta",
			filerDefault:   "",
			ruleCollection: "rulecol",
			want:           "filer-meta",
		},
		{
			name:           "no override but rule set: rule used (unchanged fallback chain)",
			target:         "",
			filerDefault:   "",
			ruleCollection: "rulecol",
			want:           "rulecol",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Drive the same setter a running filer would use post-construction.
			f := &Filer{}
			f.metaLogTargetCollection = tc.target
			f.metaLogCollection = tc.filerDefault
			if got := f.metaLogCollectionFor(tc.ruleCollection); got != tc.want {
				t.Errorf("metaLogCollectionFor(%q) = %q, want %q", tc.ruleCollection, got, tc.want)
			}
		})
	}
}

func TestMetaLogReplicationResolution(t *testing.T) {
	f := &Filer{}
	f.metaLogTargetReplication = "110"
	f.metaLogReplication = "010"
	if got := f.metaLogReplicationFor("001"); got != "110" {
		t.Errorf("override must win, got %q", got)
	}
	f.metaLogTargetReplication = ""
	if got := f.metaLogReplicationFor("001"); got != "010" {
		t.Errorf("filer replication must win when no override, got %q", got)
	}
	f.metaLogReplication = ""
	if got := f.metaLogReplicationFor("001"); got != "001" {
		t.Errorf("rule replication must be used last, got %q", got)
	}
}

// TestViperReadsMetaLogOverrides proves the exact viper keys the docs promise
// are read into the fields a running filer uses.
func TestViperReadsMetaLogOverrides(t *testing.T) {
	v := util.GetViper()
	v.Set("filer.options.metaLog.collection", "filer-meta")
	v.Set("filer.options.metaLog.replication", "100")
	defer func() {
		// Reset so other tests see the unset default.
		v.Set("filer.options.metaLog.collection", "")
		v.Set("filer.options.metaLog.replication", "")
	}()

	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	if got := f.metaLogTargetCollection; got != "filer-meta" {
		t.Fatalf("NewFiler did not read the collection override: %q", got)
	}
	if got := f.metaLogTargetReplication; got != "100" {
		t.Fatalf("NewFiler did not read the replication override: %q", got)
	}
}

// TestBucketCollectionKeepsMetaLogTargetCollection mirrors the guarantee that
// protects the filer's default collection: when a bucket resolves to the
// collection the system metadata log was redirected to, deleting the bucket
// must not drop that collection (it backs internal log volumes).
func TestBucketCollectionKeepsMetaLogTargetCollection(t *testing.T) {
	f, store, master := newFilerWithFakeMaster(t)
	// Operator redirected the meta log to its own collection...
	f.metaLogTargetCollection = "filer-meta"
	// ...and a bucket happens to resolve to that very collection.
	f.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/a",
		Collection:     "filer-meta",
	})
	seedBucket(t, store, util.FullPath("/buckets/a"))

	if err := f.DeleteEntryMetaAndData(context.Background(), "/buckets/a", true, false, true, false, nil, 0); err != nil {
		t.Fatalf("DeleteEntryMetaAndData: %v", err)
	}

	select {
	case call := <-master.calls:
		t.Fatalf("the meta-log target collection was deleted by a bucket delete: %q", call.name)
	default:
	}
}
