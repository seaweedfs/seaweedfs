package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const ttlRulePrefix = "/buckets/ttl/"

// addTtlRule gives ttlRulePrefix a 3 minute volume TTL, the fs.configure setting
// whose effect every write path below has to reproduce on the entries it stores.
func addTtlRule(t *testing.T, f *filer.Filer) {
	t.Helper()
	if err := f.FilerConf.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: ttlRulePrefix,
		Ttl:            "3m",
	}); err != nil {
		t.Fatalf("AddLocationConf: %v", err)
	}
}

// An object written through ObjectTransaction (the routed S3 write path) must
// pick up the path's TTL rule, the same as one written through CreateEntry.
func TestObjectTransactionPutAppliesRuleTtl(t *testing.T) {
	store := newRenameTestStore()
	store.entries[ttlRulePrefix] = newDirectoryEntry(ttlRulePrefix, 10)

	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	addTtlRule(t, server.filer)

	if _, err := server.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: ttlRulePrefix + "obj",
		Mutations: []*filer_pb.ObjectMutation{{
			Type:      filer_pb.ObjectMutation_PUT,
			Directory: "/buckets/ttl",
			Entry: &filer_pb.Entry{
				Name:       "obj",
				Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
			},
		}},
	}); err != nil {
		t.Fatalf("ObjectTransaction: %v", err)
	}

	entry, err := store.FindEntry(context.Background(), ttlRulePrefix+"obj")
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if entry.TtlSec != 180 {
		t.Errorf("entry TtlSec = %d, want 180", entry.TtlSec)
	}
}
