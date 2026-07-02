package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func mustSetObjectTxnStorageConf(t *testing.T, fs *FilerServer, conf *filer_pb.FilerConf_PathConf) {
	t.Helper()

	if err := fs.filer.FilerConf.SetLocationConf(conf); err != nil {
		t.Fatalf("SetLocationConf(%q): %v", conf.LocationPrefix, err)
	}
}

func TestObjectTransactionPutAppliesConfiguredTTLAndExpires(t *testing.T) {
	fs, store := newTxnTestServer(nil)

	mustSetObjectTxnStorageConf(t, fs, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/video/",
		Ttl:            "1h",
	})

	old := time.Now().Add(-2 * time.Hour)

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/video/expired.mp4",
		Mutations: []*filer_pb.ObjectMutation{
			{
				Type:      filer_pb.ObjectMutation_PUT,
				Directory: "/buckets/video",
				Entry: &filer_pb.Entry{
					Name: "expired.mp4",
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   old.Unix(),
						Mtime:    old.Unix(),
						FileMode: 0644,
						FileSize: 123,
						TtlSec:   0,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ObjectTransaction transport error: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("ObjectTransaction response error: %q", resp.Error)
	}

	entry := store.entries["/buckets/video/expired.mp4"]
	if entry == nil {
		t.Fatalf("PUT should create /buckets/video/expired.mp4")
	}
	if got := entry.TtlSec; got != 3600 {
		t.Fatalf("TtlSec = %d, want 3600 from fs.configure storage rule", got)
	}

	_, err = fs.filer.FindEntry(context.Background(), util.FullPath("/buckets/video/expired.mp4"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("expired entry lookup error = %v, want %v", err, filer_pb.ErrNotFound)
	}

	if _, ok := store.entries["/buckets/video/expired.mp4"]; ok {
		t.Fatalf("expired entry should be deleted from metadata store")
	}
}

func TestObjectTransactionPutPreservesExplicitTTL(t *testing.T) {
	fs, store := newTxnTestServer(nil)

	mustSetObjectTxnStorageConf(t, fs, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/video/",
		Ttl:            "1h",
	})

	now := time.Unix(1700000000, 0)

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/video/lifecycle.mp4",
		Mutations: []*filer_pb.ObjectMutation{
			{
				Type:      filer_pb.ObjectMutation_PUT,
				Directory: "/buckets/video",
				Entry: &filer_pb.Entry{
					Name: "lifecycle.mp4",
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   now.Unix(),
						Mtime:    now.Unix(),
						FileMode: 0644,
						FileSize: 123,
						TtlSec:   7200,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ObjectTransaction transport error: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("ObjectTransaction response error: %q", resp.Error)
	}

	entry := store.entries["/buckets/video/lifecycle.mp4"]
	if entry == nil {
		t.Fatalf("PUT should create /buckets/video/lifecycle.mp4")
	}
	if got := entry.TtlSec; got != 7200 {
		t.Fatalf("TtlSec = %d, want explicit TTL 7200 to win over fs.configure", got)
	}
}

func TestObjectTransactionPutClearsTTLForRemoteEntry(t *testing.T) {
	fs, store := newTxnTestServer(nil)

	mustSetObjectTxnStorageConf(t, fs, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/video/",
		Ttl:            "1h",
	})

	now := time.Unix(1700000000, 0)

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/video/remote",
		Mutations: []*filer_pb.ObjectMutation{
			{
				Type:      filer_pb.ObjectMutation_PUT,
				Directory: "/buckets/video",
				Entry: &filer_pb.Entry{
					Name: "remote",
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   now.Unix(),
						Mtime:    now.Unix(),
						FileMode: 0644,
						FileSize: 123,
						TtlSec:   7200,
					},
					RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 123},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ObjectTransaction transport error: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("ObjectTransaction response error: %q", resp.Error)
	}

	entry := store.entries["/buckets/video/remote"]
	if entry == nil {
		t.Fatalf("remote entry should be created")
	}
	if got := entry.TtlSec; got != 0 {
		t.Fatalf("remote entry TtlSec = %d, want 0", got)
	}
}

func TestObjectTransactionPutFailsOnReadOnlyStorageConfig(t *testing.T) {
	// An explicit TTL must not bypass the read-only rule.
	for _, ttlSec := range []int32{0, 7200} {
		fs, store := newTxnTestServer(nil)

		mustSetObjectTxnStorageConf(t, fs, &filer_pb.FilerConf_PathConf{
			LocationPrefix: "/buckets/video/",
			ReadOnly:       true,
		})

		now := time.Unix(1700000000, 0)

		resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
			LockKey: "/buckets/video/readonly.mp4",
			Mutations: []*filer_pb.ObjectMutation{
				{
					Type:      filer_pb.ObjectMutation_PUT,
					Directory: "/buckets/video",
					Entry: &filer_pb.Entry{
						Name: "readonly.mp4",
						Attributes: &filer_pb.FuseAttributes{
							Crtime:   now.Unix(),
							Mtime:    now.Unix(),
							FileMode: 0644,
							FileSize: 123,
							TtlSec:   ttlSec,
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("TtlSec=%d: ObjectTransaction transport error: %v", ttlSec, err)
		}
		if resp.Error == "" {
			t.Fatalf("TtlSec=%d: ObjectTransaction should report read-only storage config error", ttlSec)
		}

		if _, ok := store.entries["/buckets/video/readonly.mp4"]; ok {
			t.Fatalf("TtlSec=%d: read-only storage config must not create the entry", ttlSec)
		}
	}
}
