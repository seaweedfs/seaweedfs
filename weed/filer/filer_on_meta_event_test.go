package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type bucketTrackingStore struct {
	created []string
	deleted []string
}

func (s *bucketTrackingStore) GetName() string { return "bucket-tracking" }
func (s *bucketTrackingStore) Initialize(configuration util.Configuration, prefix string) error {
	return nil
}
func (s *bucketTrackingStore) InsertEntry(context.Context, *Entry) error { return nil }
func (s *bucketTrackingStore) UpdateEntry(context.Context, *Entry) error { return nil }
func (s *bucketTrackingStore) FindEntry(context.Context, util.FullPath) (*Entry, error) {
	return nil, filer_pb.ErrNotFound
}
func (s *bucketTrackingStore) DeleteEntry(context.Context, util.FullPath) error          { return nil }
func (s *bucketTrackingStore) DeleteFolderChildren(context.Context, util.FullPath) error { return nil }
func (s *bucketTrackingStore) ListDirectoryEntries(context.Context, util.FullPath, string, bool, int64, ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *bucketTrackingStore) ListDirectoryPrefixedEntries(context.Context, util.FullPath, string, bool, int64, string, ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *bucketTrackingStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *bucketTrackingStore) CommitTransaction(context.Context) error     { return nil }
func (s *bucketTrackingStore) RollbackTransaction(context.Context) error   { return nil }
func (s *bucketTrackingStore) KvPut(context.Context, []byte, []byte) error { return nil }
func (s *bucketTrackingStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, ErrKvNotFound
}
func (s *bucketTrackingStore) KvDelete(context.Context, []byte) error { return nil }
func (s *bucketTrackingStore) Shutdown()                              {}
func (s *bucketTrackingStore) OnBucketCreation(bucket string)         { s.created = append(s.created, bucket) }
func (s *bucketTrackingStore) OnBucketDeletion(bucket string)         { s.deleted = append(s.deleted, bucket) }
func (s *bucketTrackingStore) CanDropWholeBucket() bool               { return false }

func TestOnBucketEventsRenameIntoBucketsRootCreatesBucket(t *testing.T) {
	store := &bucketTrackingStore{}
	f := &Filer{
		DirBucketsPath: "/buckets",
		Store:          NewFilerStoreWrapper(store),
	}

	f.onBucketEvents(&filer_pb.SubscribeMetadataResponse{
		Directory: "/tmp",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "migrated", IsDirectory: true},
			NewEntry:      &filer_pb.Entry{Name: "migrated", IsDirectory: true},
			NewParentPath: "/buckets",
		},
	})

	if len(store.created) != 1 || store.created[0] != "migrated" {
		t.Fatalf("created buckets = %v, want [migrated]", store.created)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("deleted buckets = %v, want []", store.deleted)
	}
}
