package command

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type metaBackupTestStore struct {
	kvGetErr error
}

var _ filer.FilerStore = (*metaBackupTestStore)(nil)

func (s *metaBackupTestStore) GetName() string                             { return "meta_backup_test" }
func (s *metaBackupTestStore) Initialize(util.Configuration, string) error { return nil }
func (s *metaBackupTestStore) InsertEntry(context.Context, *filer.Entry) error {
	return nil
}
func (s *metaBackupTestStore) UpdateEntry(context.Context, *filer.Entry) error {
	return nil
}
func (s *metaBackupTestStore) FindEntry(context.Context, util.FullPath) (*filer.Entry, error) {
	return nil, filer.ErrKvNotFound
}
func (s *metaBackupTestStore) DeleteEntry(context.Context, util.FullPath) error {
	return nil
}
func (s *metaBackupTestStore) DeleteFolderChildren(context.Context, util.FullPath) error {
	return nil
}
func (s *metaBackupTestStore) ListDirectoryEntries(context.Context, util.FullPath, string, bool, int64, filer.ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *metaBackupTestStore) ListDirectoryPrefixedEntries(context.Context, util.FullPath, string, bool, int64, string, filer.ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *metaBackupTestStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *metaBackupTestStore) CommitTransaction(context.Context) error   { return nil }
func (s *metaBackupTestStore) RollbackTransaction(context.Context) error { return nil }
func (s *metaBackupTestStore) KvPut(context.Context, []byte, []byte) error {
	return nil
}
func (s *metaBackupTestStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, s.kvGetErr
}
func (s *metaBackupTestStore) KvDelete(context.Context, []byte) error { return nil }
func (s *metaBackupTestStore) Shutdown()                              {}

func TestStreamMetadataBackupReturnsOffsetError(t *testing.T) {
	offsetErr := errors.New("kv unavailable")
	metaBackup := &FilerMetaBackupOptions{
		store: &metaBackupTestStore{kvGetErr: offsetErr},
	}

	err := metaBackup.streamMetadataBackup()

	if !errors.Is(err, offsetErr) {
		t.Fatalf("streamMetadataBackup error = %v, want wrapping %v", err, offsetErr)
	}
	if !strings.Contains(err.Error(), "get metadata backup offset") {
		t.Fatalf("streamMetadataBackup error = %q, want offset context", err.Error())
	}
}
