// +build 386 arm

package tikv

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &TikvStore{})
}

type TikvStore struct {
}

func (store *TikvStore) GetName() string {
	return "tikv"
}

func (store *TikvStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) initialize(pdAddr string) (err error) {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return nil, fmt.Errorf("not implemented for 32 bit computers")
}
func (store *TikvStore) CommitTransaction(ctx context.Context) error {
	return fmt.Errorf("not implemented for 32 bit computers")
}
func (store *TikvStore) RollbackTransaction(ctx context.Context) error {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) FindEntry(ctx context.Context, fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	return nil, fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) DeleteEntry(ctx context.Context, fullpath filer2.FullPath) (err error) {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) DeleteFolderChildren(ctx context.Context, fullpath filer2.FullPath) (err error) {
	return fmt.Errorf("not implemented for 32 bit computers")
}

func (store *TikvStore) ListDirectoryEntries(ctx context.Context, fullpath filer2.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {
	return nil, fmt.Errorf("not implemented for 32 bit computers")
}
