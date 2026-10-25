package dispatcher

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// FilerStore is the small subset of filer-client operations the
// daily-replay cursor persister needs (dailyrun.FilerCursorPersister).
// Tests inject an in-memory fake.
type FilerStore interface {
	Read(ctx context.Context, dir, name string) ([]byte, error)
	Save(ctx context.Context, dir, name string, content []byte) error
}

// NewFilerStoreClient adapts a SeaweedFilerClient into a FilerStore.
func NewFilerStoreClient(client filer_pb.SeaweedFilerClient) FilerStore {
	return &filerStoreClient{client: client}
}

type filerStoreClient struct {
	client filer_pb.SeaweedFilerClient
}

func (s *filerStoreClient) Read(ctx context.Context, dir, name string) ([]byte, error) {
	return filer.ReadInsideFiler(ctx, s.client, dir, name)
}

func (s *filerStoreClient) Save(ctx context.Context, dir, name string, content []byte) error {
	return filer.SaveInsideFiler(ctx, s.client, dir, name, content)
}
