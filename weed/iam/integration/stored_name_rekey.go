package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// storedNameRekey moves records to the name this process derives for their
// key. The names hash the key with util.NewSha1, which changes under
// GODEBUG=fips140=only, so records written in the other mode are otherwise
// missed by lookups and deletes. It runs once per filer address.
type storedNameRekey struct {
	dir   string
	keyOf func(content []byte) string
	name  func(key string) string

	mu   sync.Mutex
	done sync.Map
}

func (r *storedNameRekey) ensure(ctx context.Context, filerAddress string, client filer_pb.SeaweedFilerClient) error {
	if _, ok := r.done.Load(filerAddress); ok {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.done.Load(filerAddress); ok {
		return nil
	}
	if err := r.run(ctx, client); err != nil {
		return fmt.Errorf("rename records in %s: %w", r.dir, err)
	}
	r.done.Store(filerAddress, true)
	return nil
}

func (r *storedNameRekey) run(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
	var misnamed []*filer_pb.Entry
	err := listStoredEntries(ctx, client, r.dir, func(entry *filer_pb.Entry) {
		key := r.keyOf(entry.Content)
		if key != "" && entry.Name != r.name(key) {
			misnamed = append(misnamed, entry)
		}
	})
	if err != nil {
		return err
	}
	for _, entry := range misnamed {
		if err := r.move(ctx, client, entry); err != nil {
			return err
		}
	}
	return nil
}

// move renames entry if it is unchanged, under the lock the stores' conditional
// writes of the new name take, so a record another process already moved and
// then deleted is not written back. A record already under the new name wins.
func (r *storedNameRekey) move(ctx context.Context, client filer_pb.SeaweedFilerClient, listed *filer_pb.Entry) error {
	found, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: r.dir, Name: listed.Name})
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lookup %s: %w", listed.Name, err)
	}
	entry := found.Entry
	name := r.name(r.keyOf(entry.Content))
	if name == entry.Name {
		return nil
	}
	remove := &filer_pb.ObjectMutation{Type: filer_pb.ObjectMutation_DELETE, Directory: r.dir, Name: entry.Name, IsDeleteData: true}
	put := &filer_pb.ObjectMutation{
		Type:      filer_pb.ObjectMutation_PUT,
		Directory: r.dir,
		Entry:     &filer_pb.Entry{Name: name, Attributes: entry.Attributes, Content: entry.Content},
		Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}}},
	}
	resp, err := r.transact(ctx, client, name, entry, put, remove)
	if err == nil && resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED && resp.FailedMutation != nil && *resp.FailedMutation == 0 {
		resp, err = r.transact(ctx, client, name, entry, remove)
	}
	if err != nil {
		return fmt.Errorf("rename %s to %s: %w", entry.Name, name, err)
	}
	switch {
	case resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED:
		glog.V(1).Infof("%s/%s changed before its rename to %s; leaving it", r.dir, entry.Name, name)
	case resp.Error != "":
		return fmt.Errorf("rename %s to %s: %s", entry.Name, name, resp.Error)
	default:
		glog.V(0).Infof("renamed %s/%s to %s", r.dir, entry.Name, name)
	}
	return nil
}

func (r *storedNameRekey) transact(ctx context.Context, client filer_pb.SeaweedFilerClient, name string, source *filer_pb.Entry, mutations ...*filer_pb.ObjectMutation) (*filer_pb.ObjectTransactionResponse, error) {
	target := r.dir + "/" + name
	resp, err := client.ObjectTransaction(ctx, &filer_pb.ObjectTransactionRequest{
		LockKey:      target,
		RequestId:    uuid.NewString(),
		RouteKey:     s3_constants.ObjectWriteRouteKeyPrefix + target,
		ConditionKey: r.dir + "/" + source.Name,
		Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
			Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: source,
		}}},
		Mutations: mutations,
	})
	if status.Code(err) == codes.FailedPrecondition {
		return &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
	}
	return resp, err
}

func listStoredEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string, fn func(*filer_pb.Entry)) error {
	const pageSize = 1000
	startFrom := ""
	for {
		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:         dir,
			Limit:             pageSize,
			StartFromFileName: startFrom,
		})
		if err != nil {
			return fmt.Errorf("list %s: %w", dir, err)
		}
		pageCount := 0
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("list %s: %w", dir, err)
			}
			if resp.Entry == nil {
				continue
			}
			pageCount++
			startFrom = resp.Entry.Name
			if !resp.Entry.IsDirectory {
				fn(resp.Entry)
			}
		}
		if pageCount < pageSize {
			return nil
		}
	}
}
