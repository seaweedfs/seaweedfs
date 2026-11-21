package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
		name := r.name(r.keyOf(entry.Content))
		resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: r.dir,
			Entry: &filer_pb.Entry{
				Name:       name,
				Attributes: entry.Attributes,
				Content:    entry.Content,
			},
			Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}}},
		})
		if !isEntryWriteConflict(resp, err) {
			if err != nil {
				return fmt.Errorf("create %s: %w", name, err)
			}
			if resp.Error != "" {
				return fmt.Errorf("create %s: %s", name, resp.Error)
			}
		}
		if err := filer_pb.DoRemove(ctx, client, r.dir, entry.Name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("remove %s: %w", entry.Name, err)
		}
		glog.V(0).Infof("renamed %s/%s to %s", r.dir, entry.Name, name)
	}
	return nil
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
