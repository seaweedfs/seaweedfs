package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

// FilerStore is the small subset of filer-client operations the persistence
// layer needs. The default implementation calls filer.ReadInsideFiler /
// filer.SaveInsideFiler; tests inject an in-memory fake.
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

// CursorDir is the filer directory holding per-shard cursor files.
const CursorDir = "/etc/s3/lifecycle/cursors"

// FilerPersister persists per-shard cursor maps to /etc/s3/lifecycle/cursors/
// as JSON. One file per shard keeps Save atomic — the filer writes the entry
// in a single mutation, so a crash mid-write doesn't leak partial state.
type FilerPersister struct {
	Store FilerStore
}

// cursorFile is the on-disk JSON shape. cursorFileEntry repeats the
// ActionKey fields explicitly so the format stays human-readable and stable
// against Go-side struct rearrangements.
type cursorFile struct {
	Version int                 `json:"version"`
	ShardID int                 `json:"shard_id"`
	Entries []cursorFileEntry   `json:"entries"`
}

type cursorFileEntry struct {
	Bucket     string `json:"bucket"`
	RuleHash   []byte `json:"rule_hash"` // base64 in JSON
	ActionKind int    `json:"action_kind"`
	TsNs       int64  `json:"ts_ns"`
}

const cursorFileVersion = 1

func cursorFileName(shardID int) string {
	return fmt.Sprintf("shard-%02d.json", shardID)
}

func (p *FilerPersister) Load(ctx context.Context, shardID int) (map[s3lifecycle.ActionKey]int64, error) {
	if p.Store == nil {
		return nil, errors.New("FilerPersister: nil Store")
	}
	content, err := p.Store.Read(ctx, CursorDir, cursorFileName(shardID))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return map[s3lifecycle.ActionKey]int64{}, nil
		}
		return nil, fmt.Errorf("cursor read shard=%d: %w", shardID, err)
	}
	if len(content) == 0 {
		return map[s3lifecycle.ActionKey]int64{}, nil
	}
	var cf cursorFile
	if err := json.Unmarshal(content, &cf); err != nil {
		return nil, fmt.Errorf("cursor decode shard=%d: %w", shardID, err)
	}
	out := make(map[s3lifecycle.ActionKey]int64, len(cf.Entries))
	for _, e := range cf.Entries {
		k := s3lifecycle.ActionKey{
			Bucket:     e.Bucket,
			ActionKind: s3lifecycle.ActionKind(e.ActionKind),
		}
		copy(k.RuleHash[:], e.RuleHash)
		out[k] = e.TsNs
	}
	return out, nil
}

func (p *FilerPersister) Save(ctx context.Context, shardID int, state map[s3lifecycle.ActionKey]int64) error {
	if p.Store == nil {
		return errors.New("FilerPersister: nil Store")
	}
	cf := cursorFile{Version: cursorFileVersion, ShardID: shardID}
	cf.Entries = make([]cursorFileEntry, 0, len(state))
	for k, v := range state {
		hash := k.RuleHash
		cf.Entries = append(cf.Entries, cursorFileEntry{
			Bucket:     k.Bucket,
			RuleHash:   hash[:],
			ActionKind: int(k.ActionKind),
			TsNs:       v,
		})
	}
	// Stable order so the on-disk file diffs cleanly across saves.
	sort.Slice(cf.Entries, func(i, j int) bool {
		a, b := cf.Entries[i], cf.Entries[j]
		if a.Bucket != b.Bucket {
			return a.Bucket < b.Bucket
		}
		if a.ActionKind != b.ActionKind {
			return a.ActionKind < b.ActionKind
		}
		return bytes.Compare(a.RuleHash, b.RuleHash) < 0
	})
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(cf); err != nil {
		return fmt.Errorf("cursor encode shard=%d: %w", shardID, err)
	}
	if err := p.Store.Save(ctx, CursorDir, cursorFileName(shardID), buf.Bytes()); err != nil {
		return fmt.Errorf("cursor save shard=%d: %w", shardID, err)
	}
	return nil
}

// Compile-time interface check.
var _ reader.Persister = (*FilerPersister)(nil)
