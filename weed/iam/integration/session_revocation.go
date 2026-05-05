package integration

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// RevocationEntry is one row in the revocation list. ExpiresAt is set to the
// session's natural expiration so the store can garbage-collect entries that
// can no longer be reused.
type RevocationEntry struct {
	JTI       string    `json:"jti"`
	RevokedAt time.Time `json:"revokedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	Reason    string    `json:"reason,omitempty"`
}

// SessionRevocationStore is the per-deployment blocklist of revoked sessions.
// Implementations must be safe for concurrent use; the IsRevoked path is hot
// (checked on every signed request) and must not block on slow IO.
type SessionRevocationStore interface {
	Revoke(ctx context.Context, filerAddress string, entry *RevocationEntry) error
	IsRevoked(ctx context.Context, filerAddress string, jti string) (bool, error)
	Purge(ctx context.Context, filerAddress string, before time.Time) (int, error)
}

// MemorySessionRevocationStore keeps the blocklist in process memory. Suitable
// for single-node deployments and tests; for HA, swap in the filer-backed
// implementation so revocations propagate across `weed` instances.
type MemorySessionRevocationStore struct {
	mu      sync.RWMutex
	entries map[string]*RevocationEntry
}

// NewMemorySessionRevocationStore returns an empty in-memory blocklist.
func NewMemorySessionRevocationStore() *MemorySessionRevocationStore {
	return &MemorySessionRevocationStore{entries: make(map[string]*RevocationEntry)}
}

func (m *MemorySessionRevocationStore) Revoke(ctx context.Context, _ string, entry *RevocationEntry) error {
	if entry == nil || entry.JTI == "" {
		return fmt.Errorf("entry with JTI is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *entry
	if cp.RevokedAt.IsZero() {
		cp.RevokedAt = time.Now().UTC()
	}
	m.entries[entry.JTI] = &cp
	return nil
}

func (m *MemorySessionRevocationStore) IsRevoked(ctx context.Context, _ string, jti string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.entries[jti]
	return ok, nil
}

func (m *MemorySessionRevocationStore) Purge(ctx context.Context, _ string, before time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for k, v := range m.entries {
		if !v.ExpiresAt.IsZero() && v.ExpiresAt.Before(before) {
			delete(m.entries, k)
			count++
		}
	}
	return count, nil
}

// FilerSessionRevocationStore persists entries under a filer directory.
// Files are named after the JTI so IsRevoked is a single LookupDirectoryEntry
// call — O(1) on a name-indexed filer. Purge enumerates the directory; that
// cost is operator-controlled (cron-driven) so the hot path stays cheap.
type FilerSessionRevocationStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
}

// NewFilerSessionRevocationStore returns a filer-backed blocklist store.
// Default basePath `/etc/iam/revoked-sessions` aligns with the other IAM
// directories and is safe to back up alongside roles + providers.
func NewFilerSessionRevocationStore(config map[string]interface{}, filerAddressProvider func() string) *FilerSessionRevocationStore {
	store := &FilerSessionRevocationStore{
		basePath:             "/etc/iam/revoked-sessions",
		filerAddressProvider: filerAddressProvider,
	}
	if config != nil {
		if bp, ok := config["basePath"].(string); ok && bp != "" {
			store.basePath = strings.TrimSuffix(bp, "/")
		}
	}
	glog.V(2).Infof("Initialized FilerSessionRevocationStore with basePath %s", store.basePath)
	return store
}

func (f *FilerSessionRevocationStore) resolveFilerAddress(filerAddress string) string {
	if filerAddress != "" {
		return filerAddress
	}
	if f.filerAddressProvider != nil {
		return f.filerAddressProvider()
	}
	return ""
}

// fileName hashes the JTI before using it as a filename. RevokeSession is
// an exported API that accepts an arbitrary string; even though every
// SeaweedFS-issued session id is a safe random token, an external caller
// could pass "../../etc/passwd" or similar. Hashing produces a fixed-width
// hex name that's both filesystem-safe and stable, so the lookup-on-revoke
// path still finds the right entry.
func (f *FilerSessionRevocationStore) fileName(jti string) string {
	sum := sha1.Sum([]byte(jti))
	return hex.EncodeToString(sum[:]) + ".json"
}

func (f *FilerSessionRevocationStore) Revoke(ctx context.Context, filerAddress string, entry *RevocationEntry) error {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return fmt.Errorf("filer address is required")
	}
	if entry == nil || entry.JTI == "" {
		return fmt.Errorf("entry with JTI is required")
	}
	if entry.RevokedAt.IsZero() {
		entry.RevokedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal revocation entry: %v", err)
	}
	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: f.basePath,
			Entry: &filer_pb.Entry{
				Name:        f.fileName(entry.JTI),
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0o600),
				},
				Content: data,
			},
		})
		if err != nil {
			return fmt.Errorf("revoke %s: %v", entry.JTI, err)
		}
		return nil
	})
}

func (f *FilerSessionRevocationStore) IsRevoked(ctx context.Context, filerAddress string, jti string) (bool, error) {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return false, fmt.Errorf("filer address is required")
	}
	revoked := false
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.fileName(jti),
		})
		if err != nil {
			if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such") {
				return nil
			}
			return err
		}
		revoked = resp.Entry != nil
		return nil
	})
	return revoked, err
}

func (f *FilerSessionRevocationStore) Purge(ctx context.Context, filerAddress string, before time.Time) (int, error) {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return 0, fmt.Errorf("filer address is required")
	}
	count := 0
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		// Stream-paginate the directory: ListEntriesRequest has no built-in
		// "page until done" semantics, so we use StartFromFileName to walk
		// the directory in chunks and avoid the previous hardcoded 10k cap.
		const pageSize = 1000
		startFrom := ""
		for {
			stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
				Directory:          f.basePath,
				Limit:              pageSize,
				StartFromFileName:  startFrom,
				InclusiveStartFrom: false,
			})
			if err != nil {
				return fmt.Errorf("list revocation entries: %w", err)
			}
			lastName := ""
			pageCount := 0
			for {
				resp, recvErr := stream.Recv()
				if recvErr != nil {
					if errors.Is(recvErr, io.EOF) {
						break
					}
					return fmt.Errorf("recv revocation entry: %w", recvErr)
				}
				if resp.Entry == nil || resp.Entry.IsDirectory {
					continue
				}
				lastName = resp.Entry.Name
				pageCount++
				var entry RevocationEntry
				if err := json.Unmarshal(resp.Entry.Content, &entry); err != nil {
					continue
				}
				if entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(before) {
					continue
				}
				if _, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
					Directory:    f.basePath,
					Name:         resp.Entry.Name,
					IsDeleteData: true,
				}); err == nil {
					count++
				}
			}
			if pageCount < pageSize {
				return nil
			}
			startFrom = lastName
		}
	})
	return count, err
}

func (f *FilerSessionRevocationStore) withFilerClient(filerAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), f.grpcDialOption, fn)
}
