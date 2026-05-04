package integration

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// Sentinel errors returned by the IAM manager and OIDCProviderStore. Callers
// in the s3api layer use errors.Is rather than substring-matching the
// formatted message, so the message can be edited freely without changing
// the IAM error code surfaced to the API caller.
var (
	ErrOIDCProviderNotFound      = errors.New("OIDC provider not found")
	ErrOIDCProviderAlreadyExists = errors.New("OIDC provider already exists")
)

// OIDCProviderRecord is the persisted, IAM-managed view of an OIDC identity
// provider. It is the source of truth consulted at AssumeRoleWithWebIdentity
// time. Static configuration entries are loaded into this same store at
// startup so the resolution path is uniform.
type OIDCProviderRecord struct {
	// AccountID scopes the record. Empty means "global" — usable from any
	// account. Non-empty records are only resolvable when the assuming
	// caller's RoleArn lives in the same account. Phase 2a leaves this
	// field unenforced; Phase 3c lights up the cross-account check.
	AccountID string `json:"accountId,omitempty"`

	// ARN is the canonical IAM identifier for the provider:
	// arn:aws:iam::<account>:oidc-provider/<host>/<path>.
	ARN string `json:"arn"`

	// URL is the issuer URL (no trailing slash), e.g.
	// https://token.actions.githubusercontent.com.
	URL string `json:"url"`

	// ClientIDs are the audiences AWS calls "client IDs". A token's `aud` or
	// `azp` must match one of these for the token to be accepted.
	ClientIDs []string `json:"clientIds,omitempty"`

	// Thumbprints are SHA-1 hex digests of the IDP's TLS certificate, matching
	// the AWS-compatible thumbprint algorithm. Used at JWKS-fetch time when
	// non-empty; an empty list means "trust the system root store".
	Thumbprints []string `json:"thumbprints,omitempty"`

	// AllowedPrincipalTagKeys is the per-provider ABAC allowlist for the
	// `https://aws.amazon.com/tags/principal_tags` claim namespace. Empty
	// means no tags are surfaced from this provider.
	AllowedPrincipalTagKeys []string `json:"allowedPrincipalTagKeys,omitempty"`

	// PolicyClaim, when non-empty, enables claim-based policy mode for this
	// provider: tokens whose RoleArn matches the configured sentinel will
	// derive their effective policies from this JWT claim.
	PolicyClaim string `json:"policyClaim,omitempty"`

	// Tags is the AWS-style tag set attached to the IAM resource itself
	// (audit/inventory metadata, not propagated into sessions).
	Tags map[string]string `json:"tags,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// OIDCProviderStore stores OIDCProviderRecord entries. Implementations are
// expected to be safe for concurrent use.
type OIDCProviderStore interface {
	StoreProvider(ctx context.Context, filerAddress string, record *OIDCProviderRecord) error
	GetProviderByARN(ctx context.Context, filerAddress string, arn string) (*OIDCProviderRecord, error)
	GetProviderByIssuer(ctx context.Context, filerAddress string, issuer string) (*OIDCProviderRecord, error)
	GetProviderByIssuerAndAccount(ctx context.Context, filerAddress string, issuer, accountID string) (*OIDCProviderRecord, error)
	ListProviders(ctx context.Context, filerAddress string) ([]*OIDCProviderRecord, error)
	DeleteProvider(ctx context.Context, filerAddress string, arn string) error
}

// MemoryOIDCProviderStore is a process-local store, suitable for tests and
// single-node deployments. It also acts as the in-memory cache hydrated from
// static config at boot.
type MemoryOIDCProviderStore struct {
	mu        sync.RWMutex
	providers map[string]*OIDCProviderRecord // keyed by ARN
}

// NewMemoryOIDCProviderStore creates an empty in-memory store.
func NewMemoryOIDCProviderStore() *MemoryOIDCProviderStore {
	return &MemoryOIDCProviderStore{providers: make(map[string]*OIDCProviderRecord)}
}

// StoreProvider replaces any existing record with the same ARN.
func (m *MemoryOIDCProviderStore) StoreProvider(ctx context.Context, _ string, record *OIDCProviderRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if record.ARN == "" {
		return fmt.Errorf("record.ARN is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[record.ARN] = copyOIDCProviderRecord(record)
	return nil
}

// GetProviderByARN returns the record with the given ARN, or an error if absent.
func (m *MemoryOIDCProviderStore) GetProviderByARN(ctx context.Context, _ string, arn string) (*OIDCProviderRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.providers[arn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrOIDCProviderNotFound, arn)
	}
	return copyOIDCProviderRecord(rec), nil
}

// GetProviderByIssuer scans for the first record whose URL matches `issuer`.
// Comparison strips trailing slashes and is case-insensitive on host.
func (m *MemoryOIDCProviderStore) GetProviderByIssuer(ctx context.Context, _ string, issuer string) (*OIDCProviderRecord, error) {
	want := normalizeIssuer(issuer)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, rec := range m.providers {
		if normalizeIssuer(rec.URL) == want {
			return copyOIDCProviderRecord(rec), nil
		}
	}
	return nil, fmt.Errorf("no OIDC provider registered for issuer: %s", issuer)
}

// GetProviderByIssuerAndAccount returns the record matching `issuer` whose
// AccountID is empty (global) or equals `accountID`. AWS-style account
// scoping: cross-account references must go through a shared/global
// provider, which is why an empty AccountID is treated as a wildcard.
//
// `accountID` may itself be empty when the caller has no specific account
// context (e.g. claim-based mode); in that case any registered provider
// matching the issuer is acceptable.
func (m *MemoryOIDCProviderStore) GetProviderByIssuerAndAccount(ctx context.Context, _ string, issuer, accountID string) (*OIDCProviderRecord, error) {
	want := normalizeIssuer(issuer)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, rec := range m.providers {
		if normalizeIssuer(rec.URL) != want {
			continue
		}
		if rec.AccountID == "" || accountID == "" || rec.AccountID == accountID {
			return copyOIDCProviderRecord(rec), nil
		}
	}
	return nil, fmt.Errorf("no OIDC provider registered for issuer %s in account %s", issuer, accountID)
}

// ListProviders returns every record sorted by ARN for stable output.
func (m *MemoryOIDCProviderStore) ListProviders(ctx context.Context, _ string) ([]*OIDCProviderRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*OIDCProviderRecord, 0, len(m.providers))
	for _, rec := range m.providers {
		out = append(out, copyOIDCProviderRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ARN < out[j].ARN })
	return out, nil
}

// DeleteProvider removes the record. Idempotent — deleting a missing ARN is success.
func (m *MemoryOIDCProviderStore) DeleteProvider(ctx context.Context, _ string, arn string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.providers, arn)
	return nil
}

// FilerOIDCProviderStore persists records as JSON files in a filer directory,
// mirroring FilerRoleStore.
type FilerOIDCProviderStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
}

// NewFilerOIDCProviderStore returns a filer-backed store. Default path
// `/etc/iam/oidc-providers` aligns with the existing roles directory.
func NewFilerOIDCProviderStore(config map[string]interface{}, filerAddressProvider func() string) *FilerOIDCProviderStore {
	store := &FilerOIDCProviderStore{
		basePath:             "/etc/iam/oidc-providers",
		filerAddressProvider: filerAddressProvider,
	}
	if config != nil {
		if bp, ok := config["basePath"].(string); ok && bp != "" {
			store.basePath = strings.TrimSuffix(bp, "/")
		}
	}
	glog.V(2).Infof("Initialized FilerOIDCProviderStore with basePath %s", store.basePath)
	return store
}

func (f *FilerOIDCProviderStore) resolveFilerAddress(filerAddress string) string {
	if filerAddress != "" {
		return filerAddress
	}
	if f.filerAddressProvider != nil {
		return f.filerAddressProvider()
	}
	return ""
}

func (f *FilerOIDCProviderStore) fileName(arn string) string {
	// Hash the ARN to a fixed-width filename so we can store any character set
	// (including the URL fragments AWS lets through) without filer-path issues.
	h := sha1.Sum([]byte(arn))
	return hex.EncodeToString(h[:]) + ".json"
}

// StoreProvider persists `record` as JSON.
func (f *FilerOIDCProviderStore) StoreProvider(ctx context.Context, filerAddress string, record *OIDCProviderRecord) error {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return fmt.Errorf("filer address is required")
	}
	if record == nil || record.ARN == "" {
		return fmt.Errorf("record with ARN is required")
	}

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal OIDC provider: %v", err)
	}

	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: f.basePath,
			Entry: &filer_pb.Entry{
				Name:        f.fileName(record.ARN),
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
			return fmt.Errorf("store OIDC provider %s: %v", record.ARN, err)
		}
		return nil
	})
}

// GetProviderByARN looks up the record file by its ARN-derived filename.
func (f *FilerOIDCProviderStore) GetProviderByARN(ctx context.Context, filerAddress string, arn string) (*OIDCProviderRecord, error) {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required")
	}

	var data []byte
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.fileName(arn),
		})
		if err != nil {
			return fmt.Errorf("%w: %v", ErrOIDCProviderNotFound, err)
		}
		if resp.Entry == nil {
			return fmt.Errorf("OIDC provider not found: %s", arn)
		}
		data = resp.Entry.Content
		return nil
	})
	if err != nil {
		return nil, err
	}

	var rec OIDCProviderRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal OIDC provider: %v", err)
	}
	return &rec, nil
}

// GetProviderByIssuer linearly scans the directory. Phase 2b adds an issuer
// index when write throughput grows; for read-only this is good enough.
func (f *FilerOIDCProviderStore) GetProviderByIssuer(ctx context.Context, filerAddress string, issuer string) (*OIDCProviderRecord, error) {
	records, err := f.ListProviders(ctx, filerAddress)
	if err != nil {
		return nil, err
	}
	want := normalizeIssuer(issuer)
	for _, rec := range records {
		if normalizeIssuer(rec.URL) == want {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("no OIDC provider registered for issuer: %s", issuer)
}

// GetProviderByIssuerAndAccount filters by AccountID on top of the issuer scan.
// See the MemoryOIDCProviderStore equivalent for semantics.
func (f *FilerOIDCProviderStore) GetProviderByIssuerAndAccount(ctx context.Context, filerAddress string, issuer, accountID string) (*OIDCProviderRecord, error) {
	records, err := f.ListProviders(ctx, filerAddress)
	if err != nil {
		return nil, err
	}
	want := normalizeIssuer(issuer)
	for _, rec := range records {
		if normalizeIssuer(rec.URL) != want {
			continue
		}
		if rec.AccountID == "" || accountID == "" || rec.AccountID == accountID {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("no OIDC provider registered for issuer %s in account %s", issuer, accountID)
}

// ListProviders enumerates every record in the directory.
func (f *FilerOIDCProviderStore) ListProviders(ctx context.Context, filerAddress string) ([]*OIDCProviderRecord, error) {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required")
	}

	var out []*OIDCProviderRecord
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		// Stream-paginate via StartFromFileName so deployments with more
		// than 1000 providers don't get a silently truncated list.
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
				return fmt.Errorf("list OIDC providers: %v", err)
			}
			lastName := ""
			pageCount := 0
			for {
				resp, recvErr := stream.Recv()
				if recvErr != nil {
					if errors.Is(recvErr, io.EOF) {
						break
					}
					return fmt.Errorf("recv OIDC provider entry: %w", recvErr)
				}
				if resp.Entry == nil || resp.Entry.IsDirectory {
					continue
				}
				lastName = resp.Entry.Name
				pageCount++
				if !strings.HasSuffix(resp.Entry.Name, ".json") {
					continue
				}
				var rec OIDCProviderRecord
				if err := json.Unmarshal(resp.Entry.Content, &rec); err != nil {
					glog.Warningf("skipping malformed OIDC provider record %s: %v", resp.Entry.Name, err)
					continue
				}
				out = append(out, &rec)
			}
			if pageCount < pageSize {
				break
			}
			startFrom = lastName
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ARN < out[j].ARN })
	return out, nil
}

// DeleteProvider removes the record. Missing ARN is treated as success.
func (f *FilerOIDCProviderStore) DeleteProvider(ctx context.Context, filerAddress string, arn string) error {
	filerAddress = f.resolveFilerAddress(filerAddress)
	if filerAddress == "" {
		return fmt.Errorf("filer address is required")
	}
	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory:    f.basePath,
			Name:         f.fileName(arn),
			IsDeleteData: true,
		})
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return nil
			}
			return fmt.Errorf("delete OIDC provider %s: %v", arn, err)
		}
		if resp.Error != "" && !strings.Contains(resp.Error, "not found") {
			return fmt.Errorf("delete OIDC provider %s: %s", arn, resp.Error)
		}
		return nil
	})
}

func (f *FilerOIDCProviderStore) withFilerClient(filerAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), f.grpcDialOption, fn)
}

// DeriveOIDCProviderARN turns an issuer URL into the canonical IAM ARN AWS uses.
// Empty `accountID` produces a global-style ARN.
//
// Examples:
//
//	https://accounts.google.com -> arn:aws:iam::<acct>:oidc-provider/accounts.google.com
//	https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED ->
//	    arn:aws:iam::<acct>:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED
func DeriveOIDCProviderARN(accountID, issuerURL string) (string, error) {
	if issuerURL == "" {
		return "", fmt.Errorf("issuer URL is required")
	}
	u, err := url.Parse(issuerURL)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid issuer URL: %s", issuerURL)
	}
	host := strings.ToLower(u.Host)
	resource := host + strings.TrimSuffix(u.Path, "/")
	return fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", accountID, resource), nil
}

// normalizeIssuer compares-friendly form: lowercased host, no trailing slash.
func normalizeIssuer(issuer string) string {
	u, err := url.Parse(issuer)
	if err != nil || u.Host == "" {
		return strings.TrimSuffix(strings.ToLower(issuer), "/")
	}
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimSuffix(u.Path, "/")
	return u.String()
}

func copyOIDCProviderRecord(rec *OIDCProviderRecord) *OIDCProviderRecord {
	if rec == nil {
		return nil
	}
	cp := *rec
	if rec.ClientIDs != nil {
		cp.ClientIDs = append([]string(nil), rec.ClientIDs...)
	}
	if rec.Thumbprints != nil {
		cp.Thumbprints = append([]string(nil), rec.Thumbprints...)
	}
	if rec.AllowedPrincipalTagKeys != nil {
		cp.AllowedPrincipalTagKeys = append([]string(nil), rec.AllowedPrincipalTagKeys...)
	}
	if rec.Tags != nil {
		cp.Tags = make(map[string]string, len(rec.Tags))
		for k, v := range rec.Tags {
			cp.Tags[k] = v
		}
	}
	return &cp
}
