package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// OIDCProviderAuditEventType enumerates the lifecycle events emitted when an
// IAM-managed OIDC provider record is mutated. Use-events (every successful
// token validation) are intentionally not emitted by the lifecycle path:
// they're too hot to stream into a filer-backed sink without spikes. A
// future PR can add a sampled-use sink behind an explicit opt-in flag.
type OIDCProviderAuditEventType string

const (
	OIDCAuditEventCreated         OIDCProviderAuditEventType = "Create"
	OIDCAuditEventDeleted         OIDCProviderAuditEventType = "Delete"
	OIDCAuditEventClientIDAdded   OIDCProviderAuditEventType = "AddClientID"
	OIDCAuditEventClientIDRemoved OIDCProviderAuditEventType = "RemoveClientID"
	OIDCAuditEventThumbprintsSet  OIDCProviderAuditEventType = "UpdateThumbprints"
	OIDCAuditEventTagsAdded       OIDCProviderAuditEventType = "Tag"
	OIDCAuditEventTagsRemoved     OIDCProviderAuditEventType = "Untag"
)

// OIDCProviderAuditEvent is the payload emitted for each lifecycle event.
type OIDCProviderAuditEvent struct {
	Type      OIDCProviderAuditEventType `json:"type"`
	ARN       string                     `json:"arn"`
	URL       string                     `json:"url,omitempty"`
	Detail    map[string]string          `json:"detail,omitempty"`
	OccurredAt time.Time                 `json:"occurredAt"`
}

// OIDCProviderAuditSink consumes lifecycle events. Implementations must be
// safe for concurrent use; emit happens with the IAM manager's mutation lock
// held, so a slow sink slows the IAM API.
type OIDCProviderAuditSink interface {
	Emit(ctx context.Context, event *OIDCProviderAuditEvent) error
}

// GlogAuditSink writes one structured log line per event. Always-on default
// when an explicit sink isn't configured — events still appear in stdout/the
// log aggregator, just not in a queryable record.
type GlogAuditSink struct{}

func (GlogAuditSink) Emit(_ context.Context, event *OIDCProviderAuditEvent) error {
	if event == nil {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		glog.V(0).Infof("oidc-audit: %s arn=%s err-marshal=%v", event.Type, event.ARN, err)
		return nil
	}
	glog.V(0).Infof("oidc-audit: %s", string(data))
	return nil
}

// MemoryAuditSink keeps events in process memory for tests and short-lived
// inspection. Not durable; drop entries by reading from Events() and
// discarding.
type MemoryAuditSink struct {
	mu     sync.Mutex
	events []*OIDCProviderAuditEvent
}

func NewMemoryAuditSink() *MemoryAuditSink {
	return &MemoryAuditSink{}
}

func (m *MemoryAuditSink) Emit(_ context.Context, event *OIDCProviderAuditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *event
	m.events = append(m.events, &cp)
	return nil
}

// Events returns a copy of the captured event log so tests can assert.
func (m *MemoryAuditSink) Events() []*OIDCProviderAuditEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*OIDCProviderAuditEvent, len(m.events))
	copy(out, m.events)
	return out
}

// FilerAuditSink appends event records as separate files under a filer
// directory. Each event is its own file so concurrent writers don't conflict;
// the filename is `<unixnano>-<type>-<arnhash>.json`. For higher volumes,
// switch to an append-only journal — the contract is just `Emit`.
type FilerAuditSink struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
}

// NewFilerAuditSink returns a filer-backed sink. Default basePath
// `/etc/iam/audit/oidc-providers` keeps audit records out of the active
// IAM data directories.
func NewFilerAuditSink(config map[string]interface{}, filerAddressProvider func() string) *FilerAuditSink {
	sink := &FilerAuditSink{
		basePath:             "/etc/iam/audit/oidc-providers",
		filerAddressProvider: filerAddressProvider,
	}
	if config != nil {
		if bp, ok := config["basePath"].(string); ok && bp != "" {
			sink.basePath = strings.TrimSuffix(bp, "/")
		}
	}
	return sink
}

func (f *FilerAuditSink) resolveFilerAddress() string {
	if f.filerAddressProvider != nil {
		return f.filerAddressProvider()
	}
	return ""
}

func (f *FilerAuditSink) Emit(ctx context.Context, event *OIDCProviderAuditEvent) error {
	addr := f.resolveFilerAddress()
	if addr == "" {
		return fmt.Errorf("filer address not available")
	}
	if event == nil {
		return nil
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal audit event: %v", err)
	}
	name := fmt.Sprintf("%d-%s.json", event.OccurredAt.UnixNano(), event.Type)
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(addr), f.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: f.basePath,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0o600),
				},
				Content: data,
			},
		})
		return err
	})
}
