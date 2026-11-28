package filer_ttl

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ttlFiler lists like a filer on a store without native TTL: an expired
// entry is skipped and its row deleted when a listing reaches it.
type ttlFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	signature int32
	conf      []byte
	volumeUrl string
	mu        sync.Mutex
	dirs      map[string]map[string]*filer_pb.Entry
	expired   map[string]bool
}

func newTTLFiler(signature int32) *ttlFiler {
	return &ttlFiler{signature: signature, dirs: map[string]map[string]*filer_pb.Entry{}, expired: map[string]bool{}}
}

func (f *ttlFiler) add(fullPath string, isDirectory, expired bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, name := util.FullPath(fullPath).DirAndName()
	if f.dirs[dir] == nil {
		f.dirs[dir] = map[string]*filer_pb.Entry{}
	}
	f.dirs[dir][name] = &filer_pb.Entry{Name: name, IsDirectory: isDirectory}
	if expired {
		f.expired[fullPath] = true
	}
}

func (f *ttlFiler) rows() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, entries := range f.dirs {
		n += len(entries)
	}
	return n
}

func (f *ttlFiler) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{Signature: f.signature}, nil
}

func (f *ttlFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if req.Directory != filer.DirectoryEtcSeaweedFS || req.Name != filer.FilerConfName || f.conf == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	if f.volumeUrl != "" {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(f.conf))},
			Chunks:     []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: uint64(len(f.conf))}},
		}}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: req.Name, Content: f.conf}}, nil
}

func (f *ttlFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	resp := &filer_pb.LookupVolumeResponse{LocationsMap: map[string]*filer_pb.Locations{}}
	for _, vid := range req.VolumeIds {
		resp.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.volumeUrl}}}
	}
	return resp, nil
}

func (f *ttlFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	dir := strings.TrimSuffix(req.Directory, "/")
	if dir == "" {
		dir = "/"
	}
	f.mu.Lock()
	var names []string
	for name := range f.dirs[dir] {
		if strings.HasPrefix(name, req.Prefix) && name > req.StartFromFileName {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var page []*filer_pb.Entry
	for _, name := range names {
		if uint32(len(page)) >= req.Limit {
			break
		}
		fullPath := string(util.FullPath(dir).Child(name))
		if f.expired[fullPath] {
			delete(f.dirs[dir], name)
			continue
		}
		page = append(page, proto.Clone(f.dirs[dir][name]).(*filer_pb.Entry))
	}
	f.mu.Unlock()
	for _, entry := range page {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func serveFiler(t *testing.T, f *ttlFiler) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(s, f)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	host, port, _ := net.SplitHostPort(lis.Addr().String())
	return fmt.Sprintf("%s:1.%s", host, port)
}

func filerConf(t *testing.T, rules ...*filer_pb.FilerConf_PathConf) []byte {
	t.Helper()
	fc := filer.NewFilerConf()
	for _, rule := range rules {
		if err := fc.SetLocationConf(rule); err != nil {
			t.Fatal(err)
		}
	}
	var buf strings.Builder
	if err := fc.ToText(&buf); err != nil {
		t.Fatal(err)
	}
	return []byte(buf.String())
}

type detectRecorder struct {
	proposals []*plugin_pb.JobProposal
	completes []*plugin_pb.DetectionComplete
}

func (r *detectRecorder) SendProposals(p *plugin_pb.DetectionProposals) error {
	r.proposals = append(r.proposals, p.Proposals...)
	return nil
}
func (r *detectRecorder) SendComplete(c *plugin_pb.DetectionComplete) error {
	r.completes = append(r.completes, c)
	return nil
}
func (r *detectRecorder) SendActivity(*plugin_pb.ActivityEvent) error { return nil }

type executeRecorder struct {
	completed []*plugin_pb.JobCompleted
}

func (r *executeRecorder) SendProgress(*plugin_pb.JobProgressUpdate) error { return nil }
func (r *executeRecorder) SendCompleted(c *plugin_pb.JobCompleted) error {
	r.completed = append(r.completed, c)
	return nil
}

func newTestHandler() *Handler {
	return NewHandler(grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func TestTTLLocationPrefixesFoldsNestedRules(t *testing.T) {
	fc := filer.NewFilerConf()
	for _, rule := range []*filer_pb.FilerConf_PathConf{
		{LocationPrefix: "/buckets/logs/", Ttl: "7d"},
		{LocationPrefix: "/buckets/logs/debug/", Ttl: "1d"},
		{LocationPrefix: "/buckets/archive/", Replication: "001"},
		{LocationPrefix: "/tmp", Ttl: "1h"},
	} {
		if err := fc.SetLocationConf(rule); err != nil {
			t.Fatal(err)
		}
	}
	got := ttlLocationPrefixes(fc)
	want := []string{"/buckets/logs/", "/tmp"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("prefixes = %v, want %v", got, want)
	}
}

func TestDetectProposesOneJobPerStore(t *testing.T) {
	conf := filerConf(t, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/cam/", Ttl: "11d"})
	shared1, shared2, own := newTTLFiler(7), newTTLFiler(7), newTTLFiler(9)
	for _, f := range []*ttlFiler{shared1, shared2, own} {
		f.conf = conf
	}
	addresses := []string{serveFiler(t, shared1), serveFiler(t, shared2), serveFiler(t, own)}

	sender := &detectRecorder{}
	err := newTestHandler().Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:        jobType,
		ClusterContext: &plugin_pb.ClusterContext{FilerAddresses: addresses},
	}, sender)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range sender.proposals {
		got = append(got, p.Parameters[filerAddressParam].GetStringValue())
		if prefixes := p.Parameters[locationPrefixesParam].GetStringList().GetValues(); fmt.Sprint(prefixes) != "[/buckets/cam/]" {
			t.Fatalf("prefixes = %v", prefixes)
		}
	}
	if want := []string{addresses[0], addresses[2]}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("proposed filers = %v, want %v", got, want)
	}
}

func TestDetectReadsEachStoresOwnFilerConf(t *testing.T) {
	plain, clips := newTTLFiler(1), newTTLFiler(2)
	plain.conf = filerConf(t, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/a/", Replication: "001"})
	clips.conf = filerConf(t, &filer_pb.FilerConf_PathConf{LocationPrefix: "/clips/", Ttl: "1d"})
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(clips.conf)
	}))
	t.Cleanup(volume.Close)
	clips.volumeUrl = strings.TrimPrefix(volume.URL, "http://")
	addresses := []string{serveFiler(t, plain), serveFiler(t, clips)}

	sender := &detectRecorder{}
	err := newTestHandler().Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:        jobType,
		ClusterContext: &plugin_pb.ClusterContext{FilerAddresses: addresses},
	}, sender)
	if err != nil {
		t.Fatal(err)
	}
	if len(sender.proposals) != 1 {
		t.Fatalf("proposals = %d, want 1", len(sender.proposals))
	}
	p := sender.proposals[0]
	if got := p.Parameters[filerAddressParam].GetStringValue(); got != addresses[1] {
		t.Fatalf("proposed filer = %s, want %s", got, addresses[1])
	}
	if prefixes := p.Parameters[locationPrefixesParam].GetStringList().GetValues(); fmt.Sprint(prefixes) != "[/clips/]" {
		t.Fatalf("prefixes = %v", prefixes)
	}
}

func TestDetectWithoutTTLRulesProposesNothing(t *testing.T) {
	f := newTTLFiler(1)
	f.conf = filerConf(t, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/a/", Replication: "001"})
	sender := &detectRecorder{}
	err := newTestHandler().Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:        jobType,
		ClusterContext: &plugin_pb.ClusterContext{FilerAddresses: []string{serveFiler(t, f)}},
	}, sender)
	if err != nil {
		t.Fatal(err)
	}
	if len(sender.proposals) != 0 || len(sender.completes) != 1 || !sender.completes[0].Success {
		t.Fatalf("proposals=%d completes=%v", len(sender.proposals), sender.completes)
	}
}

func TestExecuteRemovesExpiredRowsNobodyVisits(t *testing.T) {
	f := newTTLFiler(1)
	f.add("/buckets/cam/2026-11-01", true, false)
	for i := 0; i < 2*walkPageSize+10; i++ {
		f.add(fmt.Sprintf("/buckets/cam/2026-11-01/clip-%05d.mp4", i), false, true)
	}
	f.add("/buckets/cam/2026-11-26", true, false)
	for i := 0; i < walkPageSize+100; i++ {
		f.add(fmt.Sprintf("/buckets/cam/2026-11-26/clip-%05d.mp4", i), false, false)
	}
	f.add("/buckets/cam/2026-11-26/zz-old.mp4", false, true)
	f.add("/buckets/cam/2026-11-27", true, false)
	f.add("/buckets/cam/2026-11-27/deep", true, false)
	f.add("/buckets/cam/2026-11-27/deep/old.mp4", false, true)
	f.add("/buckets/cam/2026-11-27/deep/new.mp4", false, false)
	for _, dir := range []string{"/ttl-http-a", "/ttl-http-b", "/keep"} {
		f.add(dir, true, false)
		f.add(dir+"/old.bin", false, true)
	}
	address := serveFiler(t, f)

	sender := &executeRecorder{}
	err := newTestHandler().Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: &plugin_pb.JobSpec{
		JobType: jobType,
		Parameters: map[string]*plugin_pb.ConfigValue{
			filerAddressParam:     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: address}},
			locationPrefixesParam: {Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{Values: []string{"/buckets/cam/", "/ttl-http-"}}}},
		},
	}}, sender)
	if err != nil {
		t.Fatal(err)
	}
	if len(sender.completed) != 1 || !sender.completed[0].Success {
		t.Fatalf("completed = %v", sender.completed)
	}
	for _, expired := range []string{"/buckets/cam/2026-11-01/clip-02057.mp4", "/buckets/cam/2026-11-26/zz-old.mp4", "/buckets/cam/2026-11-27/deep/old.mp4", "/ttl-http-a/old.bin", "/ttl-http-b/old.bin"} {
		dir, name := util.FullPath(expired).DirAndName()
		if _, found := f.dirs[dir][name]; found {
			t.Fatalf("%s is still stored", expired)
		}
	}
	if _, found := f.dirs["/keep"]["old.bin"]; !found {
		t.Fatal("an entry outside every TTL path was listed")
	}
	if _, found := f.dirs["/buckets/cam/2026-11-27/deep"]["new.mp4"]; !found {
		t.Fatal("a live entry was removed")
	}
	if got := f.rows(); got != 8+1+walkPageSize+100 {
		t.Fatalf("rows left = %d, want %d", got, 8+1+walkPageSize+100)
	}
}
