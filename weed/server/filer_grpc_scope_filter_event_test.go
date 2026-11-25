package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestScopeSubscriptionSelectors(t *testing.T) {
	sc := &scope{grants: []scopeGrant{{path: "/visible"}}}
	for _, tc := range []struct {
		name string
		req  *filer_pb.SubscribeMetadataRequest
		deny bool
	}{
		{"prefix", &filer_pb.SubscribeMetadataRequest{PathPrefix: "/visible/"}, false},
		{"prefix list", &filer_pb.SubscribeMetadataRequest{PathPrefixes: []string{"/visible/sub"}}, false},
		{"directory", &filer_pb.SubscribeMetadataRequest{Directories: []string{"/visible"}}, false},
		{"empty", &filer_pb.SubscribeMetadataRequest{}, true},
		{"ancestor", &filer_pb.SubscribeMetadataRequest{PathPrefix: "/"}, true},
		{"prefix collision", &filer_pb.SubscribeMetadataRequest{PathPrefix: "/visible-other"}, true},
		{"unclean", &filer_pb.SubscribeMetadataRequest{PathPrefix: "/visible/../other"}, true},
		{"bad extra prefix", &filer_pb.SubscribeMetadataRequest{PathPrefix: "/visible", PathPrefixes: []string{"/other"}}, true},
		{"bad extra directory", &filer_pb.SubscribeMetadataRequest{PathPrefix: "/visible", Directories: []string{"/other"}}, true},
		{"unclean directory", &filer_pb.SubscribeMetadataRequest{Directories: []string{"/visible/"}}, true},
		{"mount config watch", &filer_pb.SubscribeMetadataRequest{PathPrefixes: []string{"/visible", scopeFilerConfDir + "/" + scopeFilerConfName}}, false},
		{"config only", &filer_pb.SubscribeMetadataRequest{PathPrefixes: []string{scopeFilerConfDir + "/" + scopeFilerConfName}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.req.ClientSupportsMetadataChunks = true
			err := sc.checkSubscribe("SubscribeMetadata", tc.req)
			if tc.deny {
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("selector error = %v, want PermissionDenied", err)
				}
				return
			}
			if err != nil || tc.req.ClientSupportsMetadataChunks {
				t.Fatalf("selector error = %v, metadata chunks = %v", err, tc.req.ClientSupportsMetadataChunks)
			}
			for _, p := range tc.req.PathPrefixes {
				if p == scopeFilerConfDir+"/"+scopeFilerConfName {
					t.Fatal("mount config watch was not removed")
				}
			}
		})
	}
}

func TestScopeFilterEventVisibility(t *testing.T) {
	sc := &scope{exp: time.Now().Add(time.Hour), grants: []scopeGrant{{path: "/visible"}, {path: scopeFilerConfDir}}}
	entry := func(name string) *filer_pb.Entry { return &filer_pb.Entry{Name: name, Content: []byte("test content")} }
	for _, tc := range []struct {
		name, dir, oldName, newDir, newName   string
		wantDir, wantOld, wantNew, wantParent string
	}{
		{name: "create", dir: "/visible", newName: "file", wantDir: "/visible", wantNew: "file", wantParent: "/visible"},
		{name: "nested create", dir: "/visible/nested", newDir: "/visible/nested", newName: "report.txt", wantDir: "/visible/nested", wantNew: "report.txt", wantParent: "/visible/nested"},
		{name: "delete", dir: "/visible", oldName: "file", wantDir: "/visible", wantOld: "file"},
		{name: "update", dir: "/visible", oldName: "file", newName: "file", wantDir: "/visible", wantOld: "file", wantNew: "file", wantParent: "/visible"},
		{name: "outside", dir: "/other", newName: "file"},
		{name: "prefix collision", dir: "/visible-other", newName: "file"},
		{name: "unclean directory", dir: "/visible/../other", newName: "file"},
		{name: "unclean name", dir: "/visible", newName: "../file"},
		{name: "rename within", dir: "/visible", oldName: "old", newDir: "/visible/sub", newName: "new", wantDir: "/visible", wantOld: "old", wantNew: "new", wantParent: "/visible/sub"},
		{name: "rename out", dir: "/visible", oldName: "old", newDir: "/other", newName: "new", wantDir: "/visible", wantOld: "old"},
		{name: "rename in", dir: "/other", oldName: "old", newDir: "/visible/nested", newName: "new", wantDir: "/visible/nested", wantNew: "new", wantParent: "/visible/nested"},
		{name: "config create", dir: scopeFilerConfDir, newName: scopeFilerConfName},
		{name: "config delete", dir: scopeFilerConfDir, oldName: scopeFilerConfName},
		{name: "config update", dir: scopeFilerConfDir, oldName: scopeFilerConfName, newName: scopeFilerConfName},
		{name: "config rename out", dir: scopeFilerConfDir, oldName: scopeFilerConfName, newDir: "/visible", newName: "copy"},
		{name: "config rename in", dir: "/visible", oldName: "old", newDir: scopeFilerConfDir, newName: scopeFilerConfName, wantDir: "/visible", wantOld: "old"},
		{name: "config rename from hidden", dir: "/other", oldName: "old", newDir: scopeFilerConfDir, newName: scopeFilerConfName},
		{name: "config sibling", dir: scopeFilerConfDir, newName: "visible.json", wantDir: scopeFilerConfDir, wantNew: "visible.json", wantParent: scopeFilerConfDir},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := &filer_pb.EventNotification{NewParentPath: tc.newDir}
			if tc.oldName != "" {
				ev.OldEntry = entry(tc.oldName)
			}
			if tc.newName != "" {
				ev.NewEntry = entry(tc.newName)
			}
			in := &filer_pb.SubscribeMetadataResponse{Directory: tc.dir, EventNotification: ev, TsNs: 10, MutationTsNs: 9}
			before := proto.Clone(in)
			out := sc.filterEvent(in)
			if !proto.Equal(in, before) {
				t.Fatal("filter modified the shared input")
			}
			if tc.wantDir == "" {
				if out != nil {
					t.Fatalf("hidden event delivered: %v", out)
				}
				return
			}
			if out == nil || out.Directory != tc.wantDir || out.GetEventNotification().GetOldEntry().GetName() != tc.wantOld || out.GetEventNotification().GetNewEntry().GetName() != tc.wantNew || out.GetEventNotification().GetNewParentPath() != tc.wantParent {
				t.Fatalf("unexpected visible event: %v", out)
			}
			if out.TsNs != 10 || out.MutationTsNs != 9 {
				t.Fatalf("event lost timestamps: %v", out)
			}
			if tc.wantNew != "" {
				replayed := filer.FromPbEntry(out.EventNotification.NewParentPath, out.EventNotification.NewEntry)
				if wantPath := tc.wantParent + "/" + tc.wantNew; string(replayed.FullPath) != wantPath {
					t.Fatalf("replayed path = %q, want %q", replayed.FullPath, wantPath)
				}
			}
		})
	}
}

func TestScopeFilterEventBatchesAndMarkers(t *testing.T) {
	sc := &scope{grants: []scopeGrant{{path: scopeFilerConfDir}}}
	visible := func(ts int64) *filer_pb.SubscribeMetadataResponse {
		return scopeTestEvent(scopeFilerConfDir, "visible.json", ts)
	}
	hidden := func(ts int64) *filer_pb.SubscribeMetadataResponse {
		return scopeTestEvent(scopeFilerConfDir, scopeFilerConfName, ts)
	}
	for _, tc := range []struct {
		name       string
		head, tail *filer_pb.SubscribeMetadataResponse
		want       []int64
	}{
		{"hidden head", hidden(1), visible(2), []int64{2}},
		{"hidden tail", visible(1), hidden(2), []int64{1}},
		{"all hidden", hidden(1), hidden(2), nil},
		{"all visible", visible(1), visible(2), []int64{1, 2}},
		{"refs head", &filer_pb.SubscribeMetadataResponse{TsNs: 1, LogFileRefs: []*filer_pb.LogFileChunkRef{{}}}, visible(2), []int64{2}},
		{"refs tail", visible(1), &filer_pb.SubscribeMetadataResponse{TsNs: 2, LogFileRefs: []*filer_pb.LogFileChunkRef{{}}}, []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.head.Events = []*filer_pb.SubscribeMetadataResponse{tc.tail}
			tc.head.FlushedTsNs = 7
			before := proto.Clone(tc.head)
			out := sc.filterEvent(tc.head)
			if !proto.Equal(before, tc.head) {
				t.Fatal("batch filter modified input")
			}
			var stamps []int64
			var inspect func(*filer_pb.SubscribeMetadataResponse)
			inspect = func(e *filer_pb.SubscribeMetadataResponse) {
				if e == nil {
					return
				}
				if len(e.LogFileRefs) != 0 || e.GetEventNotification().GetNewEntry().GetName() == scopeFilerConfName {
					t.Fatalf("hidden content leaked: %v", e)
				}
				if e.GetEventNotification().GetNewEntry() != nil {
					stamps = append(stamps, e.TsNs)
				}
				for _, tail := range e.Events {
					inspect(tail)
				}
			}
			inspect(out)
			if len(stamps) != len(tc.want) {
				t.Fatalf("stamps %v, want %v", stamps, tc.want)
			}
			for i := range stamps {
				if stamps[i] != tc.want[i] {
					t.Fatalf("stamps %v, want %v", stamps, tc.want)
				}
			}
			if out != nil && out.FlushedTsNs != 7 {
				t.Fatal("lost flush watermark")
			}
			if len(tc.want) == 0 && out != nil {
				t.Fatalf("all-hidden batch must be withheld: %v", out)
			}
		})
	}
	for _, marker := range []*filer_pb.SubscribeMetadataResponse{
		{TsNs: 8}, {FlushedTsNs: 9},
		{TsNs: 10, EventNotification: &filer_pb.EventNotification{}},
	} {
		out := sc.filterEvent(marker)
		if out == nil || out.TsNs != marker.TsNs || out.FlushedTsNs != marker.FlushedTsNs || out.Directory != "" || out.GetEventNotification().GetNewParentPath() != "" {
			t.Fatalf("unsafe or lost marker: %v", out)
		}
	}
	for _, dir := range []string{"/hidden", scopeFilerConfDir + "/../hidden", scopeFilerConfDir + "/" + scopeFilerConfName} {
		if out := sc.filterEvent(&filer_pb.SubscribeMetadataResponse{Directory: dir, EventNotification: &filer_pb.EventNotification{}}); out != nil {
			t.Fatalf("hidden directory marker delivered: %v", out)
		}
	}
	if out := sc.filterEvent(&filer_pb.SubscribeMetadataResponse{Directory: scopeFilerConfDir, EventNotification: &filer_pb.EventNotification{NewParentPath: "/hidden"}}); out == nil || out.Directory != scopeFilerConfDir || out.GetEventNotification().GetNewParentPath() != "" {
		t.Fatalf("unsafe or lost directory marker: %v", out)
	}
	nested := hidden(1)
	nested.Events = []*filer_pb.SubscribeMetadataResponse{hidden(2)}
	nested.Events[0].Events = []*filer_pb.SubscribeMetadataResponse{visible(3)}
	if out := sc.filterEvent(nested); out == nil || len(out.Events) != 1 || len(out.Events[0].Events) != 1 || out.Events[0].Events[0].TsNs != 3 {
		t.Fatalf("nested visible event lost: %v", out)
	}
}
