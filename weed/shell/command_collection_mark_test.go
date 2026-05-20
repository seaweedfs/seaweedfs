package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestCollectionMarkNormalizeCollectionName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "named collection", in: "photos", want: "photos"},
		{name: "default keyword", in: "_default", want: ""},
		{name: "delete command default keyword", in: "_default_", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeCollectionMarkName(tt.in)
			if got != tt.want {
				t.Fatalf("normalizeCollectionMarkName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestCollectionMarkParseOptions(t *testing.T) {
	opts, err := parseCollectionMarkOptions([]string{"-collection", "photos", "-readonly"})
	if err != nil {
		t.Fatalf("parse readonly options: %v", err)
	}
	if opts.collection != "photos" {
		t.Fatalf("collection = %q, want photos", opts.collection)
	}
	if opts.markWritable {
		t.Fatalf("markWritable = true, want false for -readonly")
	}
	if opts.apply {
		t.Fatalf("apply = true, want false without -apply")
	}

	opts, err = parseCollectionMarkOptions([]string{"-collection", "_default_", "-writable", "-apply"})
	if err != nil {
		t.Fatalf("parse writable options: %v", err)
	}
	if opts.collection != "" {
		t.Fatalf("collection = %q, want empty default collection", opts.collection)
	}
	if !opts.markWritable {
		t.Fatalf("markWritable = false, want true for -writable")
	}
	if !opts.apply {
		t.Fatalf("apply = false, want true with -apply")
	}
}

func TestCollectionMarkParseOptionsRequiresCollectionAndSingleMode(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing collection", args: []string{"-readonly"}},
		{name: "missing mode", args: []string{"-collection", "photos"}},
		{name: "conflicting modes", args: []string{"-collection", "photos", "-readonly", "-writable"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseCollectionMarkOptions(tt.args); err == nil {
				t.Fatalf("parseCollectionMarkOptions(%v) returned nil error", tt.args)
			}
		})
	}
}

func TestCollectionMarkCollectTargets(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id:       "node-a",
								Address:  "10.0.0.1:8080",
								GrpcPort: 9080,
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 3, Collection: "photos", ReadOnly: false},
											{Id: 4, Collection: "other", ReadOnly: false},
										},
										EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
											{Id: 9, Collection: "photos"},
										},
									},
								},
							},
							{
								Id:       "node-b",
								Address:  "10.0.0.2:8080",
								GrpcPort: 9081,
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 3, Collection: "photos", ReadOnly: true},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	targets, skippedEC := collectCollectionMarkTargets(topo, "photos")
	if skippedEC != 1 {
		t.Fatalf("skippedEC = %d, want 1", skippedEC)
	}
	if len(targets) != 2 {
		t.Fatalf("len(targets) = %d, want 2: %#v", len(targets), targets)
	}

	if got := uint32(targets[0].volumeId); got != 3 {
		t.Fatalf("target[0].volumeId = %d, want 3", got)
	}
	if got := string(targets[0].server); got != "10.0.0.1:8080.9080" {
		t.Fatalf("target[0].server = %q, want 10.0.0.1:8080.9080", got)
	}
	if targets[0].readOnly {
		t.Fatalf("target[0].readOnly = true, want false")
	}

	if got := uint32(targets[1].volumeId); got != 3 {
		t.Fatalf("target[1].volumeId = %d, want 3", got)
	}
	if got := string(targets[1].server); got != "10.0.0.2:8080.9081" {
		t.Fatalf("target[1].server = %q, want 10.0.0.2:8080.9081", got)
	}
	if !targets[1].readOnly {
		t.Fatalf("target[1].readOnly = false, want true")
	}
}
