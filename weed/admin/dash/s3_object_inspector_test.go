package dash

import (
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestValidateInspectedS3Object(t *testing.T) {
	valid := [][2]string{{"b", "k"}, {"b", "/a/b.txt"}, {"b", "a/b c/d"}}
	for _, c := range valid {
		if err := validateInspectedS3Object(c[0], c[1]); err != nil {
			t.Errorf("validateInspectedS3Object(%q, %q) = %v", c[0], c[1], err)
		}
	}
	invalid := [][2]string{{"", "k"}, {"a/b", "k"}, {"..", "k"}, {"b", ""}, {"b", "dir/"}, {"b", "a/../k"}, {"b", "a//k"}, {"b", "./k"}}
	for _, c := range invalid {
		if err := validateInspectedS3Object(c[0], c[1]); err == nil {
			t.Errorf("validateInspectedS3Object(%q, %q) accepted", c[0], c[1])
		}
	}
}

func TestSplitInspectedS3ObjectKey(t *testing.T) {
	dir, name := splitInspectedS3ObjectKey("/buckets/b", "a/b/c.txt")
	if dir != "/buckets/b/a/b" || name != "c.txt" {
		t.Fatalf("got %q %q", dir, name)
	}
	dir, name = splitInspectedS3ObjectKey("/buckets/b", "c.txt")
	if dir != "/buckets/b" || name != "c.txt" {
		t.Fatalf("got %q %q", dir, name)
	}
}

func inspectorTestEntry(name string, mtime int64, ext map[string]string, chunks ...*filer_pb.FileChunk) *filer_pb.Entry {
	extended := make(map[string][]byte, len(ext))
	for k, v := range ext {
		extended[k] = []byte(v)
	}
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileSize: 10, Mtime: mtime},
		Extended:   extended,
		Chunks:     chunks,
	}
}

func TestAssembleS3ObjectVersions(t *testing.T) {
	versionsEntry := inspectorTestEntry("k.versions", 0, map[string]string{s3_constants.ExtLatestVersionIdKey: "v2"})
	locked := inspectorTestEntry("v_v1", 100, map[string]string{
		s3_constants.ExtVersionIdKey:          "v1",
		s3_constants.ExtETagKey:               `"abc"`,
		s3_constants.ExtChecksumAlgorithm:     "SHA256",
		s3_constants.ExtChecksumValue:         "c2hh",
		s3_constants.ExtObjectLockModeKey:     s3_constants.RetentionModeCompliance,
		s3_constants.ExtRetentionUntilDateKey: "1893456000",
		s3_constants.ExtLegalHoldKey:          s3_constants.LegalHoldOn,
	}, &filer_pb.FileChunk{FileId: "7,01637037d6", Size: 10}, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: 1, Cookie: 2}, Offset: 10, Size: 5, IsChunkManifest: true})
	locked.WormEnforcedAtTsNs = time.Unix(200, 0).UnixNano()
	marker := inspectorTestEntry("v_v2", 300, map[string]string{
		s3_constants.ExtVersionIdKey:    "v2",
		s3_constants.ExtDeleteMarkerKey: "true",
	})
	base := inspectorTestEntry("k", 50, nil, &filer_pb.FileChunk{FileId: "3,02637037d6", Size: 10})

	versions := assembleS3ObjectVersions("/buckets/b/d", "k", base, versionsEntry, []*filer_pb.Entry{locked, marker}, false)
	if len(versions) != 3 {
		t.Fatalf("got %d versions", len(versions))
	}
	if v := versions[0]; v.VersionId != "v2" || !v.IsLatest || !v.IsDeleteMarker || v.Path != "/buckets/b/d/k.versions/v_v2" {
		t.Fatalf("latest = %+v", v)
	}
	v1 := versions[1]
	if v1.VersionId != "v1" || v1.IsLatest || v1.ETag != "abc" || v1.ChecksumAlgorithm != "SHA256" || v1.ChecksumValue != "c2hh" {
		t.Fatalf("v1 = %+v", v1)
	}
	if v1.ObjectLockMode != s3_constants.RetentionModeCompliance || v1.LegalHold != s3_constants.LegalHoldOn {
		t.Fatalf("v1 lock = %+v", v1)
	}
	if v1.RetainUntil == nil || !v1.RetainUntil.Equal(time.Unix(1893456000, 0)) {
		t.Fatalf("v1 retain until = %v", v1.RetainUntil)
	}
	if v1.WormEnforcedAt == nil || !v1.WormEnforcedAt.Equal(time.Unix(200, 0)) {
		t.Fatalf("v1 worm = %v", v1.WormEnforcedAt)
	}
	if len(v1.Chunks) != 2 || v1.Chunks[0].VolumeId != 7 || v1.Chunks[1].VolumeId != 9 || !v1.Chunks[1].IsManifest || v1.Chunks[1].FileId == "" {
		t.Fatalf("v1 chunks = %+v", v1.Chunks)
	}
	if v := versions[2]; v.VersionId != "null" || v.IsLatest || v.Path != "/buckets/b/d/k" {
		t.Fatalf("null = %+v", v)
	}
	if ids := s3ObjectVolumeIds(versions); !reflect.DeepEqual(ids, []uint32{3, 7, 9}) {
		t.Fatalf("volume ids = %v", ids)
	}
}

func TestAssembleS3ObjectVersionsNullLatest(t *testing.T) {
	base := inspectorTestEntry("k", 50, nil)
	if versions := assembleS3ObjectVersions("/buckets/b", "k", base, nil, nil, false); len(versions) != 1 || !versions[0].IsLatest || versions[0].VersionId != "null" {
		t.Fatalf("unversioned = %+v", versions)
	}
	versionsEntry := inspectorTestEntry("k.versions", 0, map[string]string{
		s3_constants.ExtLatestVersionIdKey:     "v1",
		s3_constants.ExtNullVersionIsLatestKey: "true",
	})
	v1 := inspectorTestEntry("v_v1", 100, map[string]string{s3_constants.ExtVersionIdKey: "v1"})
	versions := assembleS3ObjectVersions("/buckets/b", "k", base, versionsEntry, []*filer_pb.Entry{v1}, false)
	if len(versions) != 2 || versions[0].VersionId != "null" || !versions[0].IsLatest || versions[1].IsLatest {
		t.Fatalf("suspended = %+v", versions)
	}
}

func TestAssembleS3ObjectVersionsWithoutPointer(t *testing.T) {
	older := inspectorTestEntry("v_6713f0ad56f535bcb4717d38b89d2800", 200, map[string]string{s3_constants.ExtVersionIdKey: "6713f0ad56f535bcb4717d38b89d2800"})
	newer := inspectorTestEntry("v_6713f0ad569d387d178b2ca8fab47783", 100, map[string]string{s3_constants.ExtVersionIdKey: "6713f0ad569d387d178b2ca8fab47783"})
	versionsEntry := inspectorTestEntry("k.versions", 0, map[string]string{s3_constants.ExtNullVersionIsLatestKey: "true"})

	versions := assembleS3ObjectVersions("/buckets/b", "k", nil, versionsEntry, []*filer_pb.Entry{older, newer}, false)
	if len(versions) != 2 || versions[0].VersionId != newer.Name[2:] || !versions[0].IsLatest || versions[1].IsLatest {
		t.Fatalf("recovered = %+v", versions)
	}
	for _, v := range assembleS3ObjectVersions("/buckets/b", "k", nil, versionsEntry, []*filer_pb.Entry{older, newer}, true) {
		if v.IsLatest {
			t.Fatalf("truncated scan marked %s latest", v.VersionId)
		}
	}
}

func TestInspectS3ObjectVersionRemoteETag(t *testing.T) {
	entry := inspectorTestEntry("k", 1, nil)
	entry.RemoteEntry = &filer_pb.RemoteEntry{StorageName: "s3remote", RemoteETag: "abc", RemoteSize: 10}
	if v := inspectS3ObjectVersion("/buckets/b/k", entry); v.ETag != "abc" || v.RemoteStorage != "s3remote" {
		t.Fatalf("remote = %+v", v)
	}
}

func TestInspectS3ObjectVersionSizeFromChunks(t *testing.T) {
	entry := inspectorTestEntry("k", 1, nil, &filer_pb.FileChunk{FileId: "3,02637037d6", Offset: 0, Size: 64})
	entry.Attributes.FileSize = 0
	if v := inspectS3ObjectVersion("/buckets/b/k", entry); v.Size != 64 {
		t.Fatalf("size = %d", v.Size)
	}
}

func TestSummarizeS3ObjectVolumes(t *testing.T) {
	node := func(id string, disks ...*master_pb.DiskInfo) *master_pb.DataNodeInfo {
		m := make(map[string]*master_pb.DiskInfo)
		for i, d := range disks {
			m[string(rune('a'+i))] = d
		}
		return &master_pb.DataNodeInfo{Id: id, DiskInfos: m}
	}
	topology := &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		Id: "dc1",
		RackInfos: []*master_pb.RackInfo{{
			Id: "r1",
			DataNodeInfos: []*master_pb.DataNodeInfo{
				node("s1:8080", &master_pb.DiskInfo{
					VolumeInfos:  []*master_pb.VolumeInformationMessage{{Id: 1, Collection: "b", ReplicaPlacement: 1}, {Id: 2, ReplicaPlacement: 0}, {Id: 99}},
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{Id: 3, Collection: "b", EcIndexBits: 0x3ff}},
				}),
				node("s2:8080", &master_pb.DiskInfo{
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{Id: 3, Collection: "b", EcIndexBits: 0x400}, {Id: 4, EcIndexBits: 0x1ff}},
				}),
			},
		}},
	}}}

	got := summarizeS3ObjectVolumes([]uint32{1, 2, 3, 4, 5}, topology)
	if len(got) != 5 {
		t.Fatalf("got %d volumes", len(got))
	}
	if v := got[0]; v.Kind != s3ObjectVolumeReplicated || v.CopiesPresent != 1 || v.CopiesExpected != 2 || v.Healthy {
		t.Errorf("volume 1 = %+v", v)
	}
	if v := got[1]; v.Kind != s3ObjectVolumeReplicated || v.CopiesPresent != 1 || v.CopiesExpected != 1 || !v.Healthy {
		t.Errorf("volume 2 = %+v", v)
	}
	if v := got[2]; v.Kind != s3ObjectVolumeEc || v.EcShardsPresent != 11 || !v.Healthy || !reflect.DeepEqual(v.EcMissingShards, []uint32{11, 12, 13}) || !reflect.DeepEqual(v.Servers, []string{"s1:8080", "s2:8080"}) {
		t.Errorf("volume 3 = %+v", v)
	}
	if v := got[3]; v.Kind != s3ObjectVolumeEc || v.EcShardsPresent != 9 || v.Healthy {
		t.Errorf("volume 4 = %+v", v)
	}
	if v := got[4]; v.Kind != s3ObjectVolumeMissing || v.Healthy {
		t.Errorf("volume 5 = %+v", v)
	}
}

func TestSummarizeS3ObjectVolumesPrefersReplicaOverEcShards(t *testing.T) {
	topology := &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		RackInfos: []*master_pb.RackInfo{{
			DataNodeInfos: []*master_pb.DataNodeInfo{
				{Id: "a-ec:8080", DiskInfos: map[string]*master_pb.DiskInfo{"": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{Id: 6, EcIndexBits: 0x3fff}}}}},
				{Id: "b-vol:8080", DiskInfos: map[string]*master_pb.DiskInfo{"": {VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 6}}}}},
			},
		}},
	}}}
	got := summarizeS3ObjectVolumes([]uint32{6}, topology)
	if v := got[0]; v.Kind != s3ObjectVolumeReplicated || !reflect.DeepEqual(v.Servers, []string{"b-vol:8080"}) || v.EcTotalShards != 0 || v.EcShardsPresent != 0 || !v.Healthy {
		t.Fatalf("volume 6 = %+v", v)
	}
}
