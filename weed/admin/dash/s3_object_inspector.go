package dash

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

const (
	maxInspectedS3ObjectVersions      = 10000
	inspectedS3ObjectVersionsPageSize = 1024
)

type S3ObjectInspection struct {
	Bucket    string                      `json:"bucket"`
	Key       string                      `json:"key"`
	Versioned bool                        `json:"versioned"`
	Truncated bool                        `json:"truncated,omitempty"`
	Versions  []S3ObjectVersionInspection `json:"versions"`
	Volumes   []S3ObjectVolumeHealth      `json:"volumes"`
}

type S3ObjectVersionInspection struct {
	VersionId         string                    `json:"versionId"`
	Path              string                    `json:"path"`
	IsLatest          bool                      `json:"isLatest"`
	IsDeleteMarker    bool                      `json:"isDeleteMarker"`
	Size              uint64                    `json:"size"`
	ModifiedAt        time.Time                 `json:"modifiedAt"`
	ETag              string                    `json:"etag,omitempty"`
	ChecksumAlgorithm string                    `json:"checksumAlgorithm,omitempty"`
	ChecksumValue     string                    `json:"checksumValue,omitempty"`
	ChecksumType      string                    `json:"checksumType,omitempty"`
	ObjectLockMode    string                    `json:"objectLockMode,omitempty"`
	RetainUntil       *time.Time                `json:"retainUntil,omitempty"`
	LegalHold         string                    `json:"legalHold,omitempty"`
	WormEnforcedAt    *time.Time                `json:"wormEnforcedAt,omitempty"`
	InlineBytes       int                       `json:"inlineBytes,omitempty"`
	RemoteStorage     string                    `json:"remoteStorage,omitempty"`
	Chunks            []S3ObjectChunkInspection `json:"chunks"`
}

type S3ObjectChunkInspection struct {
	FileId     string `json:"fileId"`
	VolumeId   uint32 `json:"volumeId"`
	Offset     int64  `json:"offset"`
	Size       uint64 `json:"size"`
	IsManifest bool   `json:"isManifest,omitempty"`
}

type S3ObjectVolumeHealth struct {
	VolumeId        uint32   `json:"volumeId"`
	Collection      string   `json:"collection,omitempty"`
	Kind            string   `json:"kind"`
	Servers         []string `json:"servers,omitempty"`
	CopiesPresent   int      `json:"copiesPresent,omitempty"`
	CopiesExpected  int      `json:"copiesExpected,omitempty"`
	ReadOnly        bool     `json:"readOnly,omitempty"`
	EcShardsPresent int      `json:"ecShardsPresent,omitempty"`
	EcDataShards    int      `json:"ecDataShards,omitempty"`
	EcTotalShards   int      `json:"ecTotalShards,omitempty"`
	EcMissingShards []uint32 `json:"ecMissingShards,omitempty"`
	Healthy         bool     `json:"healthy"`
}

const (
	s3ObjectVolumeReplicated = "replicated"
	s3ObjectVolumeEc         = "ec"
	s3ObjectVolumeMissing    = "missing"
)

func (s *AdminServer) InspectS3ObjectAPI(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := r.URL.Query().Get("key")
	if err := validateInspectedS3Object(bucket, key); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	inspection, err := s.InspectS3Object(r.Context(), bucket, strings.TrimPrefix(key, "/"))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, inspection)
}

func validateInspectedS3Object(bucket, key string) error {
	if bucket == "" || strings.ContainsAny(bucket, "/\\") || bucket == "." || bucket == ".." {
		return fmt.Errorf("invalid bucket name %q", bucket)
	}
	key = strings.TrimPrefix(key, "/")
	if key == "" || strings.HasSuffix(key, "/") {
		return fmt.Errorf("object key is required")
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("invalid object key %q", key)
		}
	}
	return nil
}

func (s *AdminServer) InspectS3Object(ctx context.Context, bucket, key string) (*S3ObjectInspection, error) {
	inspection := &S3ObjectInspection{Bucket: bucket, Key: key}
	var objectDir, objectName string
	var baseEntry, versionsEntry *filer_pb.Entry
	var versionEntries []*filer_pb.Entry
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		config, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		bucketsPath := s3_constants.DefaultBucketsPath
		if config.DirBuckets != "" {
			bucketsPath = config.DirBuckets
		}
		objectDir, objectName = splitInspectedS3ObjectKey(bucketsPath+"/"+bucket, key)
		versionsName := objectName + s3_constants.VersionsFolder
		versionsDir := objectDir + "/" + versionsName
		if _, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: bucketsPath, Name: bucket}); err != nil {
			return fmt.Errorf("bucket %s: %w", bucket, err)
		}
		if resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: objectDir, Name: objectName}); err == nil {
			if !resp.Entry.IsDirectory || resp.Entry.IsPrefixObject() {
				baseEntry = resp.Entry
			}
		} else if !errors.Is(err, filer_pb.ErrNotFound) {
			return fmt.Errorf("lookup %s/%s: %w", objectDir, objectName, err)
		}
		if resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: objectDir, Name: versionsName}); err == nil {
			if resp.Entry.IsDirectory {
				versionsEntry = resp.Entry
			}
		} else if !errors.Is(err, filer_pb.ErrNotFound) {
			return fmt.Errorf("lookup %s: %w", versionsDir, err)
		}
		if versionsEntry == nil {
			return nil
		}
		versionEntries, inspection.Truncated, err = listInspectedS3ObjectVersions(ctx, client, versionsDir)
		return err
	})
	if err != nil {
		return nil, err
	}
	if baseEntry == nil && len(versionEntries) == 0 {
		return nil, fmt.Errorf("object %s/%s: %w", bucket, key, filer_pb.ErrNotFound)
	}

	inspection.Versioned = versionsEntry != nil
	inspection.Versions = assembleS3ObjectVersions(objectDir, objectName, baseEntry, versionsEntry, versionEntries, inspection.Truncated)

	volumeIds := s3ObjectVolumeIds(inspection.Versions)
	if len(volumeIds) == 0 {
		return inspection, nil
	}
	var topology *master_pb.TopologyInfo
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := pb.CollectVolumeList(ctx, client, &master_pb.VolumeListRequest{VolumeIds: volumeIds})
		if err != nil {
			return err
		}
		topology = resp.TopologyInfo
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("volume list: %w", err)
	}
	inspection.Volumes = summarizeS3ObjectVolumes(volumeIds, topology)
	return inspection, nil
}

// listInspectedS3ObjectVersions pages through versionsDir, skipping entries
// without a version id, until it holds maxInspectedS3ObjectVersions versions.
func listInspectedS3ObjectVersions(ctx context.Context, client filer_pb.SeaweedFilerClient, versionsDir string) (versions []*filer_pb.Entry, truncated bool, err error) {
	startFrom := ""
	for {
		listed, last := 0, false
		err = filer_pb.SeaweedList(ctx, client, versionsDir, "", func(entry *filer_pb.Entry, isLast bool) error {
			listed++
			last = isLast
			startFrom = entry.Name
			if s3api.VersionIdFromEntry(entry) == "" {
				return nil
			}
			if len(versions) >= maxInspectedS3ObjectVersions {
				truncated = true
				return nil
			}
			versions = append(versions, entry)
			return nil
		}, startFrom, false, inspectedS3ObjectVersionsPageSize)
		if err != nil || truncated || last || listed == 0 {
			return versions, truncated, err
		}
	}
}

func splitInspectedS3ObjectKey(bucketDir, key string) (dir, name string) {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return bucketDir + "/" + key[:i], key[i+1:]
	}
	return bucketDir, key
}

func assembleS3ObjectVersions(objectDir, objectName string, baseEntry, versionsEntry *filer_pb.Entry, versionEntries []*filer_pb.Entry, truncated bool) []S3ObjectVersionInspection {
	var latestVersionId string
	nullIsLatest := versionsEntry == nil
	if versionsEntry != nil {
		latestVersionId = string(versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey])
		nullIsLatest = latestVersionId == ""
	}
	if baseEntry == nil && latestVersionId == "" {
		nullIsLatest = false
		if !truncated {
			latestVersionId = s3api.SelectLatestVersionId(versionEntries)
		}
	}

	var versions []S3ObjectVersionInspection
	if baseEntry != nil {
		version := inspectS3ObjectVersion(objectDir+"/"+objectName, baseEntry)
		if version.VersionId == "" {
			version.VersionId = "null"
		}
		version.IsLatest = nullIsLatest || version.VersionId == latestVersionId
		versions = append(versions, version)
	}
	versionsDir := objectDir + "/" + objectName + s3_constants.VersionsFolder
	for _, entry := range versionEntries {
		version := inspectS3ObjectVersion(versionsDir+"/"+entry.Name, entry)
		version.VersionId = s3api.VersionIdFromEntry(entry)
		version.IsLatest = !nullIsLatest && version.VersionId == latestVersionId
		versions = append(versions, version)
	}
	sort.SliceStable(versions, func(i, j int) bool {
		if versions[i].IsLatest != versions[j].IsLatest {
			return versions[i].IsLatest
		}
		return versions[i].ModifiedAt.After(versions[j].ModifiedAt)
	})
	return versions
}

func inspectS3ObjectVersion(path string, entry *filer_pb.Entry) S3ObjectVersionInspection {
	ext := entry.Extended
	version := S3ObjectVersionInspection{
		VersionId:         string(ext[s3_constants.ExtVersionIdKey]),
		Path:              path,
		IsDeleteMarker:    string(ext[s3_constants.ExtDeleteMarkerKey]) == "true",
		ETag:              strings.Trim(s3api.ObjectETag(entry), `"`),
		ChecksumAlgorithm: string(ext[s3_constants.ExtChecksumAlgorithm]),
		ChecksumValue:     string(ext[s3_constants.ExtChecksumValue]),
		ChecksumType:      string(ext[s3_constants.ExtChecksumType]),
		ObjectLockMode:    string(ext[s3_constants.ExtObjectLockModeKey]),
		LegalHold:         string(ext[s3_constants.ExtLegalHoldKey]),
		InlineBytes:       len(entry.Content),
		Size:              filer.FileSize(entry),
	}
	if attr := entry.Attributes; attr != nil {
		version.ModifiedAt = time.Unix(attr.Mtime, int64(attr.MtimeNs)).UTC()
	}
	if entry.WormEnforcedAtTsNs > 0 {
		t := time.Unix(0, entry.WormEnforcedAtTsNs).UTC()
		version.WormEnforcedAt = &t
	}
	if raw := string(ext[s3_constants.ExtRetentionUntilDateKey]); raw != "" {
		if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
			t := time.Unix(seconds, 0).UTC()
			version.RetainUntil = &t
		}
	}
	if entry.RemoteEntry != nil {
		version.RemoteStorage = entry.RemoteEntry.StorageName
	}
	for _, chunk := range entry.Chunks {
		c := S3ObjectChunkInspection{
			FileId:     chunk.GetFileIdString(),
			Offset:     chunk.Offset,
			Size:       chunk.Size,
			IsManifest: chunk.IsChunkManifest,
		}
		if chunk.Fid != nil {
			c.VolumeId = chunk.Fid.VolumeId
		} else if fid, err := filer_pb.ToFileIdObject(chunk.FileId); err == nil {
			c.VolumeId = fid.VolumeId
		}
		version.Chunks = append(version.Chunks, c)
	}
	return version
}

func s3ObjectVolumeIds(versions []S3ObjectVersionInspection) []uint32 {
	seen := make(map[uint32]bool)
	var ids []uint32
	for _, version := range versions {
		for _, chunk := range version.Chunks {
			if chunk.VolumeId == 0 || seen[chunk.VolumeId] {
				continue
			}
			seen[chunk.VolumeId] = true
			ids = append(ids, chunk.VolumeId)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func summarizeS3ObjectVolumes(volumeIds []uint32, topology *master_pb.TopologyInfo) []S3ObjectVolumeHealth {
	health := make(map[uint32]*S3ObjectVolumeHealth, len(volumeIds))
	for _, id := range volumeIds {
		health[id] = &S3ObjectVolumeHealth{VolumeId: id, Kind: s3ObjectVolumeMissing}
	}
	ecRuns := make(map[uint32]map[int64]*s3ObjectEcRun)
	if topology != nil {
		for _, dc := range topology.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for _, disk := range node.DiskInfos {
						for _, v := range disk.VolumeInfos {
							h := health[v.Id]
							if h == nil {
								continue
							}
							h.Kind = s3ObjectVolumeReplicated
							h.Collection = v.Collection
							h.Servers = append(h.Servers, node.Id)
							h.CopiesPresent++
							h.ReadOnly = h.ReadOnly || v.ReadOnly
							if rp, err := super_block.NewReplicaPlacementFromByte(byte(v.ReplicaPlacement)); err == nil {
								h.CopiesExpected = rp.GetCopyCount()
							}
						}
						for _, ec := range disk.EcShardInfos {
							h := health[ec.Id]
							if h == nil || h.Kind == s3ObjectVolumeReplicated {
								continue
							}
							h.Kind = s3ObjectVolumeEc
							h.Collection = ec.Collection
							if ecRuns[ec.Id] == nil {
								ecRuns[ec.Id] = make(map[int64]*s3ObjectEcRun)
							}
							run := ecRuns[ec.Id][ec.EncodeTsNs]
							if run == nil {
								run = &s3ObjectEcRun{}
								ecRuns[ec.Id][ec.EncodeTsNs] = run
							}
							run.servers = append(run.servers, node.Id)
							run.dataShards = erasure_coding.EcShardsVolumeDataShards(ec)
							run.totalShards = run.dataShards + erasure_coding.EcShardsVolumeParityShards(ec)
							run.bits |= erasure_coding.ShardBits(ec.EcIndexBits)
						}
					}
				}
			}
		}
	}
	result := make([]S3ObjectVolumeHealth, 0, len(volumeIds))
	for _, id := range volumeIds {
		h := health[id]
		switch h.Kind {
		case s3ObjectVolumeReplicated:
			h.Healthy = h.CopiesPresent >= h.CopiesExpected
		case s3ObjectVolumeEc:
			run := currentS3ObjectEcRun(ecRuns[id])
			h.Servers = run.servers
			h.EcDataShards = run.dataShards
			h.EcTotalShards = run.totalShards
			bits := run.bits
			for shardId := 0; shardId < h.EcTotalShards; shardId++ {
				if bits.Has(erasure_coding.ShardId(shardId)) {
					h.EcShardsPresent++
				} else {
					h.EcMissingShards = append(h.EcMissingShards, uint32(shardId))
				}
			}
			h.Healthy = h.EcShardsPresent >= h.EcDataShards
		}
		sort.Strings(h.Servers)
		result = append(result, *h)
	}
	return result
}

type s3ObjectEcRun struct {
	bits        erasure_coding.ShardBits
	dataShards  int
	totalShards int
	servers     []string
}

// currentS3ObjectEcRun returns the newest encode run of a volume's EC shards,
// joined by shards whose server reports no encode run.
func currentS3ObjectEcRun(runs map[int64]*s3ObjectEcRun) s3ObjectEcRun {
	var newest int64
	for encodeTsNs := range runs {
		newest = max(newest, encodeTsNs)
	}
	current := *runs[newest]
	if unknown := runs[0]; newest != 0 && unknown != nil {
		current.bits |= unknown.bits
		current.servers = append(append([]string(nil), current.servers...), unknown.servers...)
	}
	return current
}

type S3ObjectInspectorData struct {
	Username   string              `json:"username"`
	Bucket     string              `json:"bucket"`
	Key        string              `json:"key"`
	Inspection *S3ObjectInspection `json:"inspection,omitempty"`
	Error      string              `json:"error,omitempty"`
}

func (s *AdminServer) GetS3ObjectInspectorData(ctx context.Context, bucket, key string) S3ObjectInspectorData {
	data := S3ObjectInspectorData{Bucket: bucket, Key: key}
	if key == "" {
		return data
	}
	if err := validateInspectedS3Object(bucket, key); err != nil {
		data.Error = err.Error()
		return data
	}
	inspection, err := s.InspectS3Object(ctx, bucket, strings.TrimPrefix(key, "/"))
	if err != nil {
		data.Error = err.Error()
		return data
	}
	data.Inspection = inspection
	return data
}
