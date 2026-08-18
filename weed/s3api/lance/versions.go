package lance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// versionsDirName holds the catalog's record of a table's versions, next to the
// dataset's own _versions/ rather than inside it: the dataset stays portable,
// and this directory only orders the commits that produced it.
const versionsDirName = "_lance_versions"

// versionEntryWidth zero-pads version entry names so a filer listing, which is
// lexicographic, is also numeric.
const versionEntryWidth = 20

type TableVersion struct {
	Version         int64             `json:"version"`
	ManifestPath    string            `json:"manifest_path"`
	ManifestSize    int64             `json:"manifest_size,omitempty"`
	ETag            string            `json:"e_tag,omitempty"`
	TimestampMillis int64             `json:"timestamp_millis,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	NamingScheme    string            `json:"naming_scheme,omitempty"`
}

type CreateTableVersionRequest struct {
	ID           []string          `json:"id,omitempty"`
	Version      int64             `json:"version"`
	ManifestPath string            `json:"manifest_path"`
	ManifestSize int64             `json:"manifest_size,omitempty"`
	ETag         string            `json:"e_tag,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	NamingScheme string            `json:"naming_scheme,omitempty"`
	Branch       string            `json:"branch,omitempty"`
}

type CreateTableVersionResponse struct {
	Version TableVersion `json:"version"`
}

type ListTableVersionsRequest struct {
	ID         []string `json:"id,omitempty"`
	PageToken  string   `json:"page_token,omitempty"`
	Limit      *int     `json:"limit,omitempty"`
	Descending bool     `json:"descending,omitempty"`
	Branch     string   `json:"branch,omitempty"`
}

type ListTableVersionsResponse struct {
	Versions  []TableVersion `json:"versions"`
	PageToken string         `json:"page_token,omitempty"`
}

type DescribeTableVersionRequest struct {
	ID      []string `json:"id,omitempty"`
	Version int64    `json:"version"`
	Branch  string   `json:"branch,omitempty"`
}

type DescribeTableVersionResponse struct {
	Version TableVersion `json:"version"`
}

type VersionRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type BatchDeleteTableVersionsRequest struct {
	ID     []string       `json:"id,omitempty"`
	Ranges []VersionRange `json:"ranges"`
	Branch string         `json:"branch,omitempty"`
}

type BatchDeleteTableVersionsResponse struct {
	Deleted int `json:"deleted"`
}

func versionEntryName(version int64) string {
	return fmt.Sprintf("%0*d", versionEntryWidth, version)
}

func versionFromEntryName(name string) (int64, bool) {
	parsed, err := strconv.ParseInt(name, 10, 64)
	if err != nil || len(name) != versionEntryWidth {
		return 0, false
	}
	return parsed, true
}

func (s *Server) versionsDir(location string) string {
	dir := datasetDir(location)
	if dir == "" {
		return ""
	}
	return path.Join(dir, versionsDirName)
}

// reserveVersion claims a version slot, failing if another writer already holds
// it. The exclusive create is the whole point of managed versioning: it is a
// real put-if-not-exists, which the S3 conditional headers in front of the same
// filer do not give a caller.
func (s *Server) reserveVersion(r *http.Request, location string, version TableVersion) error {
	dir := s.versionsDir(location)
	if dir == "" {
		return fmt.Errorf("table has no usable location")
	}
	body, err := json.Marshal(version)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// The directory is created on the first commit; a race to create it is
		// harmless because only the version entry below has to be exclusive.
		parent, name := path.Split(dir)
		if _, err := client.CreateEntry(r.Context(), &filer_pb.CreateEntryRequest{
			Directory: path.Clean(parent),
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{Mtime: now, Crtime: now, FileMode: uint32(0755 | os.ModeDir)},
			},
		}); err != nil {
			glog.V(2).Infof("lance: could not ensure %s: %v", dir, err)
		}
		resp, err := client.CreateEntry(r.Context(), &filer_pb.CreateEntryRequest{
			Directory: dir,
			OExcl:     true,
			Entry: &filer_pb.Entry{
				Name:    versionEntryName(version.Version),
				Content: body,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    now,
					Crtime:   now,
					FileMode: uint32(0644),
					FileSize: uint64(len(body)),
				},
			},
		})
		if err != nil {
			return err
		}
		if resp.ErrorCode != filer_pb.FilerError_OK {
			if sentinel := filer_pb.FilerErrorToSentinel(resp.ErrorCode); sentinel != nil {
				return sentinel
			}
			return fmt.Errorf("reserve version: %s", resp.Error)
		}
		if resp.Error != "" {
			return fmt.Errorf("reserve version: %s", resp.Error)
		}
		return nil
	})
}

func (s *Server) readVersion(r *http.Request, location string, version int64) (*TableVersion, error) {
	dir := s.versionsDir(location)
	if dir == "" {
		return nil, filer_pb.ErrNotFound
	}
	var found *TableVersion
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      versionEntryName(version),
		})
		if err != nil {
			return err
		}
		var decoded TableVersion
		if err := json.Unmarshal(resp.Entry.Content, &decoded); err != nil {
			return fmt.Errorf("version %d record is unreadable: %w", version, err)
		}
		found = &decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (s *Server) listVersions(r *http.Request, location string) ([]TableVersion, error) {
	dir := s.versionsDir(location)
	if dir == "" {
		return nil, nil
	}
	var versions []TableVersion
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.SeaweedList(r.Context(), client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				return nil
			}
			if _, ok := versionFromEntryName(entry.Name); !ok {
				return nil
			}
			var decoded TableVersion
			if err := json.Unmarshal(entry.Content, &decoded); err != nil {
				glog.V(2).Infof("lance: ignoring unreadable version record %s: %v", entry.Name, err)
				return nil
			}
			versions = append(versions, decoded)
			return nil
		}, "", false, 0)
	})
	if err != nil {
		// A table that has never committed through the namespace has no
		// directory, which is an empty list rather than a failure.
		if err == filer_pb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Version < versions[j].Version })
	return versions, nil
}

func (s *Server) deleteVersion(r *http.Request, location string, version int64) error {
	dir := s.versionsDir(location)
	if dir == "" {
		return nil
	}
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(r.Context(), client, dir, versionEntryName(version), true, false, true, false, nil)
	})
}

// handleCreateTableVersion reserves a version slot for a commit.
func (s *Server) handleCreateTableVersion(w http.ResponseWriter, r *http.Request) {
	table, req, ok := resolveVersionTable[CreateTableVersionRequest](s, w, r, func(v *CreateTableVersionRequest) []string { return v.ID })
	if !ok {
		return
	}
	if req.ManifestPath == "" {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "manifest_path is required")
		return
	}

	version := TableVersion{
		Version:         req.Version,
		ManifestPath:    req.ManifestPath,
		ManifestSize:    req.ManifestSize,
		ETag:            req.ETag,
		TimestampMillis: time.Now().UnixMilli(),
		Metadata:        req.Metadata,
		NamingScheme:    req.NamingScheme,
	}
	if err := s.reserveVersion(r, table.location, version); err != nil {
		// Losing the race is the expected outcome for one of two concurrent
		// writers, and the caller rebases on it.
		writeError(w, r, http.StatusConflict, codeConcurrentModification,
			fmt.Sprintf("version %d is already committed", req.Version))
		return
	}
	writeJSON(w, http.StatusOK, CreateTableVersionResponse{Version: version})
}

// handleListTableVersions lists the versions the namespace has recorded.
func (s *Server) handleListTableVersions(w http.ResponseWriter, r *http.Request) {
	table, req, ok := resolveVersionTable[ListTableVersionsRequest](s, w, r, func(v *ListTableVersionsRequest) []string { return v.ID })
	if !ok {
		return
	}
	versions, err := s.listVersions(r, table.location)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}
	if req.Descending {
		for i, j := 0, len(versions)-1; i < j; i, j = i+1, j-1 {
			versions[i], versions[j] = versions[j], versions[i]
		}
	}
	if req.Limit != nil && *req.Limit > 0 && len(versions) > *req.Limit {
		versions = versions[:*req.Limit]
	}
	if versions == nil {
		versions = []TableVersion{}
	}
	writeJSON(w, http.StatusOK, ListTableVersionsResponse{Versions: versions})
}

// handleDescribeTableVersion returns one recorded version.
func (s *Server) handleDescribeTableVersion(w http.ResponseWriter, r *http.Request) {
	table, req, ok := resolveVersionTable[DescribeTableVersionRequest](s, w, r, func(v *DescribeTableVersionRequest) []string { return v.ID })
	if !ok {
		return
	}
	version, err := s.readVersion(r, table.location, req.Version)
	if err != nil {
		writeError(w, r, http.StatusNotFound, codeTableVersionNotFound,
			fmt.Sprintf("version %d is not recorded for this table", req.Version))
		return
	}
	writeJSON(w, http.StatusOK, DescribeTableVersionResponse{Version: *version})
}

// handleBatchDeleteTableVersions drops version records in the given ranges. The
// manifests themselves belong to the dataset and are left alone.
func (s *Server) handleBatchDeleteTableVersions(w http.ResponseWriter, r *http.Request) {
	table, req, ok := resolveVersionTable[BatchDeleteTableVersionsRequest](s, w, r, func(v *BatchDeleteTableVersionsRequest) []string { return v.ID })
	if !ok {
		return
	}
	if len(req.Ranges) == 0 {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "ranges is required")
		return
	}
	versions, err := s.listVersions(r, table.location)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}

	deleted := 0
	for _, version := range versions {
		for _, span := range req.Ranges {
			if version.Version < span.Start || version.Version >= span.End {
				continue
			}
			if err := s.deleteVersion(r, table.location, version.Version); err != nil {
				writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
				return
			}
			deleted++
			break
		}
	}
	writeJSON(w, http.StatusOK, BatchDeleteTableVersionsResponse{Deleted: deleted})
}

// resolveVersionTable decodes a version request body, reconciles its identifier
// with the route and loads the table, so each handler above starts from a table
// it is allowed to touch.
func resolveVersionTable[T any](s *Server, w http.ResponseWriter, r *http.Request, bodyID func(*T) []string) (*lanceTable, *T, bool) {
	req := new(T)
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return nil, nil, false
	}
	if err := decodeBody(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return nil, nil, false
	}
	if len(id) == 0 {
		id = bodyID(req)
	}
	if !checkBodyIdentifier(w, r, id, bodyID(req)) {
		return nil, nil, false
	}
	if !s.managedVersioning {
		writeError(w, r, http.StatusNotImplemented, codeUnsupported,
			"managed versioning is off; the dataset owns its versions")
		return nil, nil, false
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return nil, nil, false
	}
	table, err := s.loadLanceTable(r, bucket, ns, name)
	if err != nil {
		writeStorageError(w, r, err)
		return nil, nil, false
	}
	return table, req, true
}
