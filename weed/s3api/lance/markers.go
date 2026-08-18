package lance

import (
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// Marker files the Lance Directory Catalog reads. Writing them beside the
// dataset keeps a client that bypasses this namespace, and lists the storage
// prefix directly, seeing the same table states the catalog reports.
const (
	reservedMarker     = ".lance-reserved"
	deregisteredMarker = ".lance-deregistered"
	versionsDir        = "_versions"
)

// datasetDir maps a table location onto the filer directory holding it.
func datasetDir(location string) string {
	return s3tables.TableDataDirFromMetadataLocation(location)
}

func (s *Server) entryExists(r *http.Request, dir, name string) (bool, error) {
	found := false
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, lookupErr := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr == filer_pb.ErrNotFound {
			return nil
		}
		if lookupErr != nil {
			return lookupErr
		}
		found = true
		return nil
	})
	return found, err
}

// writeMarker drops a zero-length marker into the dataset directory. The
// directory has to exist already: a table declared through this namespace has
// one, and a table registered at a location that does not yet exist has no
// storage to mark.
func (s *Server) writeMarker(r *http.Request, location, name string) error {
	dir := datasetDir(location)
	if dir == "" {
		return nil
	}
	now := time.Now().Unix()
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.CreateEntry(r.Context(), client, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name: name,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    now,
					Crtime:   now,
					FileMode: uint32(0644),
				},
			},
		})
	})
}

func (s *Server) removeMarker(r *http.Request, location, name string) error {
	dir := datasetDir(location)
	if dir == "" {
		return nil
	}
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(r.Context(), client, dir, name, true, false, true, false, nil)
	})
}

// datasetState reports the two things the spec asks about a table's storage:
// whether it has been deregistered, and whether it holds data yet. A dataset
// holds data once it has a version manifest, which is how a directory catalog
// decides the same question.
func (s *Server) datasetState(r *http.Request, location string) (deregistered, hasData bool, err error) {
	dir := datasetDir(location)
	if dir == "" {
		return false, false, nil
	}
	if deregistered, err = s.entryExists(r, dir, deregisteredMarker); err != nil {
		return false, false, err
	}
	hasData, err = s.entryExists(r, dir, versionsDir)
	return deregistered, hasData, err
}
