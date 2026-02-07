package s3tables

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

var (
	ErrAttributeNotFound = errors.New("attribute not found")
)

// Filer operations - Common functions for interacting with the filer

// createDirectory creates a new directory at the specified path
func (h *S3TablesHandler) createDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	now := time.Now().Unix()
	_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    now,
				Crtime:   now,
				FileMode: uint32(0755 | os.ModeDir), // Directory mode
			},
		},
	})
	return err
}

// ensureDirectory ensures a directory exists at the specified path
func (h *S3TablesHandler) ensureDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, filer_pb.ErrNotFound) {
		return h.createDirectory(ctx, client, path)
	}
	return err
}

// upsertFile creates or updates a small file with the given content
func (h *S3TablesHandler) upsertFile(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, data []byte) error {
	dir, name := splitPath(path)
	now := time.Now().Unix()
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		if !errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}
		_, err = client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:    name,
				Content: data,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    now,
					Crtime:   now,
					FileMode: uint32(0644),
					FileSize: uint64(len(data)),
				},
			},
		})
		return err
	}

	entry := resp.Entry
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	entry.Attributes.Mtime = now
	entry.Attributes.FileSize = uint64(len(data))
	entry.Content = data
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: dir,
		Entry:     entry,
	})
	return err
}

// deleteEntryIfExists removes an entry if it exists, ignoring missing errors
func (h *S3TablesHandler) deleteEntryIfExists(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	return filer_pb.DoRemove(ctx, client, dir, name, true, false, true, false, nil)
}

// setExtendedAttribute sets an extended attribute on an existing entry
func (h *S3TablesHandler) setExtendedAttribute(ctx context.Context, client filer_pb.SeaweedFilerClient, path, key string, data []byte) error {
	dir, name := splitPath(path)

	// First, get the existing entry
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return err
	}

	entry := resp.Entry

	// Update the extended attributes
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	entry.Extended[key] = data

	// Save the updated entry
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: dir,
		Entry:     entry,
	})
	return err
}

// getExtendedAttribute gets an extended attribute from an entry
func (h *S3TablesHandler) getExtendedAttribute(ctx context.Context, client filer_pb.SeaweedFilerClient, path, key string) ([]byte, error) {
	dir, name := splitPath(path)
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}

	if resp.Entry.Extended == nil {
		return nil, fmt.Errorf("%w: %s", ErrAttributeNotFound, key)
	}

	data, ok := resp.Entry.Extended[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAttributeNotFound, key)
	}

	return data, nil
}

// deleteExtendedAttribute deletes an extended attribute from an entry
func (h *S3TablesHandler) deleteExtendedAttribute(ctx context.Context, client filer_pb.SeaweedFilerClient, path, key string) error {
	dir, name := splitPath(path)

	// Get the existing entry
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return err
	}

	entry := resp.Entry

	// Remove the extended attribute
	if entry.Extended != nil {
		delete(entry.Extended, key)
	}

	// Save the updated entry
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: dir,
		Entry:     entry,
	})
	return err
}

// deleteDirectory deletes a directory and all its contents
func (h *S3TablesHandler) deleteDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory:            dir,
		Name:                 name,
		IsDeleteData:         true,
		IsRecursive:          true,
		IgnoreRecursiveError: true,
	})
	return err
}

// entryExists checks if an entry exists at the given path
func (h *S3TablesHandler) entryExists(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) bool {
	dir, name := splitPath(path)
	_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	return err == nil
}
