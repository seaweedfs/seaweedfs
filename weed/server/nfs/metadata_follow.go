package nfs

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type chunkInvalidator interface {
	UnCache(fileID string)
}

type metadataInvalidation struct {
	path  util.FullPath
	entry *filer_pb.Entry
}

func (s *Server) runMetadataInvalidationLoop(ctx context.Context) {
	if s == nil || s.chunkInvalidator == nil || s.withInternalClient == nil {
		return
	}

	waitTime := time.Second
	for ctx.Err() == nil {
		err := s.followMetadataStream(ctx)
		if err == nil || errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}

		glog.V(0).Infof("retry nfs metadata invalidation stream for %s in %v: %v", s.exportRoot, waitTime, err)

		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if waitTime < util.RetryWaitTime {
			waitTime += waitTime / 2
		}
	}
}

func (s *Server) followMetadataStream(ctx context.Context) error {
	req := &filer_pb.SubscribeMetadataRequest{
		ClientName:             "nfs",
		PathPrefix:             string(s.exportRoot),
		ClientId:               s.signature,
		ClientEpoch:            1,
		ClientSupportsBatching: true,
	}

	return s.withInternalClient(true, func(client nfsFilerClient) error {
		stream, err := client.SubscribeMetadata(ctx, req)
		if err != nil {
			return err
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			s.applyMetadataInvalidationResponse(resp)
		}
	})
}

func (s *Server) applyMetadataInvalidationResponse(resp *filer_pb.SubscribeMetadataResponse) {
	if s == nil || s.chunkInvalidator == nil || resp == nil {
		return
	}

	uncached := make(map[string]struct{})
	apply := func(event *filer_pb.SubscribeMetadataResponse) {
		for _, invalidation := range metadataInvalidationsForEvent(event) {
			if invalidation.entry == nil || !pathVisibleFromExport(invalidation.path, s.exportRoot) {
				continue
			}
			for _, chunk := range invalidation.entry.GetChunks() {
				fileID := chunk.GetFileIdString()
				if fileID == "" {
					continue
				}
				if _, seen := uncached[fileID]; seen {
					continue
				}
				uncached[fileID] = struct{}{}
				s.chunkInvalidator.UnCache(fileID)
			}
		}
	}

	apply(resp)
	for _, event := range resp.Events {
		apply(event)
	}
}

func metadataInvalidationsForEvent(resp *filer_pb.SubscribeMetadataResponse) []metadataInvalidation {
	message := resp.GetEventNotification()
	if message == nil {
		return nil
	}

	var invalidations []metadataInvalidation
	if message.OldEntry != nil && message.NewEntry != nil {
		oldPath := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		invalidations = append(invalidations, metadataInvalidation{path: oldPath, entry: message.OldEntry})

		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		if message.OldEntry.Name != message.NewEntry.Name || resp.Directory != newDir {
			newPath := util.NewFullPath(newDir, message.NewEntry.Name)
			invalidations = append(invalidations, metadataInvalidation{path: newPath, entry: message.NewEntry})
		}
		return invalidations
	}

	if message.NewEntry != nil {
		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		newPath := util.NewFullPath(newDir, message.NewEntry.Name)
		invalidations = append(invalidations, metadataInvalidation{path: newPath, entry: message.NewEntry})
	}

	if message.OldEntry != nil {
		oldPath := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		invalidations = append(invalidations, metadataInvalidation{path: oldPath, entry: message.OldEntry})
	}

	return invalidations
}
