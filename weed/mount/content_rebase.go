package mount

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A flush owns its pending content until publication succeeds. An attribute
// race is retried against the latest entry, never by replacing the guard alone.
// Callers hold the handle's active lock before touching its live entry/base.
func (wfs *WFS) publishPendingContent(ctx context.Context, fh *FileHandle, req *filer_pb.CreateEntryRequest) (result *filer_pb.CreateEntryResponse, resultErr error) {
	// The handle was bound before any async work or publication-lock wait.
	// Never recapture here: Forget may already have started a new lifetime.
	failure := fh.writebackContentError
	if wfs.option.WritebackCache && (failure == nil || failure.Load()) {
		return nil, fmt.Errorf("writeback inode awaits Forget after content refusal: %w", filer_pb.ErrPreconditionFailed)
	}
	defer func() {
		if failure != nil && errors.Is(resultErr, filer_pb.ErrPreconditionFailed) {
			failure.Store(true)
		}
	}()
	base := fh.baseEntry.Load()
	if base != nil {
		base = proto.Clone(base).(*filer_pb.Entry)
		wfs.mapPbIdFromLocalToFiler(base)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	budgetError := func(cause error) error {
		if cause != nil {
			return cause
		}
		return ctx.Err()
	}
	var resp *filer_pb.CreateEntryResponse
	var err error
	for {
		if ctx.Err() != nil {
			return resp, budgetError(err)
		}
		var storedEOF uint64
		if base != nil {
			storedEOF = filer.FileSize(base)
		}
		// The conditional write below makes this EOF check atomic with the
		// stored version. Check the original WRITE frontier, not a manifest's
		// bounding range, which could conceal an abandoned append's gap.
		if !wfs.option.WritebackCache && fh.pendingAppend && fh.pendingAppendOffset > storedEOF {
			return nil, fmt.Errorf("append offset %d exceeds committed EOF %d: %w", fh.pendingAppendOffset, storedEOF, filer_pb.ErrPreconditionFailed)
		}
		clause := &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}
		if base != nil {
			clause.Kind, clause.ExpectedEntry = filer_pb.WriteCondition_IF_ENTRY_EQUAL, base
		}
		req.Condition = &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{clause}}
		var refusal error
		err = wfs.withFilerClient(ctx, false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			var callErr error
			resp, callErr = filer_pb.CreateEntryWithResponse(ctx, client, req)
			if errors.Is(callErr, filer_pb.ErrPreconditionFailed) {
				// A provider refusal is final for this attempt, not a transport
				// failure to replay on another filer address.
				refusal = callErr
				return nil
			}
			return callErr
		})
		if err == nil {
			err = refusal
		}
		if err == nil {
			return resp, nil
		}
		if ctx.Err() != nil {
			return resp, budgetError(err)
		}
		if !errors.Is(err, filer_pb.ErrPreconditionFailed) || base == nil {
			return resp, err
		}
		var latest *filer_pb.Entry
		lookupErr := wfs.withFilerClient(ctx, false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			found, e := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: req.Directory, Name: req.Entry.Name})
			if errors.Is(e, filer_pb.ErrNotFound) || status.Code(e) == codes.NotFound {
				return nil
			}
			if e == nil {
				latest = found.Entry
			}
			return e
		})
		if lookupErr != nil {
			return resp, lookupErr
		}
		if !sameContentIdentity(base, latest) && !appendCompatible(base, latest, req.Entry) {
			return resp, fmt.Errorf("content conflict during rebase: %w", err)
		}
		if samePublicationVersion(base, latest) {
			return resp, fmt.Errorf("content rebase made no progress: %w", err)
		}
		rebaseContentAttributes(req.Entry, base, latest)
		base = latest
	}
}

// Match the filer's serialized-entry comparison, including the commit stamp.
// A representation-only fid change is not progress towards a new CAS guard.
func samePublicationVersion(a, b *filer_pb.Entry) bool {
	left, right := proto.Clone(a).(*filer_pb.Entry), proto.Clone(b).(*filer_pb.Entry)
	filer_pb.BeforeEntrySerialization(left.Chunks)
	filer_pb.BeforeEntrySerialization(right.Chunks)
	return proto.Equal(filer.FromPbEntry("", left).ToProtoEntry(), filer.FromPbEntry("", right).ToProtoEntry())
}

// A stored prefix is safe only along the same append chain: the observed
// content is itself a prefix of the stored content, which is a prefix of ours.
// Never shift a pending offset or undo a peer truncate/replacement.
func appendCompatible(base, stored, pending *filer_pb.Entry) bool {
	if base == nil || stored == nil || pending == nil || stored.Attributes == nil || pending.Attributes == nil {
		return false
	}
	identity := proto.Clone(stored).(*filer_pb.Entry)
	identity.Chunks = proto.Clone(base).(*filer_pb.Entry).Chunks
	identity.Attributes.FileSize = base.GetAttributes().GetFileSize()
	if !sameContentIdentity(base, identity) || filer.FileSize(stored) < filer.FileSize(base) || filer.FileSize(pending) < filer.FileSize(stored) {
		return false
	}
	return chunkPrefix(base.Chunks, stored.Chunks) && chunkPrefix(stored.Chunks, pending.Chunks)
}

func chunkPrefix(prefix, chunks []*filer_pb.FileChunk) bool {
	if len(prefix) > len(chunks) {
		return false
	}
	for i, c := range prefix {
		a, b := proto.Clone(c).(*filer_pb.FileChunk), proto.Clone(chunks[i]).(*filer_pb.FileChunk)
		filer_pb.BeforeEntrySerialization([]*filer_pb.FileChunk{a, b})
		if !proto.Equal(a, b) {
			return false
		}
	}
	return true
}

// Only attribute differences may be reconciled here. A replacement, truncate,
// append, remote-content change or changed chunk descriptor stays a conflict.
func sameContentIdentity(base, latest *filer_pb.Entry) bool {
	if base == nil || latest == nil || base.GetAttributes().GetInode() == 0 || base.GetAttributes().GetInode() != latest.GetAttributes().GetInode() {
		return false
	}
	a, b := proto.Clone(base).(*filer_pb.Entry), proto.Clone(latest).(*filer_pb.Entry)
	rebaseContentAttributes(a, base, latest)
	for _, e := range []*filer_pb.Entry{a, b} {
		e.Name, e.CommitTsNs = "", 0
		filer_pb.BeforeEntrySerialization(e.Chunks)
		e.Attributes.FileSize = filer.FileSize(e)
	}
	return proto.Equal(a, b)
}

func rebaseField[T comparable](pending *T, base, latest T) {
	if latest != base {
		*pending = latest
	}
}

// Preserve local content and local attribute edits that the peer did not
// touch. A peer's acknowledged attribute wins when both changed that field.
func rebaseContentAttributes(pending, base, latest *filer_pb.Entry) {
	p, b, n := pending.Attributes, base.Attributes, latest.Attributes
	if p == nil || b == nil || n == nil {
		return
	}
	rebaseField(&p.FileMode, b.FileMode, n.FileMode)
	rebaseField(&p.Uid, b.Uid, n.Uid)
	rebaseField(&p.Gid, b.Gid, n.Gid)
	if b.Mtime != n.Mtime || b.MtimeNs != n.MtimeNs {
		p.Mtime, p.MtimeNs = n.Mtime, n.MtimeNs
	}
	if b.Ctime != n.Ctime || b.CtimeNs != n.CtimeNs {
		p.Ctime, p.CtimeNs = n.Ctime, n.CtimeNs
	}
	if b.Atime != n.Atime || b.AtimeNs != n.AtimeNs {
		p.Atime, p.AtimeNs = n.Atime, n.AtimeNs
	}
	keys := make(map[string]bool)
	for k := range base.Extended {
		keys[k] = true
	}
	for k := range latest.Extended {
		keys[k] = true
	}
	for k := range keys {
		old, hadOld := base.Extended[k]
		value, exists := latest.Extended[k]
		if hadOld == exists && bytes.Equal(old, value) {
			continue
		}
		if !exists {
			delete(pending.Extended, k)
			continue
		}
		if pending.Extended == nil {
			pending.Extended = make(map[string][]byte)
		}
		pending.Extended[k] = append([]byte(nil), value...)
	}
}

func (wfs *WFS) installContentAcknowledgement(fh *FileHandle, req *filer_pb.CreateEntryRequest, resp *filer_pb.CreateEntryResponse) {
	acked := resp.GetMetadataEvent().GetEventNotification().GetNewEntry()
	if acked == nil {
		acked = req.Entry
	}
	acked = proto.Clone(acked).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(acked)
	fh.setAuthoritativeBase(proto.Clone(acked).(*filer_pb.Entry))
	fh.UpdateEntry(func(live *filer_pb.Entry) {
		size := filer.FileSize(live)
		live.Attributes = proto.Clone(acked.Attributes).(*filer_pb.FuseAttributes)
		if size > live.Attributes.FileSize {
			live.Attributes.FileSize = size
		}
		live.Extended = proto.Clone(acked).(*filer_pb.Entry).Extended
		live.CommitTsNs = acked.CommitTsNs
	})
}
