package filer

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// deleteExpiredEntry removes an entry a listing found expired. The row is
// re-read under the lock writers take, so a replacement written after the
// listing read the row is kept; a contended path is left to a later listing.
func (f *Filer) deleteExpiredEntry(ctx context.Context, p util.FullPath) error {
	if lt := f.EntryLockTable; lt != nil {
		lock, acquired := lt.TryAcquireLock("deleteExpiredEntry", p, util.ExclusiveLock)
		if !acquired {
			return nil
		}
		defer lt.ReleaseLock(p, lock)
	}
	if !f.RefusesConflictingCommits(p) || ctx.Value(storeTransactionKey{}) != nil {
		return f.deleteIfExpired(ctx, p)
	}
	txCtx, err := f.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	if err = f.deleteIfExpired(txCtx, p); err == nil {
		err = f.CommitTransaction(txCtx)
	}
	if err != nil {
		f.RollbackTransaction(txCtx)
		if errors.Is(err, ErrTransactionConflict) {
			return nil
		}
	}
	return err
}

func (f *Filer) deleteIfExpired(ctx context.Context, p util.FullPath) error {
	current, err := f.Store.FindEntry(ctx, p)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		return err
	}
	if current.IsDirectory() || current.TtlSec <= 0 || current.IsExpireS3Enabled() || !current.Crtime.Add(time.Duration(current.TtlSec)*time.Second).Before(time.Now()) {
		return nil
	}
	return f.Store.DeleteOneEntry(ctx, current)
}
