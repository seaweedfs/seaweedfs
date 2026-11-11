package command

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

type kvOnlyStore struct {
	filer.FilerStore
	kv     map[string][]byte
	getErr error
}

func (s *kvOnlyStore) KvGet(_ context.Context, key []byte) ([]byte, error) {
	if s.getErr != nil && string(key) == string(MetaBackupKey) {
		return nil, s.getErr
	}
	value, found := s.kv[string(key)]
	if !found {
		return nil, filer.ErrKvNotFound
	}
	return value, nil
}

func (s *kvOnlyStore) KvPut(_ context.Context, key []byte, value []byte) error {
	s.kv[string(key)] = value
	return nil
}

func (s *kvOnlyStore) KvDelete(_ context.Context, key []byte) error {
	delete(s.kv, string(key))
	return nil
}

// TestCopyMetadataStreamsFromAnUnfinishedWalk: a walk only inserts, so a walk
// that restarts after an interrupted one must stream from the first walk's
// start, or the deletes between the two walks never reach the backup.
func TestCopyMetadataStreamsFromAnUnfinishedWalk(t *testing.T) {
	backup := &FilerMetaBackupOptions{store: &kvOnlyStore{kv: map[string][]byte{}}}

	beforeFirst := time.Now()
	interrupted := errors.New("killed")
	if err := backup.copyMetadata(func() error { return interrupted }); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted walk: err = %v, want %v", err, interrupted)
	}
	if _, err := backup.getOffset(); !errors.Is(err, filer.ErrKvNotFound) {
		t.Fatalf("offset after an interrupted walk: err = %v, want none saved", err)
	}
	firstStart, err := backup.getTime(MetaBackupWalkKey)
	if err != nil || firstStart.Before(beforeFirst) {
		t.Fatalf("walk start = %v, %v; want the first walk's start", firstStart, err)
	}

	time.Sleep(time.Millisecond)
	if err := backup.copyMetadata(func() error { return interrupted }); err == nil {
		t.Fatal("second interrupted walk: err = nil")
	}
	if got, _ := backup.getTime(MetaBackupWalkKey); !got.Equal(firstStart) {
		t.Fatalf("walk start after a second interruption = %v, want the first walk's %v", got, firstStart)
	}

	if err := backup.copyMetadata(func() error { return nil }); err != nil {
		t.Fatalf("finished walk: %v", err)
	}
	offset, err := backup.getOffset()
	if err != nil || !offset.Equal(firstStart) {
		t.Fatalf("offset = %v, %v; want the first walk's start %v", offset, err, firstStart)
	}
	if _, err := backup.getTime(MetaBackupWalkKey); !errors.Is(err, filer.ErrKvNotFound) {
		t.Fatalf("walk start after a finished walk: err = %v, want it cleared", err)
	}

	beforeRestart := time.Now()
	if err := backup.copyMetadata(func() error { return nil }); err != nil {
		t.Fatalf("walk after a finished one: %v", err)
	}
	if offset, _ := backup.getOffset(); offset.Before(beforeRestart) {
		t.Fatalf("offset = %v, want a fresh walk's start after %v", offset, beforeRestart)
	}
}

// TestCopyMetadataIgnoresAWalkStartTheStreamPassed: a walk start left behind
// (a crash between saving the offset and clearing it, or an interrupted
// -restart followed by a plain restart) only matters until the stream saves an
// offset past it; after that a new walk starts fresh.
func TestCopyMetadataIgnoresAWalkStartTheStreamPassed(t *testing.T) {
	backup := &FilerMetaBackupOptions{store: &kvOnlyStore{kv: map[string][]byte{}}}
	walkStart := time.Now().Add(-time.Hour)
	if err := backup.setTime(MetaBackupWalkKey, walkStart); err != nil {
		t.Fatal(err)
	}

	if err := backup.setOffset(walkStart); err != nil {
		t.Fatal(err)
	}
	if got, err := backup.walkStartTime(); err != nil || !got.Equal(walkStart) {
		t.Fatalf("walk start with the offset at it = %v, %v; want %v", got, err, walkStart)
	}

	if err := backup.setOffset(walkStart.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	store := backup.store.(*kvOnlyStore)
	store.getErr = errors.New("connection reset")
	if _, err := backup.walkStartTime(); !errors.Is(err, store.getErr) {
		t.Fatalf("walk start with an unreadable offset: err = %v, want the read error", err)
	}
	store.getErr = nil
	beforeWalk := time.Now()
	if err := backup.copyMetadata(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if offset, _ := backup.getOffset(); offset.Before(beforeWalk) {
		t.Fatalf("offset = %v, want a fresh walk's start after %v", offset, beforeWalk)
	}
}

// TestCopyMetadataResumesOnlyItsOwnWalk: the saved walk path belongs to the
// walk whose start the stream replays from; a fresh walk must not skip ahead.
func TestCopyMetadataResumesOnlyItsOwnWalk(t *testing.T) {
	backup := &FilerMetaBackupOptions{store: &kvOnlyStore{kv: map[string][]byte{}}}
	walkedTo := func() string {
		value, err := backup.store.KvGet(context.Background(), MetaBackupWalkPathKey)
		if errors.Is(err, filer.ErrKvNotFound) {
			return ""
		}
		if err != nil {
			t.Fatal(err)
		}
		return string(value)
	}

	interrupted := errors.New("killed")
	if err := backup.copyMetadata(func() error {
		return errors.Join(backup.store.KvPut(context.Background(), MetaBackupWalkPathKey, []byte("/a/b")), interrupted)
	}); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted walk: err = %v", err)
	}
	if err := backup.copyMetadata(func() error {
		if got := walkedTo(); got != "/a/b" {
			t.Fatalf("resumed walk sees %q, want /a/b", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := walkedTo(); got != "" {
		t.Fatalf("walk path after a finished walk = %q, want it cleared", got)
	}

	if err := backup.setTime(MetaBackupWalkKey, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := backup.store.KvPut(context.Background(), MetaBackupWalkPathKey, []byte("/x")); err != nil {
		t.Fatal(err)
	}
	if err := backup.copyMetadata(func() error {
		if got := walkedTo(); got != "" {
			t.Fatalf("fresh walk sees %q left by a walk the stream passed", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
