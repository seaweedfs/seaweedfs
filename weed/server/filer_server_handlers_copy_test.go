package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCopyEntryRefreshesDestinationTimestamps(t *testing.T) {
	fs := &FilerServer{}

	oldTime := time.Unix(123, 0)
	srcEntry := &filer.Entry{
		FullPath: util.FullPath("/src.txt"),
		Attr: filer.Attr{
			Mtime:  oldTime,
			Crtime: oldTime,
		},
		Content: []byte("hello"),
	}

	before := time.Now().Add(-time.Second)
	copied, err := fs.copyEntry(context.Background(), srcEntry, util.FullPath("/dst.txt"), nil)
	after := time.Now().Add(time.Second)
	if err != nil {
		t.Fatalf("copyEntry: %v", err)
	}

	if copied.Crtime.Before(before) || copied.Crtime.After(after) {
		t.Fatalf("copied Crtime = %v, want between %v and %v", copied.Crtime, before, after)
	}
	if copied.Mtime.Before(before) || copied.Mtime.After(after) {
		t.Fatalf("copied Mtime = %v, want between %v and %v", copied.Mtime, before, after)
	}
	if copied.Crtime.Equal(oldTime) || copied.Mtime.Equal(oldTime) {
		t.Fatalf("destination timestamps should differ from source timestamps: src=%v copied=(%v,%v)", oldTime, copied.Crtime, copied.Mtime)
	}
}

func TestPreserveDestinationMetadataForDataCopy(t *testing.T) {
	dstTime := time.Unix(100, 0)
	srcTime := time.Unix(200, 0)

	dstEntry := &filer.Entry{
		FullPath: util.FullPath("/dst.txt"),
		Attr: filer.Attr{
			Mtime:         dstTime,
			Crtime:        dstTime,
			Mode:          0100600,
			Uid:           101,
			Gid:           202,
			TtlSec:        17,
			UserName:      "dst-user",
			GroupNames:    []string{"dst-group"},
			SymlinkTarget: "",
			Rdev:          9,
			Inode:         1234,
		},
		Extended: map[string][]byte{
			"user.color": []byte("blue"),
		},
		Remote: &filer_pb.RemoteEntry{
			StorageName: "remote-store",
			RemoteETag:  "remote-etag",
			RemoteSize:  7,
		},
		Quota:              77,
		WORMEnforcedAtTsNs: 88,
		HardLinkId:         filer.HardLinkId([]byte("hard-link")),
		HardLinkCounter:    3,
	}
	copiedEntry := &filer.Entry{
		FullPath: util.FullPath("/dst.txt"),
		Attr: filer.Attr{
			Mtime:      srcTime,
			Crtime:     srcTime,
			Mode:       0100644,
			Uid:        11,
			Gid:        22,
			Mime:       "text/plain",
			Md5:        []byte("source-md5"),
			FileSize:   5,
			UserName:   "src-user",
			GroupNames: []string{"src-group"},
		},
		Content: []byte("hello"),
		Extended: map[string][]byte{
			"user.color": []byte("red"),
		},
		Quota: 5,
	}

	before := time.Now().Add(-time.Second)
	preserveDestinationMetadataForDataCopy(dstEntry, copiedEntry)
	after := time.Now().Add(time.Second)

	if copiedEntry.Mode != dstEntry.Mode || copiedEntry.Uid != dstEntry.Uid || copiedEntry.Gid != dstEntry.Gid {
		t.Fatalf("destination ownership/mode not preserved: got mode=%#o uid=%d gid=%d", copiedEntry.Mode, copiedEntry.Uid, copiedEntry.Gid)
	}
	if copiedEntry.Crtime != dstEntry.Crtime || copiedEntry.Inode != dstEntry.Inode {
		t.Fatalf("destination identity not preserved: got crtime=%v inode=%d", copiedEntry.Crtime, copiedEntry.Inode)
	}
	if copiedEntry.Mtime.Before(before) || copiedEntry.Mtime.After(after) {
		t.Fatalf("destination mtime = %v, want between %v and %v", copiedEntry.Mtime, before, after)
	}
	if copiedEntry.FileSize != 5 || copiedEntry.Mime != "text/plain" || string(copiedEntry.Md5) != "source-md5" {
		t.Fatalf("copied data attributes changed unexpectedly: size=%d mime=%q md5=%q", copiedEntry.FileSize, copiedEntry.Mime, string(copiedEntry.Md5))
	}
	if string(copiedEntry.Extended["user.color"]) != "blue" {
		t.Fatalf("destination xattrs not preserved: got %q", string(copiedEntry.Extended["user.color"]))
	}
	if copiedEntry.Remote == nil || copiedEntry.Remote.StorageName != "remote-store" {
		t.Fatalf("destination remote metadata not preserved: %+v", copiedEntry.Remote)
	}
	if copiedEntry.Quota != 77 || copiedEntry.WORMEnforcedAtTsNs != 88 {
		t.Fatalf("destination quota/WORM not preserved: quota=%d worm=%d", copiedEntry.Quota, copiedEntry.WORMEnforcedAtTsNs)
	}
	if string(copiedEntry.HardLinkId) != "hard-link" || copiedEntry.HardLinkCounter != 3 {
		t.Fatalf("destination hard-link metadata not preserved: id=%q count=%d", string(copiedEntry.HardLinkId), copiedEntry.HardLinkCounter)
	}

	dstEntry.GroupNames[0] = "mutated"
	dstEntry.Extended["user.color"][0] = 'g'
	dstEntry.Remote.StorageName = "mutated-remote"
	if copiedEntry.GroupNames[0] != "dst-group" {
		t.Fatalf("group names should be cloned, got %q", copiedEntry.GroupNames[0])
	}
	if string(copiedEntry.Extended["user.color"]) != "blue" {
		t.Fatalf("extended metadata should be cloned, got %q", string(copiedEntry.Extended["user.color"]))
	}
	if copiedEntry.Remote.StorageName != "remote-store" {
		t.Fatalf("remote metadata should be cloned, got %q", copiedEntry.Remote.StorageName)
	}
}

func TestValidateCopySourcePreconditions(t *testing.T) {
	srcInode := uint64(101)
	srcMtime := int64(200)
	srcSize := int64(5)
	preconditions := copyRequestPreconditions{
		srcInode: &srcInode,
		srcMtime: &srcMtime,
		srcSize:  &srcSize,
	}

	srcEntry := &filer.Entry{
		FullPath: util.FullPath("/src.txt"),
		Attr: filer.Attr{
			Inode:    srcInode,
			Mtime:    time.Unix(srcMtime, 0),
			FileSize: uint64(srcSize),
		},
		Content: []byte("hello"),
	}

	if err := validateCopySourcePreconditions(preconditions, srcEntry); err != nil {
		t.Fatalf("validate source preconditions: %v", err)
	}

	changedSize := int64(6)
	preconditions.srcSize = &changedSize
	if err := validateCopySourcePreconditions(preconditions, srcEntry); err == nil {
		t.Fatal("expected source size mismatch to fail")
	}
}

func TestValidateCopyDestinationPreconditions(t *testing.T) {
	dstInode := uint64(202)
	dstMtime := int64(300)
	dstSize := int64(0)
	preconditions := copyRequestPreconditions{
		dstInode: &dstInode,
		dstMtime: &dstMtime,
		dstSize:  &dstSize,
	}

	dstEntry := &filer.Entry{
		FullPath: util.FullPath("/dst.txt"),
		Attr: filer.Attr{
			Inode:    dstInode,
			Mtime:    time.Unix(dstMtime, 0),
			FileSize: uint64(dstSize),
		},
	}

	if err := validateCopyDestinationPreconditions(preconditions, dstEntry); err != nil {
		t.Fatalf("validate destination preconditions: %v", err)
	}

	if err := validateCopyDestinationPreconditions(preconditions, nil); err == nil {
		t.Fatal("expected missing destination to fail")
	}
}

func TestRecentCopyRequestDeduplicatesByRequestID(t *testing.T) {
	fs := &FilerServer{
		recentCopyRequests: make(map[string]recentCopyRequest),
	}

	requestID := "copy-req"
	fingerprint := "src|dst|1"
	fs.rememberRecentCopyRequest(requestID, fingerprint)

	handled, err := fs.handleRecentCopyRequest(requestID, fingerprint)
	if err != nil {
		t.Fatalf("handle recent copy request: %v", err)
	}
	if !handled {
		t.Fatal("expected recent copy request to be recognized")
	}

	handled, err = fs.handleRecentCopyRequest(requestID, "different")
	if err == nil {
		t.Fatal("expected reused request id with different fingerprint to fail")
	}
	if handled {
		t.Fatal("reused request id with different fingerprint should not be treated as handled")
	}
}
