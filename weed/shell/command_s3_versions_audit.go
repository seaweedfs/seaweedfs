package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandS3VersionsAudit{})
}

type commandS3VersionsAudit struct{}

func (c *commandS3VersionsAudit) Name() string {
	return "s3.versions.audit"
}

func (c *commandS3VersionsAudit) Help() string {
	return `audit .versions/ directories under a prefix for stranded pointer/missing-file state

	Walks every entry under the given prefix and, for each directory whose
	name ends in ".versions", checks whether its extended-attr latest-version
	pointer references a file that actually exists in the directory.

	Reports counts for:
	  - directories scanned
	  - clean (no pointer, or pointer matches existing file)
	  - stranded (pointer set but file is missing) — the symptom seen by
	    Veeam/etc. as "Storage not found" on the next GET
	  - orphan (directory has files lacking the version-id extended attr,
	    which the post-delete cleanup path will refuse to rm)

	Example:
		# Audit a whole bucket
		s3.versions.audit -prefix /buckets/mybucket

		# Audit a specific client subtree, print each finding
		s3.versions.audit -prefix /buckets/mybucket/Veeam/Backup/groupsoftware/Clients/<uuid>/ -v

		# Dry run (default) — read-only, prints what would be healed
		# Add -heal to clear stranded pointers in place (calls the same path
		# the read-side self-heal uses)
		s3.versions.audit -prefix /buckets/mybucket -heal

	This command is read-only by default. With -heal, it clears the stale
	latest-version pointer on stranded directories; the blob is already
	gone, so reads then return NoSuchKey via the clean-miss path instead
	of replaying the 10-retry self-heal loop on every request.
`
}

func (c *commandS3VersionsAudit) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3VersionsAudit) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	cmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	prefix := cmd.String("prefix", "", "filer path to audit recursively (e.g. /buckets/mybucket)")
	verbose := cmd.Bool("v", false, "print each stranded/orphan directory as it's found")
	doHeal := cmd.Bool("heal", false, "clear the latest-version pointer on stranded directories (default: read-only)")
	if err := cmd.Parse(args); err != nil {
		return err
	}
	if *prefix == "" {
		return fmt.Errorf("-prefix is required")
	}

	// Counters
	var (
		dirsScanned  uint64
		versionsDirs uint64
		clean        uint64
		stranded     uint64
		orphanOnly   uint64
		healed       uint64
		healFailed   uint64
	)

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.TraverseBfs(ctx, &filerClientWrapper{client: client}, util.FullPath(*prefix), func(parentPath util.FullPath, entry *filer_pb.Entry) error {
			atomic.AddUint64(&dirsScanned, 1)
			if !entry.IsDirectory {
				return nil
			}
			if !strings.HasSuffix(entry.Name, ".versions") {
				return nil
			}
			atomic.AddUint64(&versionsDirs, 1)

			// What does the pointer name?
			var pointerFile string
			if entry.Extended != nil {
				if v, ok := entry.Extended[s3_constants.ExtLatestVersionFileNameKey]; ok {
					pointerFile = string(v)
				}
			}

			// List the children to see if the pointer's file exists and to
			// count entries without an ExtVersionIdKey (orphans that block
			// non-recursive teardown).
			versionsPath := string(parentPath) + "/" + entry.Name
			pointerSeen := false
			hasOrphan := false
			lookupErr := filer_pb.List(ctx, &filerClientWrapper{client: client}, versionsPath, "", func(child *filer_pb.Entry, isLast bool) error {
				if child == nil {
					return nil
				}
				hasVersionId := false
				if child.Extended != nil {
					if _, ok := child.Extended[s3_constants.ExtVersionIdKey]; ok {
						hasVersionId = true
					}
				}
				if pointerFile != "" && child.Name == pointerFile {
					pointerSeen = true
				}
				if !hasVersionId {
					hasOrphan = true
				}
				return nil
			}, "", false, 1024)
			if lookupErr != nil {
				fmt.Fprintf(writer, "list %s: %v\n", versionsPath, lookupErr)
				return nil
			}

			switch {
			case pointerFile == "":
				// No pointer set — neither stranded nor blocking.
				atomic.AddUint64(&clean, 1)
				if hasOrphan {
					atomic.AddUint64(&orphanOnly, 1)
				}
			case pointerSeen:
				atomic.AddUint64(&clean, 1)
			default:
				// Pointer names a file that the listing does NOT contain.
				atomic.AddUint64(&stranded, 1)
				if *verbose {
					fmt.Fprintf(writer, "stranded: %s pointer=%s orphan=%v\n", versionsPath, pointerFile, hasOrphan)
				}
				if *doHeal {
					if err := healStrandedPointer(ctx, client, parentPath, entry); err != nil {
						atomic.AddUint64(&healFailed, 1)
						fmt.Fprintf(writer, "heal failed: %s: %v\n", versionsPath, err)
					} else {
						atomic.AddUint64(&healed, 1)
					}
				}
			}
			return nil
		})
	})

	elapsed := time.Since(start)
	fmt.Fprintf(writer, "audit complete in %s\n", elapsed)
	fmt.Fprintf(writer, "  total entries scanned : %d\n", dirsScanned)
	fmt.Fprintf(writer, "  .versions/ directories: %d\n", versionsDirs)
	fmt.Fprintf(writer, "  clean                 : %d\n", clean)
	fmt.Fprintf(writer, "  stranded              : %d\n", stranded)
	fmt.Fprintf(writer, "  orphan-only           : %d\n", orphanOnly)
	if *doHeal {
		fmt.Fprintf(writer, "  healed                : %d\n", healed)
		fmt.Fprintf(writer, "  heal failed           : %d\n", healFailed)
	}
	return err
}

// healStrandedPointer clears the latest-version pointer extended attrs
// on a stranded .versions/ directory. The blob the pointer names is
// already gone; clearing the pointer makes subsequent reads return
// NoSuchKey via the clean-miss path instead of replaying the read-side
// self-heal on every request.
func healStrandedPointer(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath util.FullPath, entry *filer_pb.Entry) error {
	if entry.Extended == nil {
		return nil
	}
	delete(entry.Extended, s3_constants.ExtLatestVersionIdKey)
	delete(entry.Extended, s3_constants.ExtLatestVersionFileNameKey)
	// Also clear the cached list metadata so a stale size/mtime/etag
	// can't be served back; those will be repopulated on the next PUT.
	delete(entry.Extended, s3_constants.ExtLatestVersionSizeKey)
	delete(entry.Extended, s3_constants.ExtLatestVersionMtimeKey)
	delete(entry.Extended, s3_constants.ExtLatestVersionETagKey)
	delete(entry.Extended, s3_constants.ExtLatestVersionOwnerKey)
	delete(entry.Extended, s3_constants.ExtLatestVersionIsDeleteMarker)
	_, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: string(parentPath),
		Entry:     entry,
	})
	return err
}

// filerClientWrapper adapts a raw SeaweedFilerClient to the
// filer_pb.FilerClient interface that List / TraverseBfs expect.
type filerClientWrapper struct {
	client filer_pb.SeaweedFilerClient
}

func (w *filerClientWrapper) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(w.client)
}

func (w *filerClientWrapper) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (w *filerClientWrapper) GetDataCenter() string {
	return ""
}
