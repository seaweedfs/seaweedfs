package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdFilerMetaScan.Run = runFilerMetaScan // break init cycle
}

var cmdFilerMetaScan = &Command{
	UsageLine: "filer.meta.scan -pathPrefix=/some/dir [-since=...] [-until=...]",
	Short:     "audit what happened under a directory, one line per change",
	Long: `Replay the filer's metadata log for one directory and print one line per change.

	Unlike filer.meta.tail this stops when it reaches the end of the requested
	range instead of following, so it can be piped straight into grep or awk.

	Each line is:

		<time>	<OP>	<path>	<details>

	OP is CREATE, DELETE, UPDATE or RENAME. Times are printed with the offset of
	the machine running the command.

	Ranges may be given as absolute timestamps or as durations before now:

	weed filer.meta.scan -pathPrefix=/buckets/b -since="2026-08-03 00:00:00" -until="2026-08-03 06:00:00"
	weed filer.meta.scan -pathPrefix=/buckets/b -timeAgo=48h -untilTimeAgo=24h
	weed filer.meta.scan -pathPrefix=/buckets/b -timeAgo=1h -follow

	-since and -until are read in the machine's timezone unless -tz is given:

	weed filer.meta.scan -pathPrefix=/buckets/b -since="2026-08-03 01:00:00" -tz=America/Sao_Paulo

	On a versioned bucket an object is stored as <key>.versions/v_<versionId>.
	Those are reported against <key> with the version id in the details, so
	-name matches the key a client would ask for rather than the internal path:

	weed filer.meta.scan -pathPrefix=/buckets/b -timeAgo=720h -name=summary.xml
	weed filer.meta.scan -pathPrefix=/buckets/b -timeAgo=720h -op=DELETE

	Pass -raw to report the stored paths instead.

	Persisted ranges are read straight from the volume servers, so the filer
	hands out log chunk ids instead of decoding and filtering the whole range
	itself — worth having on a busy cluster, where that decode is the expensive
	part and is charged to the filer no matter how narrow the prefix is. It
	needs a route to the volume servers; without one the scan falls back to
	reading through the filer, and -directRead=false forces that path.

  `,
}

var (
	scanFiler        = cmdFilerMetaScan.Flag.String("filer", "localhost:8888", "filer hostname:port")
	scanPathPrefix   = cmdFilerMetaScan.Flag.String("pathPrefix", "/", "directory to audit; filtered on the filer, so keep it as tight as possible")
	scanSince        = cmdFilerMetaScan.Flag.String("since", "", "start time, \"2006-01-02 15:04:05\" or RFC3339")
	scanUntil        = cmdFilerMetaScan.Flag.String("until", "", "stop time, same formats as -since; defaults to now")
	scanTimeAgo      = cmdFilerMetaScan.Flag.Duration("timeAgo", 0, "start this long before now, e.g. \"48h\"; ignored when -since is set")
	scanUntilTimeAgo = cmdFilerMetaScan.Flag.Duration("untilTimeAgo", 0, "stop this long before now; ignored when -until is set")
	scanTz           = cmdFilerMetaScan.Flag.String("tz", "", "timezone for -since/-until, e.g. \"America/Sao_Paulo\" or \"UTC\"; defaults to this machine's")
	scanName         = cmdFilerMetaScan.Flag.String("name", "", "only report paths containing this text, case-insensitive")
	scanOp           = cmdFilerMetaScan.Flag.String("op", "", "only report these operations, comma-separated: CREATE,DELETE,UPDATE,RENAME")
	scanRaw          = cmdFilerMetaScan.Flag.Bool("raw", false, "report stored paths instead of resolving .versions/v_<id> back to the object key")
	scanFollow       = cmdFilerMetaScan.Flag.Bool("follow", false, "keep following after reaching the end of the range")
	scanDirectRead   = cmdFilerMetaScan.Flag.Bool("directRead", true, "read log chunks straight from the volume servers, so the filer does not decode the range; needs volume server access, falls back automatically")
)

// scanFilerClient adapts a filer address to filer_pb.FilerClient so the chunk
// reader can resolve a log chunk's volume locations.
type scanFilerClient struct {
	address        pb.ServerAddress
	grpcDialOption grpc.DialOption
	signature      int32
}

func (c *scanFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithFilerClient(streamingMode, c.signature, c.address, c.grpcDialOption, fn)
}

func (c *scanFilerClient) AdjustedUrl(location *filer_pb.Location) string { return location.Url }

func (c *scanFilerClient) GetDataCenter() string { return "" }

const scanTimeLayout = "2006-01-02 15:04:05"

// parseScanTime accepts either the human layout or RFC3339. Without an explicit
// zone in the string, loc decides — which is the difference between finding the
// window and missing it by the UTC offset.
func parseScanTime(value string, loc *time.Location) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	t, err := time.ParseInLocation(scanTimeLayout, value, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse %q: want %q or RFC3339", value, scanTimeLayout)
	}
	return t, nil
}

// scanEvent is one metadata change reduced to what a path audit needs.
type scanEvent struct {
	tsNs    int64
	op      string
	path    string
	details string
}

const versionsSuffix = ".versions"

// logicalPath maps a stored path back to the object key a client would use. A
// versioned object lives at <key>.versions/v_<id>, so both the container and the
// version files are reported against <key>; the version id moves to the details.
// kind distinguishes the container from the versions inside it, since a change to
// the container is a pointer flip rather than a write of object data.
func logicalPath(dir, name string) (path, version, kind string) {
	full := dir + "/" + name
	if *scanRaw {
		return full, "", ""
	}
	if strings.HasPrefix(name, "v_") && strings.HasSuffix(dir, versionsSuffix) {
		return strings.TrimSuffix(dir, versionsSuffix), strings.TrimPrefix(name, "v_"), ""
	}
	if strings.HasSuffix(name, versionsSuffix) {
		return strings.TrimSuffix(full, versionsSuffix), "", "versions-container"
	}
	return full, "", ""
}

func describeEntry(entry *filer_pb.Entry, version, kind string) string {
	var parts []string
	if version != "" {
		parts = append(parts, "version="+version)
	}
	if kind != "" {
		parts = append(parts, kind)
	}
	if entry != nil {
		// A retraction is stored as a zero-length version carrying this flag.
		// Reporting it as an ordinary write would read as "the object was
		// written" when it means the opposite.
		if marker, ok := entry.Extended[s3_constants.ExtDeleteMarkerKey]; ok && string(marker) == "true" {
			parts = append(parts, "delete-marker")
		} else if entry.IsDirectory {
			if kind == "" {
				parts = append(parts, "dir")
			}
		} else {
			parts = append(parts, fmt.Sprintf("size=%d", filer.FileSize(entry)))
			if n := len(entry.GetChunks()); n > 0 {
				parts = append(parts, fmt.Sprintf("chunks=%d", n))
			}
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

// toScanEvent classifies one notification. Which of old/new is present is what
// distinguishes the operations; when both are, the path tells a rename from an
// in-place update.
func toScanEvent(resp *filer_pb.SubscribeMetadataResponse) *scanEvent {
	n := resp.EventNotification
	switch {
	case n.OldEntry == nil && n.NewEntry == nil:
		return nil

	case n.OldEntry == nil:
		path, version, kind := logicalPath(n.NewParentPath, n.NewEntry.Name)
		return &scanEvent{resp.TsNs, "CREATE", path, describeEntry(n.NewEntry, version, kind)}

	case n.NewEntry == nil:
		path, version, kind := logicalPath(resp.Directory, n.OldEntry.Name)
		return &scanEvent{resp.TsNs, "DELETE", path, describeEntry(n.OldEntry, version, kind)}

	default:
		oldPath, oldVersion, _ := logicalPath(resp.Directory, n.OldEntry.Name)
		newPath, newVersion, newKind := logicalPath(n.NewParentPath, n.NewEntry.Name)
		if oldPath == newPath && oldVersion == newVersion {
			return &scanEvent{resp.TsNs, "UPDATE", newPath, describeEntry(n.NewEntry, newVersion, newKind)}
		}
		details := describeEntry(n.NewEntry, newVersion, newKind) + " from=" + oldPath
		if oldVersion != "" {
			details += "#" + oldVersion
		}
		return &scanEvent{resp.TsNs, "RENAME", newPath, details}
	}
}

func runFilerMetaScan(cmd *Command, args []string) bool {

	loc := time.Local
	if *scanTz != "" {
		parsed, err := time.LoadLocation(*scanTz)
		if err != nil {
			fmt.Fprintf(os.Stderr, "-tz: %v\n", err)
			return false
		}
		loc = parsed
	}

	now := time.Now()

	var startTs time.Time
	switch {
	case *scanSince != "":
		parsed, err := parseScanTime(*scanSince, loc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "-since: %v\n", err)
			return false
		}
		startTs = parsed
	case *scanTimeAgo > 0:
		startTs = now.Add(-*scanTimeAgo)
	default:
		fmt.Fprintln(os.Stderr, "need a start: pass -since or -timeAgo")
		return false
	}

	// Default the end to now so the command terminates; following is opt-in.
	stopTs := now
	switch {
	case *scanUntil != "":
		parsed, err := parseScanTime(*scanUntil, loc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "-until: %v\n", err)
			return false
		}
		stopTs = parsed
	case *scanUntilTimeAgo > 0:
		stopTs = now.Add(-*scanUntilTimeAgo)
	}

	if !stopTs.After(startTs) && !*scanFollow {
		fmt.Fprintf(os.Stderr, "empty range: start %s is not before stop %s\n",
			startTs.Format(time.RFC3339), stopTs.Format(time.RFC3339))
		return false
	}

	wantOps := map[string]bool{}
	for _, op := range strings.Split(*scanOp, ",") {
		if op = strings.ToUpper(strings.TrimSpace(op)); op != "" {
			wantOps[op] = true
		}
	}
	nameFilter := strings.ToLower(*scanName)

	var stopTsNs int64
	if !*scanFollow {
		stopTsNs = stopTs.UnixNano()
	}

	fmt.Fprintf(os.Stderr, "scanning %s from %s to %s\n", *scanPathPrefix,
		startTs.Format(time.RFC3339), stopTs.Format(time.RFC3339))

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	filerAddress := pb.ServerAddress(*scanFiler)
	matched := 0

	emit := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if filer_pb.IsEmpty(resp) {
			return nil
		}
		event := toScanEvent(resp)
		if event == nil {
			return nil
		}
		if len(wantOps) > 0 && !wantOps[event.op] {
			return nil
		}
		if nameFilter != "" && !strings.Contains(strings.ToLower(event.path), nameFilter) {
			return nil
		}
		matched++
		fmt.Printf("%s\t%s\t%s\t%s\n",
			time.Unix(0, event.tsNs).Format(time.RFC3339), event.op, event.path, event.details)
		return nil
	}

	scan := func(directRead bool) error {
		option := &pb.MetadataFollowOption{
			ClientName:     "scan",
			ClientId:       util.RandomInt32(),
			ClientEpoch:    0,
			SelfSignature:  0,
			PathPrefix:     *scanPathPrefix,
			StartTsNs:      startTs.UnixNano(),
			StopTsNs:       stopTsNs,
			EventErrorType: pb.TrivialOnError,
		}
		if directRead {
			// The server hands out log chunk fids and this reads them straight
			// from the volume servers, so the filer never decodes the range.
			// The prefix filter is re-applied client-side by ReadLogFileRefs,
			// so the result is the same either way.
			client := &scanFilerClient{address: filerAddress, grpcDialOption: grpcDialOption, signature: option.SelfSignature}
			lookupFn := filer.LookupFn(client)
			option.LogFileReaderFn = func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
				return filer.NewChunkStreamReaderFromLookup(context.Background(), lookupFn, chunks), nil
			}
		}
		return pb.FollowMetadata(filerAddress, grpcDialOption, option, emit)
	}

	followErr := scan(*scanDirectRead)
	if *scanDirectRead && matched == 0 {
		// Two ways direct read can come back with nothing: it failed outright
		// (reading chunks needs a route to the volume servers that the filer
		// does not), or it succeeded and found none. The second is the
		// dangerous one — for an audit an empty answer reads as "nothing
		// happened here", so it has to be confirmed rather than trusted.
		// Re-running is safe precisely because nothing was printed; after
		// partial output a replay would duplicate lines instead.
		if followErr != nil {
			fmt.Fprintf(os.Stderr, "direct read failed (%v); retrying through the filer\n", followErr)
		} else {
			fmt.Fprintln(os.Stderr, "direct read found nothing; confirming through the filer")
		}
		followErr = scan(false)
		if followErr == nil && matched > 0 {
			fmt.Fprintf(os.Stderr, "warning: direct read missed %d change(s) the filer returned; "+
				"please report this, and pass -directRead=false meanwhile\n", matched)
		}
	}

	if followErr != nil {
		fmt.Fprintf(os.Stderr, "scan %s: %v\n", *scanFiler, followErr)
		return false
	}

	fmt.Fprintf(os.Stderr, "%d matching change(s)\n", matched)
	return true
}
