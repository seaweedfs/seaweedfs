package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func init() {
	Commands = append(Commands, &commandFsLog{})
	Commands = append(Commands, &commandFsLogPurge{})
}

type commandFsLog struct {
}

func (c *commandFsLog) Name() string {
	return "fs.log"
}

func (c *commandFsLog) Help() string {
	return `print filer log entries stored under ` + filer.SystemLogDir + `

	fs.log [-file /topics/.system/log/YYYY-MM-DD/HH-MM.<filerIdHex>] [-date YYYY-MM-DD] [-begin "ISO-8601" -end "ISO-8601"] [-s] [-raw-data]

examples:
	# print the latest log file (default)
	fs.log

	# print a specific date (equivalent to -begin DATE -end DATE)
	fs.log -date 2025-12-23

	# print logs within time range (ISO-8601)
	fs.log -begin "2025-12-23T10:15" -end "2025-12-23T11:00"
	fs.log -begin "2025-12-23T10:15:00+09:00" -end "2025-12-23T11:00:00+09:00"

	# print a specific log file
	fs.log -file /topics/.system/log/2025-12-23/10-15.00000000

	# print one-line summary per entry
	fs.log -s

	# print raw protobuf json
	fs.log -raw-data
`
}

func (c *commandFsLog) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsLog) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if handleHelpRequest(c, args, writer) {
		return nil
	}

	fsLogCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	filePath := fsLogCommand.String("file", "", "log file full path under "+filer.SystemLogDir)
	date := fsLogCommand.String("date", "", "date (YYYY-MM-DD), equivalent to -begin DATE -end DATE")
	begin := fsLogCommand.String("begin", "", "begin time in ISO-8601 (examples: 2025-12-23 , 2025-12-23T10:15 , 2025-12-23T10:15:00+09:00)")
	end := fsLogCommand.String("end", "", "end time in ISO-8601 (examples: 2025-12-23 , 2025-12-23T11:00 , 2025-12-23T11:00:00+09:00; default: today 24:00 local time)")
	summaryOnly := fsLogCommand.Bool("s", false, "print one-line summary: [time] [C/U/D/R] [path]")
	rawData := fsLogCommand.Bool("raw-data", false, "print raw protobuf (json) instead of formatted output")

	if err = fsLogCommand.Parse(args); err != nil {
		return err
	}

	target := strings.TrimSpace(*filePath)
	if target != "" {
		return printLogFile(context.Background(), commandEnv, writer, target, *summaryOnly, *rawData, 0, 0, time.Local)
	}

	beginStr := strings.TrimSpace(*begin)
	endStr := strings.TrimSpace(*end)
	dateStr := strings.TrimSpace(*date)

	if dateStr != "" && (beginStr != "" || endStr != "") {
		return fmt.Errorf("-date cannot be used together with -begin/-end")
	}
	if dateStr != "" {
		beginStr, endStr = dateStr, dateStr
	}

	// Time range mode
	if beginStr != "" || endStr != "" {
		if beginStr == "" {
			return fmt.Errorf("-begin is required when -end is set")
		}
		beginTime, endTime, parseErr := parseBeginEndISO8601(beginStr, endStr)
		if parseErr != nil {
			return parseErr
		}

		paths, listErr := listLogFilePathsInRange(context.Background(), commandEnv, beginTime, endTime)
		if listErr != nil {
			return listErr
		}
		// Always print in local time without timezone suffix.
		outputLoc := time.Local
		for _, p := range paths {
			if err := printLogFile(context.Background(), commandEnv, writer, p, *summaryOnly, *rawData, beginTime.UnixNano(), endTime.UnixNano(), outputLoc); err != nil {
				return err
			}
		}
		return nil
	}

	// Default / day mode
	target, err = pickLatestLogFilePath(context.Background(), commandEnv, "")
	if err != nil {
		return err
	}
	return printLogFile(context.Background(), commandEnv, writer, target, *summaryOnly, *rawData, 0, 0, time.Local)
}

type commandFsLogPurge struct {
}

func (c *commandFsLogPurge) Name() string {
	return "fs.log.purge"
}

func (c *commandFsLogPurge) Help() string {
	return `purge filer logs

	fs.log.purge [-v] [-daysAgo 365]
`
}

func (c *commandFsLogPurge) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsLogPurge) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	fsLogPurgeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	daysAgo := fsLogPurgeCommand.Uint("daysAgo", 365, "purge logs older than N days")
	verbose := fsLogPurgeCommand.Bool("v", false, "verbose mode")

	if err = fsLogPurgeCommand.Parse(args); err != nil {
		return err
	}

	modificationTimeAgo := time.Now().Add(-time.Hour * 24 * time.Duration(*daysAgo)).Unix()
	err = filer_pb.ReadDirAllEntries(context.Background(), commandEnv, filer.SystemLogDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.Attributes.Mtime > modificationTimeAgo {
			return nil
		}
		if errDel := filer_pb.Remove(context.Background(), commandEnv, filer.SystemLogDir, entry.Name, true, true, true, false, nil); errDel != nil {
			return errDel
		}
		if *verbose {
			fmt.Fprintf(writer, "delete %s\n", entry.Name)
		}
		return nil
	})
	return err
}

func pickLatestLogFilePath(ctx context.Context, commandEnv *CommandEnv, day string) (string, error) {
	// If day is specified, use it. Otherwise find the newest day directory under SystemLogDir.
	if day == "" {
		days, err := listChildNames(ctx, commandEnv, filer.SystemLogDir)
		if err != nil {
			return "", err
		}
		if len(days) == 0 {
			return "", fmt.Errorf("no log directories under %s", filer.SystemLogDir)
		}
		sort.Strings(days)
		day = days[len(days)-1]
	}

	dayDir := util.NewFullPath(filer.SystemLogDir, day)
	files, err := listChildNames(ctx, commandEnv, dayDir)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no log files under %s", dayDir)
	}
	sort.Strings(files)
	latestFile := files[len(files)-1]
	return string(util.NewFullPath(string(dayDir), latestFile)), nil
}

func listChildNames(ctx context.Context, commandEnv *CommandEnv, dir util.FullPath) ([]string, error) {
	var names []string
	err := filer_pb.ReadDirAllEntries(ctx, commandEnv, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
		names = append(names, entry.Name)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

func printLogFile(ctx context.Context, commandEnv *CommandEnv, writer io.Writer, fullPath string, summaryOnly bool, rawData bool, beginTsNs int64, endTsNs int64, outputLoc *time.Location) error {

	dir, name := util.FullPath(fullPath).DirAndName()

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		respLookupEntry, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		})
		if err != nil {
			return err
		}

		var r io.Reader
		if len(respLookupEntry.Entry.Content) > 0 {
			r = bytes.NewReader(respLookupEntry.Entry.Content)
		} else {
			pr, pw := io.Pipe()
			r = pr
			go func() {
				streamErr := filer.StreamContent(commandEnv.MasterClient, pw, respLookupEntry.Entry.GetChunks(), 0, int64(filer.FileSize(respLookupEntry.Entry)))
				_ = pw.CloseWithError(streamErr)
			}()
		}

		enc := protojson.MarshalOptions{
			Multiline:       false,
			EmitUnpopulated: false,
			UseProtoNames:   true,
		}

		sizeBuf := make([]byte, 4)
		for {
			_, err := io.ReadFull(r, sizeBuf)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				}
				return err
			}

			size := util.BytesToUint32(sizeBuf)
			if size == 0 {
				// skip empty records
				continue
			}

			data := make([]byte, int(size))
			if _, err := io.ReadFull(r, data); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				}
				return err
			}

			logEntry := &filer_pb.LogEntry{}
			if err := proto.Unmarshal(data, logEntry); err != nil {
				return fmt.Errorf("unexpected unmarshal filer_pb.LogEntry from %s: %w", fullPath, err)
			}

			event := &filer_pb.SubscribeMetadataResponse{}
			if err := proto.Unmarshal(logEntry.Data, event); err != nil {
				// Not all log entries are guaranteed to be metadata subscription events.
				// In -raw-data mode, fall back to printing LogEntry itself.
				if rawData {
					b, mErr := enc.Marshal(logEntry)
					if mErr != nil {
						return fmt.Errorf("failed to marshal LogEntry as json: %w", mErr)
					}
					fmt.Fprintf(writer, "%s\n", string(b))
				}
				// formatted mode: skip unknown records
				continue
			}

			if rawData {
				b, mErr := enc.Marshal(event)
				if mErr != nil {
					return fmt.Errorf("failed to marshal event as json: %w", mErr)
				}
				fmt.Fprintf(writer, "%s\n", string(b))
				continue
			}

			if err := printOneEvent(writer, event, logEntry, summaryOnly, beginTsNs, endTsNs, outputLoc); err != nil {
				return err
			}
		}
	})
}

func printOneEvent(w io.Writer, event *filer_pb.SubscribeMetadataResponse, logEntry *filer_pb.LogEntry, summaryOnly bool, beginTsNs int64, endTsNs int64, outputLoc *time.Location) error {
	if event == nil || event.EventNotification == nil {
		return nil
	}

	// timestamp
	tsNs := event.GetTsNs()
	if tsNs == 0 && logEntry != nil {
		tsNs = logEntry.GetTsNs()
	}
	if beginTsNs != 0 && tsNs < beginTsNs {
		return nil
	}
	if endTsNs != 0 && tsNs > endTsNs {
		return nil
	}
	if outputLoc == nil {
		outputLoc = time.Local
	}
	t := formatISO8601(time.Unix(0, tsNs).In(outputLoc))

	// event type + path
	evType := "?"
	path := ""

	if filer_pb.IsCreate(event) {
		evType = "C"
		path = string(util.NewFullPath(event.GetDirectory(), event.EventNotification.GetNewEntry().GetName()))
	} else if filer_pb.IsUpdate(event) {
		evType = "U"
		path = string(util.NewFullPath(event.GetDirectory(), event.EventNotification.GetNewEntry().GetName()))
	} else if filer_pb.IsDelete(event) {
		evType = "D"
		path = string(util.NewFullPath(event.GetDirectory(), event.EventNotification.GetOldEntry().GetName()))
	} else if filer_pb.IsRename(event) {
		evType = "R"
		path = string(util.NewFullPath(event.GetDirectory(), event.EventNotification.GetOldEntry().GetName()))
	} else {
		return nil
	}

	fmt.Fprintf(w, "%s %s %s\n", t, evType, path)

	if summaryOnly {
		return nil
	}

	switch evType {
	case "C":
		newEntry := event.EventNotification.GetNewEntry()
		fmt.Fprintf(w, "\tmtime %s\n", entryMtimeString(newEntry, outputLoc))
		for _, c := range newEntry.GetChunks() {
			fmt.Fprintf(w, "\t+ %s\n", chunkDisplay(c))
		}
	case "D":
		oldEntry := event.EventNotification.GetOldEntry()
		fmt.Fprintf(w, "\tmtime %s\n", entryMtimeString(oldEntry, outputLoc))
		for _, c := range oldEntry.GetChunks() {
			fmt.Fprintf(w, "\t- %s\n", chunkDisplay(c))
		}
	case "U":
		oldEntry := event.EventNotification.GetOldEntry()
		newEntry := event.EventNotification.GetNewEntry()
		fmt.Fprintf(w, "\tmtime %s -> %s\n", entryMtimeString(oldEntry, outputLoc), entryMtimeString(newEntry, outputLoc))
		for _, line := range diffChunks(oldEntry.GetChunks(), newEntry.GetChunks()) {
			fmt.Fprintf(w, "\t%s\n", line)
		}
	case "R":
		newPath := string(util.NewFullPath(event.EventNotification.GetNewParentPath(), event.EventNotification.GetNewEntry().GetName()))
		fmt.Fprintf(w, "\t%s\n", newPath)
	}

	return nil
}

func chunkId(c *filer_pb.FileChunk) string {
	if c == nil {
		return "-"
	}
	if c.GetFid() != nil {
		fid := c.GetFid()
		return fmt.Sprintf("%d,%x", fid.GetVolumeId(), fid.GetFileKey())
	}
	if c.GetFileId() != "" {
		return c.GetFileId()
	}
	return "-"
}

func chunkSignature(c *filer_pb.FileChunk) string {
	if c == nil {
		return "-"
	}
	// Include enough fields to detect meaningful changes.
	return fmt.Sprintf("%s|off=%d|size=%d|etag=%s|mtime_ns=%d",
		chunkId(c),
		c.GetOffset(),
		c.GetSize(),
		c.GetETag(),
		c.GetModifiedTsNs(),
	)
}

func chunkDisplay(c *filer_pb.FileChunk) string {
	if c == nil {
		return "-"
	}
	// Compact human readable chunk display.
	// Example: "3,1a2b3c off=0 size=1048576 etag=...".
	etag := c.GetETag()
	if etag == "" {
		etag = "-"
	}
	return fmt.Sprintf("%s off=%d size=%d etag=%s", chunkId(c), c.GetOffset(), c.GetSize(), etag)
}

func diffChunks(oldChunks, newChunks []*filer_pb.FileChunk) (lines []string) {
	// Compare by offset as primary key. This matches how file chunks are normally organized.
	oldByOffset := make(map[int64]*filer_pb.FileChunk, len(oldChunks))
	newByOffset := make(map[int64]*filer_pb.FileChunk, len(newChunks))
	offsets := make(map[int64]struct{}, len(oldChunks)+len(newChunks))

	for _, c := range oldChunks {
		if c == nil {
			continue
		}
		oldByOffset[c.GetOffset()] = c
		offsets[c.GetOffset()] = struct{}{}
	}
	for _, c := range newChunks {
		if c == nil {
			continue
		}
		newByOffset[c.GetOffset()] = c
		offsets[c.GetOffset()] = struct{}{}
	}

	var sorted []int64
	for off := range offsets {
		sorted = append(sorted, off)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, off := range sorted {
		oc := oldByOffset[off]
		nc := newByOffset[off]
		switch {
		case oc == nil && nc != nil:
			lines = append(lines, "+ "+chunkDisplay(nc))
		case oc != nil && nc == nil:
			lines = append(lines, "- "+chunkDisplay(oc))
		case oc != nil && nc != nil:
			if chunkSignature(oc) != chunkSignature(nc) {
				lines = append(lines, "~ "+chunkDisplay(oc)+" -> "+chunkDisplay(nc))
			}
		}
	}

	return lines
}

func entryMtimeString(e *filer_pb.Entry, loc *time.Location) string {
	if e == nil || e.GetAttributes() == nil {
		return "-"
	}
	sec := e.GetAttributes().GetMtime()
	if sec <= 0 {
		return "-"
	}
	if loc == nil {
		loc = time.Local
	}
	return formatISO8601(time.Unix(sec, 0).In(loc))
}

func formatISO8601(t time.Time) string {
	// Print in local time, without any timezone suffix, and without spaces:
	// "YYYY-MM-DDTHH:MM:SS" (or with sub-seconds if present).
	lt := t.In(time.Local)
	if lt.Nanosecond() == 0 {
		return lt.Format("2006-01-02T15:04:05")
	}
	// Always pad sub-seconds to 9 digits.
	return lt.Format("2006-01-02T15:04:05.000000000")
}

func parseISO8601Time(input string, isEnd bool) (time.Time, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}

	// Prefer RFC3339 with timezone when provided.
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	// ISO-8601 without timezone: interpret in local timezone.
	// Accept minute precision and second precision.
	if t, err := time.ParseInLocation("2006-01-02T15:04", s, time.Local); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05", s, time.Local); err == nil {
		return t, nil
	}

	// Date-only: interpret in local timezone.
	if d, err := time.ParseInLocation("2006-01-02", s, time.Local); err == nil {
		if !isEnd {
			return d, nil // start of day
		}
		// end of day inclusive
		return d.Add(24*time.Hour - time.Nanosecond), nil
	}

	return time.Time{}, fmt.Errorf("invalid time %q (expected ISO-8601, e.g. 2025-12-23T10:15 or 2025-12-23T10:15:00+09:00)", input)
}

func parseBeginEndISO8601(begin, end string) (time.Time, time.Time, error) {
	bt, err := parseISO8601Time(begin, false)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid -begin: %v", err)
	}

	var et time.Time
	if strings.TrimSpace(end) == "" {
		// default end: today 24:00 in begin timezone, inclusive (end of today)
		now := time.Now().In(bt.Location())
		startOfTomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, bt.Location())
		et = startOfTomorrow.Add(-time.Nanosecond)
	} else {
		et, err = parseISO8601Time(end, true)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid -end: %v", err)
		}
	}
	if et.Before(bt) {
		return time.Time{}, time.Time{}, fmt.Errorf("-end must be >= -begin")
	}
	return bt, et, nil
}

func listLogFilePathsInRange(ctx context.Context, commandEnv *CommandEnv, beginTime time.Time, endTime time.Time) ([]string, error) {
	if endTime.Before(beginTime) {
		return nil, nil
	}

	// Log file names are based on UTC time (see filer.logFlushFunc: startTime.UTC()).
	// Use UTC to map time ranges to log file paths correctly even when user input has a timezone.
	beginUTC := beginTime.UTC()
	endUTC := endTime.UTC()

	// iterate days [beginDate..endDate] in UTC
	beginDate := time.Date(beginUTC.Year(), beginUTC.Month(), beginUTC.Day(), 0, 0, 0, 0, time.UTC)
	endDate := time.Date(endUTC.Year(), endUTC.Month(), endUTC.Day(), 0, 0, 0, 0, time.UTC)

	var paths []string
	for d := beginDate; !d.After(endDate); d = d.Add(24 * time.Hour) {
		dayStr := d.Format("2006-01-02")
		dayDir := util.NewFullPath(filer.SystemLogDir, dayStr)

		files, err := listChildNames(ctx, commandEnv, dayDir)
		if err != nil {
			// if day directory doesn't exist, treat as empty
			continue
		}

		for _, fn := range files {
			hm := hourMinuteFromLogFileName(fn)
			if hm == "" {
				continue
			}
			hour, minute, ok := parseHourMinute(hm)
			if !ok {
				continue
			}
			fileMinute := time.Date(d.Year(), d.Month(), d.Day(), hour, minute, 0, 0, time.UTC)
			// coarse filter by minute start
			if fileMinute.Before(beginUTC.Truncate(time.Minute)) || fileMinute.After(endUTC.Truncate(time.Minute)) {
				continue
			}
			paths = append(paths, string(util.NewFullPath(string(dayDir), fn)))
		}
	}

	sort.Strings(paths)
	return paths, nil
}

func hourMinuteFromLogFileName(name string) string {
	// expected "HH-MM.<filerId...>"
	dot := strings.IndexByte(name, '.')
	if dot <= 0 {
		return ""
	}
	hm := name[:dot]
	// quick validation: "HH-MM"
	if len(hm) != 5 || hm[2] != '-' {
		return ""
	}
	return hm
}

func parseHourMinute(hm string) (hour int, minute int, ok bool) {
	// expected "HH-MM"
	if len(hm) != 5 || hm[2] != '-' {
		return 0, 0, false
	}
	h, err := strconv.Atoi(hm[:2])
	if err != nil || h < 0 || h > 23 {
		return 0, 0, false
	}
	m, err := strconv.Atoi(hm[3:])
	if err != nil || m < 0 || m > 59 {
		return 0, 0, false
	}
	return h, m, true
}
