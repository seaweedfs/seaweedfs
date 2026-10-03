package logbuffer

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

var severityNames = map[byte]string{
	'I': "INFO",
	'W': "WARNING",
	'E': "ERROR",
	'F': "FATAL",
}

var severityFromLevel = map[int32]string{
	0: "INFO",
	1: "WARNING",
	2: "ERROR",
	3: "FATAL",
}

const requestIDPrefix = "request_id:"

// yearForMonth resolves the correct year for a log timestamp.
// glog does not include the year in its header format (mmdd only).
// If the parsed month is ahead of the current month, we assume the log
// is from the previous year (e.g., a December log queried in January).
func yearForMonth(month time.Month) int {
	now := time.Now()
	y := now.Year()
	if month > now.Month()+1 {
		y--
	}
	return y
}

// jsonLogLine is used only for JSON unmarshaling in ParseLogLine.
type jsonLogLine struct {
	Ts    string `json:"ts"`
	Level string `json:"level"`
	File  string `json:"file"`
	Line  int    `json:"line"`
	Msg   string `json:"msg"`
}

// ParseLogLine parses a raw glog line into a LogEntry.
// It auto-detects the format:
//   - JSON: {"ts":"...","level":"...","file":"...","line":N,"msg":"..."}
//   - Text: Lmmdd hh:mm:ss.uuuuuu file:line msg (SeaweedFS glog format)
func ParseLogLine(level int32, raw []byte) LogEntry {
	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     severityFromLevel[level],
	}

	if len(raw) == 0 {
		return entry
	}

	// Auto-detect JSON format (starts with '{')
	if raw[0] == '{' {
		return parseJSONLine(level, raw, entry)
	}

	return parseTextLine(level, raw, entry)
}

// parseJSONLine handles JSON-formatted glog output.
func parseJSONLine(level int32, raw []byte, entry LogEntry) LogEntry {
	var jl jsonLogLine
	if err := json.Unmarshal(raw, &jl); err != nil {
		// Fallback: treat as plain message
		entry.Message = strings.TrimSpace(string(raw))
		return entry
	}

	entry.Level = jl.Level
	entry.File = jl.File
	entry.Line = jl.Line
	entry.Message = jl.Msg

	if jl.Ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, jl.Ts); err == nil {
			entry.Timestamp = t
		}
	}

	extractRequestID(&entry)
	return entry
}

// parseTextLine handles SeaweedFS glog text format.
// SeaweedFS glog emits: Lmmdd hh:mm:ss.uuuuuu file:line msg
// (no thread id, no ] bracket — differs from standard glog).
func parseTextLine(level int32, raw []byte, entry LogEntry) LogEntry {
	line := string(raw)

	// Minimum valid: "I0318 12:34:56.123456 f:1 m" = 27+ chars
	if len(line) < 27 {
		entry.Message = strings.TrimSpace(line)
		return entry
	}

	// First char is severity
	if name, ok := severityNames[line[0]]; ok {
		entry.Level = name
	}

	// Parse date: mmdd (chars 1-4)
	month, _ := strconv.Atoi(line[1:3])
	day, _ := strconv.Atoi(line[3:5])

	// Parse time: hh:mm:ss.uuuuuu (chars 6-21)
	if len(line) > 21 && line[5] == ' ' {
		hour, _ := strconv.Atoi(line[6:8])
		min, _ := strconv.Atoi(line[9:11])
		sec, _ := strconv.Atoi(line[12:14])
		usec, _ := strconv.Atoi(line[15:21])

		m := time.Month(month)
		entry.Timestamp = time.Date(
			yearForMonth(m), m, day,
			hour, min, sec, usec*1000,
			time.Now().Location(),
		)
	}

	// After the timestamp (22 chars), the rest is "file:line msg"
	rest := line[22:]

	// SeaweedFS format: file:line msg (no bracket)
	// Also support standard glog format: threadid file:line] msg
	if bracketIdx := strings.Index(rest, "] "); bracketIdx >= 0 {
		// Standard glog format with "]"
		header := rest[:bracketIdx]
		lastSpace := strings.LastIndex(header, " ")
		if lastSpace >= 0 {
			fileLine := header[lastSpace+1:]
			colonIdx := strings.LastIndex(fileLine, ":")
			if colonIdx > 0 {
				entry.File = fileLine[:colonIdx]
				entry.Line, _ = strconv.Atoi(fileLine[colonIdx+1:])
			}
		}
		entry.Message = strings.TrimRight(rest[bracketIdx+2:], "\n\r")
	} else {
		// SeaweedFS format: file:line msg
		spaceIdx := strings.Index(rest, " ")
		if spaceIdx > 0 {
			fileLine := rest[:spaceIdx]
			colonIdx := strings.LastIndex(fileLine, ":")
			if colonIdx > 0 {
				entry.File = fileLine[:colonIdx]
				entry.Line, _ = strconv.Atoi(fileLine[colonIdx+1:])
			}
			entry.Message = strings.TrimRight(rest[spaceIdx+1:], "\n\r")
		} else {
			entry.Message = strings.TrimRight(rest, "\n\r")
		}
	}

	extractRequestID(&entry)
	return entry
}

// extractRequestID extracts request_id prefix from the entry message if present.
// Format: "request_id:XXXXX rest of message"
func extractRequestID(entry *LogEntry) {
	if !strings.HasPrefix(entry.Message, requestIDPrefix) {
		return
	}
	rest := entry.Message[len(requestIDPrefix):]
	spaceIdx := strings.Index(rest, " ")
	if spaceIdx > 0 {
		entry.RequestID = rest[:spaceIdx]
		entry.Message = rest[spaceIdx+1:]
	} else if rest != "" {
		entry.RequestID = strings.TrimSpace(rest)
		entry.Message = ""
	}
}
