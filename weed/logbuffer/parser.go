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
//   - Text: [IWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] msg...
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

	// Extract request_id if present in message
	if strings.HasPrefix(entry.Message, "request_id:") {
		spaceIdx := strings.Index(entry.Message, " ")
		if spaceIdx > 0 {
			entry.RequestID = entry.Message[11:spaceIdx]
			entry.Message = entry.Message[spaceIdx+1:]
		}
	}

	return entry
}

// parseTextLine handles classic glog text format.
func parseTextLine(level int32, raw []byte, entry LogEntry) LogEntry {
	line := string(raw)

	// Minimum valid: "I0318 12:34:56.123456 12345 f:1] m" = 30+ chars
	if len(line) < 30 {
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

	// Find the "] " separator that ends the header
	bracketIdx := strings.Index(line, "] ")
	if bracketIdx == -1 {
		bracketIdx = strings.LastIndex(line, "]")
		if bracketIdx == -1 {
			entry.Message = strings.TrimSpace(line)
			return entry
		}
	}

	// Parse file:line from the header (between last space before ] and ])
	header := line[:bracketIdx]
	lastSpace := strings.LastIndex(header, " ")
	if lastSpace > 0 {
		fileLine := header[lastSpace+1:]
		colonIdx := strings.LastIndex(fileLine, ":")
		if colonIdx > 0 {
			entry.File = fileLine[:colonIdx]
			entry.Line, _ = strconv.Atoi(fileLine[colonIdx+1:])
		}
	}

	// Message is everything after "] "
	if bracketIdx+2 < len(line) {
		entry.Message = strings.TrimRight(line[bracketIdx+2:], "\n\r")
	} else if bracketIdx+1 < len(line) {
		entry.Message = strings.TrimRight(line[bracketIdx+1:], "\n\r")
	}

	// Extract request_id if present (format: "request_id:XXXXX ...")
	if strings.HasPrefix(entry.Message, "request_id:") {
		spaceIdx := strings.Index(entry.Message, " ")
		if spaceIdx > 0 {
			entry.RequestID = entry.Message[11:spaceIdx]
			entry.Message = entry.Message[spaceIdx+1:]
		}
	}

	return entry
}
