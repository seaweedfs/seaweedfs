package dash

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// LogEntry represents a single log line with metadata
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Level     string    `json:"level"`
	User      string    `json:"user"`
	ID        int64     `json:"id"`
}

// RingBuffer is a thread-safe circular buffer for storing logs
type RingBuffer struct {
	buffer     []LogEntry
	size       int
	head       int // Points to the next write position
	full       bool
	lastID     int64
	mu         sync.RWMutex
}

// NewRingBuffer creates a new RingBuffer with the specified size
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]LogEntry, size),
		size:   size,
	}
}

// Write implements io.Writer to allow direct usage with loggers
func (rb *RingBuffer) Write(p []byte) (n int, err error) {
	// Copy slice to avoid mutation issues if p is reused
	msg := string(p)
	rb.Add(msg)
	return len(p), nil
}

// Add adds a new log message to the buffer
func (rb *RingBuffer) Add(message string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	entry := ParseLogEntry(message)
	entry.ID = rb.lastID + 1

	rb.lastID++

	rb.buffer[rb.head] = entry
	rb.head = (rb.head + 1) % rb.size
	if rb.head == 0 {
		rb.full = true
	}
}

// GetLogs retrieves logs from the buffer
func (rb *RingBuffer) GetLogs(limit int, offsetID int64) []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	var logs []LogEntry
	
	// Calculate number of items in buffer
	count := rb.head
	if rb.full {
		count = rb.size
	}

	if count == 0 {
		return []LogEntry{}
	}

	getAt := func(i int) LogEntry {
		if !rb.full {
			return rb.buffer[i]
		}
		physIdx := (rb.head + i) % rb.size
		return rb.buffer[physIdx]
	}

	for i := 0; i < count; i++ {
		entry := getAt(i)
		if entry.ID > offsetID {
			logs = append(logs, entry)
		}
	}

	if limit > 0 && len(logs) > limit {
		logs = logs[len(logs)-limit:]
	}

	return logs
}

// ParseLogEntry parses a raw log line into a LogEntry struct
func ParseLogEntry(line string) LogEntry {
	cleanMsg := strings.TrimSpace(line)
	if cleanMsg == "" {
		return LogEntry{}
	}

	// Check for JSON
	if strings.HasPrefix(cleanMsg, "{") {
		var jsonEntry struct {
			Level     string `json:"level"`
			Timestamp string `json:"ts"`
			Caller    string `json:"caller"`
			Msg       string `json:"msg"`
			// GIN specific fields
			Method  string `json:"method"`
			Path    string `json:"path"`
			Status  int    `json:"status"`
			User    string `json:"user"`
			Latency string `json:"latency"`
		}
		if err := json.Unmarshal([]byte(cleanMsg), &jsonEntry); err == nil {
			ts := time.Now()
			// Try parsing timestamp if available
			if jsonEntry.Timestamp != "" {
				// Try RFC3339 first (GIN logs)
				if t, err := time.Parse(time.RFC3339, jsonEntry.Timestamp); err == nil {
					ts = t
				} else {
					// Fallback: glog timestamp format is "mmdd hh:mm:ss.uuuuuu"
					// We could try to parse it relative to current year, but for now fallback to Now() is safe enough
					// or we just rely on Now() as before for glog entries.
				}
			}

			user := "system"
			if jsonEntry.User != "" {
				user = jsonEntry.User
			}

			message := jsonEntry.Msg
			// For HTTP logs, reconstruct a meaningful message if msg is empty or just complementary
			if jsonEntry.Level == "HTTP" {
				// Format: [GIN] | Status | Latency | Method Path | Error
				prefix := fmt.Sprintf("[GIN] | %d | %s | %s %s", jsonEntry.Status, jsonEntry.Latency, jsonEntry.Method, jsonEntry.Path)
				if message != "" {
					message = prefix + " | " + message
				} else {
					message = prefix
				}
			}

			return LogEntry{
				Timestamp: ts,
				Message:   message,
				Level:     jsonEntry.Level,
				User:      user,
			}
		}
	}

	// Fallback to text parsing
	level := "INFO"
	if len(cleanMsg) > 0 {
		switch cleanMsg[0] {
		case 'I':
			level = "INFO"
		case 'W':
			level = "WARN"
		case 'E':
			level = "ERROR"
		case 'F':
			level = "FATAL"
		case '[':
			if strings.HasPrefix(cleanMsg, "[GIN]") {
				level = "HTTP"
			}
		}
	}

	user := ""
	if level == "HTTP" {
		if idx := strings.Index(cleanMsg, "user: "); idx != -1 {
			start := idx + 6
			end := strings.Index(cleanMsg[start:], "|")
			if end != -1 {
				user = strings.TrimSpace(cleanMsg[start : start+end])
				toRemove := cleanMsg[idx : start+end+1]
				cleanMsg = strings.Replace(cleanMsg, toRemove, "", 1)
				cleanMsg = strings.Replace(cleanMsg, "|  |", "|", -1)
			} else {
				user = strings.TrimSpace(cleanMsg[start:])
			}
		}
	} else {
		user = "system"
	}

	return LogEntry{
		Timestamp: time.Now(),
		Message:   cleanMsg,
		Level:     level,
		User:      user,
	}
}

// ReadLogsFromFile reads the last N logs from a file
func ReadLogsFromFile(filename string, limit int, offsetID int64) ([]LogEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var logs []LogEntry
	scanner := bufio.NewScanner(file)
	
	// We need to support reading tail, so we might scan all and keep last N
	// Since 50MB is max, scan all is acceptable for Admin UI.
	
	var allEntries []LogEntry
	var lineID int64 = 0
	
	// Optimization: If limit is small, circular buffer? 
	// But we need to filter by offsetID which refers to line ID.
	// If offsetID is 0, we want last N.
	// If offsetID > 0, we want all lines > offsetID.
	
	for scanner.Scan() {
		lineID++
		// If we are looking for updates (offsetID > 0), skip lines before offset
		if offsetID > 0 && lineID <= offsetID {
			continue
		}
		
		entry := ParseLogEntry(scanner.Text())
		entry.ID = lineID
		
		if offsetID > 0 {
			// Collecting updates
			logs = append(logs, entry)
		} else {
			// Collecting tail
			allEntries = append(allEntries, entry)
			// Optimize memory: prevent growing indefinitely if we only need tail
			if len(allEntries) > limit * 2 { // simple hysteresis
				allEntries = allEntries[len(allEntries)-limit:]
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if offsetID > 0 {
		// Log rotation handling: if offsetID is very large (from old file) and current file has fewer lines,
		// we might return nothing. Clients usually poll. Resetting ID logic is complex.
		// For now, if we found logs, return them.
		
		// Apply limit
		if limit > 0 && len(logs) > limit {
			logs = logs[len(logs)-limit:]
		}
		return logs, nil
	} else {
		// Tail logic
		if limit > 0 && len(allEntries) > limit {
			return allEntries[len(allEntries)-limit:], nil
		}
		return allEntries, nil
	}
}
