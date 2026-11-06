package logbuffer

import "time"

// LogEntry represents a single parsed log entry stored in the ring buffer.
type LogEntry struct {
	Timestamp time.Time `json:"ts"`
	Level     string    `json:"level"`
	File      string    `json:"file"`
	Line      int       `json:"line"`
	Message   string    `json:"msg"`
	RequestID string    `json:"request_id,omitempty"`
}

// Filter defines criteria for querying log entries.
type Filter struct {
	Level     string // minimum severity: INFO, WARNING, ERROR, FATAL
	Limit     int
	Offset    int
	Since     time.Time
	Until     time.Time
	Pattern   string // regex pattern to match against message
	File      string // glob pattern to match source file
	RequestID string
}

// QueryResult holds the response for a log query.
type QueryResult struct {
	Entries []LogEntry `json:"entries"`
	Total   int        `json:"total"`
	HasMore bool       `json:"has_more"`
	Error   string     `json:"error,omitempty"`
}

// LevelResponse holds the current log level configuration.
type LevelResponse struct {
	Verbosity int    `json:"verbosity"`
	VModule   string `json:"vmodule"`
}

// LevelChangeRequest holds a request to change log verbosity.
type LevelChangeRequest struct {
	Verbosity  *int   `json:"verbosity,omitempty"`
	TTLMinutes int    `json:"ttl_minutes,omitempty"`
	VModule    string `json:"vmodule,omitempty"`
}

// LevelChangeResponse holds the response after changing log verbosity.
type LevelChangeResponse struct {
	Previous int    `json:"previous"`
	Current  int    `json:"current"`
	RevertAt string `json:"revert_at,omitempty"`
}

// FileCount represents a source file and how many log entries it generated.
type FileCount struct {
	File  string `json:"file"`
	Count int    `json:"count"`
}

// ErrorRateResult holds error rate metrics for a time window.
type ErrorRateResult struct {
	Window     string  `json:"window"`
	TotalLogs  int     `json:"total_logs"`
	ErrorCount int     `json:"error_count"`
	ErrorRate  float64 `json:"error_rate_percent"`
}

// StatsResponse is the full response for the /logs/stats endpoint.
type StatsResponse struct {
	Buffer      BufferStats     `json:"buffer"`
	RecentErrs  []LogEntry      `json:"recent_errors"`
	TopFiles    []FileCount     `json:"top_files"`
	ErrorRate1m ErrorRateResult `json:"error_rate_1m"`
	ErrorRate5m ErrorRateResult `json:"error_rate_5m"`
}
