package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// ===============================
// DATE/TIME CONSTANTS
// ===============================

// CurrentDate returns the current date as a string in YYYY-MM-DD format
func (e *SQLEngine) CurrentDate() (*schema_pb.Value, error) {
	now := time.Now()
	dateStr := now.Format("2006-01-02")

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: dateStr},
	}, nil
}

// CurrentTimestamp returns the current timestamp
func (e *SQLEngine) CurrentTimestamp() (*schema_pb.Value, error) {
	now := time.Now()

	// Return as TimestampValue with microseconds
	timestampMicros := now.UnixMicro()

	return &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: timestampMicros,
			},
		},
	}, nil
}

// CurrentTime returns the current time as a string in HH:MM:SS format
func (e *SQLEngine) CurrentTime() (*schema_pb.Value, error) {
	now := time.Now()
	timeStr := now.Format("15:04:05")

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: timeStr},
	}, nil
}

// Now is an alias for CurrentTimestamp (common SQL function name)
func (e *SQLEngine) Now() (*schema_pb.Value, error) {
	return e.CurrentTimestamp()
}

// ===============================
// EXTRACT FUNCTION
// ===============================

// DatePart represents the part of a date/time to extract
type DatePart string

const (
	PartYear      DatePart = "YEAR"
	PartMonth     DatePart = "MONTH"
	PartDay       DatePart = "DAY"
	PartHour      DatePart = "HOUR"
	PartMinute    DatePart = "MINUTE"
	PartSecond    DatePart = "SECOND"
	PartWeek      DatePart = "WEEK"
	PartDayOfYear DatePart = "DOY"
	PartDayOfWeek DatePart = "DOW"
	PartQuarter   DatePart = "QUARTER"
	PartEpoch     DatePart = "EPOCH"
)

// Extract extracts a specific part from a date/time value
func (e *SQLEngine) Extract(part DatePart, value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("EXTRACT function requires non-null value")
	}

	// Convert value to time
	t, err := e.valueToTime(value)
	if err != nil {
		return nil, fmt.Errorf("EXTRACT function time conversion error: %v", err)
	}

	var result int64

	switch strings.ToUpper(string(part)) {
	case string(PartYear):
		result = int64(t.Year())
	case string(PartMonth):
		result = int64(t.Month())
	case string(PartDay):
		result = int64(t.Day())
	case string(PartHour):
		result = int64(t.Hour())
	case string(PartMinute):
		result = int64(t.Minute())
	case string(PartSecond):
		result = int64(t.Second())
	case string(PartWeek):
		_, week := t.ISOWeek()
		result = int64(week)
	case string(PartDayOfYear):
		result = int64(t.YearDay())
	case string(PartDayOfWeek):
		result = int64(t.Weekday())
	case string(PartQuarter):
		month := t.Month()
		result = int64((month-1)/3 + 1)
	case string(PartEpoch):
		result = t.Unix()
	default:
		return nil, fmt.Errorf("unsupported date part: %s", part)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: result},
	}, nil
}

// ===============================
// DATE_TRUNC FUNCTION
// ===============================

// DateTrunc truncates a date/time to the specified precision
func (e *SQLEngine) DateTrunc(precision string, value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("DATE_TRUNC function requires non-null value")
	}

	// Convert value to time
	t, err := e.valueToTime(value)
	if err != nil {
		return nil, fmt.Errorf("DATE_TRUNC function time conversion error: %v", err)
	}

	var truncated time.Time

	switch strings.ToLower(precision) {
	case "microsecond", "microseconds":
		// No truncation needed for microsecond precision
		truncated = t
	case "millisecond", "milliseconds":
		truncated = t.Truncate(time.Millisecond)
	case "second", "seconds":
		truncated = t.Truncate(time.Second)
	case "minute", "minutes":
		truncated = t.Truncate(time.Minute)
	case "hour", "hours":
		truncated = t.Truncate(time.Hour)
	case "day", "days":
		truncated = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "week", "weeks":
		// Truncate to beginning of week (Monday)
		days := int(t.Weekday())
		if days == 0 { // Sunday = 0, adjust to make Monday = 0
			days = 6
		} else {
			days = days - 1
		}
		truncated = time.Date(t.Year(), t.Month(), t.Day()-days, 0, 0, 0, 0, t.Location())
	case "month", "months":
		truncated = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "quarter", "quarters":
		month := t.Month()
		quarterMonth := ((int(month)-1)/3)*3 + 1
		truncated = time.Date(t.Year(), time.Month(quarterMonth), 1, 0, 0, 0, 0, t.Location())
	case "year", "years":
		truncated = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "decade", "decades":
		year := (t.Year() / 10) * 10
		truncated = time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
	case "century", "centuries":
		year := ((t.Year()-1)/100)*100 + 1
		truncated = time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
	case "millennium", "millennia":
		year := ((t.Year()-1)/1000)*1000 + 1
		truncated = time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
	default:
		return nil, fmt.Errorf("unsupported date truncation precision: %s", precision)
	}

	// Return as TimestampValue
	return &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: truncated.UnixMicro(),
			},
		},
	}, nil
}
