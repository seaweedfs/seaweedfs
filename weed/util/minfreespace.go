package util

import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"strconv"
	"strings"
)

// MinFreeSpaceType is the type of MinFreeSpace.
type MinFreeSpaceType int

const (
	// AsPercent set the MinFreeSpaceType to a percentage value from 0 to 100.
	AsPercent MinFreeSpaceType = iota
	// AsBytes set the MinFreeSpaceType to a absolute value bytes.
	AsBytes
)

// MinFreeSpace is type that defines the limit for the minimum free space.
type MinFreeSpace struct {
	Type    MinFreeSpaceType
	Bytes   uint64
	Percent float32
	Raw     string
}

// IsLow tells whether the free space is low or not.
func (s MinFreeSpace) IsLow(freeBytes uint64, freePercent float32) (yes bool, desc string) {
	switch s.Type {
	case AsPercent:
		yes = freePercent < s.Percent
		op := IfElse(yes, "<", ">=")
		return yes, fmt.Sprintf("disk free %.2f%% %s required %.2f%%", freePercent, op, s.Percent)
	case AsBytes:
		yes = freeBytes < s.Bytes
		op := IfElse(yes, "<", ">=")
		return yes, fmt.Sprintf("disk free %s %s required %s",
			BytesToHumanReadable(freeBytes), op, BytesToHumanReadable(s.Bytes))
	}

	return false, ""
}

// String returns a string representation of MinFreeSpace.
func (s MinFreeSpace) String() string {
	switch s.Type {
	case AsPercent:
		return fmt.Sprintf("%.2f%%", s.Percent)
	default:
		return s.Raw
	}
}

// MustParseMinFreeSpace parses comma-separated argument for min free space setting.
// minFreeSpace has the high priority than minFreeSpacePercent if it is set.
func MustParseMinFreeSpace(minFreeSpace string, minFreeSpacePercent string) (spaces []MinFreeSpace) {
	ss := strings.Split(EmptyTo(minFreeSpace, minFreeSpacePercent), ",")
	for _, freeString := range ss {
		if vv, e := ParseMinFreeSpace(freeString); e == nil {
			spaces = append(spaces, *vv)
		} else {
			glog.Fatalf("The value specified in -minFreeSpace not a valid value %s", freeString)
		}
	}

	return spaces
}

var ErrMinFreeSpaceBadValue = errors.New("minFreeSpace is invalid")

// ParseMinFreeSpace parses min free space expression s as percentage like 1,10 or human readable size like 10G
func ParseMinFreeSpace(s string) (*MinFreeSpace, error) {
	if percent, e := strconv.ParseFloat(s, 32); e == nil {
		if percent < 0 || percent > 100 {
			return nil, ErrMinFreeSpaceBadValue
		}
		return &MinFreeSpace{Type: AsPercent, Percent: float32(percent), Raw: s}, nil
	}

	if directSize, e := ParseBytes(s); e == nil {
		if directSize <= 100 {
			return nil, ErrMinFreeSpaceBadValue
		}
		return &MinFreeSpace{Type: AsBytes, Bytes: directSize, Raw: s}, nil
	}

	return nil, ErrMinFreeSpaceBadValue
}
