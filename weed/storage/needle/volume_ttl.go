package needle

import (
	"fmt"
	"strconv"
)

const (
	// stored unit types
	Empty byte = iota
	Minute
	Hour
	Day
	Week
	Month
	Year
)

type TTL struct {
	Count byte
	Unit  byte
}

var EMPTY_TTL = &TTL{}

// translate a readable ttl to internal ttl
// Supports format example:
// 3m: 3 minutes
// 4h: 4 hours
// 5d: 5 days
// 6w: 6 weeks
// 7M: 7 months
// 8y: 8 years
func ReadTTL(ttlString string) (*TTL, error) {
	if ttlString == "" {
		return EMPTY_TTL, nil
	}
	ttlBytes := []byte(ttlString)
	unitByte := ttlBytes[len(ttlBytes)-1]
	countBytes := ttlBytes[0 : len(ttlBytes)-1]
	if '0' <= unitByte && unitByte <= '9' {
		countBytes = ttlBytes
		unitByte = 'm'
	}
	count, err := strconv.Atoi(string(countBytes))
	unit := toStoredByte(unitByte)
	return fitTtlCount(count, unit), err
}

func fitTtlCount(count int, unit byte) *TTL {
	seconds := ToSeconds(count, unit)
	if seconds == 0 {
		return EMPTY_TTL
	}
	if seconds%(3600*24*365) == 0 && seconds/(3600*24*365) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 365)), Unit: Year}
	}
	if seconds%(3600*24*30) == 0 && seconds/(3600*24*30) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 30)), Unit: Month}
	}
	if seconds%(3600*24*7) == 0 && seconds/(3600*24*7) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 7)), Unit: Week}
	}
	if seconds%(3600*24) == 0 && seconds/(3600*24) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24)), Unit: Day}
	}
	if seconds%(3600) == 0 && seconds/(3600) < 256 {
		return &TTL{Count: byte(seconds / (3600)), Unit: Hour}
	}
	if seconds/60 < 256 {
		return &TTL{Count: byte(seconds / 60), Unit: Minute}
	}
	if seconds/(3600) < 256 {
		return &TTL{Count: byte(seconds / (3600)), Unit: Hour}
	}
	if seconds/(3600*24) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24)), Unit: Day}
	}
	if seconds/(3600*24*7) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 7)), Unit: Week}
	}
	if seconds/(3600*24*30) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 30)), Unit: Month}
	}
	if seconds/(3600*24*365) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 365)), Unit: Year}
	}
	return EMPTY_TTL
}

// read stored bytes to a ttl
func LoadTTLFromBytes(input []byte) (t *TTL) {
	if input[0] == 0 && input[1] == 0 {
		return EMPTY_TTL
	}
	return &TTL{Count: input[0], Unit: input[1]}
}

// read stored bytes to a ttl
func LoadTTLFromUint32(ttl uint32) (t *TTL) {
	input := make([]byte, 2)
	input[1] = byte(ttl)
	input[0] = byte(ttl >> 8)
	return LoadTTLFromBytes(input)
}

// save stored bytes to an output with 2 bytes
func (t *TTL) ToBytes(output []byte) {
	output[0] = t.Count
	output[1] = t.Unit
}

func (t *TTL) ToUint32() (output uint32) {
	if t == nil || t.Count == 0 {
		return 0
	}
	output = uint32(t.Count) << 8
	output += uint32(t.Unit)
	return output
}

func (t *TTL) String() string {
	if t == nil || t.Count == 0 {
		return ""
	}
	if t.Unit == Empty {
		return ""
	}
	countString := strconv.Itoa(int(t.Count))
	switch t.Unit {
	case Minute:
		return countString + "m"
	case Hour:
		return countString + "h"
	case Day:
		return countString + "d"
	case Week:
		return countString + "w"
	case Month:
		return countString + "M"
	case Year:
		return countString + "y"
	}
	return ""
}

func (t *TTL) ToSeconds() uint64 {
	return ToSeconds(int(t.Count), t.Unit)
}

func ToSeconds(count int, unit byte) uint64 {
	switch unit {
	case Empty:
		return 0
	case Minute:
		return uint64(count) * 60
	case Hour:
		return uint64(count) * 60 * 60
	case Day:
		return uint64(count) * 60 * 24 * 60
	case Week:
		return uint64(count) * 60 * 24 * 7 * 60
	case Month:
		return uint64(count) * 60 * 24 * 30 * 60
	case Year:
		return uint64(count) * 60 * 24 * 365 * 60
	}
	return 0
}

func toStoredByte(readableUnitByte byte) byte {
	switch readableUnitByte {
	case 'm':
		return Minute
	case 'h':
		return Hour
	case 'd':
		return Day
	case 'w':
		return Week
	case 'M':
		return Month
	case 'y':
		return Year
	}
	return 0
}

func (t TTL) Minutes() uint32 {
	switch t.Unit {
	case Empty:
		return 0
	case Minute:
		return uint32(t.Count)
	case Hour:
		return uint32(t.Count) * 60
	case Day:
		return uint32(t.Count) * 60 * 24
	case Week:
		return uint32(t.Count) * 60 * 24 * 7
	case Month:
		return uint32(t.Count) * 60 * 24 * 30
	case Year:
		return uint32(t.Count) * 60 * 24 * 365
	}
	return 0
}

func SecondsToTTL(seconds int32) string {
	if seconds == 0 {
		return ""
	}
	if seconds%(3600*24*365) == 0 && seconds/(3600*24*365) < 256 {
		return fmt.Sprintf("%dy", seconds/(3600*24*365))
	}
	if seconds%(3600*24*30) == 0 && seconds/(3600*24*30) < 256 {
		return fmt.Sprintf("%dM", seconds/(3600*24*30))
	}
	if seconds%(3600*24*7) == 0 && seconds/(3600*24*7) < 256 {
		return fmt.Sprintf("%dw", seconds/(3600*24*7))
	}
	if seconds%(3600*24) == 0 && seconds/(3600*24) < 256 {
		return fmt.Sprintf("%dd", seconds/(3600*24))
	}
	if seconds%(3600) == 0 && seconds/(3600) < 256 {
		return fmt.Sprintf("%dh", seconds/(3600))
	}
	if seconds/60 < 256 {
		return fmt.Sprintf("%dm", seconds/60)
	}
	if seconds/(3600) < 256 {
		return fmt.Sprintf("%dh", seconds/(3600))
	}
	if seconds/(3600*24) < 256 {
		return fmt.Sprintf("%dd", seconds/(3600*24))
	}
	if seconds/(3600*24*7) < 256 {
		return fmt.Sprintf("%dw", seconds/(3600*24*7))
	}
	if seconds/(3600*24*30) < 256 {
		return fmt.Sprintf("%dM", seconds/(3600*24*30))
	}
	if seconds/(3600*24*365) < 256 {
		return fmt.Sprintf("%dy", seconds/(3600*24*365))
	}
	return ""
}
