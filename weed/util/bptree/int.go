package bptree

import (
	"encoding/binary"
	"fmt"
)

type Int8 int8
type UInt8 uint8
type Int16 int16
type UInt16 uint16
type Int32 int32
type UInt32 uint32
type Int64 int64
type UInt64 uint64
type Int int
type UInt uint

func (self *Int8) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 0)
	bytes[0] = uint8(*self)
	return bytes, nil
}

func (self *Int8) UnmarshalBinary(data []byte) error {
	if len(data) != 1 {
		return fmt.Errorf("data wrong size")
	}
	*self = Int8(data[0])
	return nil
}

func (self Int8) Equals(other Equatable) bool {
	if o, ok := other.(Int8); ok {
		return self == o
	} else {
		return false
	}
}

func (self Int8) Less(other Sortable) bool {
	if o, ok := other.(Int8); ok {
		return self < o
	} else {
		return false
	}
}

func (self Int8) Hash() int {
	return int(self)
}

func (self *UInt8) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 0)
	bytes[0] = uint8(*self)
	return bytes, nil
}

func (self *UInt8) UnmarshalBinary(data []byte) error {
	if len(data) != 1 {
		return fmt.Errorf("data wrong size")
	}
	*self = UInt8(data[0])
	return nil
}

func (self UInt8) Equals(other Equatable) bool {
	if o, ok := other.(UInt8); ok {
		return self == o
	} else {
		return false
	}
}

func (self UInt8) Less(other Sortable) bool {
	if o, ok := other.(UInt8); ok {
		return self < o
	} else {
		return false
	}
}

func (self UInt8) Hash() int {
	return int(self)
}

func (self *Int16) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, uint16(*self))
	return bytes, nil
}

func (self *Int16) UnmarshalBinary(data []byte) error {
	if len(data) != 2 {
		return fmt.Errorf("data wrong size")
	}
	*self = Int16(binary.BigEndian.Uint16(data))
	return nil
}

func (self Int16) Equals(other Equatable) bool {
	if o, ok := other.(Int16); ok {
		return self == o
	} else {
		return false
	}
}

func (self Int16) Less(other Sortable) bool {
	if o, ok := other.(Int16); ok {
		return self < o
	} else {
		return false
	}
}

func (self Int16) Hash() int {
	return int(self)
}

func (self *UInt16) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, uint16(*self))
	return bytes, nil
}

func (self *UInt16) UnmarshalBinary(data []byte) error {
	if len(data) != 2 {
		return fmt.Errorf("data wrong size")
	}
	*self = UInt16(binary.BigEndian.Uint16(data))
	return nil
}

func (self UInt16) Equals(other Equatable) bool {
	if o, ok := other.(UInt16); ok {
		return self == o
	} else {
		return false
	}
}

func (self UInt16) Less(other Sortable) bool {
	if o, ok := other.(UInt16); ok {
		return self < o
	} else {
		return false
	}
}

func (self UInt16) Hash() int {
	return int(self)
}

func (self *Int32) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(*self))
	return bytes, nil
}

func (self *Int32) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("data wrong size")
	}
	*self = Int32(binary.BigEndian.Uint32(data))
	return nil
}

func (self Int32) Equals(other Equatable) bool {
	if o, ok := other.(Int32); ok {
		return self == o
	} else {
		return false
	}
}

func (self Int32) Less(other Sortable) bool {
	if o, ok := other.(Int32); ok {
		return self < o
	} else {
		return false
	}
}

func (self *UInt32) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(*self))
	return bytes, nil
}

func (self *UInt32) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("data wrong size")
	}
	*self = UInt32(binary.BigEndian.Uint32(data))
	return nil
}

func (self Int32) Hash() int {
	return int(self)
}

func (self UInt32) Equals(other Equatable) bool {
	if o, ok := other.(UInt32); ok {
		return self == o
	} else {
		return false
	}
}

func (self UInt32) Less(other Sortable) bool {
	if o, ok := other.(UInt32); ok {
		return self < o
	} else {
		return false
	}
}

func (self UInt32) Hash() int {
	return int(self)
}

func (self *Int64) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(*self))
	return bytes, nil
}

func (self *Int64) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("data wrong size")
	}
	*self = Int64(binary.BigEndian.Uint64(data))
	return nil
}

func (self Int64) Equals(other Equatable) bool {
	if o, ok := other.(Int64); ok {
		return self == o
	} else {
		return false
	}
}

func (self Int64) Less(other Sortable) bool {
	if o, ok := other.(Int64); ok {
		return self < o
	} else {
		return false
	}
}

func (self Int64) Hash() int {
	return int(self>>32) ^ int(self)
}

func (self *UInt64) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(*self))
	return bytes, nil
}

func (self *UInt64) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("data wrong size")
	}
	*self = UInt64(binary.BigEndian.Uint64(data))
	return nil
}

func (self UInt64) Equals(other Equatable) bool {
	if o, ok := other.(UInt64); ok {
		return self == o
	} else {
		return false
	}
}

func (self UInt64) Less(other Sortable) bool {
	if o, ok := other.(UInt64); ok {
		return self < o
	} else {
		return false
	}
}

func (self UInt64) Hash() int {
	return int(self>>32) ^ int(self)
}

func (self *Int) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(*self))
	return bytes, nil
}

func (self *Int) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("data wrong size")
	}
	*self = Int(binary.BigEndian.Uint32(data))
	return nil
}

func (self Int) Equals(other Equatable) bool {
	if o, ok := other.(Int); ok {
		return self == o
	} else {
		return false
	}
}

func (self Int) Less(other Sortable) bool {
	if o, ok := other.(Int); ok {
		return self < o
	} else {
		return false
	}
}

func (self Int) Hash() int {
	return int(self)
}

func (self *UInt) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(*self))
	return bytes, nil
}

func (self *UInt) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("data wrong size")
	}
	*self = UInt(binary.BigEndian.Uint32(data))
	return nil
}

func (self UInt) Equals(other Equatable) bool {
	if o, ok := other.(UInt); ok {
		return self == o
	} else {
		return false
	}
}

func (self UInt) Less(other Sortable) bool {
	if o, ok := other.(UInt); ok {
		return self < o
	} else {
		return false
	}
}

func (self UInt) Hash() int {
	return int(self)
}
