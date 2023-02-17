package data

import "fmt"

type Datum interface {
	Compare(other Datum) (int, error)
}
type Datums []Datum

type DUint16 uint16
type DUint32 uint32
type DUint64 uint64
type dNull struct{}

var (
	DNull Datum = dNull{}
)

func (d dNull) Compare(other Datum) (int, error) {
	if other == DNull {
		return 0, nil
	}
	return -1, nil
}

func NewDUint16(d DUint16) *DUint16 {
	return &d
}
func NewDUint32(d DUint32) *DUint32 {
	return &d
}
func NewDUint64(d DUint64) *DUint64 {
	return &d
}

func (d *DUint16) Compare(other Datum) (int, error) {
	if other == DNull {
		return 1, nil
	}
	thisV := *d
	var otherV DUint16
	switch t := other.(type) {
	case *DUint16:
		otherV = *t
	default:
		return 0, fmt.Errorf("unsupported")
	}
	if thisV < otherV {
		return -1, nil
	}
	if thisV > otherV {
		return 1, nil
	}
	return 0, nil
}
func (d *DUint32) Compare(other Datum) (int, error) {
	if other == DNull {
		return 1, nil
	}
	thisV := *d
	var otherV DUint32
	switch t := other.(type) {
	case *DUint32:
		otherV = *t
	}
	if thisV < otherV {
		return -1, nil
	}
	if thisV > otherV {
		return 1, nil
	}
	return 0, nil
}
func (d *DUint64) Compare(other Datum) (int, error) {
	if other == DNull {
		return 1, nil
	}
	thisV := *d
	var otherV DUint64
	switch t := other.(type) {
	case *DUint64:
		otherV = *t
	}
	if thisV < otherV {
		return -1, nil
	}
	if thisV > otherV {
		return 1, nil
	}
	return 0, nil
}
