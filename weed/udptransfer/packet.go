package udptransfer

import (
	"encoding/binary"
	"fmt"
)

const (
	_F_NIL  = 0
	_F_SYN  = 1
	_F_ACK  = 1 << 1
	_F_SACK = 1 << 2
	_F_TIME = 1 << 3
	_F_DATA = 1 << 4
	// reserved  = 1 << 5
	_F_RESET = 1 << 6
	_F_FIN   = 1 << 7
)

var packetTypeNames = map[byte]string{
	0:   "NOOP",
	1:   "SYN",
	2:   "ACK",
	3:   "SYN+ACK",
	4:   "SACK",
	8:   "TIME",
	12:  "SACK+TIME",
	16:  "DATA",
	64:  "RESET",
	128: "FIN",
	192: "FIN+RESET",
}

const (
	_S_FIN = iota
	_S_FIN0
	_S_FIN1
	_S_SYN0
	_S_SYN1
	_S_EST0
	_S_EST1
)

// Magic-6 | TH-10 | CH-10 | payload
const (
	_MAGIC_SIZE = 6
	_TH_SIZE    = 10 + _MAGIC_SIZE
	_CH_SIZE    = 10
	_AH_SIZE    = _TH_SIZE + _CH_SIZE
)

const (
	// Max UDP payload: 1500 MTU - 20 IP hdr - 8 UDP hdr  = 1472 bytes
	// Then: MSS = 1472-26 = 1446
	// And For ADSL: 1446-8 = 1438
	_MSS = 1438
)

const (
	_SENT_OK = 0xff
)

type packet struct {
	seq     uint32
	ack     uint32
	flag    uint8
	scnt    uint8
	payload []byte
	buffer  []byte
}

func (p *packet) marshall(id connID) []byte {
	buf := p.buffer
	if buf == nil {
		buf = make([]byte, _AH_SIZE+len(p.payload))
		copy(buf[_TH_SIZE+10:], p.payload)
	}
	binary.BigEndian.PutUint16(buf[_MAGIC_SIZE:], uint16(len(buf)))
	binary.BigEndian.PutUint32(buf[_MAGIC_SIZE+2:], id.rid)
	binary.BigEndian.PutUint32(buf[_MAGIC_SIZE+6:], id.lid)
	binary.BigEndian.PutUint32(buf[_TH_SIZE:], p.seq)
	binary.BigEndian.PutUint32(buf[_TH_SIZE+4:], p.ack)
	buf[_TH_SIZE+8] = p.flag
	buf[_TH_SIZE+9] = p.scnt
	return buf
}

func unmarshall(pk *packet, buf []byte) {
	if len(buf) >= _CH_SIZE {
		pk.seq = binary.BigEndian.Uint32(buf)
		pk.ack = binary.BigEndian.Uint32(buf[4:])
		pk.flag = buf[8]
		pk.scnt = buf[9]
		pk.payload = buf[10:]
	}
}

func (n *qNode) String() string {
	now := Now()
	return fmt.Sprintf("type=%s seq=%d scnt=%d sndtime~=%d,%d miss=%d",
		packetTypeNames[n.flag], n.seq, n.scnt, n.sent-now, n.sent_1-now, n.miss)
}

func maxI64(a, b int64) int64 {
	if a >= b {
		return a
	} else {
		return b
	}
}

func maxU32(a, b uint32) uint32 {
	if a >= b {
		return a
	} else {
		return b
	}
}

func minI64(a, b int64) int64 {
	if a <= b {
		return a
	} else {
		return b
	}
}

func maxI32(a, b int32) int32 {
	if a >= b {
		return a
	} else {
		return b
	}
}

func minI32(a, b int32) int32 {
	if a <= b {
		return a
	} else {
		return b
	}
}
