package udptransfer

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	_10ms  = time.Millisecond * 10
	_100ms = time.Millisecond * 100
)

const (
	_FIN_ACK_SEQ uint32 = 0xffFF0000
	_INVALID_SEQ uint32 = 0xffFFffFF
)

var (
	ErrIOTimeout        error = &TimeoutError{}
	ErrUnknown                = errors.New("Unknown error")
	ErrInexplicableData       = errors.New("Inexplicable data")
	ErrTooManyAttempts        = errors.New("Too many attempts to connect")
)

type TimeoutError struct{}

func (e *TimeoutError) Error() string   { return "i/o timeout" }
func (e *TimeoutError) Timeout() bool   { return true }
func (e *TimeoutError) Temporary() bool { return true }

type Conn struct {
	sock   *net.UDPConn
	dest   *net.UDPAddr
	edp    *Endpoint
	connID connID // 8 bytes
	// events
	evRecv  chan []byte
	evRead  chan byte
	evSend  chan byte
	evSWnd  chan byte
	evAck   chan byte
	evClose chan byte
	// protocol state
	inlock       sync.Mutex
	outlock      sync.Mutex
	state        int32
	mySeq        uint32
	swnd         int32
	cwnd         int32
	missed       int32
	outPending   int32
	lastAck      uint32
	lastAckTime  int64
	lastAckTime2 int64
	lastShrink   int64
	lastRstMis   int64
	ato          int64
	rto          int64
	rtt          int64
	srtt         int64
	mdev         int64
	rtmo         int64
	wtmo         int64
	tSlot        int64
	tSlotT0      int64
	lastSErr     int64
	// queue
	outQ        *linkedMap
	inQ         *linkedMap
	inQReady    []byte
	inQDirty    bool
	lastReadSeq uint32 // last user read seq
	// params
	bandwidth      int64
	fastRetransmit bool
	flatTraffic    bool
	mss            int
	// statistics
	urgent    int
	inPkCnt   int
	inDupCnt  int
	outPkCnt  int
	outDupCnt int
	fRCnt     int
}

func NewConn(e *Endpoint, dest *net.UDPAddr, id connID) *Conn {
	c := &Conn{
		sock:    e.udpconn,
		dest:    dest,
		edp:     e,
		connID:  id,
		evRecv:  make(chan []byte, 128),
		evRead:  make(chan byte, 1),
		evSWnd:  make(chan byte, 2),
		evSend:  make(chan byte, 4),
		evAck:   make(chan byte, 1),
		evClose: make(chan byte, 2),
		outQ:    newLinkedMap(_QModeOut),
		inQ:     newLinkedMap(_QModeIn),
	}
	p := e.params
	c.bandwidth = p.Bandwidth
	c.fastRetransmit = p.FastRetransmit
	c.flatTraffic = p.FlatTraffic
	c.mss = _MSS
	if dest.IP.To4() == nil {
		// typical ipv6 header length=40
		c.mss -= 20
	}
	return c
}

func (c *Conn) initConnection(buf []byte) (err error) {
	if buf == nil {
		err = c.initDialing()
	} else { //server
		err = c.acceptConnection(buf[_TH_SIZE:])
	}
	if err != nil {
		return
	}
	if c.state == _S_EST1 {
		c.lastReadSeq = c.lastAck
		c.inQ.maxCtnSeq = c.lastAck
		c.rtt = maxI64(c.rtt, _MIN_RTT)
		c.mdev = c.rtt << 1
		c.srtt = c.rtt << 3
		c.rto = maxI64(c.rtt*2, _MIN_RTO)
		c.ato = maxI64(c.rtt>>4, _MIN_ATO)
		c.ato = minI64(c.ato, _MAX_ATO)
		// initial cwnd
		c.swnd = calSwnd(c.bandwidth, c.rtt) >> 1
		c.cwnd = 8
		go c.internalRecvLoop()
		go c.internalSendLoop()
		go c.internalAckLoop()
		if debug >= 0 {
			go c.internal_state()
		}
		return nil
	} else {
		return ErrUnknown
	}
}

func (c *Conn) initDialing() error {
	// first syn
	pk := &packet{
		seq:  c.mySeq,
		flag: _F_SYN,
	}
	item := nodeOf(pk)
	var buf []byte
	c.state = _S_SYN0
	t0 := Now()
	for i := 0; i < _MAX_RETRIES && c.state == _S_SYN0; i++ {
		// send syn
		c.internalWrite(item)
		select {
		case buf = <-c.evRecv:
			c.rtt = Now() - t0
			c.state = _S_SYN1
			c.connID.setRid(buf)
			buf = buf[_TH_SIZE:]
		case <-time.After(time.Second):
			continue
		}
	}
	if c.state == _S_SYN0 {
		return ErrTooManyAttempts
	}

	unmarshall(pk, buf)
	// expected syn+ack
	if pk.flag == _F_SYN|_F_ACK && pk.ack == c.mySeq {
		if scnt := pk.scnt - 1; scnt > 0 {
			c.rtt -= int64(scnt) * 1e3
		}
		log.Println("rtt", c.rtt)
		c.state = _S_EST0
		// build ack3
		pk.scnt = 0
		pk.ack = pk.seq
		pk.flag = _F_ACK
		item := nodeOf(pk)
		// send ack3
		c.internalWrite(item)
		// update lastAck
		c.logAck(pk.ack)
		c.state = _S_EST1
		return nil
	} else {
		return ErrInexplicableData
	}
}

func (c *Conn) acceptConnection(buf []byte) error {
	var pk = new(packet)
	var item *qNode
	unmarshall(pk, buf)
	// expected syn
	if pk.flag == _F_SYN {
		c.state = _S_SYN1
		// build syn+ack
		pk.ack = pk.seq
		pk.seq = c.mySeq
		pk.flag |= _F_ACK
		// update lastAck
		c.logAck(pk.ack)
		item = nodeOf(pk)
		item.scnt = pk.scnt - 1
	} else {
		dumpb("Syn1 ?", buf)
		return ErrInexplicableData
	}
	for i := 0; i < 5 && c.state == _S_SYN1; i++ {
		t0 := Now()
		// reply syn+ack
		c.internalWrite(item)
		// recv ack3
		select {
		case buf = <-c.evRecv:
			c.state = _S_EST0
			c.rtt = Now() - t0
			buf = buf[_TH_SIZE:]
			log.Println("rtt", c.rtt)
		case <-time.After(time.Second):
			continue
		}
	}
	if c.state == _S_SYN1 {
		return ErrTooManyAttempts
	}

	pk = new(packet)
	unmarshall(pk, buf)
	// expected ack3
	if pk.flag == _F_ACK && pk.ack == c.mySeq {
		c.state = _S_EST1
	} else {
		// if ack3 lost, resend syn+ack 3-times
		// and drop these coming data
		if pk.flag&_F_DATA != 0 && pk.seq > c.lastAck {
			c.internalWrite(item)
			c.state = _S_EST1
		} else {
			dumpb("Ack3 ?", buf)
			return ErrInexplicableData
		}
	}
	return nil
}

// 20,20,20,20, 100,100,100,100, 1s,1s,1s,1s
func selfSpinWait(fn func() bool) error {
	const _MAX_SPIN = 12
	for i := 0; i < _MAX_SPIN; i++ {
		if fn() {
			return nil
		} else if i <= 3 {
			time.Sleep(_10ms * 2)
		} else if i <= 7 {
			time.Sleep(_100ms)
		} else {
			time.Sleep(time.Second)
		}
	}
	return ErrIOTimeout
}

func (c *Conn) IsClosed() bool {
	return atomic.LoadInt32(&c.state) <= _S_FIN1
}

/*
active close:
1 <- send fin-W: closeW()
	 before sending, ensure all outQ items has beed sent out and all of them has been acked.
2 -> wait to recv ack{fin-W}
	 then trigger closeR, including send fin-R and wait to recv ack{fin-R}

passive close:
-> fin:
	if outQ is not empty then self-spin wait.
	if outQ empty, send ack{fin-W} then goto closeW().
*/
func (c *Conn) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&c.state, _S_EST1, _S_FIN0) {
		return selfSpinWait(func() bool {
			return atomic.LoadInt32(&c.state) == _S_FIN
		})
	}
	var err0 error
	err0 = c.closeW()
	// waiting for fin-2 of peer
	err = selfSpinWait(func() bool {
		select {
		case v := <-c.evClose:
			if v == _S_FIN {
				return true
			} else {
				time.AfterFunc(_100ms, func() { c.evClose <- v })
			}
		default:
		}
		return false
	})
	defer c.afterShutdown()
	if err != nil {
		// backup path for wait ack(finW) timeout
		c.closeR(nil)
	}
	if err0 != nil {
		return err0
	} else {
		return
	}
}

func (c *Conn) beforeCloseW() (err error) {
	// check outQ was empty and all has been acked.
	// self-spin waiting
	for i := 0; i < 2; i++ {
		err = selfSpinWait(func() bool {
			return atomic.LoadInt32(&c.outPending) <= 0
		})
		if err == nil {
			break
		}
	}
	// send fin, reliably
	c.outlock.Lock()
	c.mySeq++
	c.outPending++
	pk := &packet{seq: c.mySeq, flag: _F_FIN}
	item := nodeOf(pk)
	c.outQ.appendTail(item)
	c.internalWrite(item)
	c.outlock.Unlock()
	c.evSWnd <- _VSWND_ACTIVE
	return
}

func (c *Conn) closeW() (err error) {
	// close resource of sending
	defer c.afterCloseW()
	// send fin
	err = c.beforeCloseW()
	var closed bool
	var max = 20
	if c.rtt > 200 {
		max = int(c.rtt) / 10
	}
	// waiting for outQ means:
	// 1. all outQ has been acked, for passive
	// 2. fin has been acked, for active
	for i := 0; i < max && (atomic.LoadInt32(&c.outPending) > 0 || !closed); i++ {
		select {
		case v := <-c.evClose:
			if v == _S_FIN0 {
				// namely, last fin has been acked.
				closed = true
			} else {
				time.AfterFunc(_100ms, func() { c.evClose <- v })
			}
		case <-time.After(_100ms):
		}
	}
	if closed || err != nil {
		return
	} else {
		return ErrIOTimeout
	}
}

func (c *Conn) afterCloseW() {
	// can't close(c.evRecv), avoid endpoint dispatch exception
	// stop pending inputAndSend
	select {
	case c.evSend <- _CLOSE:
	default:
	}
	// stop internalSendLoop
	c.evSWnd <- _CLOSE
}

// called by active and passive close()
func (c *Conn) afterShutdown() {
	// stop internalRecvLoop
	c.evRecv <- nil
	// remove registry
	c.edp.removeConn(c.connID, c.dest)
	log.Println("shutdown", c.state)
}

// trigger by reset
func (c *Conn) forceShutdownWithLock() {
	c.outlock.Lock()
	defer c.outlock.Unlock()
	c.forceShutdown()
}

// called by:
// 	1/ send exception
//	2/ recv reset
// drop outQ and force shutdown
func (c *Conn) forceShutdown() {
	if atomic.CompareAndSwapInt32(&c.state, _S_EST1, _S_FIN) {
		defer c.afterShutdown()
		// stop sender
		for i := 0; i < cap(c.evSend); i++ {
			select {
			case <-c.evSend:
			default:
			}
		}
		select {
		case c.evSend <- _CLOSE:
		default:
		}
		c.outQ.reset()
		// stop reader
		close(c.evRead)
		c.inQ.reset()
		// stop internalLoops
		c.evSWnd <- _CLOSE
		c.evAck <- _CLOSE
		//log.Println("force shutdown")
	}
}

// for sending fin failed
func (c *Conn) fakeShutdown() {
	select {
	case c.evClose <- _S_FIN0:
	default:
	}
}

func (c *Conn) closeR(pk *packet) {
	var passive = true
	for {
		state := atomic.LoadInt32(&c.state)
		switch state {
		case _S_FIN:
			return
		case _S_FIN1: // multiple FIN, maybe lost
			c.passiveCloseReply(pk, false)
			return
		case _S_FIN0: // active close preformed
			passive = false
		}
		if !atomic.CompareAndSwapInt32(&c.state, state, _S_FIN1) {
			continue
		}
		c.passiveCloseReply(pk, true)
		break
	}
	// here, R is closed.
	// ^^^^^^^^^^^^^^^^^^^^^
	if passive {
		// passive closing call closeW contains sending fin and recv ack
		// may the ack of fin-2 was lost, then the closeW will timeout
		c.closeW()
	}
	// here, R,W both were closed.
	// ^^^^^^^^^^^^^^^^^^^^^
	atomic.StoreInt32(&c.state, _S_FIN)
	// stop internalAckLoop
	c.evAck <- _CLOSE

	if passive {
		// close evRecv within here
		c.afterShutdown()
	} else {
		// notify active close thread
		select {
		case c.evClose <- _S_FIN:
		default:
		}
	}
}

func (c *Conn) passiveCloseReply(pk *packet, first bool) {
	if pk != nil && pk.flag&_F_FIN != 0 {
		if first {
			c.checkInQ(pk)
			close(c.evRead)
		}
		// ack the FIN
		pk = &packet{seq: _FIN_ACK_SEQ, ack: pk.seq, flag: _F_ACK}
		item := nodeOf(pk)
		c.internalWrite(item)
	}
}

// check inQ ends orderly, and copy queue data to user space
func (c *Conn) checkInQ(pk *packet) {
	if nil != selfSpinWait(func() bool {
		return c.inQ.maxCtnSeq+1 == pk.seq
	}) { // timeout for waiting inQ to finish
		return
	}
	c.inlock.Lock()
	defer c.inlock.Unlock()
	if c.inQ.size() > 0 {
		for i := c.inQ.head; i != nil; i = i.next {
			c.inQReady = append(c.inQReady, i.payload...)
		}
	}
}
