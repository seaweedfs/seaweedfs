package udptransfer

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

const (
	_MAX_RETRIES = 6
	_MIN_RTT     = 8
	_MIN_RTO     = 30
	_MIN_ATO     = 2
	_MAX_ATO     = 10
	_MIN_SWND    = 10
	_MAX_SWND    = 960
)

const (
	_VACK_SCHED = iota + 1
	_VACK_QUICK
	_VACK_MUST
	_VSWND_ACTIVE
	_VRETR_IMMED
)

const (
	_RETR_REST = -1
	_CLOSE     = 0xff
)

var debug int

func nodeOf(pk *packet) *qNode {
	return &qNode{packet: pk}
}

func (c *Conn) internalRecvLoop() {
	defer func() {
		// avoid send to closed channel while some replaying
		// data packets were received in shutting down.
		_ = recover()
	}()
	var buf, body []byte
	for {
		select {
		case buf = <-c.evRecv:
			if buf != nil {
				body = buf[_TH_SIZE:]
			} else { // shutdown
				return
			}
		}
		pk := new(packet)
		// keep the original buffer, so we could recycle it in future
		pk.buffer = buf
		unmarshall(pk, body)
		if pk.flag&_F_SACK != 0 {
			c.processSAck(pk)
			continue
		}
		if pk.flag&_F_ACK != 0 {
			c.processAck(pk)
		}
		if pk.flag&_F_DATA != 0 {
			c.insertData(pk)
		} else if pk.flag&_F_FIN != 0 {
			if pk.flag&_F_RESET != 0 {
				go c.forceShutdownWithLock()
			} else {
				go c.closeR(pk)
			}
		}
	}
}

func (c *Conn) internalSendLoop() {
	var timer = time.NewTimer(time.Duration(c.rtt) * time.Millisecond)
	for {
		select {
		case v := <-c.evSWnd:
			switch v {
			case _VRETR_IMMED:
				c.outlock.Lock()
				c.retransmit2()
				c.outlock.Unlock()
			case _VSWND_ACTIVE:
				timer.Reset(time.Duration(c.rtt) * time.Millisecond)
			case _CLOSE:
				return
			}
		case <-timer.C: // timeout yet
			var notifySender bool
			c.outlock.Lock()
			rest, _ := c.retransmit()
			switch rest {
			case _RETR_REST, 0: // nothing to send
				if c.outQ.size() > 0 {
					timer.Reset(time.Duration(c.rtt) * time.Millisecond)
				} else {
					timer.Stop()
					// avoid sender blocking
					notifySender = true
				}
			default: // recent rto point
				timer.Reset(time.Duration(minI64(rest, c.rtt)) * time.Millisecond)
			}
			c.outlock.Unlock()
			if notifySender {
				select {
				case c.evSend <- 1:
				default:
				}
			}
		}
	}
}

func (c *Conn) internalAckLoop() {
	// var ackTimer = time.NewTicker(time.Duration(c.ato))
	var ackTimer = time.NewTimer(time.Duration(c.ato) * time.Millisecond)
	var lastAckState byte
	for {
		var v byte
		select {
		case <-ackTimer.C:
			// may cause sending duplicated ack if ato>rtt
			v = _VACK_QUICK
		case v = <-c.evAck:
			ackTimer.Reset(time.Duration(c.ato) * time.Millisecond)
			state := lastAckState
			lastAckState = v
			if state != v {
				if v == _CLOSE {
					return
				}
				v = _VACK_MUST
			}
		}
		c.inlock.Lock()
		if pkAck := c.makeAck(v); pkAck != nil {
			c.internalWrite(nodeOf(pkAck))
		}
		c.inlock.Unlock()
	}
}

func (c *Conn) retransmit() (rest int64, count int32) {
	var now, rto = Now(), c.rto
	var limit = c.cwnd
	for item := c.outQ.head; item != nil && limit > 0; item = item.next {
		if item.scnt != _SENT_OK { // ACKed has scnt==-1
			diff := now - item.sent
			if diff > rto { // already rto
				c.internalWrite(item)
				count++
			} else {
				// continue search next min rto duration
				if rest > 0 {
					rest = minI64(rest, rto-diff+1)
				} else {
					rest = rto - diff + 1
				}
				limit--
			}
		}
	}
	c.outDupCnt += int(count)
	if count > 0 {
		shrcond := (c.fastRetransmit && count > maxI32(c.cwnd>>5, 4)) || (!c.fastRetransmit && count > c.cwnd>>3)
		if shrcond && now-c.lastShrink > c.rto {
			log.Printf("shrink cwnd from=%d to=%d s/4=%d", c.cwnd, c.cwnd>>1, c.swnd>>2)
			c.lastShrink = now
			// shrink cwnd and ensure cwnd >= swnd/4
			if c.cwnd > c.swnd>>1 {
				c.cwnd >>= 1
			}
		}
	}
	if c.outQ.size() > 0 {
		return
	}
	return _RETR_REST, 0
}

func (c *Conn) retransmit2() (count int32) {
	var limit, now = minI32(c.outPending>>4, 8), Now()
	var fRtt = c.rtt
	if now-c.lastShrink > c.rto {
		fRtt += maxI64(c.rtt>>4, 1)
	} else {
		fRtt += maxI64(c.rtt>>1, 2)
	}
	for item := c.outQ.head; item != nil && count < limit; item = item.next {
		if item.scnt != _SENT_OK { // ACKed has scnt==-1
			if item.miss >= 3 && now-item.sent >= fRtt {
				item.miss = 0
				c.internalWrite(item)
				count++
			}
		}
	}
	c.fRCnt += int(count)
	c.outDupCnt += int(count)
	return
}

func (c *Conn) inputAndSend(pk *packet) error {
	item := &qNode{packet: pk}
	if c.mySeq&3 == 1 {
		c.tSlotT0 = NowNS()
	}
	c.outlock.Lock()
	// inflight packets exceeds cwnd
	// inflight includes: 1, unacked; 2, missed
	for c.outPending >= c.cwnd+c.missed {
		c.outlock.Unlock()
		if c.wtmo > 0 {
			var tmo int64
			tmo, c.wtmo = c.wtmo, 0
			select {
			case v := <-c.evSend:
				if v == _CLOSE {
					return io.EOF
				}
			case <-NewTimerChan(tmo):
				return ErrIOTimeout
			}
		} else {
			if v := <-c.evSend; v == _CLOSE {
				return io.EOF
			}
		}
		c.outlock.Lock()
	}
	c.outPending++
	c.outPkCnt++
	c.mySeq++
	pk.seq = c.mySeq
	c.outQ.appendTail(item)
	c.internalWrite(item)
	c.outlock.Unlock()
	// active resending timer, must blocking
	c.evSWnd <- _VSWND_ACTIVE
	if c.mySeq&3 == 0 && c.flatTraffic {
		// calculate time error bewteen tslot with actual usage.
		// consider last sleep time error
		t1 := NowNS()
		terr := c.tSlot<<2 - c.lastSErr - (t1 - c.tSlotT0)
		// rest terr/2 if current time usage less than tslot of 100us.
		if terr > 1e5 { // 100us
			time.Sleep(time.Duration(terr >> 1))
			c.lastSErr = maxI64(NowNS()-t1-terr, 0)
		} else {
			c.lastSErr >>= 1
		}
	}
	return nil
}

func (c *Conn) internalWrite(item *qNode) {
	if item.scnt >= 20 {
		// no exception of sending fin
		if item.flag&_F_FIN != 0 {
			c.fakeShutdown()
			c.dest = nil
			return
		} else {
			log.Println("Warn: too many retries", item)
			if c.urgent > 0 { // abort
				c.forceShutdown()
				return
			} else { // continue to retry 10
				c.urgent++
				item.scnt = 10
			}
		}
	}
	// update current sent time and prev sent time
	item.sent, item.sent_1 = Now(), item.sent
	item.scnt++
	buf := item.marshall(c.connID)
	if debug >= 3 {
		var pkType = packetTypeNames[item.flag]
		if item.flag&_F_SACK != 0 {
			log.Printf("send %s trp=%d on=%d %x", pkType, item.seq, item.ack, buf[_AH_SIZE+4:])
		} else {
			log.Printf("send %s seq=%d ack=%d scnt=%d len=%d", pkType, item.seq, item.ack, item.scnt, len(buf)-_TH_SIZE)
		}
	}
	c.sock.WriteToUDP(buf, c.dest)
}

func (c *Conn) logAck(ack uint32) {
	c.lastAck = ack
	c.lastAckTime = Now()
}

func (c *Conn) makeLastAck() (pk *packet) {
	c.inlock.Lock()
	defer c.inlock.Unlock()
	if Now()-c.lastAckTime < c.rtt {
		return nil
	}
	pk = &packet{
		ack:  maxU32(c.lastAck, c.inQ.maxCtnSeq),
		flag: _F_ACK,
	}
	c.logAck(pk.ack)
	return
}

func (c *Conn) makeAck(level byte) (pk *packet) {
	now := Now()
	if level < _VACK_MUST && now-c.lastAckTime < c.ato {
		if level < _VACK_QUICK || now-c.lastAckTime < minI64(c.ato>>2, 1) {
			return
		}
	}
	//	    ready Q <-|
	//	              |-> outQ start (or more right)
	//	              |-> bitmap start
	//	[predecessor]  [predecessor+1]  [predecessor+2] .....
	var fakeSAck bool
	var predecessor = c.inQ.maxCtnSeq
	bmap, tbl := c.inQ.makeHolesBitmap(predecessor)
	if len(bmap) <= 0 { // fake sack
		bmap = make([]uint64, 1)
		bmap[0], tbl = 1, 1
		fakeSAck = true
	}
	// head 4-byte: TBL:1 | SCNT:1 | DELAY:2
	buf := make([]byte, len(bmap)*8+4)
	pk = &packet{
		ack:     predecessor + 1,
		flag:    _F_SACK,
		payload: buf,
	}
	if fakeSAck {
		pk.ack--
	}
	buf[0] = byte(tbl)
	// mark delayed time according to the time reference point
	if trp := c.inQ.lastIns; trp != nil {
		delayed := now - trp.sent
		if delayed < c.rtt {
			pk.seq = trp.seq
			pk.flag |= _F_TIME
			buf[1] = trp.scnt
			if delayed <= 0 {
				delayed = 1
			}
			binary.BigEndian.PutUint16(buf[2:], uint16(delayed))
		}
	}
	buf1 := buf[4:]
	for i, b := range bmap {
		binary.BigEndian.PutUint64(buf1[i*8:], b)
	}
	c.logAck(predecessor)
	return
}

func unmarshallSAck(data []byte) (bmap []uint64, tbl uint32, delayed uint16, scnt uint8) {
	if len(data) > 0 {
		bmap = make([]uint64, len(data)>>3)
	} else {
		return
	}
	tbl = uint32(data[0])
	scnt = data[1]
	delayed = binary.BigEndian.Uint16(data[2:])
	data = data[4:]
	for i := 0; i < len(bmap); i++ {
		bmap[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return
}

func calSwnd(bandwidth, rtt int64) int32 {
	w := int32(bandwidth * rtt / (8000 * _MSS))
	if w <= _MAX_SWND {
		if w >= _MIN_SWND {
			return w
		} else {
			return _MIN_SWND
		}
	} else {
		return _MAX_SWND
	}
}

func (c *Conn) measure(seq uint32, delayed int64, scnt uint8) {
	target := c.outQ.get(seq)
	if target != nil {
		var lastSent int64
		switch target.scnt - scnt {
		case 0:
			// not sent again since this ack was sent out
			lastSent = target.sent
		case 1:
			// sent again once since this ack was sent out
			// then use prev sent time
			lastSent = target.sent_1
		default:
			// can't measure here because the packet was sent too many times
			return
		}
		// real-time rtt
		rtt := Now() - lastSent - delayed
		// reject these abnormal measures:
		// 1. rtt too small -> rtt/8
		// 2. backlogging too long
		if rtt < maxI64(c.rtt>>3, 1) || delayed > c.rtt>>1 {
			return
		}
		// srtt: update 1/8
		err := rtt - (c.srtt >> 3)
		c.srtt += err
		c.rtt = c.srtt >> 3
		if c.rtt < _MIN_RTT {
			c.rtt = _MIN_RTT
		}
		// s-swnd: update 1/4
		swnd := c.swnd<<3 - c.swnd + calSwnd(c.bandwidth, c.rtt)
		c.swnd = swnd >> 3
		c.tSlot = c.rtt * 1e6 / int64(c.swnd)
		c.ato = c.rtt >> 4
		if c.ato < _MIN_ATO {
			c.ato = _MIN_ATO
		} else if c.ato > _MAX_ATO {
			c.ato = _MAX_ATO
		}
		if err < 0 {
			err = -err
			err -= c.mdev >> 2
			if err > 0 {
				err >>= 3
			}
		} else {
			err -= c.mdev >> 2
		}
		// mdev: update 1/4
		c.mdev += err
		rto := c.rtt + maxI64(c.rtt<<1, c.mdev)
		if rto >= c.rto {
			c.rto = rto
		} else {
			c.rto = (c.rto + rto) >> 1
		}
		if c.rto < _MIN_RTO {
			c.rto = _MIN_RTO
		}
		if debug >= 1 {
			log.Printf("--- rtt=%d srtt=%d rto=%d swnd=%d", c.rtt, c.srtt, c.rto, c.swnd)
		}
	}
}

func (c *Conn) processSAck(pk *packet) {
	c.outlock.Lock()
	bmap, tbl, delayed, scnt := unmarshallSAck(pk.payload)
	if bmap == nil { // bad packet
		c.outlock.Unlock()
		return
	}
	if pk.flag&_F_TIME != 0 {
		c.measure(pk.seq, int64(delayed), scnt)
	}
	deleted, missed, continuous := c.outQ.deleteByBitmap(bmap, pk.ack, tbl)
	if deleted > 0 {
		c.ackHit(deleted, missed)
		// lock is released
	} else {
		c.outlock.Unlock()
	}
	if c.fastRetransmit && !continuous {
		// peer Q is uncontinuous, then trigger FR
		if deleted == 0 {
			c.evSWnd <- _VRETR_IMMED
		} else {
			select {
			case c.evSWnd <- _VRETR_IMMED:
			default:
			}
		}
	}
	if debug >= 2 {
		log.Printf("SACK qhead=%d deleted=%d outPending=%d on=%d %016x",
			c.outQ.distanceOfHead(0), deleted, c.outPending, pk.ack, bmap)
	}
}

func (c *Conn) processAck(pk *packet) {
	c.outlock.Lock()
	if end := c.outQ.get(pk.ack); end != nil { // ack hit
		_, deleted := c.outQ.deleteBefore(end)
		c.ackHit(deleted, 0) // lock is released
		if debug >= 2 {
			log.Printf("ACK hit on=%d", pk.ack)
		}
		// special case: ack the FIN
		if pk.seq == _FIN_ACK_SEQ {
			select {
			case c.evClose <- _S_FIN0:
			default:
			}
		}
	} else { // duplicated ack
		if debug >= 2 {
			log.Printf("ACK miss on=%d", pk.ack)
		}
		if pk.flag&_F_SYN != 0 { // No.3 Ack lost
			if pkAck := c.makeLastAck(); pkAck != nil {
				c.internalWrite(nodeOf(pkAck))
			}
		}
		c.outlock.Unlock()
	}
}

func (c *Conn) ackHit(deleted, missed int32) {
	// must in outlock
	c.outPending -= deleted
	now := Now()
	if c.cwnd < c.swnd && now-c.lastShrink > c.rto {
		if c.cwnd < c.swnd>>1 {
			c.cwnd <<= 1
		} else {
			c.cwnd += deleted << 1
		}
	}
	if c.cwnd > c.swnd {
		c.cwnd = c.swnd
	}
	if now-c.lastRstMis > c.ato {
		c.lastRstMis = now
		c.missed = missed
	} else {
		c.missed = c.missed>>1 + missed
	}
	if qswnd := c.swnd >> 4; c.missed > qswnd {
		c.missed = qswnd
	}
	c.outlock.Unlock()
	select {
	case c.evSend <- 1:
	default:
	}
}

func (c *Conn) insertData(pk *packet) {
	c.inlock.Lock()
	defer c.inlock.Unlock()
	exists := c.inQ.contains(pk.seq)
	// duplicated with already queued or history
	// means: last ACK were lost
	if exists || pk.seq <= c.inQ.maxCtnSeq {
		// then send ACK for dups
		select {
		case c.evAck <- _VACK_MUST:
		default:
		}
		if debug >= 2 {
			dumpQ(fmt.Sprint("duplicated ", pk.seq), c.inQ)
		}
		c.inDupCnt++
		return
	}
	// record current time in sent and regard as received time
	item := &qNode{packet: pk, sent: Now()}
	dis := c.inQ.searchInsert(item, c.lastReadSeq)
	if debug >= 3 {
		log.Printf("\t\t\trecv DATA seq=%d dis=%d maxCtn=%d lastReadSeq=%d", item.seq, dis, c.inQ.maxCtnSeq, c.lastReadSeq)
	}

	var ackState byte = _VACK_MUST
	var available bool
	switch dis {
	case 0: // impossible
		return
	case 1:
		if c.inQDirty {
			available = c.inQ.updateContinuous(item)
			if c.inQ.isWholeContinuous() { // whole Q is ordered
				c.inQDirty = false
			} else { //those holes still exists.
				ackState = _VACK_QUICK
			}
		} else {
			// here is an ideal situation
			c.inQ.maxCtnSeq = pk.seq
			available = true
			ackState = _VACK_SCHED
		}

	default: // there is an unordered packet, hole occurred here.
		if !c.inQDirty {
			c.inQDirty = true
		}
	}

	// write valid received count
	c.inPkCnt++
	c.inQ.lastIns = item
	// try notify ack
	select {
	case c.evAck <- ackState:
	default:
	}
	if available { // try notify reader
		select {
		case c.evRead <- 1:
		default:
		}
	}
}

func (c *Conn) readInQ() bool {
	c.inlock.Lock()
	defer c.inlock.Unlock()
	// read already <-|-> expected Q
	//  [lastReadSeq] | [lastReadSeq+1] [lastReadSeq+2] ......
	if c.inQ.isEqualsHead(c.lastReadSeq+1) && c.lastReadSeq < c.inQ.maxCtnSeq {
		c.lastReadSeq = c.inQ.maxCtnSeq
		availabled := c.inQ.get(c.inQ.maxCtnSeq)
		availabled, _ = c.inQ.deleteBefore(availabled)
		for i := availabled; i != nil; i = i.next {
			c.inQReady = append(c.inQReady, i.payload...)
			// data was copied, then could recycle memory
			bpool.Put(i.buffer)
			i.payload = nil
			i.buffer = nil
		}
		return true
	}
	return false
}

// should not call this function concurrently.
func (c *Conn) Read(buf []byte) (nr int, err error) {
	for {
		if len(c.inQReady) > 0 {
			n := copy(buf, c.inQReady)
			c.inQReady = c.inQReady[n:]
			return n, nil
		}
		if !c.readInQ() {
			if c.rtmo > 0 {
				var tmo int64
				tmo, c.rtmo = c.rtmo, 0
				select {
				case _, y := <-c.evRead:
					if !y && len(c.inQReady) == 0 {
						return 0, io.EOF
					}
				case <-NewTimerChan(tmo):
					return 0, ErrIOTimeout
				}
			} else {
				// only when evRead is closed and inQReady is empty
				// then could reply eof
				if _, y := <-c.evRead; !y && len(c.inQReady) == 0 {
					return 0, io.EOF
				}
			}
		}
	}
}

// should not call this function concurrently.
func (c *Conn) Write(data []byte) (nr int, err error) {
	for len(data) > 0 && err == nil {
		//buf := make([]byte, _MSS+_AH_SIZE)
		buf := bpool.Get(c.mss + _AH_SIZE)
		body := buf[_TH_SIZE+_CH_SIZE:]
		n := copy(body, data)
		nr += n
		data = data[n:]
		pk := &packet{flag: _F_DATA, payload: body[:n], buffer: buf[:_AH_SIZE+n]}
		err = c.inputAndSend(pk)
	}
	return
}

func (c *Conn) LocalAddr() net.Addr {
	return c.sock.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.dest
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	c.SetWriteDeadline(t)
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	if d := t.UnixNano()/Millisecond - Now(); d > 0 {
		c.rtmo = d
	}
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	if d := t.UnixNano()/Millisecond - Now(); d > 0 {
		c.wtmo = d
	}
	return nil
}
