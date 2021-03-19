package penet

import (
	"container/list"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"github.com/chrislusf/seaweedfs/weed/glog"
	mrand "math/rand"
	"net"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

type DataSend struct {
	Seq    uint32
	Acked  bool
	Resend byte
	Fast   bool
	Code   byte
	Time   uint32
	Data   []byte
}

type DataRecv struct {
	Seq    uint32
	AckCnt byte
	Code   byte
	Data   []byte
}

type UdpSend struct {
	Id           uint64
	sock         *net.UDPConn
	remote       *net.UDPAddr
	seq          uint32
	rtt          float64
	rttMax       float64
	rttMin       float64
	rate         uint32
	mss          uint32
	interval     uint32
	data         []DataSend
	dataBegin    int
	dataLen      int
	sendRnd      int
	sendList     *list.List
	sendListLock sync.Mutex
	writable     chan bool
	writeMax     int
	isClose      bool
	closing      bool
	opening      byte
	conn         *Conn
	resendCnt    int
	name         string
	acked        uint32
	recvWnd      uint32
}

const (
	TypeData  uint8 = 1
	TypeAck   uint8 = 2
	TypeClose uint8 = 3
	TypeSYN   uint8 = 8
)

var (
	ErrClose   = errors.New("conn close")
	ErrTimeout = errors.New("conn timeout")

	mss         uint32 = 1200
	defaultRate        = mss * 3000
	dropRate           = 0.0
	writeMaxSep        = 5
	resendLimit        = true
)

func NewUdpSend(conn *Conn, id uint64, sock *net.UDPConn, remote *net.UDPAddr, name string) *UdpSend {
	u := &UdpSend{
		conn:     conn,
		Id:       id,
		sock:     sock,
		remote:   remote,
		mss:      mss,
		interval: 20,
		data:     make([]DataSend, 4),
		seq:      1,
		rate:     defaultRate, // 修复bug: 太高导致压测的时候内存爆炸，速度慢
		rtt:      200,
		rttMax:   200,
		rttMin:   200,
		sendList: list.New(),
		writable: make(chan bool, 1),
		name:     name,
	}
	u.writeMax = int(u.rate/u.mss) / writeMaxSep // fixed bug: 初始化  修复bug: 写入太多，应该一点点写
	u.recvWnd = uint32(u.writeMax)
	return u
}

func (u *UdpSend) send(nowTime time.Time, buf []byte) {
	var sendMax = int(u.rate / u.mss)
	u.writeMax = sendMax / writeMaxSep
	// glog.V(4).Info("send", u.dataLen, u.name, sendMax, uint32(u.rtt))
	var sendCount = uint32(u.rate / u.mss / (1000 / u.interval))
	var sendTotal = sendCount
	if sendCount > u.recvWnd/5 {
		sendCount = u.recvWnd / 5
	}
	if sendCount <= 0 {
		sendCount = 1
	}

	now := uint32(nowTime.UnixNano() / int64(time.Millisecond))
	resendCnt := 0
	for i := 0; i < u.dataLen; i++ {
		if sendCount <= 0 {
			break
		}
		index := (i + u.dataBegin) % len(u.data)
		d := &u.data[index]
		if d.Acked == false && now-d.Time >= uint32(u.rtt*2.0) {
			glog.V(4).Infof("resend, seq:%v id:%v rtt:%v name:%v", d.Seq, u.Id, uint32(u.rtt), u.name)
			// data包:  type0 flag1 id2 len3 seq4 tm5 sndwnd6
			// 发送窗口就是配置值，其实发送窗口不需要发送出去
			// 接收端能发送了，要告诉对面开始发送。这个逻辑可以通过接受端的发送通道来发送。
			headLen := structPack(buf, "BBQHIII", uint8(TypeData),
				d.Code, uint64(u.Id), uint16(len(d.Data)+4+4+4), d.Seq, uint32(now), uint32(sendMax))
			copy(buf[headLen:], d.Data)
			u.sock.WriteToUDP(buf[:headLen+len(d.Data)], u.remote)
			d.Time = now
			d.Resend++
			d.Fast = false
			sendCount--
			resendCnt++
			u.resendCnt++
		}

	}

	u.sendRnd++
	if u.sendRnd > 100 && u.dataLen > 0 {
		u.sendRnd = 0

		var resendMax = uint32(float64(u.dataLen) * 0.6)
		if u.resendCnt > 2 {
			glog.V(0).Infof("resendCnt:%v resendMax:%v remain:%v rtt:%v id:%v", u.resendCnt,
				resendMax, u.dataLen, uint32(u.rtt), u.Id)
			u.resendCnt = 0
		}
		// 修复bug: 之前使用lastrecvtime，存在新增发送数据数据那瞬间，出现超时情况
		if u.data[u.dataBegin].Resend > 10 {
			glog.V(0).Infof("resend too much, close, id:%v name:%v drop seq:%v remain:%v",
				u.Id, u.name, u.data[u.dataBegin].Seq, u.dataLen)
			u.conn.close(true, true)
		}
	}

	var resendToomuch bool
	if uint32(resendCnt) > sendTotal/3 && resendLimit { // 修复bug: 限速
		resendToomuch = true
		glog.V(4).Infof("resend too much, slow, %v %v", resendCnt, sendTotal)
	}

	u.sendListLock.Lock()
	for ; sendCount > 0; sendCount-- {
		sendData := u.sendList.Front()
		if sendData == nil {
			break
		}

		if sendMax-u.dataLen <= 0 || resendToomuch {
			break
		}
		if u.dataLen >= len(u.data) {
			newData := make([]DataSend, 2*len(u.data))
			copy(newData, u.data[u.dataBegin:])
			copy(newData[len(u.data)-u.dataBegin:], u.data[:u.dataBegin])
			u.dataBegin = 0
			u.data = newData
		}

		u.sendList.Remove(sendData)
		hType := uint8(0)
		sdata, ok := sendData.Value.([]byte)
		if !ok {
			// 发送控制消息
			mesCode, _ := sendData.Value.(byte)
			hType = mesCode
		}
		headLen := structPack(buf, "BBQHIII", uint8(TypeData), hType,
			uint64(u.Id), uint16(len(sdata)+4+4+4), u.seq, uint32(now), uint32(sendMax))
		copy(buf[headLen:], sdata)
		u.sock.WriteToUDP(buf[:headLen+len(sdata)], u.remote)
		glog.V(4).Infof("send, seq:%v id:%v %v", u.seq, u.Id, u.name)

		index := (u.dataBegin + u.dataLen) % len(u.data)
		u.data[index] = DataSend{
			Seq:  u.seq,
			Time: now,
			Code: hType,
			Data: sdata,
		}
		u.seq++
		u.dataLen++
	}
	listRemain := u.writeMax - u.sendList.Len()
	u.sendListLock.Unlock()

	if listRemain > 0 {
		select {
		case u.writable <- !u.isClose:
		default:
		}
	}

	if u.isClose && u.dataLen == 0 && u.closing == false { // 修复bug: 之前5秒删除，实际发送没完成
		// 修复bug: 接受端write端close为true, 导致5秒后呗删除，来不及接受数据。
		glog.V(1).Infof("close and emtpy, rm conn, id:%v name:%v seq:%v listlen:%v",
			u.Id, u.name, u.seq, u.sendList.Len())
		u.closing = true
		u.conn.close(false, true)
	}
}

func testDrop() bool {
	if dropRate < 0.01 {
		return false
	}
	var v uint32
	var b [4]byte
	if _, err := crand.Read(b[:]); err != nil {
		v = mrand.Uint32()
	} else {
		v = binary.BigEndian.Uint32(b[:])
	}
	if v%1000 < uint32(dropRate*1000) {
		return true
	}
	return false
}

func (u *UdpSend) recv(buf []byte) {

	if testDrop() {
		return
	}

	now := uint32(time.Now().UnixNano() / int64(time.Millisecond))

	// ack包:  type0 flag1 id2 len3 tm4 rcvwnd5 acked6
	head, headLen := structUnPack(buf, "BBQHIII")
	if head[0] == uint64(TypeAck) && u.dataLen > 0 {

		firstSeq := u.data[u.dataBegin].Seq
		acked := uint32(head[6])
		offset := int(int32(acked - firstSeq))
		glog.V(4).Infof("id:%v recv ack: %v offset:%v databegin:%v dataLen:%v firstSeq:%v name:%v",
			u.Id, acked, offset, u.dataBegin, u.dataLen, firstSeq, u.name)
		if offset >= 0 && offset < u.dataLen {
			offset++
			for i := 0; i < offset; i++ { // 修复bug: 清除引用等
				index := (u.dataBegin + i) % len(u.data) // 修复bug，i写成offset
				d := &u.data[index]
				d.Data = nil
				d.Acked = true
			}
			u.acked += uint32(offset)
			u.dataBegin += offset
			u.dataBegin = u.dataBegin % len(u.data)
			u.dataLen -= offset
			glog.V(4).Infof("2 acked ok:%v, id:%v %v", u.acked, u.Id, u.name)
		}
		if u.dataLen > 0 {
			curSeq := u.data[u.dataBegin].Seq
			// var ackedSeq = []uint32{}
			for i := headLen; i < len(buf); i += 4 {
				seq := binary.BigEndian.Uint32(buf[i:])
				offset := int(int32(seq - curSeq))
				if offset < 0 || offset >= u.dataLen {
					continue
				}
				index := (u.dataBegin + offset) % len(u.data)
				d := &u.data[index]
				if d.Seq == seq {
					d.Acked = true
					d.Data = nil // 修复bug: memory leak
					// ackedSeq = append(ackedSeq, seq)
				} else {
					panic("index not correct")
				}
			}
			// if len(ackedSeq) > 0 {
			// 	glog.V(0).Info("seq:", ackedSeq)
			// }
		}

		var i = 0
		for ; i < u.dataLen; i++ {
			index := (i + u.dataBegin) % len(u.data)
			d := &u.data[index]
			if d.Acked == false {
				break
			}
			// fmt.Println("acked->", d.Seq)
			// if d.Seq == 7 {
			// 	fmt.Println("data:", u.data[u.dataBegin:u.dataBegin+5])
			// }
		}
		if i > 0 {
			u.acked += uint32(i)
			u.dataBegin += i
			u.dataBegin = u.dataBegin % len(u.data)
			u.dataLen -= i
			glog.V(4).Infof("3 acked ok:%v, id:%v %v %v", u.acked, u.Id, u.dataBegin, u.name)
		}

		sendTime := uint32(head[4])
		rtt := now - sendTime
		if rtt > 0 {
			if firstSeq < 3 {
				u.rtt = float64(rtt) // 初始值
			} else {
				u.rtt = u.rtt*0.8 + float64(rtt)*0.2
			}
			if u.rtt < 50.0 {
				u.rtt = 50
			}
			// glog.V(4).Infof("rtt:%v u.rtt:%v id:%v", rtt, u.rtt, u.Id)
		}

		u.recvWnd = uint32(head[5])
	}

}

func structPack(b []byte, format string, param ...interface{}) int {
	j := 0
	for i, s := range format {
		switch s {
		case 'I':
			p, _ := param[i].(uint32)
			binary.BigEndian.PutUint32(b[j:], p)
			j += 4
		case 'B':
			p, _ := param[i].(uint8)
			b[j] = p
			j++
		case 'H':
			p, _ := param[i].(uint16)
			binary.BigEndian.PutUint16(b[j:], p)
			j += 2
		case 'Q':
			p, _ := param[i].(uint64)
			binary.BigEndian.PutUint64(b[j:], p)
			j += 8
		default:
			panic("structPack not found")
		}
	}
	return j
}

func structUnPack(b []byte, format string) ([]uint64, int) {
	var re = make([]uint64, 0, len(format))
	defer func() {
		if err := recover(); err != nil {
			// log.Error(err)
			re[0] = 0
		}
	}()
	j := 0
	for _, s := range format {
		switch s {
		case 'I':
			re = append(re, uint64(binary.BigEndian.Uint32(b[j:])))
			j += 4
		case 'B':
			re = append(re, uint64(b[j]))
			j++
		case 'H':
			re = append(re, uint64(binary.BigEndian.Uint16(b[j:])))
			j += 2
		case 'Q':
			re = append(re, uint64(binary.BigEndian.Uint64(b[j:])))
			j += 8
		default:
			panic("structUnPack not found")
		}
	}
	return re, j
}

type UdpRecv struct {
	conn         *Conn
	Id           uint64
	sock         *net.UDPConn
	remote       *net.UDPAddr
	acked        uint32
	lastTm       uint32
	sndWnd       uint32
	recvCnt      uint32
	isClose      bool
	isNew        byte
	isRecved     bool
	recvList     *list.List
	recvListLock sync.Mutex
	readable     chan byte
	readDeadline *time.Time
	seqData      map[uint32]*DataRecv
	dataList     *list.List
	name         string
}

func NewUdpRecv(conn *Conn, id uint64, sock *net.UDPConn, remote *net.UDPAddr, name string) *UdpRecv {
	return &UdpRecv{
		Id:       id,
		conn:     conn,
		sock:     sock,
		remote:   remote,
		acked:    0,
		recvList: list.New(),
		dataList: list.New(),
		seqData:  make(map[uint32]*DataRecv),
		readable: make(chan byte, 1),
		isNew:    50,
		name:     name,
		sndWnd:   1000,
	}
}

func (u *UdpRecv) SetReadDeadline(t time.Time) {
	u.readDeadline = &t
}

func (u *UdpRecv) sendAck(nowTime time.Time, buf []byte) {

	u.recvListLock.Lock()
	for {
		if d, ok := u.seqData[u.acked+1]; ok {
			u.acked++
			if d.Code == TypeClose {
				if u.isClose == false {
					glog.V(1).Info("recv close:", u.Id)
					u.conn.close(false, true)
					u.isClose = true
				}
			} else {
				u.recvList.PushBack(d.Data)
			}
			d.Data = nil
			delete(u.seqData, u.acked)
		} else {
			break
		}
	}
	recvListLen := u.recvList.Len()
	u.recvListLock.Unlock()

	// glog.V(4).Info("acked:", u.acked, u.Id, recvListLen, u.name)
	if recvListLen > 0 { // 修复bug，用标志可能会有读不到的数据  修复bug： 去掉 && u.isClose == false，导致读取延迟
		select {
		case u.readable <- 1:
		default:
		}
	} else if u.isClose == true { // 修复bug：快速close
		select {
		case u.readable <- 0:
		default:
		}
	}
	if u.readDeadline != nil && !u.readDeadline.IsZero() {
		if u.readDeadline.Before(nowTime) { // 修复bug after
			select {
			case u.readable <- 2:
				glog.V(0).Info("read dealline: ", u.Id)
			default:
			}
		}
	}

	var b = buf[:mss]
	buf = buf[mss:]
	var n = 0
	for i := u.dataList.Front(); i != nil; {
		d := i.Value.(*DataRecv)

		next := i.Next()
		if d.AckCnt > 6 || before(d.Seq, u.acked+1) { // 发几次够了，不反复发
			// d.Removed = true
			u.dataList.Remove(i) // 修复bug，删除逻辑不对，需要保存next
			// delete(u.seqData, d.Seq) // 修复bug: 对面已经确认，这里删了，没有重发，也没有了数据。已经收到的数据并且发了ack的数据，不要删了！
			i = next
			continue
		}
		i = next

		if d.AckCnt%3 == 0 {
			binary.BigEndian.PutUint32(b[n:], d.Seq)
			n += 4
			if n >= len(b) {
				wnd := int(u.sndWnd) - recvListLen - len(u.seqData)
				if wnd < 0 {
					wnd = 0
				}
				headLen := structPack(buf, "BBQHIII", uint8(TypeAck), uint8(0), uint64(u.Id), uint16(4+4+4+n),
					u.lastTm, uint32(wnd), u.acked)
				copy(buf[headLen:], b[:n])
				u.sock.WriteToUDP(buf[:headLen+n], u.remote)
				glog.V(4).Infof("id:%v send ack n:%v", u.Id, n)
				n = 0              // 修复bug，没有置零
				u.isRecved = false // 修复bug: 有时候发多一条数据
			}
		}
		d.AckCnt++
	}
	if n > 0 || u.isRecved { // 修复bug: 一直发数据  修复bug: 有时候发多一条数据
		wnd := int(u.sndWnd) - recvListLen - len(u.seqData)
		if wnd < 0 {
			wnd = 0
		}
		headLen := structPack(buf, "BBQHIII", uint8(TypeAck), uint8(0), uint64(u.Id), uint16(4+4+4+n),
			u.lastTm, uint32(wnd), u.acked)
		copy(buf[headLen:], b[:n])
		u.sock.WriteToUDP(buf[:headLen+n], u.remote)
		glog.V(4).Infof("id:%v send ack n:%v datalen:%v", u.Id, n, u.dataList.Len())
	}
	u.isRecved = false

	// 修复bug: 如果自己只是发数据，那么自己的acked通道会没用到。所以要判断自己是否在发送数据。
	if u.isNew > 0 { // 完成的优化：超时不确认第一包，就删除链接。防止旧链接不断发包。
		u.isNew--
		if u.isNew == 0 && u.acked == 0 {
			glog.V(0).Infof("not recv first packet!!! close, id:%v name:%v", u.Id, u.name)
			u.conn.Close()
		}
		if u.acked >= 1 || u.conn.s.seq > 1 {
			u.isNew = 0
		}
	}

}

// before seq1比seq2小
func before(seq1, seq2 uint32) bool {
	return (int32)(seq1-seq2) < 0
}

func after(seq1, seq2 uint32) bool {
	return (int32)(seq2-seq1) < 0
}

func (u *UdpRecv) recv(buf []byte) {

	if testDrop() {
		return
	}

	u.isRecved = true
	u.recvCnt++
	// data包:  type0 flag1 id2 len3 seq4 tm5 sndwnd6
	head, headLen := structUnPack(buf, "BBQHIII")
	if head[0] == uint64(TypeData) {

		seq := uint32(head[4])
		u.lastTm = uint32(head[5])
		u.sndWnd = uint32(head[6])

		glog.V(4).Info(u.Id, " recv seq: ", seq, " len: ", u.dataList.Len())

		if before(seq, u.acked+1) { // 修复bug: 收到before的数据，seqData不会回收。
			// glog.V(0).Info("seq before u.acked:", seq, u.acked, u.Id)
			return
		}

		// 修复bug: 修复没重发问题。之前由于一直会重发，这个逻辑有意义。现在只重发几次。
		// if d, ok := u.seqData[seq]; ok {
		// 	d.AckCnt = 0
		// 	if d.Removed == false { // 修复bug：可能没ack导致一直重发，要直到acked全部覆盖。
		// 		return
		// 	}
		// }

		// glog.V(4).Info(u.Id, " recv 2 seq: ", seq, " len: ", u.dataList.Len())

		d := &DataRecv{
			Seq:  seq,
			Data: buf[headLen:],
			Code: byte(head[1]),
		}
		u.dataList.PushBack(d)
		u.seqData[seq] = d

	}

}

func SetRate(rate uint32) {
	defaultRate = rate
}

func SetDropRate(rate float64) {
	if rate > 0.001 {
		dropRate = rate
	}
}

type Conn struct {
	Id          uint64
	s           *UdpSend
	r           *UdpRecv
	responsed   chan bool
	isClose     bool
	isSendClose bool
	isRmConn    bool
	conns       *Conns
}

func NewConn(conns *Conns, Id uint64, localConn *net.UDPConn, remote *net.UDPAddr, name string) *Conn {
	conn := &Conn{
		Id:        Id,
		conns:     conns,
		responsed: make(chan bool, 1),
	}
	conn.s = NewUdpSend(conn, conn.Id, localConn, remote, name)
	conn.r = NewUdpRecv(conn, conn.Id, localConn, remote, name)
	return conn
}

func (c *Conn) Write(bb []byte) (n int, err error) {

	if c.isClose {
		return 0, ErrClose
	}

	// 1.对方告诉你满了，是要叫你不要发数据了，而不是还发数据。
	// 2.没有窗口就没法快速地确定对面有没有满，如果你要等到自己的的buffer满，那么可能会比较慢感知到
	// 固定buffer + 停止通知
	b := make([]byte, len(bb)) // 修复bug1：没有拷贝
	copy(b, bb)
	for {
		c.s.sendListLock.Lock()
		remain := c.s.writeMax - c.s.sendList.Len()
		for {
			if len(b) <= 0 {
				break
			}
			if remain <= 0 {
				break
			}
			remain--
			var sendLen = int(mss)
			if len(b) < sendLen {
				// 实际发送值
				sendLen = len(b)
			}
			n += sendLen
			c.s.sendList.PushBack(b[:sendLen])
			// fmt.Println("write:", b[:10])
			b = b[sendLen:]
		}
		c.s.sendListLock.Unlock()
		if len(b) <= 0 {
			break
		}
		// glog.V(0).Info("wait write: ", c.Id)
		w := <-c.s.writable
		if w == false {
			return n, ErrClose
		}
	}

	return n, nil
}

func (c *Conn) Read(b []byte) (n int, err error) {
	for {
		c.r.recvListLock.Lock()
		for {
			f := c.r.recvList.Front()
			if f != nil {
				data := f.Value.([]byte)
				copy(b[n:], data)
				maxCap := len(b[n:])
				if maxCap < len(data) {
					// b已满
					f.Value = data[maxCap:]
					n += maxCap
					break
				} else {
					// b未满
					c.r.recvList.Remove(f)
					n += len(data)
				}
			} else {
				// 读完数据了
				break
			}
		}
		c.r.recvListLock.Unlock()
		if n <= 0 {
			// glog.V(4).Info("wait read", c.Id)
			// wait for chan
			r := <-c.r.readable
			if r == 0 { // close之后总是返回初始值
				c.r.recvListLock.Lock()
				rlen := c.r.recvList.Len()
				c.r.recvListLock.Unlock()
				if rlen <= 0 { // 修复bug: 等到read完所有数据才让read返回错误
					return n, ErrClose
				}
			}
			if r == 2 {
				return n, ErrTimeout
			}
		} else {
			break
		}
	}

	return
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conns.sock.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conns.sock.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.r.SetReadDeadline(t)
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.r.SetReadDeadline(t)
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c *Conn) close(sendClose, rmConn bool) {
	if c.isClose == false {
		c.isClose = true
		c.r.isClose = true
	}
	if sendClose && c.isSendClose == false { // 修复bug:
		c.isSendClose = true
		c.s.sendListLock.Lock()
		c.s.sendList.PushBack(byte(TypeClose))
		c.s.sendListLock.Unlock()
	}
	if rmConn && c.isRmConn == false {
		c.isRmConn = true
		time.AfterFunc(time.Second*5, func() {
			select {
			case c.conns.input <- Input{
				typ:   ActRmConn,
				param: c,
			}:
			default:
			}
		})
	}
}

func (c *Conn) Close() error {
	if c.isClose == false {
		c.isClose = true
		c.s.isClose = true
		// bug: 接收不能主动关闭
		c.isSendClose = true
		c.s.sendListLock.Lock()
		c.s.sendList.PushBack(byte(TypeClose))
		c.s.sendListLock.Unlock()
		// bug: close之后，5秒数据可能无法完成发送
	}
	return nil
}

type Conns struct {
	conns    map[uint64]*Conn
	sock     *net.UDPConn
	accept   chan *Conn
	isClose  bool
	isDial   bool
	timerRnd uint32
	input    chan Input
}

func NewConns() *Conns {
	return &Conns{
		conns:  make(map[uint64]*Conn),
		accept: make(chan *Conn, 256),
		input:  make(chan Input, 2048),
	}
}

func Listen(network, address string) (net.Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	listener := NewConns()
	listener.sock = conn
	go listener.loop()
	return listener, nil
}

var dialConns *Conns
var dialConnsLock sync.Mutex

func Dial(network, address string) (net.Conn, error) {
	return DialTimeout(network, address, time.Second*3)
}

func DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return nil, err
	}
	id := binary.LittleEndian.Uint64(b[:])

	glog.V(0).Info("dial new 3:", id)

	dialConnsLock.Lock()
	if dialConns == nil {
		dialConns = NewConns()
	}
	if dialConns.sock == nil {
		s, err := net.ListenUDP("udp", &net.UDPAddr{})
		if err != nil {
			dialConnsLock.Unlock()
			return nil, err
		}
		dialConns.sock = s
		dialConns.isDial = true
		go dialConns.loop()
	}
	dialConnsLock.Unlock()

	conn := NewConn(dialConns, id, dialConns.sock, addr, "reqer")
	dialConns.input <- Input{
		typ:   ActAddConn,
		param: conn,
	}

	// conn.s.sendListLock.Lock()
	// conn.s.sendList.PushBack(byte(TypeSYN))
	// conn.s.sendListLock.Unlock()

	return conn, nil
}

func (c *Conns) Accept() (net.Conn, error) {
	for {
		if c.isClose {
			return nil, errors.New("listener close")
		}
		conn := <-c.accept
		if conn == nil {
			return nil, errors.New("listener close")
		}
		return conn, nil
	}
}

func (c *Conns) Close() error {
	c.isClose = true
	c.sock.Close()
	c.input <- Input{
		typ: ActEnd,
	}
	return nil
}

func (c *Conns) Addr() net.Addr {
	return c.sock.LocalAddr()
}

const (
	ActData    = 1
	ActTimer   = 2
	ActAddConn = 3
	ActRmConn  = 4
	ActEnd     = 5
)

type Input struct {
	typ   uint8
	data  []byte
	param interface{}
}

func (c *Conns) loop() {
	// 这里输入时间和数据
	// 只起一个timer，给所有conn发
	// 之前用setreaddeadline这个不太好，容易出现长时间没超时

	runtime.LockOSThread()

	glog.V(0).Info("loop: ", c.isDial)

	go func() {
		var buf = make([]byte, 2048)
		for {
			n, remote, err := c.sock.ReadFromUDP(buf)
			if n <= 0 || err != nil {
				c.Close()
				return
			}
			b := make([]byte, n)
			copy(b, buf[:n])
			c.input <- Input{
				typ:   ActData,
				data:  b,
				param: remote,
			}
			if c.isClose {
				return
			}
		}
	}()

	var timerRunning bool
	var releaseMemory uint32
	var buf = make([]byte, mss*3)

	for {
		data := <-c.input
		switch data.typ {
		case ActData:
			head, _ := structUnPack(data.data, "BBQ")
			// fmt.Println(head[0], head[2])
			var dataType = head[0]
			if conn, ok := c.conns[head[2]]; ok {
				// TODO: 给dial中的链接发送成功 -> 暂时不需要，现在dial不判断这些，默认成功
				if dataType == uint64(TypeData) {
					conn.r.recv(data.data)
				} else if dataType == uint64(TypeAck) {
					conn.s.recv(data.data)
				}
			} else {
				if c.isDial == false && dataType == uint64(TypeData) && c.isClose == false { // 不需要TypeSYN
					// glog.V(0).Info("create new:", head[2])
					// 只有主动listen的，才有新链接，而dial自己就会创建新连接，不用创建
					conn := NewConn(c, head[2], c.sock, data.param.(*net.UDPAddr), "rsper")
					c.conns[conn.Id] = conn
					conn.r.recv(data.data)
					select {
					case c.accept <- conn:
					default:
					}
					if timerRunning == false {
						timerRunning = true
						go c.runTimer(c.timerRnd)
						glog.V(0).Info("start timer, round:", c.timerRnd, c.isDial)
					}
				}
			}
		case ActTimer:
			now := time.Now()
			for _, conn := range c.conns {
				conn.s.send(now, buf)
				conn.r.sendAck(now, buf)
			}
			if len(c.conns) == 0 && timerRunning {
				glog.V(0).Info("no conn, stop timer, round:", c.timerRnd, c.isDial)
				c.timerRnd++
				timerRunning = false
				debug.FreeOSMemory()
			}
			releaseMemory++
			if releaseMemory > 50*60 {
				releaseMemory = 0
				go func() {
					debug.FreeOSMemory() // 修复bug: go在windows不回收内存
				}()
			}
		case ActAddConn:
			conn := data.param.(*Conn)
			c.conns[conn.Id] = conn
			if timerRunning == false {
				timerRunning = true
				go c.runTimer(c.timerRnd)
				glog.V(0).Info("start timer, round:", c.timerRnd, c.isDial)
			}
		case ActRmConn:
			conn := data.param.(*Conn)
			// fmt.Println("rm conn:", conn.Id)
			conn.isClose = true
			conn.s.isClose = true
			conn.r.isClose = true
			if _, ok := c.conns[conn.Id]; ok {
				glog.V(0).Info("rm conn ok:", conn.Id, c.isDial, len(c.conns))
				close(conn.r.readable)
				close(conn.s.writable) // 修复bug: 没有close
				delete(c.conns, conn.Id)
			}
		case ActEnd:
			c.timerRnd++
			timerRunning = false
			c.isClose = true
			for _, conn := range c.conns {
				conn.isClose = true
				conn.s.isClose = true
				conn.r.isClose = true
				close(conn.r.readable)
				close(conn.s.writable)
			}
			close(c.accept)
			c.conns = make(map[uint64]*Conn)
			return
		}

	}

}

func (c *Conns) runTimer(rnd uint32) {
	// runtime.LockOSThread()
	for {
		time.Sleep(20 * time.Millisecond)
		// C.usleep(20 * 1000)
		if rnd != c.timerRnd {
			glog.V(0).Info("timer stop, round:", rnd, c.isDial)
		}
		if c.isClose || rnd != c.timerRnd {
			return
		}
		c.input <- Input{typ: ActTimer}
	}
}
