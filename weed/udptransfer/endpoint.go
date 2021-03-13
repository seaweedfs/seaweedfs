package udptransfer

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudflare/golibs/bytepool"
)

const (
	_SO_BUF_SIZE = 8 << 20
)

var (
	bpool bytepool.BytePool
)

type Params struct {
	LocalAddr      string
	Bandwidth      int64
	Mtu            int
	IsServ         bool
	FastRetransmit bool
	FlatTraffic    bool
	EnablePprof    bool
	Stacktrace     bool
	Debug          int
}

type connID struct {
	lid uint32
	rid uint32
}

type Endpoint struct {
	udpconn    *net.UDPConn
	state      int32
	idSeq      uint32
	isServ     bool
	listenChan chan *Conn
	lRegistry  map[uint32]*Conn
	rRegistry  map[string][]uint32
	mlock      sync.RWMutex
	timeout    *time.Timer
	params     Params
}

func (c *connID) setRid(b []byte) {
	c.rid = binary.BigEndian.Uint32(b[_MAGIC_SIZE+6:])
}

func init() {
	bpool.Init(0, 2000)
	rand.Seed(NowNS())
}

func NewEndpoint(p *Params) (*Endpoint, error) {
	set_debug_params(p)
	if p.Bandwidth <= 0 || p.Bandwidth > 100 {
		return nil, fmt.Errorf("bw->(0,100]")
	}
	conn, err := net.ListenPacket("udp", p.LocalAddr)
	if err != nil {
		return nil, err
	}
	e := &Endpoint{
		udpconn:    conn.(*net.UDPConn),
		idSeq:      1,
		isServ:     p.IsServ,
		listenChan: make(chan *Conn, 1),
		lRegistry:  make(map[uint32]*Conn),
		rRegistry:  make(map[string][]uint32),
		timeout:    time.NewTimer(0),
		params:     *p,
	}
	if e.isServ {
		e.state = _S_EST0
	} else { // client
		e.state = _S_EST1
		e.idSeq = uint32(rand.Int31())
	}
	e.params.Bandwidth = p.Bandwidth << 20 // mbps to bps
	e.udpconn.SetReadBuffer(_SO_BUF_SIZE)
	go e.internal_listen()
	return e, nil
}

func (e *Endpoint) internal_listen() {
	const rtmo = time.Duration(30*time.Second)
	var id connID
	for {
		//var buf = make([]byte, 1600)
		var buf = bpool.Get(1600)
		e.udpconn.SetReadDeadline(time.Now().Add(rtmo))
		n, addr, err := e.udpconn.ReadFromUDP(buf)
		if err == nil && n >= _AH_SIZE {
			buf = buf[:n]
			e.getConnID(&id, buf)

			switch id.lid {
			case 0: // new connection
				if e.isServ {
					go e.acceptNewConn(id, addr, buf)
				} else {
					dumpb("drop", buf)
				}

			case _INVALID_SEQ:
				dumpb("drop invalid", buf)

			default: // old connection
				e.mlock.RLock()
				conn := e.lRegistry[id.lid]
				e.mlock.RUnlock()
				if conn != nil {
					e.dispatch(conn, buf)
				} else {
					e.resetPeer(addr, id)
					dumpb("drop null", buf)
				}
			}

		} else if err != nil {
			// idle process
			if nerr, y := err.(net.Error); y && nerr.Timeout() {
				e.idleProcess()
				continue
			}
			// other errors
			if atomic.LoadInt32(&e.state) == _S_FIN {
				return
			} else {
				log.Println("Error: read sock", err)
			}
		}
	}
}

func (e *Endpoint) idleProcess() {
	// recycle/shrink memory
	bpool.Drain()
	e.mlock.Lock()
	defer e.mlock.Unlock()
	// reset urgent
	for _, c := range e.lRegistry {
		c.outlock.Lock()
		if c.outQ.size() == 0 && c.urgent != 0 {
			c.urgent = 0
		}
		c.outlock.Unlock()
	}
}

func (e *Endpoint) Dial(addr string) (*Conn, error) {
	rAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	e.mlock.Lock()
	e.idSeq++
	id := connID{e.idSeq, 0}
	conn := NewConn(e, rAddr, id)
	e.lRegistry[id.lid] = conn
	e.mlock.Unlock()
	if atomic.LoadInt32(&e.state) != _S_FIN {
		err = conn.initConnection(nil)
		return conn, err
	}
	return nil, io.EOF
}

func (e *Endpoint) acceptNewConn(id connID, addr *net.UDPAddr, buf []byte) {
	rKey := addr.String()
	e.mlock.Lock()
	// map: remoteAddr => remoteConnID
	// filter duplicated syn packets
	if newArr := insertRid(e.rRegistry[rKey], id.rid); newArr != nil {
		e.rRegistry[rKey] = newArr
	} else {
		e.mlock.Unlock()
		log.Println("Warn: duplicated connection", addr)
		return
	}
	e.idSeq++
	id.lid = e.idSeq
	conn := NewConn(e, addr, id)
	e.lRegistry[id.lid] = conn
	e.mlock.Unlock()
	err := conn.initConnection(buf)
	if err == nil {
		select {
		case e.listenChan <- conn:
		case <-time.After(_10ms):
			log.Println("Warn: no listener")
		}
	} else {
		e.removeConn(id, addr)
		log.Println("Error: init_connection", addr, err)
	}
}

func (e *Endpoint) removeConn(id connID, addr *net.UDPAddr) {
	e.mlock.Lock()
	delete(e.lRegistry, id.lid)
	rKey := addr.String()
	if newArr := deleteRid(e.rRegistry[rKey], id.rid); newArr != nil {
		if len(newArr) > 0 {
			e.rRegistry[rKey] = newArr
		} else {
			delete(e.rRegistry, rKey)
		}
	}
	e.mlock.Unlock()
}

// net.Listener
func (e *Endpoint) Close() error {
	state := atomic.LoadInt32(&e.state)
	if state > 0 && atomic.CompareAndSwapInt32(&e.state, state, _S_FIN) {
		err := e.udpconn.Close()
		e.lRegistry = nil
		e.rRegistry = nil
		select { // release listeners
		case e.listenChan <- nil:
		default:
		}
		return err
	}
	return nil
}

// net.Listener
func (e *Endpoint) Addr() net.Addr {
	return e.udpconn.LocalAddr()
}

// net.Listener
func (e *Endpoint) Accept() (net.Conn, error) {
	if atomic.LoadInt32(&e.state) == _S_EST0 {
		return <-e.listenChan, nil
	} else {
		return nil, io.EOF
	}
}

func (e *Endpoint) Listen() *Conn {
	if atomic.LoadInt32(&e.state) == _S_EST0 {
		return <-e.listenChan
	} else {
		return nil
	}
}

// tmo in MS
func (e *Endpoint) ListenTimeout(tmo int64) *Conn {
	if tmo <= 0 {
		return e.Listen()
	}
	if atomic.LoadInt32(&e.state) == _S_EST0 {
		select {
		case c := <-e.listenChan:
			return c
		case <-NewTimerChan(tmo):
		}
	}
	return nil
}

func (e *Endpoint) getConnID(idPtr *connID, buf []byte) {
	// TODO determine magic header
	magicAndLen := binary.BigEndian.Uint64(buf)
	if int(magicAndLen&0xFFff) == len(buf) {
		id := binary.BigEndian.Uint64(buf[_MAGIC_SIZE+2:])
		idPtr.lid = uint32(id >> 32)
		idPtr.rid = uint32(id)
	} else {
		idPtr.lid = _INVALID_SEQ
	}
}

func (e *Endpoint) dispatch(c *Conn, buf []byte) {
	e.timeout.Reset(30*time.Millisecond)
	select {
	case c.evRecv <- buf:
	case <-e.timeout.C:
		log.Println("Warn: dispatch packet failed")
	}
}

func (e *Endpoint) resetPeer(addr *net.UDPAddr, id connID) {
	pk := &packet{flag: _F_FIN | _F_RESET}
	buf := nodeOf(pk).marshall(id)
	e.udpconn.WriteToUDP(buf, addr)
}

type u32Slice []uint32

func (p u32Slice) Len() int           { return len(p) }
func (p u32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p u32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// if the rid is not existed in array then insert it return new array
func insertRid(array []uint32, rid uint32) []uint32 {
	if len(array) > 0 {
		pos := sort.Search(len(array), func(n int) bool {
			return array[n] >= rid
		})
		if pos < len(array) && array[pos] == rid {
			return nil
		}
	}
	array = append(array, rid)
	sort.Sort(u32Slice(array))
	return array
}

// if rid was existed in array then delete it return new array
func deleteRid(array []uint32, rid uint32) []uint32 {
	if len(array) > 0 {
		pos := sort.Search(len(array), func(n int) bool {
			return array[n] >= rid
		})
		if pos < len(array) && array[pos] == rid {
			newArray := make([]uint32, len(array)-1)
			n := copy(newArray, array[:pos])
			copy(newArray[n:], array[pos+1:])
			return newArray
		}
	}
	return nil
}
