package nvme

import (
	"encoding/binary"
)

// handleSetFeatures processes SetFeatures admin commands.
func (c *Controller) handleSetFeatures(req *Request) error {
	fid := uint8(req.capsule.D10 & 0xFF)

	switch fid {
	case fidNumberOfQueues:
		// D11: NCQR[15:0] | NSQR[31:16]
		ncqr := uint16(req.capsule.D11 & 0xFFFF)
		nsqr := uint16(req.capsule.D11 >> 16)

		// Grant min(requested, max)
		if ncqr > c.maxIOQueues {
			ncqr = c.maxIOQueues
		}
		if nsqr > c.maxIOQueues {
			nsqr = c.maxIOQueues
		}
		if ncqr == 0 {
			ncqr = 1
		}
		if nsqr == 0 {
			nsqr = 1
		}
		c.grantedQueues = ncqr

		// Response DW0: (NCQR-1) | ((NSQR-1) << 16)
		req.resp.DW0 = uint32(ncqr-1) | (uint32(nsqr-1) << 16)
		return c.sendResponse(req)

	case fidKeepAliveTimer:
		// D11 contains KATO in milliseconds
		c.katoMs = req.capsule.D11
		return c.sendResponse(req)

	case fidAsyncEventConfig:
		// Stub: accept but don't deliver events
		return c.sendResponse(req)

	default:
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}
}

// handleGetFeatures returns stored feature values.
func (c *Controller) handleGetFeatures(req *Request) error {
	fid := uint8(req.capsule.D10 & 0xFF)

	switch fid {
	case fidNumberOfQueues:
		n := c.grantedQueues
		if n == 0 {
			n = c.maxIOQueues
		}
		req.resp.DW0 = uint32(n-1) | (uint32(n-1) << 16)
		return c.sendResponse(req)

	case fidKeepAliveTimer:
		req.resp.DW0 = c.katoMs
		return c.sendResponse(req)

	case fidAsyncEventConfig:
		req.resp.DW0 = 0
		return c.sendResponse(req)

	default:
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}
}

// handleGetLogPage returns log page data.
func (c *Controller) handleGetLogPage(req *Request) error {
	// D10 bits 7:0 = Log Page Identifier
	// D10 bits 27:16 and D11 bits 15:0 = Number of Dwords (NUMD)
	lid := uint8(req.capsule.D10 & 0xFF)
	numdl := (req.capsule.D10 >> 16) & 0xFFF
	numdu := req.capsule.D11 & 0xFFFF
	numd := uint32(numdu)<<16 | uint32(numdl)
	length := (numd + 1) * 4 // NUMD is 0-based, in dwords

	switch lid {
	case logPageError:
		return c.logPageError(req, length)
	case logPageSMART:
		return c.logPageSMART(req, length)
	case logPageANA:
		return c.logPageANA(req, length)
	default:
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}
}

// logPageError returns an empty error log page.
func (c *Controller) logPageError(req *Request, length uint32) error {
	if length > 64 {
		length = 64
	}
	req.c2hData = make([]byte, length)
	return c.sendC2HDataAndResponse(req)
}

// logPageSMART returns a 512-byte SMART/Health log.
func (c *Controller) logPageSMART(req *Request, length uint32) error {
	if length > 512 {
		length = 512
	}
	buf := make([]byte, 512)

	// Critical Warning - offset 0: 0 = no warnings
	buf[0] = 0

	// Composite Temperature - offset 1-2: 0 (not implemented)
	binary.LittleEndian.PutUint16(buf[1:], 0)

	// Available Spare - offset 3: 100%
	buf[3] = 100

	// Available Spare Threshold - offset 4: 10%
	buf[4] = 10

	// Percentage Used - offset 5: 0%
	buf[5] = 0

	req.c2hData = buf[:length]
	return c.sendC2HDataAndResponse(req)
}

// logPageANA returns the ANA log page with a single group.
func (c *Controller) logPageANA(req *Request, length uint32) error {
	// ANA log page format (32 bytes for single group):
	// [0:8]   CHGCNT (uint64)
	// [8:10]  NGRPS = 1 (uint16)
	// [10:16] reserved
	// Group descriptor:
	// [16:20] ANAGRPID = 1 (uint32)
	// [20:24] NNSID = 1 (uint32)
	// [24:32] Change Count (uint64)
	// [32]    ANA State
	// [33:36] reserved
	// [36:40] NSID = 1 (uint32)
	const anaLogSize = 40

	buf := make([]byte, anaLogSize)

	// CHGCNT
	binary.LittleEndian.PutUint64(buf[0:], c.anaChangeCount())

	// NGRPS
	binary.LittleEndian.PutUint16(buf[8:], 1)

	// Group descriptor
	binary.LittleEndian.PutUint32(buf[16:], 1)  // ANAGRPID=1
	binary.LittleEndian.PutUint32(buf[20:], 1)  // NNSID=1
	binary.LittleEndian.PutUint64(buf[24:], c.anaChangeCount()) // chgcnt
	buf[32] = c.anaState() // ANA state
	binary.LittleEndian.PutUint32(buf[36:], 1)  // NSID=1

	if length > anaLogSize {
		length = anaLogSize
	}
	req.c2hData = buf[:length]
	return c.sendC2HDataAndResponse(req)
}

// anaState returns the current ANA state based on the subsystem's device.
func (c *Controller) anaState() uint8 {
	if c.subsystem == nil {
		return anaInaccessible
	}
	if prov, ok := c.subsystem.Dev.(ANAProvider); ok {
		return prov.ANAState()
	}
	// Default: if healthy → optimized
	if c.subsystem.Dev.IsHealthy() {
		return anaOptimized
	}
	return anaInaccessible
}

// anaChangeCount returns a monotonic ANA change counter.
// For MVP, we use 1 as a constant (no dynamic role changes tracked).
func (c *Controller) anaChangeCount() uint64 {
	return 1
}

// handleKeepAlive resets the KATO timer and returns success.
func (c *Controller) handleKeepAlive(req *Request) error {
	c.resetKATO()
	return c.sendResponse(req)
}
