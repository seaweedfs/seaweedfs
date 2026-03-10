package nvme

// handleRead processes an NVMe Read command.
func (c *Controller) handleRead(req *Request) {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	dev := sub.Dev
	lba := req.capsule.Lba()
	nlb := req.capsule.LbaLength()
	blockSize := dev.BlockSize()
	totalBytes := uint32(nlb) * blockSize

	// Bounds check
	nsze := dev.VolumeSize() / uint64(blockSize)
	if lba+uint64(nlb) > nsze {
		req.resp.Status = uint16(StatusLBAOutOfRange)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	data, err := dev.ReadAt(lba, totalBytes)
	if err != nil {
		req.resp.Status = uint16(mapBlockError(err))
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	req.c2hData = data
	c.enqueueResponse(&response{resp: req.resp, c2hData: req.c2hData})
}

// handleWrite processes an NVMe Write command with inline or R2T data.
// By the time this handler runs, req.payload always contains the write data:
// either inline from the CapsuleCmd PDU, or collected via R2T by collectR2TData.
func (c *Controller) handleWrite(req *Request) {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	// Check ANA state (write-gating)
	if !c.isWriteAllowed() {
		req.resp.Status = uint16(StatusNSNotReady)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	dev := sub.Dev
	lba := req.capsule.Lba()
	nlb := req.capsule.LbaLength()
	blockSize := dev.BlockSize()
	expectedBytes := uint32(nlb) * blockSize

	// Bounds check
	nsze := dev.VolumeSize() / uint64(blockSize)
	if lba+uint64(nlb) > nsze {
		req.resp.Status = uint16(StatusLBAOutOfRange)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	if uint32(len(req.payload)) != expectedBytes {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	throttleOnWALPressure(dev)
	if err := writeWithRetry(dev, lba, req.payload); err != nil {
		req.resp.Status = uint16(mapBlockError(err))
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	c.enqueueResponse(&response{resp: req.resp})
}

// handleFlush processes an NVMe Flush command.
func (c *Controller) handleFlush(req *Request) {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	if !c.isWriteAllowed() {
		req.resp.Status = uint16(StatusNSNotReady)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	if err := sub.Dev.SyncCache(); err != nil {
		req.resp.Status = uint16(mapBlockError(err))
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	c.enqueueResponse(&response{resp: req.resp})
}

// handleWriteZeros processes an NVMe Write Zeroes command.
func (c *Controller) handleWriteZeros(req *Request) {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	if !c.isWriteAllowed() {
		req.resp.Status = uint16(StatusNSNotReady)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	dev := sub.Dev
	lba := req.capsule.Lba()
	nlb := req.capsule.LbaLength()
	blockSize := dev.BlockSize()
	totalBytes := uint32(nlb) * blockSize

	// Bounds check
	nsze := dev.VolumeSize() / uint64(blockSize)
	if lba+uint64(nlb) > nsze {
		req.resp.Status = uint16(StatusLBAOutOfRange)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	// D12 bit 25: DEALLOC — if set, use Trim instead of writing zeros
	if req.capsule.D12&commandBitDeallocate != 0 {
		if err := dev.Trim(lba, totalBytes); err != nil {
			req.resp.Status = uint16(mapBlockError(err))
			c.enqueueResponse(&response{resp: req.resp})
			return
		}
	} else {
		zeroBuf := getBuffer(int(totalBytes))
		for i := range zeroBuf {
			zeroBuf[i] = 0
		}
		throttleOnWALPressure(dev)
		err := writeWithRetry(dev, lba, zeroBuf)
		putBuffer(zeroBuf)
		if err != nil {
			req.resp.Status = uint16(mapBlockError(err))
			c.enqueueResponse(&response{resp: req.resp})
			return
		}
	}

	c.enqueueResponse(&response{resp: req.resp})
}

// isWriteAllowed checks if the current ANA state allows writes.
func (c *Controller) isWriteAllowed() bool {
	if c.subsystem == nil {
		return false
	}
	if prov, ok := c.subsystem.Dev.(ANAProvider); ok {
		state := prov.ANAState()
		return state == anaOptimized || state == anaNonOptimized
	}
	// No ANA provider: allow if healthy
	return c.subsystem.Dev.IsHealthy()
}
