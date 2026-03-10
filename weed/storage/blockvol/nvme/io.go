package nvme

// handleRead processes an NVMe Read command.
func (c *Controller) handleRead(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
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
		return c.sendResponse(req)
	}

	data, err := dev.ReadAt(lba, totalBytes)
	if err != nil {
		req.resp.Status = uint16(mapBlockError(err))
		return c.sendResponse(req)
	}

	req.c2hData = data
	return c.sendC2HDataAndResponse(req)
}

// handleWrite processes an NVMe Write command with inline or R2T data.
func (c *Controller) handleWrite(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}

	// Check ANA state (write-gating)
	if !c.isWriteAllowed() {
		req.resp.Status = uint16(StatusNSNotReady)
		return c.sendResponse(req)
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
		return c.sendResponse(req)
	}

	var writeData []byte

	if len(req.payload) > 0 {
		// Inline data path: data was in the CapsuleCmd PDU.
		if uint32(len(req.payload)) != expectedBytes {
			req.resp.Status = uint16(StatusInvalidField)
			return c.sendResponse(req)
		}
		writeData = req.payload
	} else {
		// R2T flow: send Ready-to-Transfer, then receive H2C Data PDUs.
		if err := c.sendR2T(req.capsule.CID, 0, 0, expectedBytes); err != nil {
			return err
		}
		data, err := c.recvH2CData(expectedBytes)
		if err != nil {
			return err
		}
		writeData = data
		defer putBuffer(data)
	}

	throttleOnWALPressure(dev)
	if err := writeWithRetry(dev, lba, writeData); err != nil {
		req.resp.Status = uint16(mapBlockError(err))
		return c.sendResponse(req)
	}

	return c.sendResponse(req)
}

// handleFlush processes an NVMe Flush command.
func (c *Controller) handleFlush(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}

	if !c.isWriteAllowed() {
		req.resp.Status = uint16(StatusNSNotReady)
		return c.sendResponse(req)
	}

	if err := sub.Dev.SyncCache(); err != nil {
		req.resp.Status = uint16(mapBlockError(err))
		return c.sendResponse(req)
	}

	return c.sendResponse(req)
}

// handleWriteZeros processes an NVMe Write Zeroes command.
func (c *Controller) handleWriteZeros(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}

	if !c.isWriteAllowed() {
		req.resp.Status = uint16(StatusNSNotReady)
		return c.sendResponse(req)
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
		return c.sendResponse(req)
	}

	// D12 bit 25: DEALLOC — if set, use Trim instead of writing zeros
	if req.capsule.D12&commandBitDeallocate != 0 {
		if err := dev.Trim(lba, totalBytes); err != nil {
			req.resp.Status = uint16(mapBlockError(err))
			return c.sendResponse(req)
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
			return c.sendResponse(req)
		}
	}

	return c.sendResponse(req)
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
