package page_writer

import (
	"sync/atomic"
)

func (up *UploadPipeline) LockForRead(startOffset, stopOffset int64) {
	startLogicChunkIndex := LogicChunkIndex(startOffset / up.ChunkSize)
	stopLogicChunkIndex := LogicChunkIndex(stopOffset / up.ChunkSize)
	if stopOffset%up.ChunkSize > 0 {
		stopLogicChunkIndex += 1
	}
	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()
	for i := startLogicChunkIndex; i < stopLogicChunkIndex; i++ {
		if count, found := up.activeReadChunks[i]; found {
			up.activeReadChunks[i] = count + 1
		} else {
			up.activeReadChunks[i] = 1
		}
	}
}

func (up *UploadPipeline) UnlockForRead(startOffset, stopOffset int64) {
	startLogicChunkIndex := LogicChunkIndex(startOffset / up.ChunkSize)
	stopLogicChunkIndex := LogicChunkIndex(stopOffset / up.ChunkSize)
	if stopOffset%up.ChunkSize > 0 {
		stopLogicChunkIndex += 1
	}
	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()
	for i := startLogicChunkIndex; i < stopLogicChunkIndex; i++ {
		if count, found := up.activeReadChunks[i]; found {
			if count == 1 {
				delete(up.activeReadChunks, i)
			} else {
				up.activeReadChunks[i] = count - 1
			}
		}
	}
}

func (up *UploadPipeline) IsLocked(logicChunkIndex LogicChunkIndex) bool {
	if count, found := up.activeReadChunks[logicChunkIndex]; found {
		return count > 0
	}
	return false
}

func (up *UploadPipeline) waitForCurrentWritersToComplete() {
	up.uploaderCountCond.L.Lock()
	t := int32(100)
	for {
		t = atomic.LoadInt32(&up.uploaderCount)
		if t <= 0 {
			break
		}
		up.uploaderCountCond.Wait()
	}
	up.uploaderCountCond.L.Unlock()
}
