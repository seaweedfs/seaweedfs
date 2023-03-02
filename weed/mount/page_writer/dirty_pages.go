package page_writer

type DirtyPages interface {
	AddPage(offset int64, data []byte, isSequential bool, tsNs int64)
	FlushData() error
	ReadDirtyDataAt(data []byte, startOffset int64, tsNs int64) (maxStop int64)
	Destroy()
	LockForRead(startOffset, stopOffset int64)
	UnlockForRead(startOffset, stopOffset int64)
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}
