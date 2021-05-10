package filesys

type DirtyPages interface {
	AddPage(offset int64, data []byte)
	FlushData() error
	ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64)
	GetStorageOptions() (collection, replication string)
	SetWriteOnly(writeOnly bool)
	GetWriteOnly() (writeOnly bool)
}
