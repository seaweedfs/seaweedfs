package sequence

type Sequencer interface {
	NextFileId(count uint64) uint64
	SetMax(uint64)
}
