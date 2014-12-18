package sequence

type Sequencer interface {
	NextFileId(count int) (uint64, int)
	SetMax(uint64)
	Peek() uint64
}
