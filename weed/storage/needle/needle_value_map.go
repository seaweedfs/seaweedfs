package needle

type NeedleValueMap interface {
	Set(key Key, offset, size uint32) (oldOffset, oldSize uint32)
	Delete(key Key) uint32
	Get(key Key) (*NeedleValue, bool)
	Visit(visit func(NeedleValue) error) error
}
