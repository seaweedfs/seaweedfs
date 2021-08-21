package tree_store

type TreeStore interface {
	Put(k int64, v []byte) error
	Get(k int64) ([]byte, error)
}
