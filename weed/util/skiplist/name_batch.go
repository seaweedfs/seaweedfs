package skiplist

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"google.golang.org/protobuf/proto"
	"slices"
	"strings"
)

type NameBatch struct {
	key   string
	names map[string]struct{}
}

func (nb *NameBatch) ContainsName(name string) (found bool) {
	_, found = nb.names[name]
	return
}
func (nb *NameBatch) WriteName(name string) {
	if nb.key == "" || strings.Compare(nb.key, name) > 0 {
		nb.key = name
	}
	nb.names[name] = struct{}{}
}
func (nb *NameBatch) DeleteName(name string) {
	delete(nb.names, name)
	if nb.key == name {
		nb.key = ""
		for n := range nb.names {
			if nb.key == "" || strings.Compare(nb.key, n) > 0 {
				nb.key = n
			}
		}
	}
}
func (nb *NameBatch) ListNames(startFrom string, visitNamesFn func(name string) bool) bool {
	var names []string
	needFilter := startFrom != ""
	for n := range nb.names {
		if !needFilter || strings.Compare(n, startFrom) >= 0 {
			names = append(names, n)
		}
	}
	slices.SortFunc(names, func(a, b string) int {
		return strings.Compare(a, b)
	})
	for _, n := range names {
		if !visitNamesFn(n) {
			return false
		}
	}
	return true
}

func NewNameBatch() *NameBatch {
	return &NameBatch{
		names: make(map[string]struct{}),
	}
}

func LoadNameBatch(data []byte) *NameBatch {
	t := &NameBatchData{}
	if len(data) > 0 {
		err := proto.Unmarshal(data, t)
		if err != nil {
			glog.Errorf("unmarshal into NameBatchData{} : %v", err)
			return nil
		}
	}
	nb := NewNameBatch()
	for _, n := range t.Names {
		name := string(n)
		if nb.key == "" || strings.Compare(nb.key, name) > 0 {
			nb.key = name
		}
		nb.names[name] = struct{}{}
	}
	return nb
}

func (nb *NameBatch) ToBytes() []byte {
	t := &NameBatchData{}
	for n := range nb.names {
		t.Names = append(t.Names, []byte(n))
	}
	data, _ := proto.Marshal(t)
	return data
}

func (nb *NameBatch) SplitBy(name string) (x, y *NameBatch) {
	x, y = NewNameBatch(), NewNameBatch()

	for n := range nb.names {
		// there should be no equal case though
		if strings.Compare(n, name) <= 0 {
			x.WriteName(n)
		} else {
			y.WriteName(n)
		}
	}
	return
}
