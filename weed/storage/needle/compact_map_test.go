package needle

import (
	"testing"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

func TestIssue52(t *testing.T) {
	m := NewCompactMap()
	m.Set(NeedleId(10002), 10002, 10002)
	if element, ok := m.Get(NeedleId(10002)); ok {
		println("key", 10002, "ok", ok, element.Key, element.Offset, element.Size)
	}
	m.Set(NeedleId(10001), 10001, 10001)
	if element, ok := m.Get(NeedleId(10002)); ok {
		println("key", 10002, "ok", ok, element.Key, element.Offset, element.Size)
	} else {
		t.Fatal("key 10002 missing after setting 10001")
	}
}

func TestXYZ(t *testing.T) {
	m := NewCompactMap()
	for i := uint32(0); i < 100*batch; i += 2 {
		m.Set(NeedleId(i), Offset(i), i)
	}

	for i := uint32(0); i < 100*batch; i += 37 {
		m.Delete(NeedleId(i))
	}

	for i := uint32(0); i < 10*batch; i += 3 {
		m.Set(NeedleId(i), Offset(i+11), i+5)
	}

	//	for i := uint32(0); i < 100; i++ {
	//		if v := m.Get(Key(i)); v != nil {
	//			glog.V(4).Infoln(i, "=", v.Key, v.Offset, v.Size)
	//		}
	//	}

	for i := uint32(0); i < 10*batch; i++ {
		v, ok := m.Get(NeedleId(i))
		if i%3 == 0 {
			if !ok {
				t.Fatal("key", i, "missing!")
			}
			if v.Size != i+5 {
				t.Fatal("key", i, "size", v.Size)
			}
		} else if i%37 == 0 {
			if ok && v.Size > 0 {
				t.Fatal("key", i, "should have been deleted needle value", v)
			}
		} else if i%2 == 0 {
			if v.Size != i {
				t.Fatal("key", i, "size", v.Size)
			}
		}
	}

	for i := uint32(10 * batch); i < 100*batch; i++ {
		v, ok := m.Get(NeedleId(i))
		if i%37 == 0 {
			if ok && v.Size > 0 {
				t.Fatal("key", i, "should have been deleted needle value", v)
			}
		} else if i%2 == 0 {
			if v == nil {
				t.Fatal("key", i, "missing")
			}
			if v.Size != i {
				t.Fatal("key", i, "size", v.Size)
			}
		}
	}

}
