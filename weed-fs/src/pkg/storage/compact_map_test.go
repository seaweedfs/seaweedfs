package storage

import (
	"testing"
)

func TestXYZ(t *testing.T) {
	m := NewCompactMap()
	for i := uint32(0); i < 100*batch; i += 2 {
		m.Set(Key(i), i, i)
	}

	for i := uint32(0); i < 100*batch; i += 37 {
		m.Delete(Key(i))
	}

	for i := uint32(0); i < 10*batch; i += 3 {
		m.Set(Key(i), i+11, i+5)
	}

//	for i := uint32(0); i < 100; i++ {
//		if v := m.Get(Key(i)); v != nil {
//			println(i, "=", v.Key, v.Offset, v.Size)
//		}
//	}
	
	for i := uint32(0); i < 10*batch; i++ {
    v, ok := m.Get(Key(i))
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
    v, ok := m.Get(Key(i))
		if i%37 == 0 {
      if ok && v.Size > 0 {
        t.Fatal("key", i, "should have been deleted needle value", v)
      }
		} else if i%2 == 0 {
		  if v==nil{
        t.Fatal("key", i, "missing")
		  }
			if v.Size != i {
				t.Fatal("key", i, "size", v.Size)
			}
		}
	}

}
