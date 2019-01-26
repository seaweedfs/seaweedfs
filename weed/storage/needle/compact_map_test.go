package needle

import (
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"testing"
)

func TestOverflow2(t *testing.T) {
	m := NewCompactMap()
	m.Set(NeedleId(150088), 8, 3000073)
	m.Set(NeedleId(150073), 8, 3000073)
	m.Set(NeedleId(150089), 8, 3000073)
	m.Set(NeedleId(150076), 8, 3000073)
	m.Set(NeedleId(150124), 8, 3000073)
	m.Set(NeedleId(150137), 8, 3000073)
	m.Set(NeedleId(150147), 8, 3000073)
	m.Set(NeedleId(150145), 8, 3000073)
	m.Set(NeedleId(150158), 8, 3000073)
	m.Set(NeedleId(150162), 8, 3000073)

	m.Visit(func(value NeedleValue) error {
		println("needle key:", value.Key)
		return nil
	})
}

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

func TestCompactMap(t *testing.T) {
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
			if ok && v.Size != TombstoneFileSize {
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
			if ok && v.Size != TombstoneFileSize {
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

func TestOverflow(t *testing.T) {
	o := Overflow(make([]SectionalNeedleValue, 0))

	o = o.setOverflowEntry(SectionalNeedleValue{Key: 1, Offset: 12, Size: 12})
	o = o.setOverflowEntry(SectionalNeedleValue{Key: 2, Offset: 12, Size: 12})
	o = o.setOverflowEntry(SectionalNeedleValue{Key: 3, Offset: 12, Size: 12})
	o = o.setOverflowEntry(SectionalNeedleValue{Key: 4, Offset: 12, Size: 12})
	o = o.setOverflowEntry(SectionalNeedleValue{Key: 5, Offset: 12, Size: 12})

	if o[2].Key != 3 {
		t.Fatalf("expecting o[2] has key 3: %+v", o[2].Key)
	}

	o = o.setOverflowEntry(SectionalNeedleValue{Key: 3, Offset: 24, Size: 24})

	if o[2].Key != 3 {
		t.Fatalf("expecting o[2] has key 3: %+v", o[2].Key)
	}

	if o[2].Size != 24 {
		t.Fatalf("expecting o[2] has size 24: %+v", o[2].Size)
	}

	o = o.deleteOverflowEntry(4)

	if len(o) != 4 {
		t.Fatalf("expecting 4 entries now: %+v", o)
	}

	x, _ := o.findOverflowEntry(5)
	if x.Key != 5 {
		t.Fatalf("expecting entry 5 now: %+v", x)
	}

	for i, x := range o {
		println("overflow[", i, "]:", x.Key)
	}
	println()

	o = o.deleteOverflowEntry(1)

	for i, x := range o {
		println("overflow[", i, "]:", x.Key)
	}
	println()

	o = o.setOverflowEntry(SectionalNeedleValue{Key: 4, Offset: 44, Size: 44})
	for i, x := range o {
		println("overflow[", i, "]:", x.Key)
	}
	println()

	o = o.setOverflowEntry(SectionalNeedleValue{Key: 1, Offset: 11, Size: 11})

	for i, x := range o {
		println("overflow[", i, "]:", x.Key)
	}
	println()

}
