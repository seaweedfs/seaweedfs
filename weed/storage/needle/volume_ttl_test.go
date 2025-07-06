package needle

import (
	"testing"
)

func TestTTLReadWrite(t *testing.T) {
	ttl, _ := ReadTTL("")
	if ttl.Minutes() != 0 {
		t.Errorf("empty ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("9")
	if ttl.Minutes() != 9 {
		t.Errorf("9 ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("8m")
	if ttl.Minutes() != 8 {
		t.Errorf("8m ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("5h")
	if ttl.Minutes() != 300 {
		t.Errorf("5h ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("5d")
	if ttl.Minutes() != 5*24*60 {
		t.Errorf("5d ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("50d")
	if ttl.Minutes() != 50*24*60 {
		t.Errorf("50d ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("365d")
	if ttl.Minutes() != 365*24*60 {
		t.Errorf("365d ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("730d")
	if ttl.Minutes() != 730*24*60 {
		t.Errorf("730d ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("5w")
	if ttl.Minutes() != 5*7*24*60 {
		t.Errorf("5w ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("5M")
	if ttl.Minutes() != 5*30*24*60 {
		t.Errorf("5M ttl:%v", ttl)
	}

	ttl, _ = ReadTTL("5y")
	if ttl.Minutes() != 5*365*24*60 {
		t.Errorf("5y ttl:%v", ttl)
	}

	output := make([]byte, 2)
	ttl.ToBytes(output)
	ttl2 := LoadTTLFromBytes(output)
	if ttl.Minutes() != ttl2.Minutes() {
		t.Errorf("ttl:%v ttl2:%v", ttl, ttl2)
	}

	ttl3 := LoadTTLFromUint32(ttl.ToUint32())
	if ttl.Minutes() != ttl3.Minutes() {
		t.Errorf("ttl:%v ttl3:%v", ttl, ttl3)
	}

}
