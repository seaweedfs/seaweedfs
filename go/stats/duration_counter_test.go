package stats

import "testing"

func TestRobinCounter(t *testing.T) {
	rrc := NewRoundRobinCounter(60)
	rrc.Add(0, 1)
	rrc.Add(50, 2)
	if rrc.Count() != 2 {
		t.Fatal()
	}
	if rrc.Sum() != 3 {
		t.Fatal()
	}
	/*
		index out of range
	*/
	rrc.Add(61, 1)
}
