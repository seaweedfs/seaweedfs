package broker

import (
	"fmt"
	"testing"
)

func TestPickMember(t *testing.T) {

	servers := []string{
		"s1:port",
		"s2:port",
		"s3:port",
		"s5:port",
		"s4:port",
	}

	total := 1000

	distribution := make(map[string]int)
	for i:=0;i<total;i++{
		tp := fmt.Sprintf("tp:%2d", i)
		m := PickMember(servers, []byte(tp))
		// println(tp, "=>", m)
		distribution[m]++
	}

	for member, count := range distribution {
		fmt.Printf("member: %s, key count: %d load=%.2f\n", member, count, float64(count*100)/float64(total/len(servers)))
	}

}