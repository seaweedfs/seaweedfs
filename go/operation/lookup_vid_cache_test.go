package operation

import (
	"fmt"
	"testing"
	"time"
)

func TestCaching(t *testing.T) {
	var (
		vc VidCache
	)
	var locations []Location
	locations = append(locations, Location{Url: "a.com:8080"})
	vc.Set("123", locations, time.Second)
	ret, _ := vc.Get("123")
	if ret == nil {
		t.Fatal("Not found vid 123")
	}
	fmt.Printf("vid 123 locations = %v\n", ret)
	time.Sleep(2 * time.Second)
	ret, _ = vc.Get("123")
	if ret != nil {
		t.Fatal("Not found vid 123")
	}
}
