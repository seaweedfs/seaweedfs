package skiplist

import (
	"math/rand"
	"strconv"
	"testing"
)

const (
	maxNameCount = 100
)

func String(x int) string {
	return strconv.Itoa(x)
}

func TestNameList(t *testing.T) {
	list := newNameList(memStore, 7)

	for i := 0; i < maxNameCount; i++ {
		list.WriteName(String(i))
	}

	counter := 0
	list.ListNames("", func(name string) bool {
		counter++
		print(name, " ")
		return true
	})
	if counter != maxNameCount {
		t.Fail()
	}

	// list.skipList.println()

	deleteBase := 5
	deleteCount := maxNameCount - 3*deleteBase

	for i := deleteBase; i < deleteBase+deleteCount; i++ {
		list.DeleteName(String(i))
	}

	counter = 0
	list.ListNames("", func(name string) bool {
		counter++
		return true
	})
	// list.skipList.println()
	if counter != maxNameCount-deleteCount {
		t.Fail()
	}

	// randomized deletion
	list = newNameList(memStore, 7)
	// Delete elements at random positions in the list.
	rList := rand.Perm(maxN)
	for _, i := range rList {
		list.WriteName(String(i))
	}
	for _, i := range rList {
		list.DeleteName(String(i))
	}
	counter = 0
	list.ListNames("", func(name string) bool {
		counter++
		print(name, " ")
		return true
	})
	if counter != 0 {
		t.Fail()
	}

}
