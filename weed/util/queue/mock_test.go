package queue

import (
	"strconv"
)

type mockItem int

func (mi mockItem) Compare(other Item) int {
	omi := other.(mockItem)
	if mi > omi {
		return 1
	} else if mi == omi {
		return 0
	}
	return -1
}

func (mi mockItem) GetUniqueID() string {
	return strconv.Itoa(int(mi))
}
