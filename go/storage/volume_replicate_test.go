package storage

import "testing"

func TestDirtyDataSearch(t *testing.T) {
	testData := DirtyDatas{
		{30, 20}, {106, 200}, {5, 20}, {512, 68}, {412, 50},
	}
	testOffset := []int64{
		0, 150, 480, 1024,
	}
	testData.Sort()
	t.Logf("TestData = %v", testData)
	for _, off := range testOffset {
		i := testData.Search(off)
		if i < testData.Len() {
			t.Logf("(%d) nearest chunk[%d]: %v", off, i, testData[i])
		} else {
			t.Logf("Search %d return %d ", off, i)
		}
	}
}
