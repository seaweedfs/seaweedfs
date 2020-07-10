package filesys

import (
	"bytes"
	"testing"
)

func TestContinuousIntervals_AddIntervalAppend(t *testing.T) {

	c := &ContinuousIntervals{}

	// 25, 25, 25
	c.AddInterval(getBytes(25, 3), 0)
	//  _,  _, 23, 23, 23, 23
	c.AddInterval(getBytes(23, 4), 2)

	expectedData(t, c, 0, 25, 25, 23, 23, 23, 23)

}

func TestContinuousIntervals_AddIntervalInnerOverwrite(t *testing.T) {

	c := &ContinuousIntervals{}

	// 25, 25, 25, 25, 25
	c.AddInterval(getBytes(25, 5), 0)
	//  _,  _, 23, 23
	c.AddInterval(getBytes(23, 2), 2)

	expectedData(t, c, 0, 25, 25, 23, 23, 25)

}

func TestContinuousIntervals_AddIntervalFullOverwrite(t *testing.T) {

	c := &ContinuousIntervals{}

	// 1,
	c.AddInterval(getBytes(1, 1), 0)
	//  _,  2,
	c.AddInterval(getBytes(2, 1), 1)
	//  _,  _,  3,  3, 3
	c.AddInterval(getBytes(3, 3), 2)
	//  _,  _,  _,  4, 4, 4
	c.AddInterval(getBytes(4, 3), 3)

	expectedData(t, c, 0, 1, 2, 3, 4, 4, 4)

}

func TestContinuousIntervals_RealCase1(t *testing.T) {

	c := &ContinuousIntervals{}

	// 25,
	c.AddInterval(getBytes(25, 1), 0)
	//  _,  _,  _,  _, 23, 23
	c.AddInterval(getBytes(23, 2), 4)
	//  _,  _,  _, 24, 24, 24, 24
	c.AddInterval(getBytes(24, 4), 3)

	//  _,  22, 22
	c.AddInterval(getBytes(22, 2), 1)

	expectedData(t, c, 0, 25, 22, 22, 24, 24, 24, 24)

}

func expectedData(t *testing.T, c *ContinuousIntervals, offset int, data ...byte) {
	start, stop := int64(offset), int64(offset+len(data))
	for _, list := range c.lists {
		nodeStart, nodeStop := max(start, list.Head.Offset), min(stop, list.Head.Offset+list.Size())
		if nodeStart < nodeStop {
			buf := make([]byte, nodeStop-nodeStart)
			list.ReadData(buf, nodeStart, nodeStop)
			if bytes.Compare(buf, data[nodeStart-start:nodeStop-start]) != 0 {
				t.Errorf("expected %v actual %v", data[nodeStart-start:nodeStop-start], buf)
			}
		}
	}
}

func getBytes(content byte, length int) []byte {
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = content
	}
	return data
}
