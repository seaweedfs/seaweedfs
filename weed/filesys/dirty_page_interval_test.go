package filesys

import (
	"bytes"
	"testing"
)

func TestContinuousIntervals_AddInterval(t *testing.T) {

	c := &ContinuousIntervals{}

	// 25, 25, 25
	c.AddInterval(getBytes(25, 3), 0)
	//  _,  _, 23, 23, 23, 23
	c.AddInterval(getBytes(23, 4), 2)

	expectedData(t, c, 0, 25, 25, 23, 23, 23, 23)
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
