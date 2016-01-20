package images

import (
	"io/ioutil"
	"testing"
)

func TestResizing(t *testing.T) {
	fname := "sample1.png"

	data, _ := ioutil.ReadFile(fname)

	new_data, _, _ := Resized(".jpg", data, 500, 0)

	ioutil.WriteFile("resized.jpg", new_data, 0644)
}

func BenchmarkResizing(b *testing.B) {
	fName := "test1.png"
	data, _ := ioutil.ReadFile(fName)
	ext := ".jpg"
	w := 0
	h := 355
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Resized(ext, data, w, h)
		b.SetBytes(int64(len(data)))
	}
}
