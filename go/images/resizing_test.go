package images

import (
	"testing"
	"io/ioutil"
)



func BenchmarkResizing(b *testing.B){
	fName := "sample1.jpg"
	data, _ := ioutil.ReadFile(fName)
	ext := "jpg"
	w := 0
	h := 355
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Resized(ext, data, w, h)
		b.SetBytes(int64(len(data)))
	}
}
