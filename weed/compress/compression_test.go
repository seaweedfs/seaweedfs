package compress

import (
	"math/rand"
	"testing"
)

func TestSortedData(t *testing.T) {
	data := make([]int32, 102400)
	for i := 1; i < len(data); i++ {
		data[i] = data[i-1] + rand.Int31n(15)
	}
	testCompressAndUncompress(t, data, "Sorted data")
}

func TestUnsortedData(t *testing.T) {
	data := make([]int32, 102400)
	for i := 0; i < len(data); i++ {
		data[i] = rand.Int31n(255)
	}
	testCompressAndUncompress(t, data, "Unsorted data")
}

func testCompressAndUncompress(t *testing.T, data []int32, desc string) {

	compressed_data, err := Compress32(data)
	if err != nil {
		t.Fatal("Compress error", err.Error())
	}
	uncompressed_data, err := Uncompress32(compressed_data, make([]int32, len(data)*2))
	if err != nil {
		t.Fatal("Compress error", err.Error())
	}
	if len(uncompressed_data) != len(data) {
		t.Fatal("Len differs", len(data), len(uncompressed_data))
	}
	for i := 0; i < len(data); i++ {
		if data[i] != uncompressed_data[i] {
			t.Fatal("Data differs:", i, data[i], uncompressed_data[i])
		}
	}

	println(desc, " Data length:", len(data), " => Compressed length:", len(compressed_data))

}
