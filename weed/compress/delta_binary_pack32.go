package compress

import (
	"github.com/dataence/encoding/cursor"
	"github.com/dataence/encoding/delta/bp32"
)

// Compress compresses in[]int32 to out[]int32
func Compress32(in []int32) (out []int32, err error) {
	out = make([]int32, len(in)*2)
	inpos := cursor.New()
	outpos := cursor.New()

	if err = bp32.New().Compress(in, inpos, len(in), out, outpos); err != nil {
		return nil, err
	}

	return out[:outpos.Get()], nil
}

// Uncompress uncompresses in[]int32 to out[]int32
func Uncompress32(in []int32, buffer []int32) (out []int32, err error) {
	out = buffer
	inpos := cursor.New()
	outpos := cursor.New()

	if err = bp32.New().Uncompress(in, inpos, len(in), out, outpos); err != nil {
		return nil, err
	}

	return out[:outpos.Get()], nil
}
