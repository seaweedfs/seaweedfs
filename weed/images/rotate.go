package images

import (
	"bytes"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/disintegration/gift"
)

func Rotate(ext string, data []byte, rotate int) (resized []byte) {
	if rotate < 1 {
		return data
	}
	srcImage, _, err := image.Decode(bytes.NewReader(data))
	if err == nil {
		var dstImage *image.NRGBA
		g := gift.New(
			gift.Rotate(float32(rotate), color.Opaque, gift.CubicInterpolation),
		)
		dstImage = image.NewNRGBA(g.Bounds(srcImage.Bounds()))
		g.Draw(dstImage, srcImage)

		var buf bytes.Buffer
		switch ext {
		case ".png":
			png.Encode(&buf, dstImage)
		case ".jpg", ".jpeg":
			jpeg.Encode(&buf, dstImage, nil)
		case ".gif":
			gif.Encode(&buf, dstImage, nil)
		}
		return buf.Bytes()
	} else {
		glog.Error(err)
	}
	return data
}
