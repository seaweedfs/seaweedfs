package images

import (
	"bytes"
	"github.com/disintegration/imaging"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
)

func FixJpgOrientation() {
}

func Resized(ext string, data []byte, width, height int) (resized []byte) {
	if width == 0 && height == 0 {
		return data
	}
	if srcImage, _, err := image.Decode(bytes.NewReader(data)); err == nil {
		bounds := srcImage.Bounds()
		var dstImage *image.NRGBA
		if width == height && bounds.Dx() != bounds.Dy() {
			dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
		} else {
			dstImage = imaging.Resize(srcImage, width, height, imaging.Lanczos)
		}
		var buf bytes.Buffer
		switch ext {
		case ".png":
			png.Encode(&buf, dstImage)
		case ".jpg":
			jpeg.Encode(&buf, dstImage, nil)
		case ".gif":
			gif.Encode(&buf, dstImage, nil)
		}
		return buf.Bytes()
	}
	return data
}
