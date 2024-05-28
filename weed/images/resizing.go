package images

import (
	"bytes"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"

	"github.com/cognusion/imaging"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	_ "golang.org/x/image/webp"
)

func Resized(ext string, read io.ReadSeeker, width, height int, mode string) (resized io.ReadSeeker, w int, h int) {
	if width == 0 && height == 0 {
		return read, 0, 0
	}
	srcImage, _, err := image.Decode(read)
	if err == nil {
		bounds := srcImage.Bounds()
		var dstImage *image.NRGBA
		if bounds.Dx() > width && width != 0 || bounds.Dy() > height && height != 0 {
			switch mode {
			case "fit":
				dstImage = imaging.Fit(srcImage, width, height, imaging.Lanczos)
			case "fill":
				dstImage = imaging.Fill(srcImage, width, height, imaging.Center, imaging.Lanczos)
			default:
				if width == height && bounds.Dx() != bounds.Dy() {
					dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
					w, h = width, height
				} else {
					dstImage = imaging.Resize(srcImage, width, height, imaging.Lanczos)
				}
			}
		} else {
			read.Seek(0, 0)
			return read, bounds.Dx(), bounds.Dy()
		}
		var buf bytes.Buffer
		switch ext {
		case ".png":
			png.Encode(&buf, dstImage)
		case ".jpg", ".jpeg":
			jpeg.Encode(&buf, dstImage, nil)
		case ".gif":
			gif.Encode(&buf, dstImage, nil)
		case ".webp":
			// Webp does not have golang encoder.
			png.Encode(&buf, dstImage)
		}
		return bytes.NewReader(buf.Bytes()), dstImage.Bounds().Dx(), dstImage.Bounds().Dy()
	} else {
		glog.Error(err)
	}
	return read, 0, 0
}
