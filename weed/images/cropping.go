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
)

func Cropped(ext string, read io.ReadSeeker, x1, y1, x2, y2 int) (cropped io.ReadSeeker, err error) {
	srcImage, _, err := image.Decode(read)
	if err != nil {
		glog.Error(err)
		return read, err
	}

	bounds := srcImage.Bounds()
	if x2 > bounds.Dx() || y2 > bounds.Dy() {
		read.Seek(0, 0)
		return read, nil
	}

	rectangle := image.Rect(x1, y1, x2, y2)
	dstImage := imaging.Crop(srcImage, rectangle)
	var buf bytes.Buffer
	switch ext {
	case ".jpg", ".jpeg":
		if err = jpeg.Encode(&buf, dstImage, nil); err != nil {
			glog.Error(err)
		}
	case ".png":
		if err = png.Encode(&buf, dstImage); err != nil {
			glog.Error(err)
		}
	case ".gif":
		if err = gif.Encode(&buf, dstImage, nil); err != nil {
			glog.Error(err)
		}
	}
	return bytes.NewReader(buf.Bytes()), err
}
