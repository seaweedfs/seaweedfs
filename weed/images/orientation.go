package images

import (
	"bytes"
	"image"
	"image/draw"
	"image/jpeg"
	"log"

	"github.com/seaweedfs/goexif/exif"
)

// many code is copied from http://camlistore.org/pkg/images/images.go
func FixJpgOrientation(data []byte) (oriented []byte) {
	ex, err := exif.Decode(bytes.NewReader(data))
	if err != nil {
		return data
	}
	tag, err := ex.Get(exif.Orientation)
	if err != nil {
		return data
	}
	angle := 0
	flipMode := FlipDirection(0)
	orient, err := tag.Int(0)
	if err != nil {
		return data
	}
	switch orient {
	case topLeftSide:
		// do nothing
		return data
	case topRightSide:
		flipMode = 2
	case bottomRightSide:
		angle = 180
	case bottomLeftSide:
		angle = 180
		flipMode = 2
	case leftSideTop:
		angle = -90
		flipMode = 2
	case rightSideTop:
		angle = -90
	case rightSideBottom:
		angle = 90
		flipMode = 2
	case leftSideBottom:
		angle = 90
	}

	if srcImage, _, err := image.Decode(bytes.NewReader(data)); err == nil {
		dstImage := flip(rotate(srcImage, angle), flipMode)
		var buf bytes.Buffer
		jpeg.Encode(&buf, dstImage, nil)
		return buf.Bytes()
	}

	return data
}

// Exif Orientation Tag values
// http://sylvana.net/jpegcrop/exif_orientation.html
const (
	topLeftSide     = 1
	topRightSide    = 2
	bottomRightSide = 3
	bottomLeftSide  = 4
	leftSideTop     = 5
	rightSideTop    = 6
	rightSideBottom = 7
	leftSideBottom  = 8
)

// The FlipDirection type is used by the Flip option in DecodeOpts
// to indicate in which direction to flip an image.
type FlipDirection int

// FlipVertical and FlipHorizontal are two possible FlipDirections
// values to indicate in which direction an image will be flipped.
const (
	FlipVertical FlipDirection = 1 << iota
	FlipHorizontal
)

type DecodeOpts struct {
	// Rotate specifies how to rotate the image.
	// If nil, the image is rotated automatically based on EXIF metadata.
	// If an int, Rotate is the number of degrees to rotate
	// counter clockwise and must be one of 0, 90, -90, 180, or
	// -180.
	Rotate interface{}

	// Flip specifies how to flip the image.
	// If nil, the image is flipped automatically based on EXIF metadata.
	// Otherwise, Flip is a FlipDirection bitfield indicating how to flip.
	Flip interface{}
}

func rotate(im image.Image, angle int) image.Image {
	var rotated *image.NRGBA
	// trigonometric (i.e counter clock-wise)
	switch angle {
	case 90:
		newH, newW := im.Bounds().Dx(), im.Bounds().Dy()
		rotated = image.NewNRGBA(image.Rect(0, 0, newW, newH))
		for y := 0; y < newH; y++ {
			for x := 0; x < newW; x++ {
				rotated.Set(x, y, im.At(newH-1-y, x))
			}
		}
	case -90:
		newH, newW := im.Bounds().Dx(), im.Bounds().Dy()
		rotated = image.NewNRGBA(image.Rect(0, 0, newW, newH))
		for y := 0; y < newH; y++ {
			for x := 0; x < newW; x++ {
				rotated.Set(x, y, im.At(y, newW-1-x))
			}
		}
	case 180, -180:
		newW, newH := im.Bounds().Dx(), im.Bounds().Dy()
		rotated = image.NewNRGBA(image.Rect(0, 0, newW, newH))
		for y := 0; y < newH; y++ {
			for x := 0; x < newW; x++ {
				rotated.Set(x, y, im.At(newW-1-x, newH-1-y))
			}
		}
	default:
		return im
	}
	return rotated
}

// flip returns a flipped version of the image im, according to
// the direction(s) in dir.
// It may flip the imput im in place and return it, or it may allocate a
// new NRGBA (if im is an *image.YCbCr).
func flip(im image.Image, dir FlipDirection) image.Image {
	if dir == 0 {
		return im
	}
	ycbcr := false
	var nrgba image.Image
	dx, dy := im.Bounds().Dx(), im.Bounds().Dy()
	di, ok := im.(draw.Image)
	if !ok {
		if _, ok := im.(*image.YCbCr); !ok {
			log.Printf("failed to flip image: input does not satisfy draw.Image")
			return im
		}
		// because YCbCr does not implement Set, we replace it with a new NRGBA
		ycbcr = true
		nrgba = image.NewNRGBA(image.Rect(0, 0, dx, dy))
		di, ok = nrgba.(draw.Image)
		if !ok {
			log.Print("failed to flip image: could not cast an NRGBA to a draw.Image")
			return im
		}
	}
	if dir&FlipHorizontal != 0 {
		for y := 0; y < dy; y++ {
			for x := 0; x < dx/2; x++ {
				old := im.At(x, y)
				di.Set(x, y, im.At(dx-1-x, y))
				di.Set(dx-1-x, y, old)
			}
		}
	}
	if dir&FlipVertical != 0 {
		for y := 0; y < dy/2; y++ {
			for x := 0; x < dx; x++ {
				old := im.At(x, y)
				di.Set(x, y, im.At(x, dy-1-y))
				di.Set(x, dy-1-y, old)
			}
		}
	}
	if ycbcr {
		return nrgba
	}
	return im
}
