package images

import (
	"path/filepath"
)

/*
* Preprocess image files on client side.
* 1. possibly adjust the orientation
* 2. resize the image to a width or height limit
* 3. remove the exif data
* Call this function on any file uploaded to weedfs
* 
*/
func MaybePreprocessImage(filename string, data []byte, width, height int) (resized []byte) {
	ext := filepath.Ext(filename)
	switch ext {
	case ".png", ".gif":
		return Resized(ext, data, width, height)
	case ".jpg", ".jpeg":
	  data = FixJpgOrientation(data)
		return Resized(ext, data, width, height)
	}
	return data
}
