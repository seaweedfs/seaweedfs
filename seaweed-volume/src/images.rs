//! JPEG EXIF orientation auto-fix, matching Go's `FixJpgOrientation`.
//!
//! Reads the EXIF orientation tag from JPEG data and rotates/flips the image
//! to normalize it to orientation 1 (top-left). If EXIF parsing fails or
//! orientation is already normal, returns the original data unchanged.

use std::io::Cursor;

use image::{DynamicImage, GenericImageView, ImageFormat, RgbaImage};

/// EXIF orientation tag values.
/// See: <http://sylvana.net/jpegcrop/exif_orientation.html>
const TOP_LEFT_SIDE: u32 = 1;
const TOP_RIGHT_SIDE: u32 = 2;
const BOTTOM_RIGHT_SIDE: u32 = 3;
const BOTTOM_LEFT_SIDE: u32 = 4;
const LEFT_SIDE_TOP: u32 = 5;
const RIGHT_SIDE_TOP: u32 = 6;
const RIGHT_SIDE_BOTTOM: u32 = 7;
const LEFT_SIDE_BOTTOM: u32 = 8;

/// Fix JPEG orientation based on EXIF data.
///
/// Reads the EXIF orientation tag and applies the appropriate rotation/flip
/// to normalize the image to orientation 1 (top-left). Re-encodes as JPEG.
///
/// Returns the original data unchanged if:
/// - EXIF data cannot be parsed
/// - No orientation tag is present
/// - Orientation is already 1 (normal)
/// - Image decoding or re-encoding fails
pub fn fix_jpg_orientation(data: &[u8]) -> Vec<u8> {
    // Parse EXIF data
    let orientation = match read_exif_orientation(data) {
        Some(o) => o,
        None => return data.to_vec(),
    };

    // Orientation 1 means normal — no transformation needed
    if orientation == TOP_LEFT_SIDE {
        return data.to_vec();
    }

    // Determine rotation angle and flip mode
    let (angle, flip_horizontal) = match orientation {
        TOP_RIGHT_SIDE => (0, true),
        BOTTOM_RIGHT_SIDE => (180, false),
        BOTTOM_LEFT_SIDE => (180, true),
        LEFT_SIDE_TOP => (-90, true),
        RIGHT_SIDE_TOP => (-90, false),
        RIGHT_SIDE_BOTTOM => (90, true),
        LEFT_SIDE_BOTTOM => (90, false),
        _ => return data.to_vec(),
    };

    // Decode the image
    let src_image = match image::load_from_memory_with_format(data, ImageFormat::Jpeg) {
        Ok(img) => img,
        Err(_) => return data.to_vec(),
    };

    // Apply rotation then flip (matching Go's flip(rotate(img, angle), flipMode))
    let transformed = flip_horizontal_if(rotate(src_image, angle), flip_horizontal);

    // Re-encode as JPEG
    let mut buf = Cursor::new(Vec::new());
    match transformed.write_to(&mut buf, ImageFormat::Jpeg) {
        Ok(_) => buf.into_inner(),
        Err(_) => data.to_vec(),
    }
}

/// Read the EXIF orientation tag from JPEG data.
/// Returns None if EXIF cannot be parsed or orientation tag is not present.
fn read_exif_orientation(data: &[u8]) -> Option<u32> {
    let exif_reader = exif::Reader::new();
    let mut cursor = Cursor::new(data);
    let exif_data = exif_reader.read_from_container(&mut cursor).ok()?;

    let orientation_field = exif_data.get_field(exif::Tag::Orientation, exif::In::PRIMARY)?;
    match orientation_field.value {
        exif::Value::Short(ref v) if !v.is_empty() => Some(v[0] as u32),
        _ => orientation_field.value.get_uint(0),
    }
}

/// Rotate an image by the given angle (counter-clockwise, in degrees).
/// Matches Go's rotate function.
fn rotate(img: DynamicImage, angle: i32) -> DynamicImage {
    let (width, height) = img.dimensions();

    match angle {
        90 => {
            // 90 degrees counter-clockwise
            let new_w = height;
            let new_h = width;
            let mut out = RgbaImage::new(new_w, new_h);
            for y in 0..new_h {
                for x in 0..new_w {
                    out.put_pixel(x, y, img.get_pixel(new_h - 1 - y, x));
                }
            }
            DynamicImage::ImageRgba8(out)
        }
        -90 => {
            // 90 degrees clockwise (or 270 counter-clockwise)
            let new_w = height;
            let new_h = width;
            let mut out = RgbaImage::new(new_w, new_h);
            for y in 0..new_h {
                for x in 0..new_w {
                    out.put_pixel(x, y, img.get_pixel(y, new_w - 1 - x));
                }
            }
            DynamicImage::ImageRgba8(out)
        }
        180 | -180 => {
            let mut out = RgbaImage::new(width, height);
            for y in 0..height {
                for x in 0..width {
                    out.put_pixel(x, y, img.get_pixel(width - 1 - x, height - 1 - y));
                }
            }
            DynamicImage::ImageRgba8(out)
        }
        _ => img,
    }
}

/// Flip the image horizontally if requested.
/// In Go, flipMode 2 == FlipHorizontal. We simplify since only horizontal flip is used.
fn flip_horizontal_if(img: DynamicImage, do_flip: bool) -> DynamicImage {
    if !do_flip {
        return img;
    }
    let (width, height) = img.dimensions();
    let mut out = RgbaImage::new(width, height);
    for y in 0..height {
        for x in 0..width {
            out.put_pixel(x, y, img.get_pixel(width - 1 - x, y));
        }
    }
    DynamicImage::ImageRgba8(out)
}

/// Returns true if the given MIME type or file path extension indicates a JPEG file.
pub fn is_jpeg(mime_type: &str, path: &str) -> bool {
    if mime_type == "image/jpeg" {
        return true;
    }
    let lower = path.to_lowercase();
    lower.ends_with(".jpg") || lower.ends_with(".jpeg")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_jpeg_data_returned_unchanged() {
        let data = b"not a jpeg file at all";
        let result = fix_jpg_orientation(data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_jpeg_without_exif_returned_unchanged() {
        // Create a minimal JPEG without EXIF data
        let img = DynamicImage::ImageRgba8(RgbaImage::new(2, 2));
        let mut buf = Cursor::new(Vec::new());
        img.write_to(&mut buf, ImageFormat::Jpeg).unwrap();
        let jpeg_data = buf.into_inner();

        let result = fix_jpg_orientation(&jpeg_data);
        // Should return data unchanged (no EXIF orientation tag)
        // Just verify it's still valid JPEG
        assert!(!result.is_empty());
        assert_eq!(&result[0..2], &[0xFF, 0xD8]); // JPEG magic bytes
    }

    #[test]
    fn test_is_jpeg() {
        assert!(is_jpeg("image/jpeg", ""));
        assert!(is_jpeg("", "/3,abc.jpg"));
        assert!(is_jpeg("", "/3,abc.JPEG"));
        assert!(is_jpeg("application/octet-stream", "/3,abc.JPG"));
        assert!(!is_jpeg("image/png", "/3,abc.png"));
        assert!(!is_jpeg("", "/3,abc.png"));
    }

    #[test]
    fn test_rotate_180() {
        // Create a 2x2 image with distinct pixel colors
        let mut img = RgbaImage::new(2, 2);
        img.put_pixel(0, 0, image::Rgba([255, 0, 0, 255]));   // red top-left
        img.put_pixel(1, 0, image::Rgba([0, 255, 0, 255]));   // green top-right
        img.put_pixel(0, 1, image::Rgba([0, 0, 255, 255]));   // blue bottom-left
        img.put_pixel(1, 1, image::Rgba([255, 255, 0, 255])); // yellow bottom-right
        let dynamic = DynamicImage::ImageRgba8(img);

        let rotated = rotate(dynamic, 180);
        let (w, h) = rotated.dimensions();
        assert_eq!((w, h), (2, 2));
        // After 180 rotation: top-left should be yellow, top-right should be blue
        assert_eq!(rotated.get_pixel(0, 0), image::Rgba([255, 255, 0, 255]));
        assert_eq!(rotated.get_pixel(1, 0), image::Rgba([0, 0, 255, 255]));
        assert_eq!(rotated.get_pixel(0, 1), image::Rgba([0, 255, 0, 255]));
        assert_eq!(rotated.get_pixel(1, 1), image::Rgba([255, 0, 0, 255]));
    }

    #[test]
    fn test_rotate_90_ccw() {
        // Create 3x2 image (width=3, height=2)
        let mut img = RgbaImage::new(3, 2);
        img.put_pixel(0, 0, image::Rgba([1, 0, 0, 255]));
        img.put_pixel(1, 0, image::Rgba([2, 0, 0, 255]));
        img.put_pixel(2, 0, image::Rgba([3, 0, 0, 255]));
        img.put_pixel(0, 1, image::Rgba([4, 0, 0, 255]));
        img.put_pixel(1, 1, image::Rgba([5, 0, 0, 255]));
        img.put_pixel(2, 1, image::Rgba([6, 0, 0, 255]));
        let dynamic = DynamicImage::ImageRgba8(img);

        let rotated = rotate(dynamic, 90);
        let (w, h) = rotated.dimensions();
        // 90 CCW: width=3,height=2 -> new_w=2, new_h=3
        assert_eq!((w, h), (2, 3));
        // Top-right (2,0) should move to top-left (0,0) in CCW 90
        assert_eq!(rotated.get_pixel(0, 0)[0], 3);
        assert_eq!(rotated.get_pixel(1, 0)[0], 6);
    }

    #[test]
    fn test_rotate_neg90_cw() {
        // Create 3x2 image
        let mut img = RgbaImage::new(3, 2);
        img.put_pixel(0, 0, image::Rgba([1, 0, 0, 255]));
        img.put_pixel(1, 0, image::Rgba([2, 0, 0, 255]));
        img.put_pixel(2, 0, image::Rgba([3, 0, 0, 255]));
        img.put_pixel(0, 1, image::Rgba([4, 0, 0, 255]));
        img.put_pixel(1, 1, image::Rgba([5, 0, 0, 255]));
        img.put_pixel(2, 1, image::Rgba([6, 0, 0, 255]));
        let dynamic = DynamicImage::ImageRgba8(img);

        let rotated = rotate(dynamic, -90);
        let (w, h) = rotated.dimensions();
        assert_eq!((w, h), (2, 3));
        // -90 (CW 90): top-left (0,0) should go to top-right
        assert_eq!(rotated.get_pixel(0, 0)[0], 4);
        assert_eq!(rotated.get_pixel(1, 0)[0], 1);
    }

    #[test]
    fn test_flip_horizontal() {
        let mut img = RgbaImage::new(2, 1);
        img.put_pixel(0, 0, image::Rgba([10, 0, 0, 255]));
        img.put_pixel(1, 0, image::Rgba([20, 0, 0, 255]));
        let dynamic = DynamicImage::ImageRgba8(img);

        let flipped = flip_horizontal_if(dynamic, true);
        assert_eq!(flipped.get_pixel(0, 0)[0], 20);
        assert_eq!(flipped.get_pixel(1, 0)[0], 10);
    }

    #[test]
    fn test_flip_horizontal_noop() {
        let mut img = RgbaImage::new(2, 1);
        img.put_pixel(0, 0, image::Rgba([10, 0, 0, 255]));
        img.put_pixel(1, 0, image::Rgba([20, 0, 0, 255]));
        let dynamic = DynamicImage::ImageRgba8(img);

        let not_flipped = flip_horizontal_if(dynamic, false);
        assert_eq!(not_flipped.get_pixel(0, 0)[0], 10);
        assert_eq!(not_flipped.get_pixel(1, 0)[0], 20);
    }
}
