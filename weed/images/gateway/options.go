package gateway

import (
	"fmt"
	"strconv"
	"strings"
)

type options struct {
	width, height, quality int
	format                 string
}

// parseOptions accepts the supported OSS subset and rejects duplicates and unknown operations.
// quality,Q is absolute quality; relative quality q has no equivalent in imgproxy.
func parseOptions(value string, maxDimension int) (options, error) {
	o := options{quality: 85, format: "webp"}
	parts := strings.Split(value, "/")
	if len(value) > 256 || len(parts) < 2 || parts[0] != "image" {
		return o, fmt.Errorf("expected image/processing-operation")
	}
	seen := make(map[string]bool)
	for _, part := range parts[1:] {
		fields := strings.Split(part, ",")
		if seen[fields[0]] {
			return o, fmt.Errorf("repeated processing operation")
		}
		seen[fields[0]] = true
		switch fields[0] {
		case "resize":
			if len(fields) < 2 {
				return o, fmt.Errorf("missing resize dimensions")
			}
			keys := make(map[string]bool)
			for _, f := range fields[1:] {
				kv := strings.SplitN(f, "_", 2)
				if len(kv) != 2 || keys[kv[0]] {
					return o, fmt.Errorf("invalid or repeated resize parameter")
				}
				keys[kv[0]] = true
				switch kv[0] {
				case "w", "h":
					n, err := strconv.Atoi(kv[1])
					if err != nil || n < 1 || n > maxDimension {
						return o, fmt.Errorf("dimension exceeds allowed range")
					}
					if kv[0] == "w" {
						o.width = n
					} else {
						o.height = n
					}
				case "m":
					if kv[1] != "lfit" {
						return o, fmt.Errorf("only aspect-preserving lfit is supported")
					}
				case "limit":
					if kv[1] != "1" {
						return o, fmt.Errorf("image enlargement is not allowed")
					}
				default:
					return o, fmt.Errorf("unsupported resize parameter")
				}
			}
			if o.width == 0 && o.height == 0 {
				return o, fmt.Errorf("missing resize dimensions")
			}
		case "quality":
			if len(fields) != 2 || !strings.HasPrefix(fields[1], "Q_") {
				return o, fmt.Errorf("only absolute quality Q is supported")
			}
			q, err := strconv.Atoi(strings.TrimPrefix(fields[1], "Q_"))
			if err != nil || q < 1 || q > 100 {
				return o, fmt.Errorf("quality must be between 1 and 100")
			}
			o.quality = q
		case "format":
			if len(fields) != 2 {
				return o, fmt.Errorf("invalid format parameter")
			}
			o.format = fields[1]
			if o.format == "jpeg" {
				o.format = "jpg"
			}
			if o.format != "jpg" && o.format != "png" && o.format != "webp" {
				return o, fmt.Errorf("only JPEG, PNG, and WebP are supported")
			}
		default:
			return o, fmt.Errorf("unsupported image processing operation")
		}
	}
	return o, nil
}

// path canonicalizes imgproxy operations so equivalent URLs share cached results.
func (o options) path() string {
	return fmt.Sprintf("/rs:fit:%d:%d:0:0/q:%d/f:%s", o.width, o.height, o.quality, o.format)
}
