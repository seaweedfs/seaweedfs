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

// parseOptions 只接受明确支持的 OSS 参数，拒绝重复项及未知操作。
// quality,Q 是绝对质量；相对质量 q 无法与 imgproxy 等价，不能静默转换。
func parseOptions(value string, maxDimension int) (options, error) {
	o := options{quality: 85, format: "webp"}
	parts := strings.Split(value, "/")
	if len(value) > 256 || len(parts) < 2 || parts[0] != "image" {
		return o, fmt.Errorf("需要 image/处理操作")
	}
	seen := make(map[string]bool)
	for _, part := range parts[1:] {
		fields := strings.Split(part, ",")
		if seen[fields[0]] {
			return o, fmt.Errorf("重复的处理操作")
		}
		seen[fields[0]] = true
		switch fields[0] {
		case "resize":
			if len(fields) < 2 {
				return o, fmt.Errorf("缺少缩放尺寸")
			}
			keys := make(map[string]bool)
			for _, f := range fields[1:] {
				kv := strings.SplitN(f, "_", 2)
				if len(kv) != 2 || keys[kv[0]] {
					return o, fmt.Errorf("无效或重复的缩放参数")
				}
				keys[kv[0]] = true
				switch kv[0] {
				case "w", "h":
					n, err := strconv.Atoi(kv[1])
					if err != nil || n < 1 || n > maxDimension {
						return o, fmt.Errorf("尺寸超出允许范围")
					}
					if kv[0] == "w" {
						o.width = n
					} else {
						o.height = n
					}
				case "m":
					if kv[1] != "lfit" {
						return o, fmt.Errorf("只支持等比适应 lfit")
					}
				case "limit":
					if kv[1] != "1" {
						return o, fmt.Errorf("不允许放大图片")
					}
				default:
					return o, fmt.Errorf("不支持的缩放参数")
				}
			}
			if o.width == 0 && o.height == 0 {
				return o, fmt.Errorf("缺少缩放尺寸")
			}
		case "quality":
			if len(fields) != 2 || !strings.HasPrefix(fields[1], "Q_") {
				return o, fmt.Errorf("只支持绝对质量 Q")
			}
			q, err := strconv.Atoi(strings.TrimPrefix(fields[1], "Q_"))
			if err != nil || q < 1 || q > 100 {
				return o, fmt.Errorf("质量必须在 1 至 100 之间")
			}
			o.quality = q
		case "format":
			if len(fields) != 2 {
				return o, fmt.Errorf("无效的格式参数")
			}
			o.format = fields[1]
			if o.format == "jpeg" {
				o.format = "jpg"
			}
			if o.format != "jpg" && o.format != "png" && o.format != "webp" {
				return o, fmt.Errorf("只支持 JPEG、PNG 或 WebP")
			}
		default:
			return o, fmt.Errorf("不支持的图片处理操作")
		}
	}
	return o, nil
}

// path 将参数规范化为固定的 imgproxy 操作，等价 URL 共用缓存。
func (o options) path() string {
	return fmt.Sprintf("/rs:fit:%d:%d:0:0/q:%d/f:%s", o.width, o.height, o.quality, o.format)
}
