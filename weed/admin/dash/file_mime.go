package dash

import (
	"mime"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func ResolveEntryMime(entry *filer_pb.Entry) string {
	if entry == nil {
		return "application/octet-stream"
	}
	if entry.IsDirectory {
		return "inode/directory"
	}
	if entry.Attributes != nil && entry.Attributes.Mime != "" {
		return normalizeMimeType(entry.Attributes.Mime)
	}
	return inferMimeTypeFromName(entry.Name)
}

func normalizeMimeType(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	if mediaType, _, err := mime.ParseMediaType(value); err == nil && mediaType != "" {
		return strings.ToLower(mediaType)
	}

	if idx := strings.Index(value, ";"); idx >= 0 {
		value = value[:idx]
	}

	return strings.ToLower(strings.TrimSpace(value))
}

func inferMimeTypeFromName(filename string) string {
	ext := strings.ToLower(path.Ext(filename))

	switch ext {
	case ".txt", ".log", ".cfg", ".conf", ".ini", ".properties":
		return "text/plain"
	case ".md", ".markdown":
		return "text/markdown"
	case ".html", ".htm":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js", ".mjs":
		return "application/javascript"
	case ".ts":
		return "text/typescript"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".yaml", ".yml":
		return "text/yaml"
	case ".csv":
		return "text/csv"
	case ".sql":
		return "text/sql"
	case ".sh", ".bash", ".zsh", ".fish":
		return "text/x-shellscript"
	case ".py":
		return "text/x-python"
	case ".go":
		return "text/x-go"
	case ".java":
		return "text/x-java"
	case ".c":
		return "text/x-c"
	case ".cpp", ".cc", ".cxx", ".c++":
		return "text/x-c++"
	case ".h", ".hpp":
		return "text/x-c-header"
	case ".php":
		return "text/x-php"
	case ".rb":
		return "text/x-ruby"
	case ".pl":
		return "text/x-perl"
	case ".rs":
		return "text/x-rust"
	case ".swift":
		return "text/x-swift"
	case ".kt":
		return "text/x-kotlin"
	case ".scala":
		return "text/x-scala"
	case ".dockerfile":
		return "text/x-dockerfile"
	case ".gitignore", ".gitattributes", ".env":
		return "text/plain"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".bmp":
		return "image/bmp"
	case ".webp":
		return "image/webp"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".pdf":
		return "application/pdf"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case ".zip":
		return "application/zip"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".bz2":
		return "application/x-bzip2"
	case ".7z":
		return "application/x-7z-compressed"
	case ".rar":
		return "application/x-rar-compressed"
	case ".mp4":
		return "video/mp4"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	case ".wmv":
		return "video/x-ms-wmv"
	case ".flv":
		return "video/x-flv"
	case ".webm":
		return "video/webm"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".flac":
		return "audio/flac"
	case ".aac":
		return "audio/aac"
	case ".ogg":
		return "audio/ogg"
	default:
		return "application/octet-stream"
	}
}
