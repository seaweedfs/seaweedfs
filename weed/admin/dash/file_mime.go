package dash

import (
	"mime"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func init() {
	// Register text files
	mime.AddExtensionType(".txt", "text/plain")
	mime.AddExtensionType(".log", "text/plain")
	mime.AddExtensionType(".cfg", "text/plain")
	mime.AddExtensionType(".conf", "text/plain")
	mime.AddExtensionType(".ini", "text/plain")
	mime.AddExtensionType(".properties", "text/plain")
	mime.AddExtensionType(".gitignore", "text/plain")
	mime.AddExtensionType(".gitattributes", "text/plain")
	mime.AddExtensionType(".env", "text/plain")

	// Register markup and styling
	mime.AddExtensionType(".md", "text/markdown")
	mime.AddExtensionType(".markdown", "text/markdown")
	mime.AddExtensionType(".html", "text/html")
	mime.AddExtensionType(".htm", "text/html")
	mime.AddExtensionType(".css", "text/css")
	mime.AddExtensionType(".xml", "application/xml")

	// Register code and scripting languages
	mime.AddExtensionType(".js", "application/javascript")
	mime.AddExtensionType(".mjs", "application/javascript")
	mime.AddExtensionType(".ts", "text/typescript")
	mime.AddExtensionType(".py", "text/x-python")
	mime.AddExtensionType(".go", "text/x-go")
	mime.AddExtensionType(".java", "text/x-java")
	mime.AddExtensionType(".c", "text/x-c")
	mime.AddExtensionType(".cpp", "text/x-c++")
	mime.AddExtensionType(".cc", "text/x-c++")
	mime.AddExtensionType(".cxx", "text/x-c++")
	mime.AddExtensionType(".c++", "text/x-c++")
	mime.AddExtensionType(".h", "text/x-c-header")
	mime.AddExtensionType(".hpp", "text/x-c-header")
	mime.AddExtensionType(".php", "text/x-php")
	mime.AddExtensionType(".rb", "text/x-ruby")
	mime.AddExtensionType(".pl", "text/x-perl")
	mime.AddExtensionType(".rs", "text/x-rust")
	mime.AddExtensionType(".swift", "text/x-swift")
	mime.AddExtensionType(".kt", "text/x-kotlin")
	mime.AddExtensionType(".scala", "text/x-scala")
	mime.AddExtensionType(".sh", "text/x-shellscript")
	mime.AddExtensionType(".bash", "text/x-shellscript")
	mime.AddExtensionType(".zsh", "text/x-shellscript")
	mime.AddExtensionType(".fish", "text/x-shellscript")
	mime.AddExtensionType(".dockerfile", "text/x-dockerfile")

	// Register data formats
	mime.AddExtensionType(".json", "application/json")
	mime.AddExtensionType(".yaml", "text/yaml")
	mime.AddExtensionType(".yml", "text/yaml")
	mime.AddExtensionType(".csv", "text/csv")
	mime.AddExtensionType(".sql", "text/sql")

	// Register image types
	mime.AddExtensionType(".jpg", "image/jpeg")
	mime.AddExtensionType(".jpeg", "image/jpeg")
	mime.AddExtensionType(".png", "image/png")
	mime.AddExtensionType(".gif", "image/gif")
	mime.AddExtensionType(".bmp", "image/bmp")
	mime.AddExtensionType(".webp", "image/webp")
	mime.AddExtensionType(".svg", "image/svg+xml")
	mime.AddExtensionType(".ico", "image/x-icon")

	// Register document types
	mime.AddExtensionType(".pdf", "application/pdf")
	mime.AddExtensionType(".doc", "application/msword")
	mime.AddExtensionType(".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
	mime.AddExtensionType(".xls", "application/vnd.ms-excel")
	mime.AddExtensionType(".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	mime.AddExtensionType(".ppt", "application/vnd.ms-powerpoint")
	mime.AddExtensionType(".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation")

	// Register archive types
	mime.AddExtensionType(".zip", "application/zip")
	mime.AddExtensionType(".tar", "application/x-tar")
	mime.AddExtensionType(".gz", "application/gzip")
	mime.AddExtensionType(".bz2", "application/x-bzip2")
	mime.AddExtensionType(".7z", "application/x-7z-compressed")
	mime.AddExtensionType(".rar", "application/x-rar-compressed")

	// Register video types
	mime.AddExtensionType(".mp4", "video/mp4")
	mime.AddExtensionType(".avi", "video/x-msvideo")
	mime.AddExtensionType(".mov", "video/quicktime")
	mime.AddExtensionType(".wmv", "video/x-ms-wmv")
	mime.AddExtensionType(".flv", "video/x-flv")
	mime.AddExtensionType(".webm", "video/webm")

	// Register audio types
	mime.AddExtensionType(".mp3", "audio/mpeg")
	mime.AddExtensionType(".wav", "audio/wav")
	mime.AddExtensionType(".flac", "audio/flac")
	mime.AddExtensionType(".aac", "audio/aac")
	mime.AddExtensionType(".ogg", "audio/ogg")
}

func ResolveEntryMime(entry *filer_pb.Entry) string {
	if entry == nil {
		return "application/octet-stream"
	}
	if entry.IsDirectory {
		return "inode/directory"
	}
	if entry.Attributes != nil {
		normalized := normalizeMimeType(entry.Attributes.Mime)
		if normalized != "" {
			return normalized
		}
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
	ext := path.Ext(filename)
	if mimeType := mime.TypeByExtension(ext); mimeType != "" {
		// mime.TypeByExtension can include parameters (e.g., charset), so we parse just the media type.
		if mediaType, _, err := mime.ParseMediaType(mimeType); err == nil {
			return mediaType
		}
	}
	return "application/octet-stream"
}
