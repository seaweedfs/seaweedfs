//go:build windows

package localsink

import "strings"

// sanitizeFsKey strips characters illegal in NTFS paths (notably ':') so the
// local filesystem sink can write the key. A leading drive-letter colon (e.g.
// "C:\...") is preserved so absolute target paths stay valid. Only the local
// sink needs this; other sinks accept the raw key.
func sanitizeFsKey(key string) string {
	prefix, rest := "", key
	if len(key) >= 2 && key[1] == ':' && isDriveLetter(key[0]) {
		prefix, rest = key[:2], key[2:]
	}
	return prefix + strings.ReplaceAll(rest, ":", "")
}

func isDriveLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}
