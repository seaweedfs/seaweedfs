package s3api

import (
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"
)

// encodePathOld is the original implementation, kept only in tests as a
// reference to prove the optimized encodePath produces identical output.
func encodePathOld(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' {
			encodedPathname = encodedPathname + string(s)
		} else {
			switch s {
			case '-', '_', '.', '~', '/':
				encodedPathname = encodedPathname + string(s)
			default:
				runeLen := utf8.RuneLen(s)
				if runeLen < 0 {
					return pathName
				}
				u := make([]byte, runeLen)
				utf8.EncodeRune(u, s)
				for _, r := range u {
					h := hex.EncodeToString([]byte{r})
					encodedPathname = encodedPathname + "%" + strings.ToUpper(h)
				}
			}
		}
	}
	return encodedPathname
}

// TestEncodePath checks encodePath against explicit expected outputs.
func TestEncodePath(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"/bucket/file.txt", "/bucket/file.txt"},
		{"/a-b_c.d~e/f", "/a-b_c.d~e/f"},
		{"", ""},
		{"/", "/"},
		{"/中", "/%E4%B8%AD"},
		{"/a b", "/a%20b"},
		{"/a&b=c", "/a%26b%3Dc"},
	}
	for _, c := range cases {
		if got := encodePath(c.in); got != c.want {
			t.Errorf("encodePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestEncodePathEqual verifies the optimized encodePath produces byte-for-byte
// identical output to the original implementation across many inputs.
func TestEncodePathEqual(t *testing.T) {
	cases := []string{
		"/bucket/file.txt",
		"/my-bucket/data/2026/07/file.parquet",
		"/我的存储桶/数据仓库/文件.数据",
		"/bucket/项目数据/report-分析.parquet",
		"/path with spaces/and&symbols=test/文件.txt",
		"/emoji/😀/file.txt", // 4-byte UTF-8 rune
		"/",
		"/a",
		"",
		"/纯中文路径没有斜杠结尾/文件名",
		"/mixed混合/path路径/2026年/data.csv",
	}
	for _, p := range cases {
		if got, want := encodePath(p), encodePathOld(p); got != want {
			t.Errorf("mismatch for %q:\n  optimized=%q\n  original =%q", p, got, want)
		}
	}
}

// benchPaths covers different path types for benchmarking.
var benchPaths = []string{
	"/bucket/file.txt", // short ASCII
	"/my-bucket/data/2026/07/07/service/module/submodule/very/long/path/to/object/file-name-1234567890.parquet", // long ASCII
	"/我的存储桶/数据仓库/2026年07月/业务数据/用户行为分析/长长的中文文件路径/这是一个很长的中文对象名称文件.数据",                                             // long non-ASCII
	"/bucket/项目数据/2026/报表/月度统计/user-behavior-分析报告-统计数据-长文件名称-1234567890.parquet",                                // mixed
}

func benchLabel(p string) string {
	if p == "" {
		return "<empty>"
	}
	if len(p) > 20 {
		return p[:20] + "..."
	}
	return p
}

// BenchmarkEncodePath benchmarks the original vs optimized implementation.
func BenchmarkEncodePath(b *testing.B) {
	for _, p := range benchPaths {
		label := benchLabel(p)
		b.Run("Old/"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = encodePathOld(p)
			}
		})
		b.Run("New/"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = encodePath(p)
			}
		})
	}
}
