package util

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestFullPath_Child(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		fp   FullPath
		args args
		want FullPath
	}{
		{"", FullPath("/"), args{"test.txt"}, FullPath("/test.txt")},
		{"", FullPath("/dir"), args{"test.txt"}, FullPath("/dir/test.txt")},
		{"", FullPath("/dir/"), args{"test.txt"}, FullPath("/dir/test.txt")},
		{"", FullPath("/"), args{"./test.txt"}, FullPath("/test.txt")},
		{"", FullPath("/"), args{"../test.txt"}, FullPath("/test.txt")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.fp.Child(tt.args.name), "Child(%v)", tt.args.name)
		})
	}
}

func TestFullPath_DirAndName(t *testing.T) {
	tests := []struct {
		name  string
		fp    FullPath
		want  string
		want1 string
	}{
		{"", FullPath("/"), "/", ""},
		{"", FullPath("/xxx"), "/", "xxx"},
		{"", FullPath("/xxx/yyy"), "/xxx", "yyy"},
		{"", FullPath("/xxx/yyy/"), "/xxx/yyy", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.fp.DirAndName()
			assert.Equalf(t, tt.want, got, "DirAndName()")
			assert.Equalf(t, tt.want1, got1, "DirAndName()")
		})
	}
}

func TestFullPath_IsDir(t *testing.T) {
	tests := []struct {
		name string
		fp   FullPath
		want bool
	}{
		{"", FullPath("/"), true},
		{"", FullPath("/xxx"), false},
		{"", FullPath("/xxx/"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.fp.IsDir(), "IsDir()")
		})
	}
}

func TestFullPath_Name(t *testing.T) {
	tests := []struct {
		name string
		fp   FullPath
		want string
	}{
		{"", FullPath("/"), ""},
		{"", FullPath("/xxx"), "xxx"},
		{"", FullPath("/xxx/"), ""},
		{"", FullPath("/xxx/yyy/."), "yyy"},
		{"", FullPath("/xxx/yyy/.."), "xxx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.fp.Name(), "Name()")
		})
	}
}

func TestFullPath_ToDir(t *testing.T) {
	tests := []struct {
		name string
		fp   FullPath
		want FullPath
	}{
		{"", FullPath("/"), FullPath("/")},
		{"", FullPath("/xxx"), FullPath("/xxx/")},
		{"", FullPath("/xxx/"), FullPath("/xxx/")},
		{"", FullPath("/xxx/."), FullPath("/xxx/")},
		{"", FullPath("/xxx/.."), FullPath("/")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.fp.ToDir(), "ToDir()")
		})
	}
}

func TestFullPath_ToFile(t *testing.T) {
	tests := []struct {
		name string
		fp   FullPath
		want FullPath
	}{
		{"", FullPath("/"), FullPath(".")},
		{"", FullPath("/xxx"), FullPath("/xxx")},
		{"", FullPath("/xxx/"), FullPath("/xxx")},
		{"", FullPath("/xxx/."), FullPath("/xxx")},
		{"", FullPath("/xxx/.."), FullPath(".")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.fp.ToFile(), "ToFile()")
		})
	}
}

func TestJoin(t *testing.T) {
	type args struct {
		names []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"", args{[]string{""}}, "."},
		{"", args{[]string{"/"}}, "/"},
		{"", args{[]string{"/."}}, "/"},
		{"", args{[]string{"/.."}}, "/"},
		{"", args{[]string{"/xxx", "yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx/", "yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx", "./yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx/", "./yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx/", "../yyy"}}, "/yyy"},
		{"", args{[]string{"/xxx/", "/yyy"}}, "/xxx/yyy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Join(tt.args.names...), "Join(%v)", tt.args.names)
		})
	}
}

func TestNewFullPath(t *testing.T) {
	invalidPathUrl, _ := url.Parse("http://localhost:8888/%B2%E2%CA%D4/%B2%E2%CA%D4/%B2%E2.txt")
	invalidPath := invalidPathUrl.Path

	type args struct {
		name []string
	}
	tests := []struct {
		name string
		args args
		want FullPath
	}{
		{"", args{[]string{invalidPath}}, "/?/?/?.txt"},
		{"", args{[]string{""}}, "."},
		{"", args{[]string{"/"}}, "/"},
		{"", args{[]string{"/."}}, "/"},
		{"", args{[]string{"/.."}}, "/"},
		{"", args{[]string{"/xxx", "yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx/", "yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx", "./yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx/", "./yyy"}}, "/xxx/yyy"},
		{"", args{[]string{"/xxx/", "../yyy"}}, "/yyy"},
		{"", args{[]string{"/xxx/", "/yyy"}}, "/xxx/yyy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewFullPath(tt.args.name...), "NewFullPath(%v)", tt.args.name)
		})
	}
}

func Test_clearName(t *testing.T) {
	invalidPathUrl, _ := url.Parse("http://localhost:8888/%B2%E2%CA%D4/%B2%E2%CA%D4/%B2%E2.txt")
	invalidPath := invalidPathUrl.Path

	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"", args{invalidPath}, "/?/?/?.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, clearName(tt.args.name), "clearName(%v)", tt.args.name)
		})
	}
}
