package util

import (
	"os/user"
	"path/filepath"
	"testing"
)

func TestToShortFileName(t *testing.T) {
	tests := []struct {
		in    string
		value string
	}{
		{"/data/a/b/c/d.txt", "/data/a/b/c/d.txt"},
		{"/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТА_ООО).txt", "/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТ354fcaf4.txt"},
		{"/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТА_ООО)_without_extension", "/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТА_О21a6e47a"},
	}
	for _, p := range tests {
		got := ToShortFileName(p.in)
		if got != p.value {
			t.Errorf("failed to test: got %v, want %v", got, p.value)
		}
	}
}

func TestResolvePath(t *testing.T) {
	usr, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}
	home := usr.HomeDir

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"absolute", "/var/data", "/var/data"},
		{"relative", "data", "data"},
		{"tilde mid path is literal", "/foo/~/bar", "/foo/~/bar"},
		{"bare tilde", "~", home},
		{"tilde slash", "~/", home},
		{"tilde with subpath", "~/data", filepath.Join(home, "data")},
		{"tilde with current user", "~" + usr.Username, home},
		{"tilde with current user and subpath", "~" + usr.Username + "/data", filepath.Join(home, "data")},
		{"tilde unknown user falls back to literal", "~no-such-user-xyz/data", "~no-such-user-xyz/data"},
		// Native separator: identical to "~/data" on Unix; exercises backslash on Windows.
		{"tilde with native separator", "~" + string(filepath.Separator) + "data", filepath.Join(home, "data")},
		{"tilde user with native separator", "~" + usr.Username + string(filepath.Separator) + "data", filepath.Join(home, "data")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ResolvePath(c.in); got != c.want {
				t.Errorf("ResolvePath(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

func TestResolveCommaSeparatedPaths(t *testing.T) {
	usr, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}
	home := usr.HomeDir

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"single absolute", "/a", "/a"},
		{"single tilde", "~/a", filepath.Join(home, "a")},
		{"two absolutes", "/a,/b", "/a,/b"},
		{"two tildes", "~/a,~/b", filepath.Join(home, "a") + "," + filepath.Join(home, "b")},
		{"mixed", "/a,~/b,/c", "/a," + filepath.Join(home, "b") + ",/c"},
		{"no tilde fast path", "/a,/b,/c", "/a,/b,/c"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ResolveCommaSeparatedPaths(c.in); got != c.want {
				t.Errorf("ResolveCommaSeparatedPaths(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}
