package util

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// `weed server` starts the volume server (SetDefault writers) and the master
// (GetStringMap readers) back to back. Reads and writes must share the proxy
// lock — a race-enabled build flags the interleaving the issue reported.
func TestViperProxySerializesMapReadsAgainstWrites(t *testing.T) {
	p := NewViperProxy(viper.New())
	p.SetDefault("storage.backend.a.enabled", true)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				p.SetDefault(fmt.Sprintf("storage.backend.d%d.enabled", j), i)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			if got := p.GetStringMap("storage.backend"); len(got) == 0 {
				t.Error("GetStringMap returned no keys")
				return
			}
		}
	}()
	wg.Wait()

	got := p.GetStringMap("storage.backend")
	assert.Contains(t, got, "a")
}

// A proxy over its own viper serves the wrapped instance, for configuration
// loaded from a source other than the shared one.
func TestNewViperProxyWrapsGivenViper(t *testing.T) {
	v := viper.New()
	v.Set("x.y", "z")
	p := NewViperProxy(v)
	assert.Equal(t, "z", p.GetString("x.y"))
	assert.Equal(t, map[string]interface{}{"y": "z"}, p.GetStringMap("x"))
}

func TestReadConfigurationDropsKeysTheFileNoLongerSets(t *testing.T) {
	dir := t.TempDir()
	saved := ConfigurationFileDirectory
	ConfigurationFileDirectory = DirectoryValueType(dir)
	t.Cleanup(func() { ConfigurationFileDirectory = saved })
	t.Setenv("WEED_READCONF_ENV", "from-env")
	write := func(content string) {
		if err := os.WriteFile(filepath.Join(dir, "readconf_test.toml"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	v, used := ReadConfiguration("readconf_test", "")
	assert.Empty(t, used, "missing file")
	assert.Equal(t, "from-env", v.GetString("readconf.env"))

	write("[readconf]\nkey = \"v1\"\n")
	v, used = ReadConfiguration("readconf_test", "")
	assert.Equal(t, filepath.Join(dir, "readconf_test.toml"), used)
	assert.Equal(t, "v1", v.GetString("readconf.key"))

	write("[readconf]\n")
	v, used = ReadConfiguration("readconf_test", "")
	assert.NotEmpty(t, used)
	assert.Empty(t, v.GetString("readconf.key"))
	assert.Equal(t, "from-env", v.GetString("readconf.env"))

	write("[readconf\n")
	v, used = ReadConfiguration("readconf_test", "")
	assert.Empty(t, used, "unparsable file")
	assert.Equal(t, "from-env", v.GetString("readconf.env"))
}

func TestReadConfigurationReadsTheGivenFile(t *testing.T) {
	searched, given := t.TempDir(), filepath.Join(t.TempDir(), "readconf_test.toml")
	saved := ConfigurationFileDirectory
	ConfigurationFileDirectory = DirectoryValueType(searched)
	t.Cleanup(func() { ConfigurationFileDirectory = saved })
	if err := os.WriteFile(filepath.Join(searched, "readconf_test.toml"), []byte("[readconf]\nkey = \"searched\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, used := ReadConfiguration("readconf_test", given)
	assert.Empty(t, used, "missing given file must not fall back to a search")

	if err := os.WriteFile(given, []byte("[readconf]\nkey = \"given\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	v, used := ReadConfiguration("readconf_test", given)
	assert.Equal(t, given, used)
	assert.Equal(t, "given", v.GetString("readconf.key"))
}
