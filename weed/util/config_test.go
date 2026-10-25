package util

import (
	"fmt"
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
