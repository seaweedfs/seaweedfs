package command

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/spf13/viper"
)

func TestReadingTomlConfiguration(t *testing.T) {

	viper.SetConfigType("toml")

	// any approach to require this configuration into your program.
	var tomlExample = []byte(`
[database]
server = "192.168.1.1"
ports = [ 8001, 8001, 8002 ]
connection_max = 5000
enabled = true

[servers]

  # You can indent as you please. Tabs or spaces. TOML don't care.
  [servers.alpha]
  ip = "10.0.0.1"
  dc = "eqdc10"

  [servers.beta]
  ip = "10.0.0.2"
  dc = "eqdc10"

`)

	viper.ReadConfig(bytes.NewBuffer(tomlExample))

	fmt.Printf("database is %v\n", viper.Get("database"))
	fmt.Printf("servers is %v\n", viper.GetStringMap("servers"))

	alpha := viper.Sub("servers.alpha")

	fmt.Printf("alpha ip is %v\n", alpha.GetString("ip"))
}
