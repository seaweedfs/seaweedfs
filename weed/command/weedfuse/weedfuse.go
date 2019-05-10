package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/command"
)

var (
	options = flag.String("o", "", "comma separated options rw,uid=xxx,gid=xxx")
)

func main() {

	flag.Parse()

	device := flag.Arg(0)
	mountPoint := flag.Arg(1)

	fmt.Printf("source: %v\n", device)
	fmt.Printf("target: %v\n", mountPoint)

	maybeSetupPath()

	parts := strings.SplitN(device, "/", 2)
	filer, filerPath := parts[0], parts[1]

	command.RunMount(
		filer, "/"+filerPath, mountPoint, "", "000", "",
		4, true, 0, 1000000)

}

func maybeSetupPath() {
	// sudo mount -av may not include PATH in some linux, e.g., Ubuntu
	hasPathEnv := false
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "PATH=") {
			hasPathEnv = true
		}
		fmt.Println(e)
	}
	if !hasPathEnv {
		os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
}
