package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"
)

var IsDebug *bool
var server *string

var commands = []*Command{
	cmdFix,
	cmdMaster,
	cmdUpload,
	cmdShell,
	cmdVersion,
	cmdVolume,
	cmdExport,
}

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if args[0] == "help" {
		help(args[1:])
		for _, cmd := range commands {
			if len(args) >= 2 && cmd.Name() == args[1] && cmd.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
		}
		return
	}

	for _, cmd := range commands {
		if cmd.Name() == args[0] && cmd.Run != nil {
			cmd.Flag.Usage = func() { cmd.Usage() }
			cmd.Flag.Parse(args[1:])
			args = cmd.Flag.Args()
			IsDebug = cmd.IsDebug
			if !cmd.Run(cmd, args) {
				fmt.Fprintf(os.Stderr, "\n")
				cmd.Flag.Usage()
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
			exit()
			return
		}
	}

	fmt.Fprintf(os.Stderr, "weed: unknown subcommand %q\nRun 'weed help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}

var usageTemplate = `WeedFS is a software to store billions of files and serve them fast!

Usage:

	weed command [arguments]

The commands are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "weed help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: weed {{.UsageLine}}
{{end}}
  {{.Long}}
`

// tmpl executes the given template text on data, writing the result to w.
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

func usage() {
	printUsage(os.Stderr)
	os.Exit(2)
}

// help implements the 'help' command.
func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'weed help'.
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: weed help command\n\nToo many arguments given.\n")
		os.Exit(2) // failed at 'weed help'
	}

	arg := args[0]

	for _, cmd := range commands {
		if cmd.Name() == arg {
			tmpl(os.Stdout, helpTemplate, cmd)
			// not exit 2: succeeded at 'weed help cmd'.
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'weed help'.\n", arg)
	os.Exit(2) // failed at 'weed help cmd'
}

var atexitFuncs []func()

func atexit(f func()) {
	atexitFuncs = append(atexitFuncs, f)
}

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	os.Exit(exitStatus)
}

func exitIfErrors() {
	if exitStatus != 0 {
		exit()
	}
}

func writeJson(w http.ResponseWriter, r *http.Request, obj interface{}) (err error) {
	w.Header().Set("Content-Type", "application/javascript")
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		_, err = w.Write(bytes)
	} else {
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}
	return
}

// wrapper for writeJson - just logs errors
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, obj interface{}) {
	if err := writeJson(w, r, obj); err != nil {
		log.Printf("error writing JSON %s: %s", obj, err)
	}
}

func debug(params ...interface{}) {
	if *IsDebug {
		log.Println(params)
	}
}
