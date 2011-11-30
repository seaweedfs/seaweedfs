package main

import (
	"http"
	//	"runtime"
	"log"
	"os"
	"exec"
	"fmt"
)

func HelloServer(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("hello, world!\n"))
}
func ShellHandler(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("cmd")
	args := r.Form["arg"]
	dir := r.FormValue("dir")
	w.Header().Set("Content-Type", "text/plain")

	cmd := run(w, dir, name, args)
	cmd.Wait()
}

func run(w http.ResponseWriter, dir string, name string, args []string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	cmd.Stdin = os.Stdin
	cmd.Stdout = w
	cmd.Stderr = w

	if err := cmd.Start(); err != nil {
		fmt.Fprint(w, "could not execute", args, ":", cmd.Args, "\n", err,"\n")
	}
	return cmd
}

func main() {
	//	runtime.GOMAXPROCS(1)
	http.HandleFunc("/", HelloServer)
	http.HandleFunc("/shell", ShellHandler)

	log.Println("Serving at http://127.0.0.1:8080/")
	http.ListenAndServe(":8080", nil)
}
