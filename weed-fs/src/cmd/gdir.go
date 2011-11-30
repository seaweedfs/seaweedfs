package main

import (
	"directory"
	//    "runtime"
	"log"
)

func main() {
	m := directory.NewMapper("/tmp", "directory")
	log.Println("map size", len(m.Virtual2physical))
	m.Add(10, 11,12,13)
	m.Add(20, 21,22,23)
    log.Println("map(10)", m.Get(10))
    log.Println("map size", len(m.Virtual2physical))
	m.Save()
	defer m.Save()
}
