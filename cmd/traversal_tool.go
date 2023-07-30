package main

import (
	"flag"
	"fmt"
	"github.com/yottachain/YTFS"
	"path"
)

func main() {
	var daPath string
	mainDBPath := path.Join("root/YTFS", ytfs.MdbFileName)
	flag.StringVar(&daPath, "p", mainDBPath, "kv db path")

	db, err := ytfs.OpenDB(daPath)
	if err != nil {
		fmt.Printf("open db fail, error %s", err.Error())
		return
	}

	db.ScanDB()
}
