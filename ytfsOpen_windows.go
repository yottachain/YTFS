package ytfs

import (
	"fmt"
	"github.com/yottachain/YTFS/opt"
)

func openYTFS(dir string, config *opt.Options, init bool) (*YTFS, error) {
	fmt.Println("use indexdb")
	return openYTFSI(dir,config, init)
}

func startYTFS(dir string, config *opt.Options, dnid uint32) (*YTFS, error) {
	fmt.Println("use indexdb")
	return startYTFSI(dir, config, dnid, false)
}