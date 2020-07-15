package ytfs

import (
	"fmt"
	"github.com/yottachain/YTFS/opt"
)

func openYTFS(dir string, config *opt.Options) (*YTFS, error) {
	fmt.Println("use indexdb")
	return openYTFSI(dir,config)
}