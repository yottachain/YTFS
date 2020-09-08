package ytfs

import (
	"fmt"
	"github.com/yottachain/YTFS/opt"
)

func openYTFS(dir string, config *opt.Options) (*YTFS, error) {
	if config.UseKvDb {
		fmt.Println("use rocksdb")
		return openYTFSK(dir,config)
	}
	fmt.Println("use indexdb")
	return openYTFSI(dir,config)
}