package ytfs

import (
	"fmt"
	"github.com/yottachain/YTFS/opt"
)

func openYTFS(dir string, config *opt.Options, init bool, dnId uint32) (*YTFS, error) {
	if config.UseKvDb {
		fmt.Println("use rocksdb")
		return openYTFSK(dir, config, init, dnId)
	}
	fmt.Println("use indexdb")
	return openYTFSI(dir, config, init, dnId)
}

func startYTFS(dir string, config *opt.Options, dnid uint32) (*YTFS, error) {
	if config.UseKvDb {
		fmt.Println("use rocksdb")
		return startYTFSK(dir, config, dnid, false)
	}
	fmt.Println("use indexdb")
	return startYTFSI(dir, config, dnid, false)
}
