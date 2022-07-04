package util

import (
	"io/ioutil"
	"os"
	"path"
)

func DelPath(dirName string) {
	if fd, err := os.Stat(dirName); err == nil {
		if fd.IsDir() {
			dir, _ := ioutil.ReadDir(dirName)
			for _, d := range dir {
				_ = os.RemoveAll(path.Join([]string{dirName, d.Name()}...))
			}
		} else {
			_ = os.Remove(dirName)
		}
	}
}
