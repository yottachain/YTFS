package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	// "log"
	"testing"

	"github.com/yotta-disk/common"
	"github.com/yotta-disk/opt"
)

func testOptions() *opt.Options {
	tmpfile, err := ioutil.TempFile("", "yotta-test")
	if err != nil {
		panic(err)
	}

	return &opt.Options{
		StorageName: tmpfile.Name(),
		StorageType: common.FileStorageType,
		ReadOnly: false,
		Sync: true,
		M: 0,
		N: 32,		// 32
		T: 1 << 16, // 64k
		D: 16, 		// 16
	}
}

func TestFileStorageRW(t *testing.T) {
	config := testOptions()
	defer os.Remove(config.StorageName)

	fs, err := OpenFileStorage(config)
	if err != nil {
		t.Fatal(err)
	}

	reader, _ := fs.Reader()
	defer reader.Close()
	writer, _ := fs.Writer()
	defer writer.Close()

	writer.Seek(0, io.SeekStart)
	binary.Write(writer, binary.LittleEndian, (uint8)(2))
	binary.Write(writer, binary.LittleEndian, (uint16)(5))
	binary.Write(writer, binary.LittleEndian, (uint32)(8))
	binary.Write(writer, binary.LittleEndian, []byte{0x1, 0x2, 0x3})
	writer.Seek(20, io.SeekStart)
	binary.Write(writer, binary.LittleEndian, []byte{0x4, 0x5, 0x6})

	buf := struct {
			Data []uint16
		} {
			Data: []uint16{0x2020, 0x2021, 0x2022},
		}
	writer.Seek(40, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, buf.Data)
	if err != nil {
		t.Fatal(err)
	}

	reader.Seek(0, io.SeekStart)
	var a uint8
	binary.Read(reader, binary.LittleEndian, &a)
	var b uint16
	binary.Read(reader, binary.LittleEndian, &b)
	var c uint32
	binary.Read(reader, binary.LittleEndian, &c)
	d := [3]byte{}
	binary.Read(reader, binary.LittleEndian, &d)

	reader.Seek(20, io.SeekStart)
	e := [3]byte{}
	binary.Read(reader, binary.LittleEndian, &e)

	var f uint16
	reader.Seek(40, io.SeekStart)
	binary.Read(reader, binary.LittleEndian, &f)


	fmt.Println(a, b, c, d, e, f)
}