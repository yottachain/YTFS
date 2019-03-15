package storage

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	types "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/errors"
)

func testOptions() *opt.StorageOptions {
	tmpfile, err := ioutil.TempFile("", "yotta-test")
	if err != nil {
		panic(err)
	}

	return &opt.StorageOptions{
		StorageName:   tmpfile.Name(),
		StorageType:   types.FileStorageType,
		ReadOnly:      false,
		SyncPeriod:    0,
		StorageVolume: 1 << 20, // 1m
		DataBlockSize: 1 << 15, // 32k
	}
}

func TestCreateYottaDiskWithFileStorage(t *testing.T) {
	config := testOptions()
	defer os.Remove(config.StorageName)

	yd, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer yd.Close()
}

func TestValidateYottaDiskWithFileStorage(t *testing.T) {
	config := testOptions()
	defer os.Remove(config.StorageName)

	yd, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer yd.Close()

	yd.WriteData(0, []byte{0})
	yd.WriteData(1, []byte{1})

	// header not sync
	ydRef1, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer ydRef1.Close()

	if reflect.DeepEqual(yd.meta, ydRef1.meta) {
		t.Fatal()
	}

	yd.Close()
	// header synx
	ydRef2, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer ydRef2.Close()

	if !reflect.DeepEqual(yd.meta, ydRef2.meta) {
		t.Fatal()
	}
}

func TestOpenYottaDiskWithAnotherConfig(t *testing.T) {
	config := testOptions()
	defer os.Remove(config.StorageName)

	yd, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	yd.Close()

	config.StorageVolume /= 2
	opt.IgnoreStorageHeaderErr = true
	ydNew, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	ydNew.Close()

	config.StorageVolume /= 2
	opt.IgnoreStorageHeaderErr = false
	_, err = OpenYottaDisk(config)
	if err != errors.ErrStorageHeader {
		t.Fatal(err)
	}
}