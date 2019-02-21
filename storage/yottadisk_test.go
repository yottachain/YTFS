package storage
import (
	"reflect"
	"io/ioutil"
	"os"
	"testing"

	types "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

const (
	dataBlockSize = 16
)

func testOptions() *opt.StorageOptions {
	tmpfile, err := ioutil.TempFile("", "yotta-test")
	if err != nil {
		panic(err)
	}

	return &opt.StorageOptions{
		StorageName: tmpfile.Name(),
		StorageType: types.FileStorageType,
		ReadOnly: false,
		Sync: true,
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


