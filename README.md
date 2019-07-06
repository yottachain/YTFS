# Yotta Disk

A data block save/load lib based on key-value styled db APIs.

## Getting Started

### Prerequisites
YottaDisk is developped by Golang, so install golang developping environment first.
Also, YottaDisk has 2 external dependencies. a simple golang-lru and ethereum.
Install these 3 before using YottaDisk
```
go get github.com/hashicorp/golang-lru
go get github.com/ethereum/go-ethereum
go get github.com/klauspost/reedsolomon
```

### Installing
Just download from github.com, no other process needs to be done.

## Running the tests
There are 2 ways to run the test:
### 1. go test

run all test:

```go
go test
```

or run specific:

```go
go test -timeout 300s github.com/yotta-disk -run ^(TestNewYTFS)$ -v -count=1
```
### 2. playground test
```go
bash$ cd playground; go build .
bash$ ./playground --help
Usage of ./playground:
  -config string
    	Config json file name
  -cpuprofile string
    	cpuprofile
  -format
    	format storage
  -home string
    	root directory of YTFS
  -memprofile string
    	memprofile
  -test string
    	Testmode: simple, stress, hybrid
```

Run playgroud as below:

```go
bash$ ./playground --test hybrid --config config-file.json --home /tmp/.ytfs
```

If anything goes wrong, e.g.

```go
bash$ ./playground --test hybrid --config config-file.json 
Open YottaDisk Success @/tmp/yotta-disk-storage.test
Starting hybrid test on 15384 data blocks
panic: yotta-disk: Range is full

goroutine 7 [running]:...
```

Format the storage, then re-execute the command.

```go
bash$ ./playground --test hybrid --config config-file.json --format
Open YottaDisk Success @/tmp/yotta-disk-storage.test
play completed.
bash$ ./playground --test hybrid --config config-file.json --home /tmp/.ytfs
Open YottaDisk Success @/tmp/yotta-disk-storage.test
Starting hybrid test on 130048 data blocks
[========================================================================] 100%
play completed.
```

## Deployment

First, check your storage device, then, update config according to config description.

### config description

A sample config file:

```json
{
	"ytfs": "ytfs default setting",
	"storages": [
	{
        "storage": "/tmp/yotta-play-1781595343",
        "type": 0,
        "readonly": false,
        "writesync": false,
        "storageSize": 1048576,
        "dataBlockSize": 32768
	},
	{
        "storage": "/tmp/yotta-play-2127581154",
        "type": 0,
        "readonly": false,
        "writesync": false,
        "storageSize": 1048576,
        "dataBlockSize": 32768
	}
	],
	"readonly": false,
	"M": 0,
	"N": 16384,
	"D": 32768,
	"C": 2147483648
}
```

Config file includes 2 structures, first level is YTFS general config

| Name     | Values            | Comments                                                     |
| -------- | ----------------- | :----------------------------------------------------------- |
| ytfs     | string            | Storage config name/tag.                                     |
| storages | array of storages | The stroage options, include all writable devices. [device number<255] |
| readonly | true, false       | If readonly mode, i.e. no append writing.                    |
| M        | N/A               | How many items one table can hold, it is calculated by a equotion:$M=\frac{C}{N*D}$<br />YTFS v0.3 expends M with a ratio, e.g. 1.2, to cover un-even distributed Hash key. |
| N        | [0,MAXUINT32)     | How many ranges is divided from the whole hash space. <br />Must be power of 2. |
| C        | (0,DeviceVolumn]  | The total writing space of storage. Basically the larger the better as it is the upper limit of YTFS expension. |
| D        | [0,DeviceVolumn)  | The data block size, normally it is 32k.                     |

The second level is storage device config.

| Name          | Values | Comments                                                     |
| ------------- | ------ | ------------------------------------------------------------ |
| storage       | string | Storage device path, e.g. /tmp/ytfs-storage or /dev/sda.     |
| type          | enum   | Storage type: File, Block device, etc.                       |
| readonly      | bool   | If storage is read only.                                     |
| writesync     | bool   | If write device in explicit sync mode.                       |
| storageSize   | uint64 | Storage device volumn.                                       |
| dataBlockSize | uint32 | Datablock size, should be consistent with YTFS, normally 32k. |

So, if we want to store a lots of 32k data block to 2 files, 8G and 4G respectively, and considering the expension in future, we set YTFS capacity to 16t. The config file may looks like:

```json
{
	"ytfs": "ytfs default setting",
	"storages": [
	{
        "storage": "/tmp/yotta-play-1781595343",
        "type": 0,
        "readonly": false,
        "writesync": false,
        "storageSize": 8589934592,
        "dataBlockSize": 32768
	},
	{
        "storage": "/tmp/yotta-play-2127581154",
        "type": 0,
        "readonly": false,
        "writesync": false,
        "storageSize": 4294967296,
        "dataBlockSize": 32768
	}
	],
	"readonly": false,
	"M": 0,
	"N": 65536,
	"D": 32768,
	"C": 17592186044416
}
```

## Contributing

N/A

## Versioning

N/A

## Authors

N/A

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

N/A
