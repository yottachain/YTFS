# YTFS

A data block save/load lib based on key-value styled db APIs.

## Getting Started

### Prerequisites
YTFS is developped by Golang, so install golang developping environment first.
Also, YTFS has 2 external dependencies. a simple golang-lru and ethereum.
Install these 2 before using YTFS
```
go get github.com/hashicorp/golang-lru
go get github.com/ethereum/go-ethereum
```

### Installing
Just download YTFS from github, no other process needs to be done.

## Running the tests
There are 2 ways to run the test:
### 1. go test
```go
go test -timeout 300s github.com/yottachain/YTFS -run ^(TestYTFSPutGetWithFileStorage)$ -v -count=1
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
  -test string
		Testmode: simple(default), stress, hybrid
```

Run playgroud as below:

```go
bash$ ./playground --test hybrid --config config-file.json 
```

If anything goes wrong, e.g.

```go
bash$ ./playground --test hybrid --config config-file.json 
Open YTFS Success @/tmp/yotta-disk-storage.test
Starting hybrid test on 15384 data blocks
panic: YTFS: Range is full

goroutine 7 [running]:...
```

Format the storage, then re-execute the command.

```go
bash$ ./playground --test hybrid --config config-file.json --format
Open YTFS Success @/tmp/yotta-disk-storage.test
play completed.
bash$ ./playground --test hybrid --config config-file.json
Open YTFS Success @/tmp/yotta-disk-storage.test
Starting hybrid test on 130048 data blocks
[========================================================================] 100%
play completed.
```

## Deployment

First, check storage device, make sure it is available and writable!
Then update config according to config description.

### config description

A sample config file:

```
{
  "storage": "/tmp/testFileStorage",
  "type": 0,
  "readonly": false,
  "writesync": true,
  "metadatasync": 0,
  "cache": 0,
  "M": 0,
  "N": 8,
  "T": 1048576,
  "D": 32
}
```

| Name         | Values           | Comments                                                     |
| ------------ | ---------------- | :----------------------------------------------------------- |
| storage      | N/A              | Storage file/device name                                     |
| type         | 0, 1             | 0 for file storage, i.e. a file in disk.<br />1 for dev storage, i.e. a block device. |
| readonly     | true, false      | If readonly mode, i.e. no append writing.                    |
| writesync    | true, false      | If data is written synchronizily to file/device.             |
| metadatasync | [0,INT_MAX]      | A period. YottaDiks updates metadata every ${metadatasync} writings.<br />0 means do NOT sync meta data. |
| cache        | [0,MemSize]      | LRU cache size which holds available table. <br />Unit in Byte, for example: cache: 1073741824, (1G) |
| M            | N/A              | How many items one table can hold, it is calculated by a equotion. |
| N            | [0,32768)        | How many ranges is divided from the whole hash space. <br />The larger the better, and must be power of 2. |
| T            | (0,DeviceVolumn] | The total writing space of storage.                          |
| D            | [0,DeviceVolumn) | The data block size, normally it is 32k.                     |

So, if we want to store a lots of 32k data block to a 16G file, and considering the writing performance, we disable the write sync (leave it to OS), the config looks like below:

```
{
  "storage": "/tmp/yotta-disk-storage.test",
  "type": 0,
  "readonly": false,
  "writesync": false,
  "metadatasync": 0,
  "cache": 0,
  "M": 0,
  "N": 1024,
  "T": 17179869184,
  "D": 32768
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
