package storage

import (
	"os"
	"sync"
	// "syscall"

	types "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

// BlockStorage is a file-system backed storage.
type BlockStorage struct {
	readOnly bool
	mu       sync.RWMutex
	fd       *FileDesc
	reader   Reader
	writer   Writer
}

// OpenblkStorage returns a new filesystem-backed storage implementation with the given
// path. This also acquire a file lock, so any subsequent attempt to open the
// same path will fail.
//
// The storage must be closed after use, by calling Close method.
func OpenBlockStorage(opt *opt.StorageOptions) (Storage, error) {
	blkStorage := BlockStorage{
		readOnly: opt.ReadOnly,
		mu:       sync.RWMutex{},
		fd: &FileDesc{
			Type: types.BlockStorageType,
			Cap:  0,
			Path: opt.StorageName,
		},
	}

	reader, err := blkStorage.Open(*blkStorage.fd)
	if err != nil {
		return nil, err
	}
	blkStorage.reader = reader

	if !opt.ReadOnly {
		writer, err := blkStorage.Create(*blkStorage.fd)
		if err != nil {
			return nil, err
		}
		blkStorage.writer = writer
	}

	err = blkStorage.validateStorageParam(opt)
	if err != nil {
		return nil, err
	}

	return &blkStorage, nil
}

func (file *BlockStorage) Reader() (Reader, error) {
	return file.reader, nil
}

func (file *BlockStorage) Writer() (Writer, error) {
	return file.writer, nil
}

// Lock locks the storage. Any subsequent attempt to call Lock will fail
// until the last lock released.
// Caller should call Unlock method after use.
func (file *BlockStorage) Lock() (Locker, error) {
	// TODO: use RW-lock.
	file.mu.Lock()
	return &file.mu, nil
}

// Close closes the storage.
// It is valid to call Close multiple times. Other methods should not be
// called after the storage has been closed.
func (file *BlockStorage) Close() error {
	file.reader.Close()
	if !file.readOnly {
		file.writer.Sync()
		file.writer.Close()
	}
	return nil
}

func (file *BlockStorage) validateStorageParam(opt *opt.StorageOptions) error {
	// TODO: enable size check
	// if file.fd.Cap < opt.StorageVolume {
	// 	return errors.ErrStorageSize
	// }

	return nil
}

// Open opens file with the given 'file descriptor' read-only.
// Returns os.ErrNotExist error if the file does not exist.
// Returns ErrClosed if the underlying storage is closed.
func (file *BlockStorage) Open(fd FileDesc) (Reader, error) {
	file.fd = &FileDesc{
		Type: types.BlockStorageType,
		Cap:  uint64(fd.Cap),
		Path: fd.Path,
	}

	// fp, err :=  syscall.Open(fd.Path, syscall.O_RDONLY, 0777)
	fp, err := os.Open(fd.Path)
	if err != nil {
		return nil, err
	}

	return fp, err
}

// Create creates file with the given 'file descriptor', truncate if already
// exist and opens write-only.
// Returns ErrClosed if the underlying storage is closed.
func (file *BlockStorage) Create(fd FileDesc) (Writer, error) {
	fp, err := os.OpenFile(fd.Path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return fp, nil
}
