package storage

import (
	"os"
	"sync"

	types "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

// FileStorage is a file-system backed storage.
type FileStorage struct {
	readOnly bool
	mu       sync.RWMutex
	fd       *FileDesc
	reader   Reader
	writer   Writer
}

// OpenFileStorage returns a new filesystem-backed storage implementation with the given
// path. This also acquire a file lock, so any subsequent attempt to open the
// same path will fail.
//
// The storage must be closed after use, by calling Close method.
func OpenFileStorage(opt *opt.StorageOptions) (Storage, error) {
	fileStorage := FileStorage{
		readOnly: opt.ReadOnly,
		mu:       sync.RWMutex{},
		fd: &FileDesc{
			Type: types.DummyStorageType,
			Cap:  0,
			Path: opt.StorageName,
		},
	}

	writer, err := fileStorage.Create(*fileStorage.fd)
	if err != nil {
		return nil, err
	}
	if !opt.ReadOnly {
		fileStorage.writer = writer
	} else {
		writer.Close()
	}

	reader, err := fileStorage.Open(*fileStorage.fd)
	if err != nil {
		return nil, err
	}
	fileStorage.reader = reader

	err = fileStorage.validateStorageParam(opt)
	if err != nil {
		return nil, err
	}

	return &fileStorage, nil
}

func (file *FileStorage) Reader() (Reader, error) {
	return file.reader, nil
}

func (file *FileStorage) Writer() (Writer, error) {
	return file.writer, nil
}

// Lock locks the storage. Any subsequent attempt to call Lock will fail
// until the last lock released.
// Caller should call Unlock method after use.
func (file *FileStorage) Lock() (Locker, error) {
	// TODO: use RW-lock.
	file.mu.Lock()
	return &file.mu, nil
}

// Close closes the storage.
// It is valid to call Close multiple times. Other methods should not be
// called after the storage has been closed.
func (file *FileStorage) Close() error {
	file.reader.Close()
	if !file.readOnly {
		file.writer.Sync()
		file.writer.Close()
	}
	return nil
}

func (file *FileStorage) validateStorageParam(opt *opt.StorageOptions) error {
	// TODO: enable pre-alloc file
	// if file.fd.Cap < opt.StorageVolume {
	// 	return errors.ErrStorageSize
	// }

	return nil
}

// Open opens file with the given 'file descriptor' read-only.
// Returns os.ErrNotExist error if the file does not exist.
// Returns ErrClosed if the underlying storage is closed.
func (file *FileStorage) Open(fd FileDesc) (Reader, error) {
	// info, err := os.Stat(fd.Path)
	// if err != nil {
	// 	return nil, err
	// }

	file.fd = &FileDesc{
		Type: types.FileStorageType,
		Cap:  uint64(fd.Cap),
		Path: fd.Path,
	}

	fp, err := os.Open(fd.Path)
	if err != nil {
		return nil, err
	}
	return fp, err
}

// Create creates file with the given 'file descriptor', truncate if already
// exist and opens write-only.
// Returns ErrClosed if the underlying storage is closed.
func (file *FileStorage) Create(fd FileDesc) (Writer, error) {
	fp, err := os.OpenFile(fd.Path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return fp, nil
}
