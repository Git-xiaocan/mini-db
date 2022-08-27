package ioselector

import (
	"errors"
	"os"
)

// ErrInvalidFsize invalid file size
var ErrInvalidFsize = errors.New("fsize can't be zero or negative")

//FilePerm default permission of the newly created logger file
const FilePerm = 0644

// IOSelector io selector for file io and mmap ,used by wal and value logger right now
type IOSelector interface {
	//Write a slice to logger file at offset
	//It returns the number of bytes written and an error ,if any
	Write(b []byte, offset int64) (int, error)
	//Read a slice from offset
	// if returns the number of bytes read and any error encountered
	Read(b []byte, offset int64) (int, error)
	// Sync commits the current contents of the file to stable storage .
	// Typically ,this means flushing the file system's in-memory copy of recently written data of disk
	Sync() error
	// Close closes the file , rendering it unstable for I/O
	//It will return an error if it has already been closed
	Close() error
	//Delete the file
	// Must close it before delete ,and will ummap if in MMapSelector.
	Delete() error
}

//openFile open file  and truncate it if necessary
func openFile(fName string, fsize int64) (*os.File, error) {
	file, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < fsize {
		if err := file.Truncate(fsize); err != nil {
			return nil, err
		}

	}
	return file, nil
}
