//+build  !windows,!plan9

package flock

import (
	"fmt"
	"os"
	"syscall"
)

// FileLockGuard hold a lock of file on a directory
type FileLockGuard struct {
	fd *os.File
}

// AcquireFileFlock acquire the lock on the directory by syscall.Flock.
// return a FileLockGuard or an error, if any.
func AcquireFileFlock(path string, readOnly bool) (*FileLockGuard, error) {
	flag := os.O_RDWR // read and write
	if readOnly {
		flag = os.O_RDONLY // read-only
	}
	file, err := os.OpenFile(path, flag, 0)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return nil, err
	}
	var lockType = syscall.LOCK_EX | syscall.LOCK_NB

	if readOnly {
		lockType = syscall.LOCK_SH | syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), lockType); err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil

}

// SyncDir commits the current contents of directory to stable storage
func SyncDir(path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	err = fd.Sync()
	closeErr := fd.Close()
	if err != nil {
		return fmt.Errorf("sync dir err : %+v", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close dir err:%+v ", err)
	}
	return nil
}

// Release   the file lock
func (fl *FileLockGuard) Release() error {
	t := syscall.LOCK_UN | syscall.LOCK_NB
	if err := syscall.Flock(int(fl.fd.Fd()), t); err != nil {
		return err
	}
	return fl.fd.Close()
}
