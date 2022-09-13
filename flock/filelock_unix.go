//+build  !windows,!plan9

package flock

import (
	"fmt"
	"os"
	"syscall"
)

// FileLockGuard 在目录上持有文件锁
type FileLockGuard struct {
	fd *os.File
}

// AcquireFileFlock 通过 syscall.Flock 获取目录上的锁
// 返回 FileLockGuard 或错误
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

// SyncDir 将目录的当前内容提交到稳定存储
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

// Release 释放文件锁
func (fl *FileLockGuard) Release() error {
	t := syscall.LOCK_UN | syscall.LOCK_NB
	if err := syscall.Flock(int(fl.fd.Fd()), t); err != nil {
		return err
	}
	return fl.fd.Close()
}
