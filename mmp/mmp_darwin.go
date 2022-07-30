package mmp

import (
	"golang.org/x/sys/unix"
	"os"
	"syscall"
	"unsafe"
)

// mmap uses the mmap system call to memory-map a file .if writable is true,
// memory protection of the pages is set so that they may be written to as well
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	t := unix.PROT_READ // readable
	if writable {
		t |= unix.PROT_WRITE //writable
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), t, unix.MAP_SHARED)
}

// munmap unmaps a previously mapped slice
func munmap(b []byte) error {
	return unix.Munmap(b)
}

// madvise this is required because the unix package does not support the madvise system call on OS X.
func madvise(b []byte, readhead bool) error {
	advice := unix.MADV_NORMAL
	if !readhead {
		advice = unix.MADV_RANDOM
	}
	_, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if err != 0 {
		return err
	}
	return nil
}

func msync(b []byte) error {

	return unix.Msync(b, unix.MS_SYNC)
}
