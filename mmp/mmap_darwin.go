package mmap

import (
	"golang.org/x/sys/unix"
	"os"
	"syscall"
	"unsafe"
)

// mmap 使用 mmap 系统调用对文件进行内存映射。如果 writable 为真，则设置页面的内存保护，以便它们也可以被写入
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	t := unix.PROT_READ // readable
	if writable {
		t |= unix.PROT_WRITE //writable
	}

	return unix.Mmap(int(fd.Fd()), 0, int(size), t, unix.MAP_SHARED) //将修改后的文件重写回底层文件
}

// munmap unmaps a previously mapped slice
func munmap(b []byte) error {
	return unix.Munmap(b)
}

// madvise unix 包不支持 OS X 上的 madvise 系统调用。
func madvise(b []byte, readhead bool) error {
	advice := unix.MADV_NORMAL //告诉linux系统执行默认操作,系统将会返回接下来前15个页面和后16个页面。
	//即使只需要一个页面,系统也会返回32个页面,带来了极大的系统开销
	if !readhead {
		advice = unix.MADV_RANDOM //告诉系统,访问页面是随机的。
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
