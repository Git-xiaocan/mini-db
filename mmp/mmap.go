package mmap

import "os"

// Mmap uses the mmap system call to memory-map a file .if writable is true,
// memory protection of the pages is set so that they may be written to as well.
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// Madvise uses the madvise system call to give advice about the use of memory
// when using a slice the is memory-mapped to a file. Set the read-head flag to false
// if page references are expected in random order.
func Madvise(b []byte, readHead bool) error {
	return madvise(b, readHead)
}

// Msync would call sync on mmaped data
func Msync(b []byte) error {
	return msync(b)
}

// Munmap unmaps a previously mapped slice
func Munmap(b []byte) error {
	return munmap(b)
}
