package util

import "unsafe"

//go:linkname runtimeMemHash runtime.memhash
//go:noescape
func runtimeMemHash(p unsafe.Pointer, seed, s uintptr) uintptr

func MemHash(buf []byte) uint64 {
	return rthash(buf, 889)
}
func rthash(b []byte, seed uint64) uint64 {
	if len(b) == 0 {
		return seed
	}
	//32 bit
	if unsafe.Sizeof(0) == 8 {

		return uint64(runtimeMemHash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b))))

	}
	// 64bit >
	hi := uint64(runtimeMemHash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b))))
	lo := uint64(runtimeMemHash(unsafe.Pointer(&b[0]), uintptr(seed>>32), uintptr(len(b))))
	return hi<<32 | lo
}
