package ioselector

import (
	"io"
	mmap "minidb/mmp"
	"os"
)

type MMapSelector struct {
	fd     *os.File
	buf    []byte // a buffer of mmap
	bufLen int64
}

func NewMMapSelector(fName string, fsize int64) (IOSelector, error) {

	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, fsize)
	if err != nil {
		return nil, err
	}

	return &MMapSelector{fd: file, buf: buf, bufLen: int64(len(buf))}, nil

}

func (mms *MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= mms.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= mms.bufLen {
		return 0, io.EOF
	}
	return copy(b, mms.buf[offset:]), nil
}

func (mms *MMapSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || length+offset > mms.bufLen {
		return 0, io.EOF
	}
	return copy(mms.buf[offset:], b), nil

}
func (mms *MMapSelector) Sync() error {
	return mmap.Msync(mms.buf)
}
func (mms *MMapSelector) Close() error {
	if err := mmap.Msync(mms.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(mms.buf); err != nil {
		return err
	}
	return mms.fd.Close()
}
func (mms *MMapSelector) Delete() error {
	if err := mmap.Munmap(mms.buf); err != nil {
		return err
	}
	if err := mms.fd.Truncate(0); err != nil {
		return err
	}
	if err := mms.fd.Close(); err != nil {
		return nil
	}
	return os.Remove(mms.fd.Name())
}

var _ IOSelector = (*MMapSelector)(nil)
