package ioselector

import "os"

// FileIOSelector 表示使用标准文件IO
type FileIOSelector struct {
	fd *os.File
}

func NewFileIOSelector(fName string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, nil
}

func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}
func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}
func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}
func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}
func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}

var _ IOSelector = (*FileIOSelector)(nil)
