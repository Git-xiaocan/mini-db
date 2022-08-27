package logfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"minidb/ioselector"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidCrc invalid crc
	ErrInvalidCrc = errors.New("logfile :invalid crc")

	ErrWriteSizeNotEqual = errors.New("logfile:write size is not equal ro entry size")

	//ErrUnsupportedLogFileType unsupported log file type ,only wal  and ValueLog now.
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
	// ErrEndOfEntry end of entry in log file
	ErrEndOfEntry        = errors.New("logfile: end of entry in log file")
	ErrUnsupportedIoType = errors.New("unsupported  io type ")
)

const (
	InitialLogFileId = 0
	FilePrefix       = "log."
)

type FileType int8

const (
	Strs FileType = iota
	Sets
	List
	Hash
	ZSet
)

var (
	FileNameMap = map[FileType]string{
		Strs: "log.strs.",
		List: "log.list.",
		Hash: "log.hash.",
		Sets: "log.sets.",
		ZSet: "log.zset.",
	}
	FileTypeMap = map[string]FileType{
		"strs": Strs,
		"list": List,
		"hash": Hash,
		"sets": Sets,
		"zset": ZSet,
	}
)

type IOType int8

const (
	FileIO IOType = iota
	MMap
)

type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IoSelector ioselector.IOSelector
}

func OpenLogFile(path string, fid uint32, fsize int64, fType FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	fname, err := lf.getLogFileName(path, fid, fType)
	if err != nil {
		return nil, err
	}
	var selector ioselector.IOSelector
	switch ioType {
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fname, fsize); err != nil {
			return nil, err
		}
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fname, fsize); err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnsupportedIoType

	}
	lf.IoSelector = selector
	return

}

// ReadLogEntry read a logEntry from logfile at offset
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// read header
	headerBuf, err := lf.ReadBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	//decode header
	header, size := DecodeHeader(headerBuf)
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}
	e := &LogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)

	var entrySize = size + kSize + vSize
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.ReadBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]

	}
	// crc32 check
	//size :header size
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil

}
func (lf *LogFile) ReadBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}
func (lf *LogFile) getLogFileName(path string, fid uint32, fileType FileType) (name string, err error) {
	if _, ok := FileNameMap[fileType]; !ok {
		return "", ErrUnsupportedLogFileType
	}
	fName := FileNameMap[fileType] + fmt.Sprintf("%09d", fid)
	name = filepath.Join(path, fName)
	return

}

// Read a byte slice in the log file at offset,slice length is the given size.
// It returns a byte slice or error .if any
func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {

	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IoSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

//Write a byte slice to the end of log file
// returns an error ,if any.
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

// Sync commits the current content of log file to stable storage.
func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

// Delete the current log-file
// File will can't be retrieved, if you do this ,so use it carefully
func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}
