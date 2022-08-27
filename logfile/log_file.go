package logfile

import (
	"errors"
	"minidb/ioselector"
	"sync"
)

var (
	// ErrInvalidCrc invalid crc
	ErrInvalidCrc = errors.New("logfile :invalid crc")

	ErrWriteSizeNotEqual = errors.New("logfile:write size is not equal ro entry size")
)

const (
	InitialLogFileId = 0

	FilePrefix = "log."
)

type FileType int8

const (
	Strs FileType = iota
	List
	Hash
	Sets
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

func OpenLogFile(path string, fid uint32, fsize int64, fType FileType, ioType IOType) (lf *LogFile) {
	lf := &LogFile{Fid: fid}
}
