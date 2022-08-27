package minidb

import "time"

type DataIndexMode int

const (
	//KeyValueMemMode key and value are both in memory ,read operation will be very fast in this mode.
	//Because there is no disk seek , just get value from the corresponding data structures in memory
	// This mode is suitable for scenarios where the value are relatively small.
	KeyValueMemMode DataIndexMode = iota
	//KeyOnlyMemMode only key in memory ,there is a disk seek while getting a value.
	// Because values are in log file on disk.
	KeyOnlyMemMode
)

type IOType int8

const (
	FileIO IOType = iota
	MMap
)

type Options struct {
	DBPath               string
	IndexMode            DataIndexMode
	IoType               IOType
	Sync                 bool
	LogFileGCRatio       float64
	LogFileGCInterval    time.Duration
	LogFileSizeThreshold int64
	DiscardBufferSize    int
}

func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		IndexMode:            KeyOnlyMemMode,
		IoType:               FileIO,
		Sync:                 false,
		LogFileGCInterval:    time.Hour * 8,
		LogFileGCRatio:       0.5,
		LogFileSizeThreshold: 512 << 20,
		DiscardBufferSize:    8 << 20,
	}
}
