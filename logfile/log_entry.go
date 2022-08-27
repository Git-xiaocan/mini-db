package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

// MaxHeaderSize max entry header size .
// crc32 type kSize vSize Expired
// 	4     1     5     5 	10   = 25
const MaxHeaderSize = 25

//EntryType type of entry
type EntryType byte

const (
	// TypeDelete represents entry type  is delete
	TypeDelete EntryType = iota + 1
	// TypeListMeta represents entry is list meta
	TypeListMeta
)

// LogEntry is the data will be appended in log file
type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64 // time.Unix
	Type      EntryType
}
type entryHeader struct {
	crc32     uint32 //sum
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // time.Unix
}

// EncodeEntry will encode entry into a byte slice
// the encoded Entry looks like:
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//         |--------------------------crc check---------------------------|
// crc32 type kSize vSize Expired
// 	4     1     5     5 	10   = 25
func EncodeEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	//encode header
	header := make([]byte, MaxHeaderSize)
	header[4] = byte(e.Type)
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	index += binary.PutVarint(header[index:], e.ExpiredAt)
	// header end
	// key and value
	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	copy(buf[:index], header[:])
	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)
	//crc32
	crc := crc32.ChecksumIEEE(buf[4:])

	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func DecodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}
	var index = 5
	ksize, n := binary.Varint(buf[index:])
	h.kSize = uint32(ksize)
	index += n
	vsize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vsize)
	index += n
	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt
	return h, int64(index + n)
}

func getEntryCrc(e *LogEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
