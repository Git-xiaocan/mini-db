package utils

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"io"
)

type MurHash128 struct {
	mur murmur3.Hash128
}

func NewMurHash128() *MurHash128 {
	return &MurHash128{mur: murmur3.New128()}
}
func (m *MurHash128) Write(b []byte) error {
	n, err := m.mur.Write(b)
	if n != len(b) {
		return io.ErrShortWrite
	}
	return err
}
func (m *MurHash128) EncodeSum128() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	s1, s2 := m.mur.Sum128()
	var index int
	index += binary.PutUvarint(buf[index:], s1)
	index += binary.PutUvarint(buf[index:], s2)
	return buf[:index]
}
func (m *MurHash128) Reset() {
	m.mur.Reset()
}
