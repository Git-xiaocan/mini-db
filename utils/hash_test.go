package utils

import "testing"

func TestMemHash(t *testing.T) {
	key := []byte("hello")
	key1 := []byte("hello1")
	key2 := []byte("hello2")
	t.Log(MemHash(key))
	t.Log(MemHash(key1))
	t.Log(MemHash(key2))
	t.Log(MemHash(key1))
	t.Log(MemHash(key1))
	t.Log(MemHash(key1))
	t.Log(MemHash(key1))
}
