package art

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewArt()
	art.Put([]byte("bba"), 123)
	art.Put([]byte("bbb"), 123)
	art.Put([]byte("bbc"), 123)
	art.Put([]byte("bbd"), 123)
	art.Put([]byte("bbf"), 123)
	art.Put([]byte("bbe"), 123)
	art.Put([]byte("bbg"), 123)
	art.Put([]byte("bca"), 123)
	art.Put([]byte("bbd"), 123)
	art.Put([]byte("bac"), 123)
	keys1 := art.PrefixScan([]byte("bb"), -1)
	assert.Equal(t, 0, len(keys1))
	keys2 := art.PrefixScan(nil, 0)
	assert.Equal(t, 0, len(keys2))
	keys3 := art.PrefixScan([]byte("b"), 1)
	assert.Equal(t, 1, len(keys3))
	keys4 := art.PrefixScan(nil, 10)
	assert.Equal(t, 9, len(keys4))

}
func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewArt()
	art.Put([]byte("bba"), 123)
	art.Put([]byte("bbb"), 123)
	art.Put([]byte("bbc"), 123)
	assert.Equal(t, 3, art.Size())
	art.Put([]byte("bbd"), 123)
	art.Put([]byte("bbf"), 123)
	art.Put([]byte("bbe"), 123)
	assert.Equal(t, 6, art.Size())

	art.Put([]byte("bbg"), 123)
	art.Put([]byte("bca"), 123)
	art.Put([]byte("bac"), 123)
	assert.Equal(t, 9, art.Size())

}
func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewArt()
	art.Put([]byte("bba"), 123)
	art.Put([]byte("bbb"), 123)
	art.Put([]byte("bbc"), 123)
	assert.Equal(t, 3, art.Size())
	v, updated := art.Delete([]byte("bba"))
	assert.Equal(t, 123, v.(int))
	assert.Equal(t, true, updated)
	assert.Equal(t, 2, art.Size())
	val := art.Get([]byte("bba"))
	assert.Nil(t, val)

}
