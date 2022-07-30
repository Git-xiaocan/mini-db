package zset

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func InitZSet() *SortedSet {
	zset := New()

	zset.ZAdd("myzset", 7, "fvc")
	zset.ZAdd("myzset", 9, "sdc")
	zset.ZAdd("myzset", 11, "fgd")
	zset.ZAdd("myzset", 12, "bdc")
	zset.ZAdd("myzset", 13, "dad")
	zset.ZAdd("myzset", 18, "asc")
	zset.ZAdd("myzset", 19, "abc")
	zset.ZAdd("myzset", 21, "sdf")
	zset.ZAdd("myzset", 22, "qqw")
	return zset
}

func TestSortedSet_ZAdd(t *testing.T) {
	zset := InitZSet()
	zset.ZAdd("myzset", 33, "dds")
	c1 := zset.ZCard("myzset")
	assert.Equal(t, 10, c1)
}

func TestSortedSet_ZScore(t *testing.T) {
	zset := InitZSet()
	score, ok := zset.ZScore("myzset", "bdc")
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(12), score)
	s2, ok := zset.ZScore("myzset", "nnn")

	assert.Equal(t, false, ok)
	assert.Equal(t, float64(0), s2)
}
func TestSortedSet_ZRank(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	rank1 := zset.ZRank(key, "fvc")
	assert.Equal(t, int64(0), rank1)
	rank2 := zset.ZRank(key, "sdc")
	assert.Equal(t, int64(1), rank2)
	rank3 := zset.ZRank(key, "not exist")
	assert.Equal(t, int64(-1), rank3)
}

func TestSortedSet_ZRevRank(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	rank1 := zset.ZRevRank(key, "qqw")
	assert.Equal(t, int64(0), rank1)
	rank2 := zset.ZRevRank(key, "fvc")
	assert.Equal(t, int64(8), rank2)
	rank3 := zset.ZRevRank(key, "not exist")
	assert.Equal(t, int64(-1), rank3)
}

func TestSortedSet_ZRange(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	zRange1 := zset.ZRange(key, 0, 3)
	assert.Equal(t, []interface{}{"fvc", "sdc", "fgd", "bdc"}, zRange1)
	range2 := zset.ZRange(key, 0, -1)
	assert.Equal(t, []interface{}{"fvc", "sdc", "fgd", "bdc", "dad", "asc", "abc", "sdf", "qqw"}, range2)

}

func TestSortedSet_ZRangeWithScores(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	range1 := zset.ZRangeWithScores(key, 0, 1)
	assert.Equal(t, []interface{}{"fvc", float64(7), "sdc", float64(9)}, range1)
	range2 := zset.ZRangeWithScores(key, 0, -8)
	assert.Equal(t, []interface{}{"fvc", float64(7), "sdc", float64(9)}, range2)
}

func TestSortedSet_ZRevRange(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	revRange := zset.ZRevRange(key, 0, -1)

	assert.NotNil(t, revRange)
	for _, v := range revRange {
		assert.NotNil(t, v)
	}
}

func TestSortedSet_ZGetByRank(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	rank := zset.ZGetByRank(key, 2)
	assert.Equal(t, []interface{}{"fgd", float64(11)}, rank)

}
func TestSortedSet_ZGetRevByRank(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	rank := zset.ZRevGetByRank(key, 0)
	assert.Equal(t, []interface{}{"qqw", float64(22)}, rank)
}
