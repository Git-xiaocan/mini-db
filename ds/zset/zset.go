package zset

import (
	"math"
	"math/rand"
)

const (
	maxLevel    = 32 //skip list的最大层数
	probability = 0.25
)

type EncodeKey func(key, subKey []byte) []byte
type (

	// skipList skipList struct
	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int64
		level  int16
	}
	// sklNode 跳表节点
	sklNode struct {
		member   string
		score    float64
		backward *sklNode
		level    []*sklLevel
	}

	sklLevel struct {
		forward *sklNode
		span    uint64
	}

	SortedSet struct {
		record map[string]*SortedSetNode
	}
	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}
)

// zSet

// New create a new sorted set
func New() *SortedSet {
	return &SortedSet{
		record: make(map[string]*SortedSetNode),
	}
}

//ZAdd adds the specified member with the specified score to the sorted set stored at key
func (z *SortedSet) ZAdd(key string, score float64, member string) {
	if !z.exist(key) {
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		z.record[key] = node
	}
	item := z.record[key]
	v, exist := item.dict[member]
	var node *sklNode
	if exist && score != v.score {
		item.skl.sklDelete(v.score, v.member)

	}
	node = item.skl.sklInsert(score, member)
	if node != nil {
		item.dict[member] = node
	}
}

// ZScore  returns the score of member in the sorted set at key
func (z *SortedSet) ZScore(key, member string) (score float64, ok bool) {
	if !z.exist(key) {
		return
	}
	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}

	return node.score, true
}

// ZCard returns the sorted set cardinality (number  of element ) of the sorted set  stored at key
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	return len(z.record[key].dict)
}

//ZRank returns the rank of member in the sorted set stored at key ,with the scores ordered from low to high
//The rank (or index) is 0-based ,with means that the member with the lowest score has rank 0.
//Returns -1 if the key or  member does not exist in the sorted set
func (z *SortedSet) ZRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}
	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}
	rank := z.record[key].skl.sklGetRank(v.score, member)
	rank--

	return rank
}

//ZRevRank returns the rank od member in the sorted set stored at key ,with the scores ordered from high to low
//the rank(or index) is 0-based ,with means that the member with the highest score has rank 0
// returns -1 if the key or index does not exist in the sorted set
func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}
	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}
	rank := z.record[key].skl.sklGetRank(v.score, member)

	return z.record[key].skl.length - rank

}

//ZIncrBy increments the score of member in the sorted set stored at key by increment.
//If member does not exist in the sorted set ，it is added with increment as its score (as if its previous score was 0.0)
//If key does not exist , a new sorted set with the specified member as its sole member is created
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}

	}
	z.ZAdd(key, increment, member)
	return increment
}

// ZRange returns the specified range of element in the sorted set stored at <key>
func (z *SortedSet) ZRange(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), false, false)
}

// ZRangeWithScores returns all the elements in the sorted set at key with a score between min and max  including elements with score equal to min or max.
// the elements are considered to be ordered from low to high scores.
func (z *SortedSet) ZRangeWithScores(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), false, true)

}

//ZRevRange returns the specified range of elements in the sorted set stored at key
// the elements are considered to be ordered form the highest to the lowest score
//descending  lexicographical order is used for elements with equal score.
func (z *SortedSet) ZRevRange(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), true, false)
}

func (z *SortedSet) ZRevRangeWithScores(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), true, true)
}
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}
	v, exist := z.record[key].dict[member]

	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true

	}

	return false
}
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return nil
	}
	score, member := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return

}
func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return nil
	}
	score, member := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if z.exist(key) || min > max {
		return
	}
	skl := z.record[key].skl
	minScore := skl.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	maxScore := skl.tail.score
	if max > maxScore {
		max = maxScore

	}
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}
	p = p.level[0].forward
	for p != nil {
		if p.score > max {
			break
		}
		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}
	return

}
func (z *SortedSet) ZRevScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) {
		return
	}
	skl := z.record[key].skl
	minScore := skl.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	maxScore := skl.tail.score
	if max > maxScore {
		max = maxScore
	}
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		if p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}
	for p != nil {
		if p.score < min {
			break
		}
		val = append(val, p.member, p.score)
		p = p.backward
	}
	return
}

// ZKeyExist check if the key exists in zset
func (z *SortedSet) ZKeyExist(key string) bool {
	return z.exist(key)
}

// ZClear clear the key in zSet
func (z *SortedSet) ZClear(key string) {
	if z.ZKeyExist(key) {
		delete(z.record, key)
	}

}

// exist
func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

// todo
func (z *SortedSet) findRange(key string, start int64, end int64, reverse bool, withScores bool) (val []interface{}) {
	skl := z.record[key].skl
	length := skl.length
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += length
	}
	if start > end || start >= length {
		return

	}
	if end >= length {
		end = length - 1
	}
	span := (end - start) + 1
	var node *sklNode

	if reverse {

		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}
	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (float64, string) {
	skl := z.record[key].skl
	if rank < 0 || rank > skl.length {
		return math.MaxInt64, ""
	}
	if reverse {
		rank = skl.length - rank

	} else {
		rank++
	}
	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return math.MaxInt64, ""
	}
	node := z.record[key].dict[n.member]
	if node == nil {
		return math.MaxInt64, ""
	}
	return node.score, node.member

}

// newSkipList todo
func newSkipList() *skipList {

	return &skipList{
		level: 1,
		head:  SklNewNode(maxLevel, 0, ""),
	}
}

// skipList

func SklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}
	for i := range node.level {
		node.level[i] = new(sklLevel)
	}
	return node
}

func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	rank := make([]uint64, maxLevel)
	updates := make([]*sklNode, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// 查找当前层级 小于score 的最后一个节点
		if p.level[i] != nil {
			for p.level[i].forward != nil && (p.level[i].forward.score < score || (p.level[i].forward.score == score && p.level[i].forward.member < member)) {

				rank[i] += p.level[i].span
				p = p.level[i].forward
			}
		}

		updates[i] = p
	}
	// 获取随机层数
	level := randomLevel()

	if level > skl.level {
		for i := skl.level; i < level; i++ {
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	p = SklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1

	}
	// 考虑随机level 层级较小的情况
	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == skl.head {
		p.backward = nil
	} else {
		p.backward = updates[0]
	}
	// 最下面一层为双向链表
	if p.level[0].forward != nil {
		p.level[0].forward.backward = p
	} else {
		skl.tail = p
	}
	skl.length++
	return p
}

func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		if updates[i].level[i].forward == p {
			// 当前节点的span 加上下一个节点的span 减 1
			updates[i].level[i].span += p.level[i].span - 1
			updates[i].level[i].forward = p.level[i].forward
		} else {
			updates[i].level[i].span--
		}
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p.backward
	} else {
		skl.tail = p.backward
	}
	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}
	skl.length--

}

func (skl *skipList) sklDelete(score float64, member string) {
	updates := make([]*sklNode, maxLevel)

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && (p.level[i].forward.score < score || (p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		updates[i] = p
	}
	p = p.level[0].forward
	if p != nil && score == p.score && p.member == member {
		skl.sklDeleteNode(p, updates)
	}

}

func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {
			rank += p.level[i].span
			p = p.level[i].forward
		}
		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	var traversed uint64 = 0
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && (traversed+p.level[i].span <= rank) {
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank {
			return p
		}
	}
	return nil

}

// randomLevel 随机层数
func randomLevel() int16 {
	var level int16 = 1
	for float32(rand.Int31()&0xFFFF) < (probability * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}
