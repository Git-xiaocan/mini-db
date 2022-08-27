package minidb

import (
	"minidb/ds/art"
	"minidb/logfile"
	"minidb/logger"
	"minidb/utils"
)

func (db *MiniDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.zsetIndex.trees[string(key)]
	scoreBuf := []byte(utils.Float64ToStr(score))
	encodeKey := db.encodeKey(key, scoreBuf)

	entry := &logfile.LogEntry{Key: encodeKey, Value: member}

	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}
	_, size := logfile.EncodeEntry(entry)
	ent := &logfile.LogEntry{Key: sum, Value: member}
	pos.entrySize = size
	if err := db.updateIndexTree(idxTree, ent, pos, true, ZSet); err != nil {
		return err
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))

	return nil
}
func (db *MiniDB) ZScore(key, member []byte) (float64, bool) {

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return 0, false
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	return db.zsetIndex.indexes.ZScore(string(key), string(sum))

}
func (db *MiniDB) ZRem(key, member []byte) error {

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}

	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	score, _ := db.zsetIndex.indexes.ZScore(string(key), string(sum))

	ok := db.zsetIndex.indexes.ZRem(string(key), string(sum))
	if !ok {
		return nil
	}

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.zsetIndex.trees[string(key)]
	oldVal, updated := idxTree.Delete(sum)
	db.sendDiscard(oldVal, updated, ZSet)

	scoreBuf := []byte(utils.Float64ToStr(score))
	encodeKey := db.encodeKey(key, scoreBuf)

	ent := &logfile.LogEntry{Key: encodeKey, Value: member, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, ZSet)
	if err != nil {
		return err
	}
	_, size := logfile.EncodeEntry(ent)
	node := &indexNode{entrySize: size, fid: pos.fid}
	select {
	case db.discards[ZSet].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil

}
func (db *MiniDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.indexes.ZCard(string(key))
}
func (db *MiniDB) ZRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, false)
}
func (db *MiniDB) ZRevRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, true)
}
func (db *MiniDB) ZRank(key []byte, member []byte) (rank int, ok bool) {
	return db.ZRankInternal(key, member, false)
}
func (db *MiniDB) ZRevRank(key []byte, member []byte) (rank int, ok bool) {
	return db.ZRankInternal(key, member, true)
}

func (db *MiniDB) zRangeInternal(key []byte, start, stop int, rev bool) ([][]byte, error) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.zsetIndex.trees[string(key)]
	var res [][]byte
	var values []interface{}
	if rev {
		values = db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
	} else {
		values = db.zsetIndex.indexes.ZRange(string(key), start, stop)
	}
	for _, value := range values {
		v, _ := value.(string)
		if val, err := db.getVal(idxTree, []byte(v), ZSet); err != nil {
			return nil, err
		} else {
			res = append(res, val)
		}
	}
	return res, nil
}
func (db *MiniDB) ZRankInternal(key, member []byte, rev bool) (rank int, ok bool) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	var result int64
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	if rev {
		result = db.zsetIndex.indexes.ZRevRank(string(key), string(sum))
	} else {
		result = db.zsetIndex.indexes.ZRank(string(key), string(sum))
	}
	if result != -1 {
		ok = true
		rank = int(result)
	}
	return

}
