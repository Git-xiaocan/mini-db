package minidb

import (
	"minidb/ds/art"
	"minidb/logfile"
	"minidb/logger"
	"minidb/utils"
)

func (db *MiniDB) SAdd(key []byte, member ...[]byte) error {
	db.setsIndex.mu.Lock()
	defer db.setsIndex.mu.Unlock()

	if db.setsIndex.trees[string(key)] == nil {
		db.setsIndex.trees[string(key)] = art.NewArt()
	}
	indexTree := db.setsIndex.trees[string(key)]

	for _, mem := range member {
		if len(mem) == 0 {
			continue
		}
		if err := db.setsIndex.mur.Write(mem); err != nil {
			return err
		}
		sum := db.setsIndex.mur.EncodeSum128()
		db.setsIndex.mur.Reset()
		ent := &logfile.LogEntry{Key: key, Value: mem}
		valPos, err := db.writeLogEntry(ent, Set)
		if err != nil {
			return err
		}
		entry := &logfile.LogEntry{Key: sum, Value: mem}
		_, size := logfile.EncodeEntry(entry)
		valPos.entrySize = size
		if err := db.updateIndexTree(indexTree, entry, valPos, true, Set); err != nil {
			return err
		}
	}
	return nil
}

func (db *MiniDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setsIndex.mu.Lock()
	defer db.setsIndex.mu.Unlock()

	if db.setsIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.setsIndex.trees[string(key)]
	var values [][]byte
	iterator := idxTree.Iterator()
	for iterator.HasNext() && count > 0 {
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	for _, value := range values {
		if err := db.sremInterval(key, value); err != nil {
			return nil, err
		}
	}

	return values, nil

}
func (db *MiniDB) SRem(key []byte, member ...[]byte) error {
	db.setsIndex.mu.Lock()
	defer db.setsIndex.mu.Unlock()
	if db.setsIndex.trees[string(key)] == nil {
		return nil
	}
	for _, value := range member {

		if err := db.sremInterval(key, value); err != nil {
			return err
		}
	}
	return nil

}
func (db *MiniDB) SIsMember(key []byte, member []byte) bool {
	db.setsIndex.mu.RLock()
	defer db.setsIndex.mu.RUnlock()
	if db.setsIndex.trees[string(key)] == nil {
		return false
	}
	idxTree := db.setsIndex.trees[string(key)]
	if err := db.setsIndex.mur.Write(member); err != nil {
		return false
	}
	sum128 := db.setsIndex.mur.EncodeSum128()
	db.setsIndex.mur.Reset()

	node := idxTree.Get(sum128)
	return node != nil
}
func (db *MiniDB) SMembers(key []byte) ([][]byte, error) {
	db.setsIndex.mu.RLock()
	defer db.setsIndex.mu.RUnlock()
	return db.sMembers(key)
}
func (db *MiniDB) SCard(key []byte) int {
	db.setsIndex.mu.RLock()
	defer db.setsIndex.mu.RUnlock()
	if db.setsIndex.trees[string(key)] == nil {
		return 0
	}
	return db.setsIndex.trees[string(key)].Size()
}

func (db *MiniDB) SDiff(keys ...[]byte) ([][]byte, error) {
	db.setsIndex.mu.RLock()
	defer db.setsIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	firstSet, err := db.sMembers(keys[0])
	if err != nil {
		return nil, err
	}
	successSet := make(map[uint64]struct{})
	for _, key := range keys[1:] {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, mem := range members {
			hash := utils.MemHash(mem)
			if _, ok := successSet[hash]; !ok {
				successSet[hash] = struct{}{}
			}
		}
	}
	if len(successSet) == 0 {
		return firstSet, err
	}
	var res [][]byte
	for _, value := range firstSet {
		hash := utils.MemHash(value)
		if _, ok := successSet[hash]; !ok {
			res = append(res, value)
		}
	}
	return res, nil

}
func (db *MiniDB) SUnion(keys ...[]byte) ([][]byte, error) {
	db.setsIndex.mu.RLock()
	defer db.setsIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	set := make(map[uint64]struct{})
	var res [][]byte

	for _, key := range keys {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, mem := range members {
			hash := utils.MemHash(mem)
			if _, ok := set[hash]; !ok {
				set[hash] = struct{}{}
				res = append(res, mem)

			}
		}
	}
	return res, nil
}

func (db *MiniDB) sremInterval(key, member []byte) error {
	idxTree := db.setsIndex.trees[string(key)]
	if err := db.setsIndex.mur.Write(member); err != nil {
		return err
	}
	sum := db.setsIndex.mur.EncodeSum128()
	db.setsIndex.mur.Reset()
	val, updated := idxTree.Delete(sum)
	if !updated {
		return nil
	}
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	valPos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return err
	}
	db.sendDiscard(val, true, Set)
	_, size := logfile.EncodeEntry(entry)
	// the deleted entry itself is also invalid
	node := &indexNode{entrySize: size, fid: valPos.fid}
	select {
	case db.discards[Set].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}
func (db *MiniDB) sMembers(key []byte) ([][]byte, error) {
	if db.setsIndex.trees[string(key)] == nil {
		return nil, nil
	}
	var values [][]byte
	idxTree := db.setsIndex.trees[string(key)]
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, _ := iter.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil

}
