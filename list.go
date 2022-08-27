package minidb

import (
	"encoding/binary"
	"minidb/ds/art"
	"minidb/logfile"
	"minidb/logger"
)

func (db *MiniDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewArt()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

func (db *MiniDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}
func (db *MiniDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewArt()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}
func (db *MiniDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *MiniDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}
func (db *MiniDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

func (db *MiniDB) LMove(srcKey, dstKey []byte, srcLeft, dstLet bool) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	value, err := db.popInternal(srcKey, srcLeft)
	if err != nil {
		return nil, err
	}
	if db.listIndex.trees[string(dstKey)] == nil {
		db.listIndex.trees[string(dstKey)] = art.NewArt()
	}
	if err := db.pushInternal(dstKey, value, dstLet); err != nil {
		return nil, err
	}
	return value, nil

}
func (db *MiniDB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}
	head, tail, err := db.listMeta(idxTree, key)
	if err != nil {
		return 0
	}
	return int(tail - head - 1)
}
func (db *MiniDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	head, tail, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	seq, err := db.listSequence(head, tail, index)
	if err != nil {
		return nil, err
	}
	if seq >= tail || seq <= head {
		return nil, ErrWrongIndex
	}
	encodeKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encodeKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}
func (db *MiniDB) LSet(key []byte, value []byte, index int) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return ErrKeyNotFound
	}
	head, tail, err := db.listMeta(idxTree, key)
	if err != nil {
		return err

	}
	seq, err := db.listSequence(head, tail, index)
	if err != nil {
		return err
	}
	if seq >= tail || seq <= head {
		return ErrWrongIndex
	}
	encodeKey := db.encodeListKey(key, seq)
	entry := &logfile.LogEntry{Key: encodeKey, Value: value}
	pos, err := db.writeLogEntry(entry, List)
	if err != nil {
		return err

	}
	return db.updateIndexTree(idxTree, entry, pos, true, List)

}
func (db *MiniDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return nil, ErrKeyNotFound
	}
	head, tail, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	startSeq, err := db.listSequence(head, tail, start)
	if err != nil {
		return nil, err
	}
	endSeq, err := db.listSequence(head, tail, end)
	if err != nil {
		return nil, err
	}
	if endSeq >= tail {
		endSeq = tail - 1
	}
	if startSeq <= head {
		startSeq = head + 1
	}
	if startSeq <= head || endSeq >= tail || startSeq > endSeq {
		return nil, ErrWrongIndex
	}
	for seq := startSeq; seq <= endSeq; seq++ {
		listKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, listKey, List)
		if err != nil {
			return nil, err

		}
		values = append(values, val)

	}
	return values, nil

}

func (db *MiniDB) listMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	val, err := db.getVal(idxTree, key, List)

	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

func (db *MiniDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq uint32, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	entry := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	pos, err := db.writeLogEntry(entry, List)
	if err != nil {
		return err
	}
	return db.updateIndexTree(idxTree, entry, pos, true, List)
}
func (db *MiniDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}
func (db *MiniDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}
func (db *MiniDB) pushInternal(key []byte, value []byte, left bool) error {
	idxTree := db.listIndex.trees[string(key)]
	head, tail, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}
	var seq = head
	if !left {
		seq = tail
	}
	listKey := db.encodeListKey(key, seq)
	entry := &logfile.LogEntry{Key: listKey, Value: value}
	pos, err := db.writeLogEntry(entry, List)
	if err != nil {
		return err
	}
	if err := db.updateIndexTree(idxTree, entry, pos, true, List); err != nil {
		return err
	}
	if left {
		head--
	} else {
		tail++
	}
	return db.saveListMeta(idxTree, key, head, tail)

}
func (db *MiniDB) popInternal(key []byte, left bool) ([]byte, error) {
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	head, tail, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	size := tail - head - 1
	if size <= 0 {
		if head != initialListSeq || tail != initialListSeq+1 {
			head = initialListSeq
			tail = initialListSeq + 1
			_ = db.saveListMeta(idxTree, key, head, tail)
		}
		return nil, nil
	}
	var seq = head + 1
	if !left {
		seq = tail - 1
	}
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	oldVal, updated := idxTree.Delete(encKey)
	if left {
		head++
	} else {
		tail--
	}
	if err := db.saveListMeta(idxTree, key, head, tail); err != nil {
		return nil, err
	}
	db.sendDiscard(oldVal, updated, List)
	_, entrySize := logfile.EncodeEntry(ent)
	idx := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].valChan <- idx:
	default:
		logger.Warn("send to discard chan fail")

	}
	return val, nil
}

func (db *MiniDB) listSequence(head, tail uint32, index int) (uint32, error) {
	var seq uint32
	if index >= 0 {
		seq = head + uint32(index) + 1
	} else {
		seq = tail - uint32(-index)
	}
	return seq, nil
}
