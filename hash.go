package minidb

import (
	"bytes"
	"errors"
	"math"
	"minidb/ds/art"
	"minidb/logfile"
	"minidb/logger"
	"minidb/utils"
	"regexp"
	"strconv"
)

func (db *MiniDB) HSet(key []byte, args ...[]byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	if len(args) == 0 || len(args)&1 == 1 {
		return ErrWrongNumberOfArgs
	}
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.hashIndex.trees[string(key)]
	for i := 0; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		hashKey := db.encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Value: value}
		pos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return err
		}
		ent := &logfile.LogEntry{Key: field, Value: value}
		_, size := logfile.EncodeEntry(ent)
		pos.entrySize = size
		err = db.updateIndexTree(idxTree, ent, pos, true, Hash)
		if err != nil {
			return err
		}
	}
	return nil
}
func (db *MiniDB) HSetNX(key, field, value []byte) (bool, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, _ := db.getVal(idxTree, field, Hash)
	if val != nil {
		return false, nil
	}
	hashKey := db.encodeKey(key, field)
	entry := &logfile.LogEntry{Key: hashKey, Value: value}
	pos, err := db.writeLogEntry(entry, Hash)
	if err != nil {
		return false, err
	}
	ent := &logfile.LogEntry{Key: field, Value: value}
	_, size := logfile.EncodeEntry(ent)
	pos.entrySize = size
	err = db.updateIndexTree(idxTree, entry, pos, true, Hash)
	if err != nil {
		return false, err
	}
	return true, nil

}
func (db *MiniDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	val, err := db.getVal(idxTree, field, Hash)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return val, err

}
func (db *MiniDB) HMGet(key []byte, fields ...[]byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	length := len(fields)
	idxTree := db.hashIndex.trees[string(key)]
	var vals [][]byte
	// key not exist
	if idxTree == nil {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
		return vals, nil
	}
	//key exist
	for _, field := range fields {
		val, err := db.getVal(idxTree, field, Hash)
		if err == ErrKeyNotFound {
			vals = append(vals, nil)
		} else {
			vals = append(vals, val)
		}
	}
	return vals, nil
}
func (db *MiniDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	idxTree := db.hashIndex.trees[string(key)]

	if idxTree == nil {
		return 0, nil
	}
	var count int
	for _, field := range fields {
		val, updated := idxTree.Delete(field)
		if !updated {
			continue
		}
		hashKey := db.encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Type: logfile.TypeDelete}
		pos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return 0, err
		}
		db.sendDiscard(val, updated, Hash)
		_, size := logfile.EncodeEntry(entry)
		node := &indexNode{entrySize: size, fid: pos.fid}
		select {
		case db.discards[Hash].valChan <- node:
		default:
			logger.Warn("send discard chan fail")
		}
		count++

	}
	return count, nil

}
func (db *MiniDB) HExists(key, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return false, nil
	}
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}
	return val != nil, nil
}
func (db *MiniDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}
	return idxTree.Size()
}
func (db *MiniDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	var values [][]byte
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		values = append(values, node.Key())

	}
	return values, nil
}
func (db *MiniDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	var values [][]byte
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getVal(idxTree, node.Key(), Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}
func (db *MiniDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	iter := idxTree.Iterator()
	pair := make([][]byte, idxTree.Size()*2)
	var index int
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getVal(idxTree, node.Key(), Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		pair[index], pair[index+1] = node.Key(), val
		index += 2
	}
	return pair[:index], nil
}
func (db *MiniDB) HStrLen(key []byte, field []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}
	val, err := db.getVal(idxTree, field, Hash)
	if err == ErrKeyNotFound {
		return 0
	}
	return len(val)
}
func (db *MiniDB) HScan(key []byte, prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	fields := idxTree.PrefixScan(prefix, count)
	if len(fields) == 0 {
		return nil, nil
	}
	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}
	var pairs = make([][]byte, len(fields)*2)
	var index int
	for _, field := range fields {
		if reg != nil && !reg.Match(field) {
			continue
		}
		val, err := db.getVal(idxTree, field, Hash)
		if err != nil {
			return nil, err
		}
		pairs[index], pairs[index+1] = field, val
		index += 2
	}
	return pairs, nil
}
func (db *MiniDB) HIncrBy(key, field []byte, incr int64) (int64, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err

	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valI64, err := utils.StrToInt64(string(val))
	if err != nil {
		return 0, ErrWrongValueType
	}
	if incr < 0 && valI64 < 0 && incr < (math.MinInt64-valI64) ||
		incr > 0 && valI64 > 0 && incr > (math.MaxInt64-valI64) {
		return 0, ErrIntegerOverflow

	}
	valI64 += incr
	val = []byte(strconv.FormatInt(valI64, 10))
	encodeKey := db.encodeKey(key, field)
	entry := &logfile.LogEntry{Key: encodeKey, Value: val}
	pos, err := db.writeLogEntry(entry, Hash)
	if err != nil {
		return 0, err
	}
	et := &logfile.LogEntry{Key: field, Value: val}
	_, size := logfile.EncodeEntry(entry)
	pos.entrySize = size
	err = db.updateIndexTree(idxTree, et, pos, true, Hash)
	if err != nil {
		return 0, err
	}
	return valI64, nil

}
