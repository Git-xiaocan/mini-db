package minidb

import (
	"bytes"
	"errors"
	"math"
	"minidb/logfile"
	"minidb/logger"
	"minidb/utils"
	"regexp"
	"strconv"
	"time"
)

func (db *MiniDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)

}
func (db *MiniDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}
func (db *MiniDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}
func (db *MiniDB) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return nil, err
	}
	oldVal, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(oldVal, updated, String)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return val, nil
}
func (db *MiniDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	val, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(val, updated, String)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")

	}
	return nil

}
func (db *MiniDB) SetEX(key, val []byte, ex time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	t := time.Now().Add(ex).Unix()
	entry := &logfile.LogEntry{Key: key, Value: val, ExpiredAt: t}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
}
func (db *MiniDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	//key exist in db
	if val != nil {
		return nil
	}
	entry := &logfile.LogEntry{Key: key, Value: value}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	return db.updateIndexTree(db.strIndex.idxTree, entry, valPos, true, String)
}
func (db *MiniDB) MSet(arg ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	if len(arg)%2 != 0 || len(arg) == 0 {
		return ErrWrongNumberOfArgs
	}
	for i := 0; i < len(arg); i += 2 {
		key, val := arg[i], arg[i+1]
		ent := &logfile.LogEntry{Key: key, Value: val}
		valPos, err := db.writeLogEntry(ent, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, ent, valPos, true, String)
		if err != nil {
			return err
		}
	}
	return nil
}
func (db *MiniDB) MSetNX(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	lens := len(args)
	if lens == 0 || lens%2 != 0 {
		return ErrWrongNumberOfArgs
	}
	//check each keys whether they are exists.
	for i := 0; i < lens; i += 2 {
		key := args[i]
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		// key exists in db , we discard the rest of key-value paris
		//it provides the atomicity of the method
		if val != nil {
			return nil
		}
	}
	addKeys := make(map[uint64]struct{})

	for i := 0; i < lens; i += 2 {
		key, val := args[i], args[i+1]
		hash := utils.MemHash(key)
		if _, ok := addKeys[hash]; ok {
			continue
		}
		entry := &logfile.LogEntry{Key: key, Value: val}
		pos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
		if err != nil {
			return err
		}
		addKeys[hash] = struct{}{}

	}
	return nil
}
func (db *MiniDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)

	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if oldVal != nil {
		value = append(oldVal, value...)
	}

	entry := &logfile.LogEntry{Key: key, Value: value}

	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, valPos, true, String)
}
func (db *MiniDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -1)

}
func (db *MiniDB) DecrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)

}
func (db *MiniDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, 1)
}
func (db *MiniDB) IncrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)
}
func (db *MiniDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongValueType
	}
	if incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64) ||
		incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64) {
		return 0, ErrIntegerOverflow
	}
	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	ent := &logfile.LogEntry{Key: key, Value: val}
	valPos, err := db.writeLogEntry(ent, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, ent, valPos, true, String)
	if err != nil {
		return 0, err
	}
	return valInt64, nil

}
func (db *MiniDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}
	return len(val)
}
func (db *MiniDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return 0
	}
	return db.strIndex.idxTree.Size()
}
func (db *MiniDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return nil, nil
	}
	keys := db.strIndex.idxTree.PrefixScan(prefix, count)
	if len(keys) == 0 {
		return nil, nil
	}
	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}
	values := make([][]byte, 2*len(keys))
	var index int
	for _, key := range keys {
		if reg != nil && !reg.Match(key) {
			continue
		}
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		values[index], values[index+1] = key, val
		index += 2

	}
	return values[:index], nil
}
