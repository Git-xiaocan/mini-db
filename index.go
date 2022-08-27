package minidb

import (
	"io"
	"log"
	"minidb/ds/art"
	"minidb/logfile"
	"minidb/logger"
	"minidb/utils"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type DataType = int8

const (
	String DataType = iota
	Set
	List
	Hash
	ZSet
)

func (db *MiniDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()
		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				log.Fatal("logfile is nil ,failed to open db")
			}
			var offset int64
			for {
				entry, size, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file error ,failed to open db :%v", err)
				}
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += size

			}
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}

	}
	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for t := String; t < logFileTypeNum; t++ {
		go iterateAndHandle(t, wg)
	}
	wg.Wait()
	return nil
}
func (db *MiniDB) buildStrIndex(entry *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(entry.Key)
		return
	}
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{entrySize: size, fid: pos.fid, offset: pos.offset}
	if entry.ExpiredAt != 0 {
		node.expiredAt = entry.ExpiredAt
	}
	if db.opts.IndexMode == KeyValueMemMode {
		node.value = entry.Value
	}
	db.strIndex.idxTree.Put(entry.Key, node)
}
func (db *MiniDB) buildSetIndex(entry *logfile.LogEntry, pos *valuePos) {
	if db.setsIndex.trees[string(entry.Key)] == nil {
		db.setsIndex.trees[string(entry.Key)] = art.NewArt()
	}

	idxTree := db.setsIndex.trees[string(entry.Key)]
	ts := time.Now().Unix()
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt < ts) {
		idxTree.Delete(entry.Value)
		return
	}

	if err := db.setsIndex.mur.Write(entry.Value); err != nil {
		logger.Warnf("failed to write mur hash :%v", err)
		return
	}

	sum := db.setsIndex.mur.EncodeSum128()
	db.setsIndex.mur.Reset()
	_, size := logfile.EncodeEntry(entry)

	node := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		node.value = entry.Value
	}
	if entry.ExpiredAt != 0 {
		node.expiredAt = entry.ExpiredAt
	}
	idxTree.Put(sum, node)

}
func (db *MiniDB) buildHashIndex(entry *logfile.LogEntry, pos *valuePos) {
	key, field := db.decodeKey(entry.Key)
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.hashIndex.trees[string(key)]
	ts := time.Now().Unix()
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt < ts) {
		idxTree.Delete(field)
		return
	}
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if entry.ExpiredAt != 0 {
		node.expiredAt = entry.ExpiredAt
	}
	if db.opts.IndexMode == KeyValueMemMode {
		node.value = entry.Value
	}
	idxTree.Put(field, node)

}
func (db *MiniDB) buildListIndex(entry *logfile.LogEntry, pos *valuePos) {
	var listKey = entry.Key
	if entry.Type != logfile.TypeListMeta {
		listKey, _ = db.decodeListKey(listKey)
	}
	if db.listIndex.trees[string(listKey)] == nil {
		db.listIndex.trees[string(listKey)] = art.NewArt()
	}

	idxTree := db.listIndex.trees[string(listKey)]
	ts := time.Now().Unix()
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt < ts) {
		idxTree.Delete(entry.Key)
		return
	}
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if entry.ExpiredAt != 0 {
		node.expiredAt = entry.ExpiredAt
	}
	if db.opts.IndexMode == KeyValueMemMode {
		node.value = entry.Value
	}
	idxTree.Put(entry.Key, node)

}
func (db *MiniDB) buildZSetIndex(entry *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt <= ts) {
		db.zsetIndex.indexes.ZRem(string(entry.Key), string(entry.Value))
		if db.zsetIndex.trees[string(entry.Key)] != nil {
			db.zsetIndex.trees[string(entry.Key)].Delete(entry.Key)
		}
		return
	}
	key, scoreBuf := db.decodeKey(entry.Key)
	score, _ := utils.StrToFloat64(string(scoreBuf))
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.zsetIndex.trees[string(key)]
	if err := db.zsetIndex.murhash.Write(entry.Value); err != nil {
		logger.Fatalf("failed to write mur hash :%v", err)
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		node.value = entry.Value
	}
	if entry.ExpiredAt != 0 {
		node.expiredAt = entry.ExpiredAt
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	idxTree.Put(sum, node)

}
func (db *MiniDB) buildIndex(dataType DataType, entry *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrIndex(entry, pos)
	case List:
		db.buildListIndex(entry, pos)
	case Set:
		db.buildSetIndex(entry, pos)
	case ZSet:
		db.buildZSetIndex(entry, pos)
	case Hash:
		db.buildHashIndex(entry, pos)

	}
}

func (db *MiniDB) updateIndexTree(idx *art.AdaptiveRadixTree, entry *logfile.LogEntry, pos *valuePos, sendDiscard bool, dataType DataType) error {

	size := pos.entrySize

	if dataType == String || dataType == List {
		_, size = logfile.EncodeEntry(entry)
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}

	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = entry.Value
	}
	if entry.ExpiredAt != 0 {
		idxNode.expiredAt = entry.ExpiredAt
	}
	oldVal, updated := idx.Put(entry.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated, dataType)
	}
	return nil
}

func (db *MiniDB) getVal(tree *art.AdaptiveRadixTree, key []byte, dataType DataType) ([]byte, error) {
	rawValue := tree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}
	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, ErrKeyNotFound
	}
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}
	logFile := db.getActiveLogFile(dataType)
	if idxNode.fid != logFile.Fid {
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}
	entry, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	// key exist , but is invalid
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt <= ts) {
		return nil, ErrKeyNotFound
	}
	return entry.Value, nil
}
