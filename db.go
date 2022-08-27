package minidb

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"minidb/ds/art"
	"minidb/ds/zset"
	"minidb/flock"
	"minidb/logfile"
	"minidb/logger"
	"minidb/utils"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ErrLogFileNotFound   = errors.New("log file not found")
	ErrKeyNotFound       = errors.New("key not found")
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")
	// ErrWrongValueType value is not a number
	ErrWrongValueType  = errors.New("value is not an integer")
	ErrIntegerOverflow = errors.New("increment or decrement overflow")
	// ErrWrongIndex index is out of range
	ErrWrongIndex = errors.New("index is out of range")
	ErrGcRunning  = errors.New("log file gc is running ,retry later")
)

const (
	initialListSeq   = math.MaxUint32 / 2
	logFileTypeNum   = 5
	discardFilePath  = "DISCARD"
	lockFileName     = "FLOCK"
	encodeHeaderSize = 10
)

type archivedFiles map[uint32]*logfile.LogFile

type MiniDB struct {
	activeLogFiles   map[DataType]*logfile.LogFile
	archivedLogFiles map[DataType]archivedFiles
	fidMap           map[DataType][]uint32
	discards         map[DataType]*discard
	fileLock         *flock.FileLockGuard
	mu               sync.RWMutex
	opts             Options
	closed           uint32
	gcState          int32
	strIndex         *strIndex
	setsIndex        *setsIndex
	listIndex        *listIndex
	hashIndex        *hashIndex
	zsetIndex        *zsetIndex
}

type valuePos struct {
	fid       uint32
	offset    int64
	entrySize int
}

type indexNode struct {
	value     []byte
	fid       uint32
	offset    int64
	entrySize int
	expiredAt int64
}
type strIndex struct {
	mu      *sync.RWMutex
	idxTree *art.AdaptiveRadixTree
}
type setsIndex struct {
	mu    *sync.RWMutex
	mur   *utils.MurHash128
	trees map[string]*art.AdaptiveRadixTree
}
type listIndex struct {
	mu    *sync.RWMutex
	trees map[string]*art.AdaptiveRadixTree
}
type hashIndex struct {
	mu    *sync.RWMutex
	trees map[string]*art.AdaptiveRadixTree
}
type zsetIndex struct {
	mu      *sync.RWMutex
	indexes *zset.SortedSet
	murhash *utils.MurHash128
	trees   map[string]*art.AdaptiveRadixTree
}

func newStrsIndex() *strIndex {
	return &strIndex{
		idxTree: art.NewArt(),
		mu:      new(sync.RWMutex),
	}
}
func newSetsIndex() *setsIndex {
	return &setsIndex{
		mu:    new(sync.RWMutex),
		mur:   utils.NewMurHash128(),
		trees: make(map[string]*art.AdaptiveRadixTree),
	}
}

func newListIndex() *listIndex {
	return &listIndex{
		mu:    new(sync.RWMutex),
		trees: make(map[string]*art.AdaptiveRadixTree),
	}
}

func newHashIndex() *hashIndex {
	return &hashIndex{
		mu:    new(sync.RWMutex),
		trees: make(map[string]*art.AdaptiveRadixTree),
	}

}

func newZSetIndex() *zsetIndex {
	return &zsetIndex{
		indexes: zset.New(),
		murhash: utils.NewMurHash128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}
func Open(opt Options) (*MiniDB, error) {
	if !utils.PathExist(opt.DBPath) {
		if err := os.MkdirAll(opt.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	lockPath := filepath.Join(opt.DBPath, lockFileName)
	fileFlock, err := flock.AcquireFileFlock(lockPath, false)
	if err != nil {
		return nil, err
	}
	miniDB := &MiniDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		fileLock:         fileFlock,
		opts:             opt,
		strIndex:         newStrsIndex(),
		setsIndex:        newSetsIndex(),
		listIndex:        newListIndex(),
		hashIndex:        newHashIndex(),
		zsetIndex:        newZSetIndex(),
	}
	if err := miniDB.initDiscard(); err != nil {
		return nil, err
	}
	if err := miniDB.loadLogFiles(); err != nil {
		return nil, err
	}
	if err := miniDB.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}
	// todo
	go miniDB.handleLogFileGC()
	return miniDB, nil

}

func (db *MiniDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.fileLock != nil {
		_ = db.fileLock.Release()
	}
	for _, activeFIle := range db.activeLogFiles {

		_ = activeFIle.Close()
	}
	for _, archived := range db.archivedLogFiles {
		for _, file := range archived {
			_ = file.Sync()
			_ = file.Close()
		}
	}
	for _, dis := range db.discards {
		dis.closeChan()
	}
	atomic.StoreUint32(&db.closed, 1)

	db.strIndex = nil
	db.hashIndex = nil
	db.listIndex = nil
	db.zsetIndex = nil
	db.setsIndex = nil
	return nil

}

func (db *MiniDB) Sync() error {
	db.mu.Lock()

	defer db.mu.Unlock()
	for _, activeFile := range db.activeLogFiles {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	for _, dis := range db.discards {
		if err := dis.sync(); err != nil {
			return err
		}
	}
	return nil
}
func (db *MiniDB) Backup(path string) error {
	if atomic.LoadInt32(&db.gcState) > 0 {
		return ErrGcRunning
	}
	if err := db.Sync(); err != nil {
		return err
	}
	if !utils.PathExist(path) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return utils.CopyDir(db.opts.DBPath, path)

}
func (db *MiniDB) initDiscard() error {
	discardPath := filepath.Join(db.opts.DBPath, discardFilePath)
	if !utils.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}

	}
	discards := make(map[DataType]*discard)
	for i := String; i < logFileTypeNum; i++ {
		name := logfile.FileNameMap[logfile.FileType(i)] + discardFileName
		discard, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = discard
	}
	db.discards = discards
	return nil
}

func (db *MiniDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	dir, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}
	fidMap := make(map[DataType][]uint32)
	for _, file := range dir {
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(logfile.FileTypeMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}
	db.fidMap = fidMap
	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		opt := db.opts
		for i, fid := range fids {
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(opt.IoType)
			lf, err := logfile.OpenLogFile(opt.DBPath, fid, opt.LogFileSizeThreshold, ftype, iotype)
			if err != nil {
				return err
			}
			if i != len(fids)-1 {
				db.archivedLogFiles[dataType][fid] = lf
			} else {
				db.activeLogFiles[dataType] = lf
			}

		}
	}
	return nil

}
func (db *MiniDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key[:])
		copy(buf[index+len(key):], subKey[:])
		return buf
	}
	return header[:index]

}
func (db *MiniDB) decodeKey(key []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(key[index:])
	index += i
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]

}

func (db *MiniDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	opts := db.opts
	ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
	lf, err := logfile.OpenLogFile(opts.DBPath, logfile.InitialLogFileId, opts.LogFileSizeThreshold, ftype, iotype)
	if err != nil {
		return err
	}
	db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = lf

	return nil
}

func (db *MiniDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}
	opts := db.opts

	// todo
	entry, esize := logfile.EncodeEntry(ent)
	if activeLogFile.WriteAt+int64(esize) > opts.LogFileSizeThreshold {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
		db.mu.Lock()
		activeFileId := activeLogFile.Fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// open a new logfile
		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, ftype, iotype)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
		db.activeLogFiles[dataType] = lf
		activeLogFile = lf
		db.mu.Unlock()

	}
	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	if err := activeLogFile.Write(entry); err != nil {
		return nil, err
	}

	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

func (db *MiniDB) sendDiscard(oldVal interface{}, updated bool, dataType DataType) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discards[dataType].valChan <- node:
	default:
		logger.Warn("send to discard  chan fail")
	}

}

func (db *MiniDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}
func (db *MiniDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFiles[dataType] != nil {
		lf = db.archivedLogFiles[dataType][fid]
	}
	return lf
}

func (db *MiniDB) RunGC(dataType DataType, specifiedId int, gcRatio float64) error {
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)
	rewriteStrs := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.setsIndex.mu.Unlock()
		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}
		node := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			pos, err := db.writeLogEntry(ent, String)
			if err != nil {
				return err
			}
			// update index
			if err := db.updateIndexTree(db.strIndex.idxTree, ent, pos, false, String); err != nil {
				return err
			}
		}
		return nil
	}
	rewriteSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.setsIndex.mu.Lock()
		defer db.setsIndex.mu.Unlock()
		if db.setsIndex.trees[string(ent.Key)] == nil {
			return nil
		}
		idxTree := db.setsIndex.trees[string(ent.Key)]
		if err := db.setsIndex.mur.Write(ent.Value); err != nil {
			return err
		}
		sum := db.setsIndex.mur.EncodeSum128()
		db.setsIndex.mur.Reset()
		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			pos, err := db.writeLogEntry(ent, Set)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			pos.entrySize = size
			if err := db.updateIndexTree(idxTree, entry, pos, false, Set); err != nil {
				return err
			}

		}

		return nil
	}
	rewriteList := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()
		var listKey = ent.Key
		if ent.Type == logfile.TypeListMeta {
			listKey, _ = db.decodeListKey(ent.Key)
		}
		if db.listIndex.trees[string(listKey)] == nil {
			return nil

		}

		idxTree := db.listIndex.trees[string(listKey)]

		indexVal := idxTree.Get(listKey)
		if idxTree == nil {
			return nil
		}
		node := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			pos, err := db.writeLogEntry(ent, List)
			if err != nil {
				return err
			}
			// update index
			if err := db.updateIndexTree(idxTree, ent, pos, false, List); err != nil {
				return err
			}
		}
		return nil

	}
	rewriteHash := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()
		key, field := db.decodeKey(ent.Key)
		if db.hashIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.hashIndex.trees[string(key)]

		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}
		node := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			pos, err := db.writeLogEntry(ent, Hash)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: field, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			pos.entrySize = size
			// update index
			if err := db.updateIndexTree(idxTree, entry, pos, false, Hash); err != nil {
				return err
			}

		}
		return nil

	}
	rewriteZSet := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.zsetIndex.mu.Lock()
		defer db.zsetIndex.mu.Unlock()
		key, _ := db.decodeKey(ent.Key)
		if db.zsetIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.zsetIndex.trees[string(key)]
		if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
			return err
		}
		sum := db.zsetIndex.murhash.EncodeSum128()
		db.zsetIndex.murhash.Reset()
		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			pos, err := db.writeLogEntry(ent, ZSet)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			pos.entrySize = size
			if err := db.updateIndexTree(idxTree, entry, pos, false, ZSet); err != nil {
				return err
			}
		}
		return nil
	}
	activeFile := db.getActiveLogFile(dataType)
	if activeFile == nil {
		return nil
	}
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}
	ccl, err := db.discards[dataType].getCCL(activeFile.Fid, gcRatio)
	if err != nil {
		return err
	}
	for _, fid := range ccl {
		if specifiedId > 0 && specifiedId != int(fid) {
			continue
		}
		archivedFile := db.getArchivedLogFile(dataType, fid)
		if archivedFile == nil {
			continue
		}
		var offset int64
		for {
			entry, size, err := archivedFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			var off = offset
			offset += size
			if entry.Type == logfile.TypeDelete {
				continue
			}
			var now = time.Now().Unix()
			if entry.ExpiredAt != 0 && entry.ExpiredAt <= now {
				continue
			}
			var rewriteErr error
			switch dataType {
			case String:
				rewriteErr = rewriteStrs(archivedFile.Fid, off, entry)
			case Set:
				rewriteErr = rewriteSets(archivedFile.Fid, off, entry)
			case ZSet:
				rewriteErr = rewriteZSet(archivedFile.Fid, off, entry)
			case Hash:
				rewriteErr = rewriteHash(archivedFile.Fid, off, entry)
			case List:
				rewriteErr = rewriteList(archivedFile.Fid, off, entry)
			}
			if rewriteErr != nil {
				return err
			}
		}
		db.mu.Lock()
		delete(db.archivedLogFiles[dataType], fid)
		db.mu.Unlock()
		db.discards[dataType].clear(fid)
	}
	return nil

}

func (db *MiniDB) handleLogFileGC() {
	if db.opts.LogFileGCInterval <= 0 {
		return
	}
	var quitSig = make(chan os.Signal, 1)
	signal.Notify(quitSig, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				logger.Warnf("logfile gc is running ,skip it ")
				break
			}
			for dType := String; dType < logFileTypeNum; dType++ {

				go func(dataType DataType) {
					if err := db.RunGC(dataType, -1, db.opts.LogFileGCRatio); err != nil {
						logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(dType)
			}
		case <-quitSig:
			return

		}

	}
}
