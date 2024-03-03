package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options Options
	mu      *sync.RWMutex
	// 文件id只能在加载索引的时候使用，不能在其他地方更新和使用
	fileIds    []int
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index      index.Indexer
	seqNo      uint64
	isMerging  bool
}

func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 判断目录是否存在，如果目录不存在就要去创建这个目录
	if _, err := os.Stat(options.DirPath); err != nil {
		if err = os.Mkdir(options.DirPath, os.ModeDir); err != nil {
			return nil, err
		}
	}
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从 hint 索引文件中加载索引

	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}
	// 加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, NonTransitionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.AppendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	print("key:", string(key))
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 现查找一下内存找key是否存在，如果存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	// 构建LogRecord 标志其是删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, NonTransitionSeqNo),
		Type: data.LogRecordDelete,
	}
	_, err := db.AppendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	// 从内存中删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 根据索引信息获取对应的value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func (db *DB) AppendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.AppendLogRecord(logRecord)
}

func (db *DB) AppendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	encRecord, size := data.EncodeLogRecord(logRecord)
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil
	}
	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 0001.data
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能损坏
			if err != nil {
				return errors.New("the database directory corrupted")
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 从小到大遍历
	sort.Ints(fileIds)
	db.fileIds = fileIds
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			// 最后一个id是最大的，说明是当前活跃的
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)-1] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件的所有记录 并更新到内存索引
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件说明是空的数据库
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDelete {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed update index in memeory")
		}
	}
	// 暂存事务的数据
	transactionRecords := make(map[uint64][]*data.Transaction)
	var currentSeqNo uint64 = NonTransitionSeqNo
	// 遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果最近未参与 merge的文件 id更小，则说明已经从Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构建内存索引
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析key 拿到seq
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == NonTransitionSeqNo {
				// 非事务操作直接更新索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.Transaction{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			// 递增offset 下一次从新的位置开始读取
			offset += size
		}
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	defer db.mu.Unlock()
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx += 1
	}
	return keys
}

func (db *DB) Fold(f func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !f(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

func (db *DB) Stat() error {
	return nil
}

func (db *DB) Backup(dir string) error {
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database datafile size is less than 0")
	}
	return nil
}
