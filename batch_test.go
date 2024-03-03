package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(1))
	t.Log(val)
	t.Log(err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	t.Log(val1)
	t.Log(err)

	//
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	t.Log(val2)
	t.Log(err)
}

func TestDB_NewWriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("/Users/gaodong/Desktop/data", "bitcask-go-batch-2")
	print(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb = db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	val, err := db2.Get(utils.GetTestKey(1))
	t.Log(val)
	t.Log(err)
	assert.Equal(t, ErrKeyNotFound, err)
	t.Log(db.seqNo)
	assert.Equal(t, uint64(2), db.seqNo)
}

func TestDB_NewWriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("/Users/gaodong/Desktop/data", "bitcask-go-batch-3")
	print(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)
	keys := db.ListKeys()
	t.Log(len(keys))
	//assert.Nil(t, err)
	//assert.NotNil(t, db)
	//wbOpts := DefaultWriteBatchOptions
	//wbOpts.MaxBatchNum = 1000000
	//wb := db.NewWriteBatch(wbOpts)
	//for i := 0; i < 500000; i++ {
	//	err = wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	//	assert.Nil(t, err)
	//}
	//err = wb.Commit()
	//assert.Nil(t, err)
	//err = db.Close()
	//assert.Nil(t, err)
}
