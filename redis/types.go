package redis

import (
	bitcask_go "bitcask-go"
	"bitcask-go/utils"
	"encoding/binary"
	"errors"
	"time"
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *bitcask_go.DB
}

func NewRedisDataStructure(options bitcask_go.Options) (*RedisDataStructure, error) {
	db, err := bitcask_go.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{
		db: db,
	}, nil
}

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	dataType := encValue[0]
	if dataType != String {
		return nil, errors.New("error wrong type ")
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[index:], nil
}

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash 数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask_go.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask_go.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	// 先查看是否存在
	var ok bool
	if _, err = rds.db.Get(encKey); err == bitcask_go.ErrKeyNotFound {
		wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	_, err = rds.db.Get(encKey)
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask_go.ErrKeyNotFound {
		return false, nil
	}

	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	_, err = rds.db.Get(encKey)
	if err != nil && err == bitcask_go.ErrKeyNotFound {
		return false, nil
	}
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	meta.size--

	_ = wb.Put(encKey, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcask_go.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, errors.New("ErrWrongTypeOperation")
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}
	meta.size--

	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}
	var exist = true
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask_go.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	} else {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}
	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}
