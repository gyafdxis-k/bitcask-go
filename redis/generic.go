package redis

import "errors"

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (RedisDataType, error) {
	envValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(envValue) == 0 {
		return 0, errors.New("value is null")
	}
	return RedisDataType(envValue[0]), nil
}
