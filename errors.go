package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrDataDirectoryCorrupted = errors.New("the database directory corrupted")
	ErrKeyNotFound            = errors.New("key not found in databases")
	ErrDataFileNotFound       = errors.New("data file is not found")

	ErrDatabaseIsUsing = errors.New("the database is using")

	ErrExceedMaxBatchNum = errors.New(" exceed max batch num")

	ErrMergeIsProgress = errors.New("merge is process, try again")
)
