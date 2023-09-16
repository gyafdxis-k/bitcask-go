package fio

const DataFilePerm = 0644

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte, int64) (int, error)
	Sync() error
	Close() error
}
