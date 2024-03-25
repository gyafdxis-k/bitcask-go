package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		{
			return NewFileIOManager(fileName)
		}
	case MemoryMap:
		{
			return NewMMapIOManager(fileName)
		}
	default:
		panic("unsupported io type")
	}
}
