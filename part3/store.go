package part3

import "github.com/visheratin/tsdb-challenges/data"

type Store interface {
	Insert(dataParts [][]Element, dtype DataType) ([]data.Block, error)
	Read(blockIds []int, blockSizes []int, blockNums []int, offset int64, dtype DataType) ([]Element, error)
}

func NewStore(sType string, path string) Store {
	switch sType {
	case "interface":
		return InterfaceStore{
			path: path,
		}
		// case "binary":
		// 	return BinaryStore{
		// 		path: path,
		// 	}
	}
	return nil
}
