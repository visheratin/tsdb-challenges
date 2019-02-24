package part2

import "github.com/visheratin/tsdb-challenges/data"

type Store interface {
	Insert(dataParts [][]data.Element) ([]data.Block, error)
	Read(blockIds []int, blockSizes []int, blockNums []int, offset int64) ([]data.Element, error)
}

func NewStore(sType string, path string) Store {
	switch sType {
	case "gob":
		return GobStore{
			path: path,
		}
	case "parquet":
		return ParquetStore{
			path: path,
		}
	case "binary":
		return BinaryStore{
			path: path,
		}
	}
	return nil
}
