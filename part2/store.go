package part2

import "github.com/visheratin/tsdb-challenges/data"

// Store is the inferface that describes a block storage for time series data.
//
// Insert loads slice of data parts, each of which is represented as Elements,
// to the storage and if successful returns a slice of index blocks.
//
// Read uses meta-information about data blocks to extract data from the store.
type Store interface {
	Insert(dataParts []Elements) ([]data.Block, error)
	Read(blockIds []int, blockSizes []int, blockNums []int, offset int64) (Elements, error)
}

// NewStore initializes new storage based on the provided type and sets the path
// for storing the serialized binary data.
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
	case "proto":
		return ProtoStore{
			path: path,
		}
	}
	return nil
}
