package part1

import (
	"errors"
	"math/rand"

	"github.com/visheratin/tsdb-challenges/data"
)

// Index is the interface that describes methods for very simple Index
// of imaginary time series database.
//
// Insert puts a block b into the appropriate node of the tree. Since in this
// example insertion always executes correctly, Insert does not return error.
//
// Search walks through the tree and returns all blocks that intersect with
// the search conditions (min and max). res is the resulting slice of blocks
// that can be initialized in advance to eliminate the need for memory allocations
// during the search.
type Index interface {
	Insert(b data.Block)
	Search(min, max float64, res []data.Block) []data.Block
}

// NewIndex creates an instance on Index depending on the index type specified
// by iType. Parameter size is used by SliceIndex to preallocate memory for the
// internal slice of blocks.
func NewIndex(iType string, size int) (Index, error) {
	switch iType {
	case "tree":
		return newTreeIndex("123"), nil
	case "advTree":
		return newAdvTreeIndex("123"), nil
	case "bTree":
		return newBTreeIndex("123"), nil
	case "slice":
		return newSliceIndex("123", size), nil
	default:
		return nil, errors.New("unknown index type")
	}
}

// CreateIndex creates an instance on Index depending on the index type specified
// by iType and inserts size random values in it.
func CreateIndex(iType string, size int) (Index, error) {
	idx, err := NewIndex(iType, size)
	if err != nil {
		return nil, err
	}
	fillIndex(idx, size)
	return idx, nil
}

func fillIndex(idx Index, size int) {
	r := rand.New(rand.NewSource(99))
	for i := 0; i < size; i++ {
		max := r.Float64() * 1000
		min := max - r.Float64()*100
		elNum := r.Int()
		s := r.Int()
		b := data.Block{
			Min:   min,
			Max:   max,
			Size:  s,
			ElNum: elNum,
		}
		idx.Insert(b)
	}
}
