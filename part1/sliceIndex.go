package part1

import (
	"github.com/visheratin/tsdb-challenges/data"
)

// SliceIndex is a very straightforward index where all blocks are
// stored in a single slice.
type SliceIndex struct {
	ID     string
	Blocks []data.Block
}

func newSliceIndex(name string, size int) *SliceIndex {
	idx := SliceIndex{
		ID:     name,
		Blocks: make([]data.Block, 0, size),
	}
	return &idx
}

// Insert loads input value b into the blocks slice.
func (idx *SliceIndex) Insert(b data.Block) {
	idx.Blocks = append(idx.Blocks, b)
}

// Search extracts from the slice blocks that intersect with the search conditions.
func (idx *SliceIndex) Search(min float64, max float64, res []data.Block) []data.Block {
	if res == nil {
		res = make([]data.Block, 0, len(idx.Blocks))
	}
	for _, b := range idx.Blocks {
		if filter(b.Min, b.Max, min, max) {
			res = append(res, b)
		}
	}
	return res
}
