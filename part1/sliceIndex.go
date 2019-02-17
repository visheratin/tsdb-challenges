package part1

import (
	"github.com/visheratin/tsdb-challenges/data"
)

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

func (idx *SliceIndex) Insert(b data.Block) {
	idx.Blocks = append(idx.Blocks, b)
}

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
