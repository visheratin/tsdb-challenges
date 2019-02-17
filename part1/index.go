package part1

import (
	"math/rand"

	"github.com/visheratin/tsdb-challenges/data"
)

type index interface {
	Insert(b data.Block)
	Search(min, max float64, res []data.Block) []data.Block
}

func createIndex(iType string, size int) index {
	var idx index
	switch iType {
	case "tree":
		idx = newTreeIndex("123")
	case "advTree":
		idx = newAdvTreeIndex("123")
	case "bTree":
		idx = newBTreeIndex("123")
	case "slice":
		idx = newSliceIndex("123", size)
	}
	fillIndex(idx, size)
	return idx
}

func fillIndex(idx index, size int) {
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
