package part1

import (
	"testing"

	"github.com/visheratin/tsdb-challenges/data"
)

type bench struct {
	name string
	size int
	init bool
}

func BenchmarkTreesIndexSearch_empty(b *testing.B) {
	trees := []string{"tree", "advTree", "bTree", "slice"}
	benches := []bench{
		{"1000 elements empty", 1000, false},
		{"1000 elements init", 1000, true},
		{"10000 elements empty", 10000, false},
		{"10000 elements init", 10000, true},
		{"100000 elements empty", 100000, false},
		{"100000 elements init", 100000, true},
		{"1000000 elements empty", 1000000, false},
		{"1000000 elements init", 1000000, true},
	}
	for _, tree := range trees {
		for _, bs := range benches {
			bName := tree + " " + bs.name
			b.Run(bName, func(b *testing.B) {
				idx, err := CreateIndex(tree, bs.size)
				if err != nil {
					b.Error(err)
				}
				min, max := searchBorders()
				var res []data.Block
				if bs.init {
					res = make([]data.Block, 0, bs.size)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = idx.Search(min, max, res)
				}
			})
		}
	}
}
