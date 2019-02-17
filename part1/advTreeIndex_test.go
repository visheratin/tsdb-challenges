package part1

import (
	"testing"

	"github.com/visheratin/tsdb-challenges/data"
)

func BenchmarkAdvTreeIndexSearch_1000_empty(b *testing.B) {
	idx := createIndex("advTree", 1000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkAdvTreeIndexSearch_1000_init(b *testing.B) {
	idx := createIndex("advTree", 1000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}
func BenchmarkAdvTreeIndexSearch_10000_empty(b *testing.B) {
	idx := createIndex("advTree", 10000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}
func BenchmarkAdvTreeIndexSearch_10000_init(b *testing.B) {
	idx := createIndex("advTree", 10000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}

func BenchmarkAdvTreeIndexSearch_100000_empty(b *testing.B) {
	idx := createIndex("advTree", 100000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkAdvTreeIndexSearch_100000_init(b *testing.B) {
	idx := createIndex("advTree", 100000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}

func BenchmarkAdvTreeIndexSearch_1000000_empty(b *testing.B) {
	idx := createIndex("advTree", 1000000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkAdvTreeIndexSearch_1000000_init(b *testing.B) {
	idx := createIndex("advTree", 1000000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}
