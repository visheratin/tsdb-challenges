package part1

import (
	"testing"

	"github.com/visheratin/tsdb-challenges/data"
)

func BenchmarkBTreeIndexSearch_1000_empty(b *testing.B) {
	idx := createIndex("bTree", 1000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkBTreeIndexSearch_1000_init(b *testing.B) {
	idx := createIndex("bTree", 1000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}

func BenchmarkBTreeIndexSearch_10000_empty(b *testing.B) {
	idx := createIndex("bTree", 10000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkBTreeIndexSearch_10000_init(b *testing.B) {
	idx := createIndex("bTree", 10000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}

func BenchmarkBTreeIndexSearch_100000_empty(b *testing.B) {
	idx := createIndex("bTree", 100000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkBTreeIndexSearch_100000_init(b *testing.B) {
	idx := createIndex("bTree", 100000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}

func BenchmarkBTreeIndexSearch_1000000_empty(b *testing.B) {
	idx := createIndex("bTree", 1000000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkBTreeIndexSearch_1000000_init(b *testing.B) {
	idx := createIndex("bTree", 1000000)
	min, max := searchBorders()
	res := make([]data.Block, 0, 1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, res)
	}
}
