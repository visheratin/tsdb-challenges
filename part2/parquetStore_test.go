package part2

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkParquetStoreCreate_small(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "parquet", "./data-parquet", testDataSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreExtract_small(b *testing.B) {
	idx, err := NewIndex("123", 3600000, "parquet", "./data-parquet", testDataSmall)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataSmall[len(testDataSmall)-1].Timestamp
	minStart := finishTimestamp - int64(500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000000
		b.StartTimer()
		_, err := idx.Extract(s, f)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreCreate_medium(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "parquet", "./data-parquet", testDataMedium)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreExtract_medium(b *testing.B) {
	idx, err := NewIndex("123", 3600000, "parquet", "./data-parquet", testDataMedium)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataMedium[len(testDataMedium)-1].Timestamp
	minStart := finishTimestamp - int64(500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000000
		b.StartTimer()
		_, err := idx.Extract(s, f)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreCreate_large(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "parquet", "./data-parquet", testDataLarge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreExtract_large(b *testing.B) {
	idx, err := NewIndex("123", 3600000, "parquet", "./data-parquet", testDataLarge)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataLarge[len(testDataLarge)-1].Timestamp
	minStart := finishTimestamp - int64(500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000000
		b.StartTimer()
		_, err := idx.Extract(s, f)
		if err != nil {
			b.Fatal(err)
		}
	}
}
