package part2

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

var gobPath = "./data-gob"
var parquetPath = "./data-parquet"
var binaryPath = "./data-binary"

func prepare(b *testing.B) {
	err := os.MkdirAll(gobPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	err = os.MkdirAll(parquetPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	err = os.MkdirAll(binaryPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkGobStoreCreate_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "gob", gobPath, testDataSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreCreate_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "parquet", parquetPath, testDataSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryStoreCreate_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "binary", binaryPath, testDataSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreExtract_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "gob", gobPath, testDataSmall)
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

func BenchmarkParquetStoreExtract_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "parquet", parquetPath, testDataSmall)
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

func BenchmarkBinaryStoreExtract_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "binary", binaryPath, testDataSmall)
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

func BenchmarkGobStoreCreate_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "gob", gobPath, testDataMedium)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreCreate_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "parquet", parquetPath, testDataMedium)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryStoreCreate_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "binary", binaryPath, testDataMedium)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreExtract_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "gob", gobPath, testDataMedium)
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

func BenchmarkParquetStoreExtract_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "parquet", parquetPath, testDataMedium)
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

func BenchmarkBinaryStoreExtract_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "binary", binaryPath, testDataMedium)
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

func BenchmarkGobStoreCreate_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "gob", gobPath, testDataLarge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetStoreCreate_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "parquet", parquetPath, testDataLarge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryStoreCreate_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "binary", binaryPath, testDataLarge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreExtract_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "gob", gobPath, testDataLarge)
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

func BenchmarkParquetStoreExtract_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "parquet", parquetPath, testDataLarge)
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

func BenchmarkBinaryStoreExtract_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "binary", binaryPath, testDataLarge)
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
