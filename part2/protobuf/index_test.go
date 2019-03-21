package protobuf

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

var protoPath = "./data-proto"

func prepare(b *testing.B) {
	err := os.MkdirAll(protoPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkProtoStoreCreate_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, protoPath, testDataSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtoStoreExtract_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, protoPath, testDataSmall)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataSmall.Data[len(testDataSmall.Data)-1].Timestamp
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

func BenchmarkProtoStoreCreate_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, protoPath, testDataMedium)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtoStoreExtract_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, protoPath, testDataMedium)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataMedium.Data[len(testDataMedium.Data)-1].Timestamp
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

func BenchmarkProtoStoreCreate_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, protoPath, testDataLarge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtoStoreExtract_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, protoPath, testDataLarge)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataLarge.Data[len(testDataLarge.Data)-1].Timestamp
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
