package part4

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

var dataPath = "./data-binary"

type bench[N Number] struct {
	name  string
	dType DataType
	data  []Element[N]
}

type benchList[N Number, B bench[N]] []B

var int32Bench = bench[int32]{"int32", Int32, testDataInt32}
var float32Bench = bench[float32]{"float32", Float32, testDataFloat32}
var float64Bench = bench[float64]{"float64", Float64, testDataFloat64}

func BenchmarkStoreCreate(b *testing.B) {
	err := os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(int32Bench.name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := NewIndex("123", 3600000, dataPath, int32Bench.data, int32Bench.dType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	err = os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(float32Bench.name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := NewIndex("123", 3600000, dataPath, float32Bench.data, float32Bench.dType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	err = os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(float64Bench.name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := NewIndex("123", 3600000, dataPath, float64Bench.data, float64Bench.dType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStoreExtract(b *testing.B) {
	err := os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	idx, err := NewIndex("123", 3600000, dataPath, int32Bench.data, int32Bench.dType)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := int32Bench.data[len(int32Bench.data)-1].Timestamp
	minStart := finishTimestamp - int64(1500000)
	b.Run(int32Bench.name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			r := rand.New(rand.NewSource(time.Now().Unix()))
			s := r.Int63n(minStart)
			f := s + 1500000
			b.StartTimer()
			_, err := Extract[int32](idx, s, f)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	err = os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	idx, err = NewIndex("123", 3600000, dataPath, float32Bench.data, float32Bench.dType)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp = float32Bench.data[len(float32Bench.data)-1].Timestamp
	minStart = finishTimestamp - int64(1500000)
	b.Run(float32Bench.name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			r := rand.New(rand.NewSource(time.Now().Unix()))
			s := r.Int63n(minStart)
			f := s + 1500000
			b.StartTimer()
			_, err := Extract[float32](idx, s, f)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	err = os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
	idx, err = NewIndex("123", 3600000, dataPath, float64Bench.data, float64Bench.dType)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp = float64Bench.data[len(float64Bench.data)-1].Timestamp
	minStart = finishTimestamp - int64(1500000)
	b.Run(float64Bench.name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			r := rand.New(rand.NewSource(time.Now().Unix()))
			s := r.Int63n(minStart)
			f := s + 1500000
			b.StartTimer()
			_, err := Extract[float64](idx, s, f)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
