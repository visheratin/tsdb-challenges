package combined

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/visheratin/tsdb-challenges/part3"
)

var dataPath = "./data-binary"

type bench struct {
	name  string
	dType part3.DataType
	data  Elements
}

var benches = []bench{
	{"int32", part3.Int32, testDataInt32},
	{"float32", part3.Float32, testDataFloat32},
	{"float64", part3.Float64, testDataFloat64},
}

func BenchmarkStoreCreate(b *testing.B) {
	for _, bs := range benches {
		err := os.MkdirAll(dataPath, 0777)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(bs.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := NewIndex("123", 3600000, dataPath, bs.data, bs.dType)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStoreExtract(b *testing.B) {
	for _, bs := range benches {
		err := os.MkdirAll(dataPath, 0777)
		if err != nil {
			b.Fatal(err)
		}
		idx, err := NewIndex("123", 3600000, dataPath, bs.data, bs.dType)
		if err != nil {
			b.Fatal(err)
		}
		finishTimestamp := bs.data.Timestamp(bs.data.Len() - 1)
		minStart := finishTimestamp - int64(500000000)
		b.Run(bs.name, func(b *testing.B) {
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
		})
	}
}
