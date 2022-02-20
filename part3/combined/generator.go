package combined

import "github.com/visheratin/tsdb-challenges/part3"

// GenerateData creates Elements instance for benchmarking.
func GenerateData(len int, dtype part3.DataType) Elements {
	res := NewElements(dtype, len)
	t := int64(0)
	for i := 0; i < len; i++ {
		switch dtype {
		case part3.Int32:
			res.I32[i] = part3.Int32Element{
				Ts:  t,
				Val: int32(i),
			}
		case part3.Float32:
			res.F32[i] = part3.Float32Element{
				Ts:  t,
				Val: float32(i),
			}
		case part3.Float64:
			res.F64[i] = part3.Float64Element{
				Ts:  t,
				Val: float64(i),
			}
		}
		t += 3
	}
	return res
}

var testDataInt32 = GenerateData(6000000, part3.Int32)

var testDataFloat32 = GenerateData(6000000, part3.Float32)

var testDataFloat64 = GenerateData(6000000, part3.Float64)
