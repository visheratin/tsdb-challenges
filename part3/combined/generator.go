package combined

import "github.com/visheratin/tsdb-challenges/part3"

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
		t += 1234
	}
	return res
}

var testDataSmall = GenerateData(600000, part3.Float64)

var testDataMedium = GenerateData(6000000, part3.Float64)

var testDataLarge = GenerateData(60000000, part3.Float64)
