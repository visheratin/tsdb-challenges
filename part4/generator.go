package part4

// GenerateData creates slice of elements for benchmarking.
func GenerateData[N Number](len int, dtype DataType) []Element[N] {
	res := make([]Element[N], len)
	t := int64(0)
	for i := 0; i < len; i++ {
		switch dtype {
		case Int32:
			res[i] = Element[N]{
				Timestamp: t,
				Val:       N(int32(i)),
			}
		case Float32:
			res[i] = Element[N]{
				Timestamp: t,
				Val:       N(float32(i)),
			}
		case Float64:
			res[i] = Element[N]{
				Timestamp: t,
				Val:       N(float64(i)),
			}
		}
		t += 3
	}
	return res
}

var testDataInt32 = GenerateData[int32](6000000, Int32)

var testDataFloat32 = GenerateData[float32](6000000, Float32)

var testDataFloat64 = GenerateData[float64](6000000, Float64)
