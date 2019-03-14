package part3

func GenerateData(len int, dtype DataType) []Element {
	res := make([]Element, len)
	t := int64(0)
	for i := 0; i < len; i++ {
		switch dtype {
		case Int32:
			res[i] = Int32Element{
				Ts:  t,
				Val: int32(i),
			}
		case Float32:
			res[i] = Float32Element{
				Ts:  t,
				Val: float32(i),
			}
		case Float64:
			res[i] = Float64Element{
				Ts:  t,
				Val: float64(i),
			}
		}
		t += 1234
	}
	return res
}

var testDataSmall = GenerateData(600000, Float64)

var testDataMedium = GenerateData(6000000, Float64)

var testDataLarge = GenerateData(60000000, Float64)
