package part2

// GenerateData is a simple function for creating datasets
// that are used in stores benchmarking.
func GenerateData(len int) []Element {
	res := make([]Element, len)
	t := int64(0)
	var v float64
	for i := 0; i < len; i++ {
		v = float64(i)
		res[i] = Element{
			Timestamp: t,
			Value:     v,
		}
		t += 1234
	}
	return res
}

var testDataSmall = GenerateData(600000)

var testDataMedium = GenerateData(6000000)

var testDataLarge = GenerateData(60000000)
