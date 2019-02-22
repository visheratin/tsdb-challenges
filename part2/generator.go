package part2

import (
	"github.com/visheratin/tsdb-challenges/data"
)

func generateData(len int) []data.Element {
	res := make([]data.Element, len)
	t := int64(0)
	var v float64
	for i := 0; i < len; i++ {
		v = float64(i)
		res[i] = data.Element{
			Timestamp: t,
			Value:     v,
		}
		t += 1234
	}
	return res
}

var testDataSmall = generateData(600000)

var testDataMedium = generateData(6000000)

var testDataLarge = generateData(60000000)
