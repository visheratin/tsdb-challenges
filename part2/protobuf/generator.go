package protobuf

func GenerateData(len int) ProtoElements {
	res := ProtoElements{Data: make([]ProtoElement, len)}
	t := int64(0)
	for i := 0; i < len; i++ {
		tsv := t
		v := float64(i)
		el := ProtoElement{
			Timestamp: tsv,
			Value:     v,
		}
		res.Data[i] = el
		t += 1234
	}
	return res
}

var testDataSmall = GenerateData(600000)

var testDataMedium = GenerateData(6000000)

var testDataLarge = GenerateData(60000000)
