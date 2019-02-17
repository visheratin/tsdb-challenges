package part1

import (
	"math/rand"
)

func filter(cMin, cMax, min float64, max float64) bool {
	// return cMin >= min && cMax <= max
	return cMin <= max && cMax >= min
}

func searchBorders() (float64, float64) {
	r := rand.New(rand.NewSource(99))
	max := r.Float64() * 1000
	min := max - 150
	return min, max
}
