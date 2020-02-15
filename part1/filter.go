package part1

import (
	"math/rand"
)

// filter performs check whether tree node borders (cMin and cMax)
// intersect with the search interval (min and max).
//
// Example:
// ________________cMin____________cMax_______________________
// _______min________________________________________max______
func filter(cMin, cMax, min, max float64) bool {
	return cMin <= max && cMax >= min
}

func searchBorders() (float64, float64) {
	r := rand.New(rand.NewSource(99))
	max := r.Float64() * 1000
	min := max - 150
	return min, max
}
