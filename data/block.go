package data

// Block is an index basic data structure that stores metainformation about the data part in the storage.
type Block struct {
	Size  int
	ElNum int
	Min   float64
	Max   float64
}
