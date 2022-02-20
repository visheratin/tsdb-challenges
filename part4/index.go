package part4

import (
	"errors"

	"github.com/visheratin/tsdb-challenges/data"
)

// Index is data structure that implements indexing logic for a time series database.
type Index struct {
	ID            string
	StartTime     int64
	Type          DataType
	BlockInterval int64
	StorePath     string
	Blocks        []data.Block
}

// NewIndex creates new index, initializes Store, loads input data d to the Store and loads
// output blocks into the index.
func NewIndex[N Number](id string, blockInterval int64, storePath string, d []Element[N],
	dtype DataType) (Index, error) {
	if len(d) == 0 {
		return Index{}, errors.New("data slice is empty")
	}
	idx := Index{
		ID:            id,
		BlockInterval: blockInterval,
		StorePath:     storePath,
		Type:          dtype,
	}
	dataParts := [][]Element[N]{}
	s := d[0].Timestamp
	f := s + blockInterval
	curIdx := 0
	for i := range d {
		if d[i].Timestamp > f {
			dataParts = append(dataParts, d[curIdx:i])
			curIdx = i
			s = f
			f += blockInterval
		}
	}
	if curIdx != len(d)-1 {
		dataParts = append(dataParts, d[curIdx:])
	}
	blocks, err := Insert(dataParts, dtype, storePath)
	if err != nil {
		return Index{}, err
	}
	idx.Blocks = blocks
	return idx, nil
}

// Extract based on start and finish parameters calculates  blocks that need to be extracted,
// and reads them from the Store.
func Extract[N Number](idx Index, start, finish int64) ([]Element[N], error) {
	if finish < idx.StartTime {
		return nil, errors.New("no data for the specified period")
	}
	startIdx := int(start / idx.BlockInterval)
	if startIdx > (len(idx.Blocks) - 1) {
		startIdx = len(idx.Blocks) - 1
	}
	finishIdx := int(finish / idx.BlockInterval)
	if finishIdx > (len(idx.Blocks) - 1) {
		finishIdx = len(idx.Blocks) - 1
	}
	blockIds := []int{}
	blockSizes := []int{}
	blockNums := []int{}
	var offset int64
	for i := 0; i < startIdx; i++ {
		offset += int64(idx.Blocks[i].Size)
	}
	for i := startIdx; i <= finishIdx; i++ {
		blockIds = append(blockIds, i)
		blockNums = append(blockNums, idx.Blocks[i].ElNum)
		blockSizes = append(blockSizes, idx.Blocks[i].Size)
	}
	els, err := Read[N](blockIds, blockSizes, blockNums, offset, idx.Type, idx.StorePath)
	if err != nil {
		return nil, err
	}
	firstIdx := 0
	lastIdx := 0
	for i := range els {
		if els[i].Timestamp >= start {
			firstIdx = i
			break
		}
	}
	for i := len(els) - 1; i >= 0; i-- {
		if els[i].Timestamp <= finish {
			lastIdx = i + 1
			break
		}
	}
	return els[firstIdx:lastIdx], nil
}
