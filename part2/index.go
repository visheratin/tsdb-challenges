package part2

import (
	"errors"

	"github.com/visheratin/tsdb-challenges/data"
)

// Index is data structure that implements indexing logic for a time series database.
//
//	ID - name of the sensor.
//
//	StartTime - Unix timestamp of the first entry in the index.
//
//	BlockInterval - time interval for blocks generation.
//
//	Store - backend storage that is responsible for data
//	serialization and deserialization.
//
//	Blocks - is a slice of index blocks. Slices were chosen
//	instead of trees in part1.
type Index struct {
	ID            string
	StartTime     int64
	BlockInterval int64
	Store         Store
	Blocks        []data.Block
}

// NewIndex creates new index, initializes Store based on the storeType parameter, loads
// input data d to the Store and loads output blocks into the index.
func NewIndex(id string, blockInterval int64, storeType string, storePath string, d []Element) (Index, error) {
	if len(d) == 0 {
		return Index{}, errors.New("data slice is empty")
	}
	idx := Index{
		ID:            id,
		BlockInterval: blockInterval,
		Store:         NewStore(storeType, storePath),
	}
	dataParts := []Elements{}
	s := d[0].Timestamp
	f := s + blockInterval
	curIdx := 0
	for i := range d {
		if d[i].Timestamp > f {
			els := Elements{Data: d[curIdx:i]}
			dataParts = append(dataParts, els)
			curIdx = i
			s = f
			f += blockInterval
		}
	}
	if curIdx != len(d)-1 {
		els := Elements{Data: d[curIdx:]}
		dataParts = append(dataParts, els)
	}
	blocks, err := idx.Store.Insert(dataParts)
	if err != nil {
		return Index{}, err
	}
	idx.Blocks = blocks
	return idx, nil
}

// Extract based on start and finish parameters calculates  blocks that need to be extracted,
// and reads them from the Store implementation.
func (idx Index) Extract(start, finish int64) ([]Element, error) {
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
	els, err := idx.Store.Read(blockIds, blockSizes, blockNums, offset)
	if err != nil {
		return nil, err
	}
	firstIdx := 0
	lastIdx := 0
	for i := range els.Data {
		if els.Data[i].Timestamp >= start {
			firstIdx = i
			break
		}
	}
	for i := len(els.Data) - 1; i >= 0; i-- {
		if els.Data[i].Timestamp <= finish {
			lastIdx = i + 1
			break
		}
	}
	return els.Data[firstIdx:lastIdx], nil
}
