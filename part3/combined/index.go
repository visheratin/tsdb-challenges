package combined

import (
	"errors"

	"github.com/visheratin/tsdb-challenges/data"
	"github.com/visheratin/tsdb-challenges/part3"
)

type Index struct {
	ID            string
	StartTime     int64
	Type          part3.DataType
	BlockInterval int64
	Store         Store
	Blocks        []data.Block
}

func NewIndex(id string, blockInterval int64, storeType string, storePath string, d Elements, dtype part3.DataType) (Index, error) {
	if d.Len() == 0 {
		return Index{}, errors.New("data slice is empty")
	}
	idx := Index{
		ID:            id,
		BlockInterval: blockInterval,
		Store:         Store{path: storePath},
		Type:          dtype,
	}
	dataParts := []Elements{}
	s := d.Timestamp(0)
	f := s + blockInterval
	curIdx := 0
	l := d.Len()
	for i := 0; i < l; i++ {
		if d.Timestamp(i) > f {
			dataParts = append(dataParts, d.Subset(curIdx, i))
			curIdx = i
			s = f
			f += blockInterval
		}
	}
	if curIdx != d.Len()-1 {
		dataParts = append(dataParts, d.Subset(curIdx, d.Len()))
	}
	blocks, err := idx.Store.Insert(dataParts, dtype)
	if err != nil {
		return Index{}, err
	}
	idx.Blocks = blocks
	return idx, nil
}

func (idx Index) Extract(start, finish int64) (Elements, error) {
	if finish < idx.StartTime {
		return Elements{}, errors.New("no data for the specified period")
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
	els, err := idx.Store.Read(blockIds, blockSizes, blockNums, offset, idx.Type)
	if err != nil {
		return Elements{}, err
	}
	firstIdx := 0
	lastIdx := 0
	l := els.Len()
	for i := 0; i < l; i++ {
		if els.Timestamp(i) >= start {
			firstIdx = i
			break
		}
	}
	for i := l - 1; i >= 0; i-- {
		if els.Timestamp(i) <= finish {
			lastIdx = i + 1
			break
		}
	}
	return els.Subset(firstIdx, lastIdx), nil
}

// func serialize(data []data.Element) ([]byte, error) {

// }
