package part3

import (
	"errors"

	"github.com/visheratin/tsdb-challenges/data"
)

type Index struct {
	ID            string
	StartTime     int64
	Type          DataType
	BlockInterval int64
	Store         Store
	Blocks        []data.Block
}

func NewIndex(id string, blockInterval int64, storeType string, storePath string, d []Element, dtype DataType) (Index, error) {
	if len(d) == 0 {
		return Index{}, errors.New("data slice is empty")
	}
	idx := Index{
		ID:            id,
		BlockInterval: blockInterval,
		Store:         NewStore(storeType, storePath),
		Type:          dtype,
	}
	dataParts := [][]Element{}
	s := d[0].Timestamp()
	f := s + blockInterval
	curIdx := 0
	for i := range d {
		if d[i].Timestamp() > f {
			dataParts = append(dataParts, d[curIdx:i])
			curIdx = i
			s = f
			f += blockInterval
		}
	}
	if curIdx != len(d)-1 {
		dataParts = append(dataParts, d[curIdx:])
	}
	blocks, err := idx.Store.Insert(dataParts, dtype)
	if err != nil {
		return Index{}, err
	}
	idx.Blocks = blocks
	return idx, nil
}

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
	els, err := idx.Store.Read(blockIds, blockSizes, blockNums, offset, idx.Type)
	if err != nil {
		return nil, err
	}
	firstIdx := 0
	lastIdx := 0
	for i := range els {
		if els[i].Timestamp() >= start {
			firstIdx = i
			break
		}
	}
	for i := len(els) - 1; i >= 0; i-- {
		if els[i].Timestamp() <= finish {
			lastIdx = i + 1
			break
		}
	}
	return els[firstIdx:lastIdx], nil
}

// func serialize(data []data.Element) ([]byte, error) {

// }
