package part1

import (
	"bytes"
	"encoding/gob"
	"math"

	"github.com/visheratin/tsdb-challenges/data"
)

type AdvTreeIndex struct {
	ID     string
	Length int
	Root   *AdvTreeNode
}

func newAdvTreeIndex(id string) *AdvTreeIndex {
	idx := AdvTreeIndex{
		ID: id,
		Root: &AdvTreeNode{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		},
	}
	return &idx
}

func (idx AdvTreeIndex) Size() int {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(idx)
	return len(buf.Bytes())
}

func (idx *AdvTreeIndex) Insert(b data.Block) {
	idx.Root.insert(b)
	idx.Length++
}

func (idx *AdvTreeIndex) Search(min float64, max float64, res []data.Block) []data.Block {
	if res == nil {
		res = make([]data.Block, 0, idx.Length)
	}
	res = idx.Root.search(min, max, res)
	return res
}

type AdvTreeNode struct {
	LeftPart  *AdvTreeNode
	RightPart *AdvTreeNode
	Min       float64
	Max       float64
	Block     data.Block
}

func (node AdvTreeNode) search(min float64, max float64, res []data.Block) []data.Block {
	if filter(node.Min, node.Max, min, max) {
		if node.Block.Size != 0 {
			res = append(res, node.Block)
			return res
		}
		res = node.LeftPart.search(min, max, res)
		res = node.RightPart.search(min, max, res)
	}
	return res
}

func (node *AdvTreeNode) insert(b data.Block) {
	if b.Max > node.Max {
		node.Max = b.Max
	}
	if b.Min < node.Min {
		node.Min = b.Min
	}
	if node.Block.Size == 0 {
		if node.LeftPart == nil {
			node.Block = b
			return
		} else {
			if b.Max > node.LeftPart.Max {
				node.RightPart.insert(b)
			} else {
				node.LeftPart.insert(b)
			}
		}
	} else {
		if node.Block.Max < b.Max {
			node.LeftPart = &AdvTreeNode{
				Min:   node.Block.Min,
				Max:   node.Block.Max,
				Block: node.Block,
			}
			node.RightPart = &AdvTreeNode{
				Min:   b.Min,
				Max:   b.Max,
				Block: b,
			}
		} else {
			node.LeftPart = &AdvTreeNode{
				Min:   b.Min,
				Max:   b.Max,
				Block: b,
			}
			node.RightPart = &AdvTreeNode{
				Min:   node.Block.Min,
				Max:   node.Block.Max,
				Block: node.Block,
			}
		}
		node.Block = data.Block{}
	}
}
