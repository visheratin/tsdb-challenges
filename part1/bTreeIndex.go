package part1

import (
	"math"

	"github.com/visheratin/tsdb-challenges/data"
)

type BTreeIndex struct {
	ID     string
	Length int
	Root   *BTreeNode
}

func newBTreeIndex(id string) *BTreeIndex {
	idx := BTreeIndex{
		ID: id,
		Root: &BTreeNode{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		},
	}
	return &idx
}

func (idx *BTreeIndex) Insert(b data.Block) {
	idx.Root.insert(b)
	idx.Length++
}

func (idx *BTreeIndex) Search(min float64, max float64, res []data.Block) []data.Block {
	if res == nil {
		res = make([]data.Block, 0, idx.Length)
	}
	return idx.Root.search(min, max, res)
}

type BTreeNode struct {
	LeftPart  *BTreeNode
	RightPart *BTreeNode
	Min       float64
	Max       float64
	Blocks    []data.Block
}

func (node BTreeNode) search(min float64, max float64, res []data.Block) []data.Block {
	if filter(node.Min, node.Max, min, max) {
		for _, b := range node.Blocks {
			if filter(b.Min, b.Max, min, max) {
				res = append(res, b)
			}
		}
		if node.LeftPart != nil {
			res = node.LeftPart.search(min, max, res)
		}
		if node.RightPart != nil {
			res = node.RightPart.search(min, max, res)
		}
	}
	return res
}

func (node *BTreeNode) insert(b data.Block) {
	if b.Max > node.Max {
		node.Max = b.Max
	}
	if b.Min < node.Min {
		node.Min = b.Min
	}
	if node.Blocks == nil {
		node.Blocks = []data.Block{b}
		return
	}
	if len(node.Blocks) < 10 {
		for i := 0; i < len(node.Blocks); i++ {
			if node.Blocks[i].Max > b.Max {
				node.Blocks = append(node.Blocks, data.Block{})
				copy(node.Blocks[i+1:], node.Blocks[i:])
				node.Blocks[i] = b
				return
			}
		}
		node.Blocks = append(node.Blocks, b)
		return
	}
	if node.LeftPart == nil {
		node.LeftPart = &BTreeNode{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		}
	}
	if node.RightPart == nil {
		node.RightPart = &BTreeNode{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		}
	}
	if b.Max < node.Blocks[0].Max {
		node.LeftPart.insert(b)
		return
	}
	if b.Max > node.Blocks[9].Max {
		node.RightPart.insert(b)
		return
	}
	for i := 0; i < len(node.Blocks); i++ {
		if node.Blocks[i].Max > b.Max {
			if i < 5 {
				v := node.Blocks[0]
				if i > 0 {
					copy(node.Blocks[0:i], node.Blocks[1:i+1])
					node.Blocks[i-1] = b
				} else {
					node.Blocks[0] = b
				}
				node.LeftPart.insert(v)
			} else {
				v := node.Blocks[9]
				if i < 9 {
					copy(node.Blocks[i:], node.Blocks[i-1:])
					node.Blocks[i-1] = b
				} else {
					node.Blocks[9] = b
				}
				node.RightPart.insert(v)
			}
			return
		}
	}
}
