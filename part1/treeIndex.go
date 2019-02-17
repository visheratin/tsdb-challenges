package part1

import (
	"math"

	"github.com/visheratin/tsdb-challenges/data"
)

type TreeIndex struct {
	ID   string
	Root *TreeNode
}

func newTreeIndex(id string) *TreeIndex {
	idx := TreeIndex{
		ID: id,
		Root: &TreeNode{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		},
	}
	return &idx
}

func (idx *TreeIndex) Insert(b data.Block) {
	idx.Root.insert(b)
}

func (idx *TreeIndex) Search(min float64, max float64, res []data.Block) []data.Block {
	return idx.Root.search(min, max)
}

type TreeNode struct {
	LeftPart  *TreeNode
	RightPart *TreeNode
	Min       float64
	Max       float64
	Block     data.Block
}

func (node TreeNode) search(min float64, max float64) []data.Block {
	if filter(node.Min, node.Max, min, max) {
		if node.Block.Size != 0 {
			return []data.Block{node.Block}
		}
		lp := node.LeftPart.search(min, max)
		rp := node.RightPart.search(min, max)
		return append(lp, rp...)
	}
	return nil
}

func (node *TreeNode) insert(b data.Block) {
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
			node.LeftPart = &TreeNode{
				Min:   node.Block.Min,
				Max:   node.Block.Max,
				Block: node.Block,
			}
			node.RightPart = &TreeNode{
				Min:   b.Min,
				Max:   b.Max,
				Block: b,
			}
		} else {
			node.LeftPart = &TreeNode{
				Min:   b.Min,
				Max:   b.Max,
				Block: b,
			}
			node.RightPart = &TreeNode{
				Min:   node.Block.Min,
				Max:   node.Block.Max,
				Block: node.Block,
			}
		}
		node.Block = data.Block{}
	}
}
