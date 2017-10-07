package aql

//import (
//	"github.com/twmb/algoimpl/go/graph"
//)

type Block struct {
	Type BlockType
	Name string
	Header interface{}
	Content interface{}
	Tests []Block
	Options Options
}

type BlockType int

const (
	Metadata BlockType = iota
	Source
	Sink
	QueryTransform
	ScriptTransform
)