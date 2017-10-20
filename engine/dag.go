package engine

import (
	"fmt"
	"github.com/twmb/algoimpl/go/graph"
)

type Coordinator interface {
	AddSource(name string, s Source) error
	AddDestination(name string, d Destination) error
	AddTransform(name string, t Transform) error
	Connect(from string, to string) error
	Compile() error
	Execute() error
}

type coordinator struct {
	g               *graph.Graph
	nodes           map[string]graph.Node
	streams         map[string]Stream
	sources         map[string]Source
	destinations    map[string]Destination
	transformations map[string]Transform
}

type sourceNode struct {
	name string
	s    Source
}

type transformNode struct {
	name string
	t    Transform
}

type destinationNode struct {
	name string
	d    Destination
}

func (c *coordinator) Compile() error {
	scc := c.g.StronglyConnectedComponents()
	for i := range scc {
		if len(scc[i]) > 1 {
			return fmt.Errorf("cannot compile: the dag for the job has cycles")
		}
	}

	for name, node := range c.nodes {
		nv := *node.Value

		switch nv.(type) {
		case *sourceNode:
			//no rules?
		case *destinationNode:
			if len(c.g.Neighbors(node)) > 0 {
				return fmt.Errorf("a destination is not allowed to have further destinations, but %s does", name)
			}
		case *transformNode:
			for _, dNode := range c.g.Neighbors(node) {
				dnv := *(dNode.Value)
				switch (dnv).(type) {
				case *sourceNode:
					return fmt.Errorf("a source cannot be a destination, but %s is", name)
				}
			}
		default:
			panic(fmt.Sprintf("Unknown node type %T for node %s", nv, name))
		}
	}
	return nil
}

func (c *coordinator) Execute() error {
	openNodes := make(map[string]bool)
	executionOrder := c.g.TopologicalSort()
	for _, node := range executionOrder {
		var upstream string
		nv := *node.Value
		switch n := nv.(type) {
		case *transformNode:
			//don't do anything, it should have been invoked by source/transform
			upstream = n.name
		case *destinationNode:
			//don't do anything, it should have been invoked by source/transform

		case *sourceNode:
			n.s.Open(c.streams[n.name])
			openNodes[n.name] = true
			upstream = n.name
		default:
			panic(fmt.Sprintf("unknown node type %T: %v", nv, nv))
		}

		for _, dNode := range c.g.Neighbors(node) {
			dnv := *dNode.Value
			switch d := dnv.(type) {
			case *transformNode:
				if !openNodes[d.name] {
					d.t.Open(c.streams[upstream], c.streams[d.name])
					openNodes[d.name] = true
				}

			case *destinationNode:
				if !openNodes[d.name] {
					d.d.Open(c.streams[upstream], c.streams[d.name])
					openNodes[d.name] = true
				}
			default:
				panic(fmt.Sprintf("unknown node type %T: %v", dnv, dnv))
			}
		}
	}
	return nil
}

func NewCoordinator() Coordinator {
	return &coordinator{
		g:               graph.New(graph.Directed),
		nodes:           make(map[string]graph.Node),
		sources:         make(map[string]Source),
		destinations:    make(map[string]Destination),
		transformations: make(map[string]Transform),
		streams:         make(map[string]Stream),
	}
}

func (c *coordinator) addNode(name string, val interface{}) error {
	if _, ok := c.nodes[name]; ok {
		return fmt.Errorf("name already exists %s", name)
	}
	n := c.g.MakeNode()
	*n.Value = val
	c.nodes[name] = n
	return nil
}

func (c *coordinator) AddSource(name string, s Source) error {
	if err := c.addNode(name, &sourceNode{name, s}); err != nil {
		return err
	}
	c.sources[name] = s
	c.streams[name] = NewStream(nil, DefaultBufferSize)
	return nil
}

func (c *coordinator) AddDestination(name string, d Destination) error {
	if err := c.addNode(name, &destinationNode{name, d}); err != nil {
		return err
	}
	c.destinations[name] = d
	c.streams[name] = NewStream([]string{"Error"}, DefaultBufferSize)
	return nil
}

func (c *coordinator) AddTransform(name string, t Transform) error {
	if err := c.addNode(name, &transformNode{name, t}); err != nil {
		return err
	}
	c.transformations[name] = t
	c.streams[name] = NewStream(nil, DefaultBufferSize)
	return nil
}

func (c *coordinator) Connect(from string, to string) error {
	if _, ok := c.nodes[from]; !ok {
		return fmt.Errorf("name does not exist %s", from)
	}
	if _, ok := c.nodes[to]; !ok {
		return fmt.Errorf("name does not exist %s", to)
	}
	if _, ok := c.destinations[from]; ok {
		return fmt.Errorf("cannot use the destination %s as a source", from)
	}
	if _, ok := c.sources[to]; ok {
		return fmt.Errorf("cannot use the source %s as a destination", to)
	}
	c.g.MakeEdge(c.nodes[from], c.nodes[to]) //discard error return as we have created nodes that belong to graph earlier
	return nil
}
