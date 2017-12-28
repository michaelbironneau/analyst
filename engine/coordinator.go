package engine

import (
	"fmt"
	"github.com/gonum/graph"
	"github.com/gonum/graph/simple"
	"github.com/gonum/graph/topo"
	"sync"
)

//  Hooks are run at Compile() time. This means that the entire DAG has been computed.
//  While we are not currently giving the hook access to graph functions (eg. neighbors),
//  this may be necessary to satisfy future use cases.
type (
	//SourceHook takes the source name and interface and does something to it, possibly
	//returning an error.
	SourceHook func(string, Source) error

	//TransformHook takes the transform name and interface and does something to it, possibly
	//returning an error.
	TransformHook func(string, Transform) error

	//DestinationHook takes the destination name and interface and does something to it, possibly
	//returning an error.
	DestinationHook func(string, Destination) error
)

type Coordinator interface {
	RegisterHooks(...interface{}) //arguments should be SourceHook, TransformHook or DestinationHook
	AddSource(name string, alias string, s Source) error
	AddDestination(name string, alias string, d Destination) error
	AddTest(node string, name string, desc string, c Condition) error
	AddTransform(name string, alias string, t Transform) error
	AddConstraint(before, after string) error
	Connect(from string, to string) error
	Compile() error
	Execute() error
	Stop()
}

type constraint struct {
	Before string
	After  string
}

type coordinator struct {
	s                Stopper
	l                Logger
	g                *simple.DirectedGraph
	sourceHooks      []SourceHook
	transformHooks   []TransformHook
	destHooks        []DestinationHook
	nodes            map[string]interface{}
	nodeIdsRev       map[int]interface{}
	nodeIds          map[string]graph.Node
	streams          map[string]Stream
	sources          map[string]Source
	destinations     map[string]Destination
	transformations  map[string]Transform
	tests            map[string]*testNode
	testStreams      map[string]Stream
	constraints      []constraint
	constraintMap    map[string][]string //map after -> before
	constraintMapRev map[string][]string //map before -> after
}

type sourceNode struct {
	name  string
	alias string
	s     Source
}

type transformNode struct {
	name  string
	alias string
	t     Transform
}

type destinationNode struct {
	name  string
	alias string
	d     Destination
}

//Stop interrupts the job immediately.
func (c *coordinator) Stop() {
	c.s.Stop()
}

func (c *coordinator) checkConstraints() error {
	for _, constraint := range c.constraints {
		fromNode := c.nodeIds[constraint.Before]
		toNode := c.nodeIds[constraint.After]
		if c.g.HasEdgeFromTo(toNode, fromNode) {
			return fmt.Errorf("AFTER constraint conflicts with required execution order for the job: %s cannot be executed after %s", constraint.After, constraint.Before)
		}
	}
	return nil
}

//makeConstraints returns a configured waitgroup if the node has dependencies,
//or nil otherwise.
func (c *coordinator) makeConstraints() map[string]*sync.WaitGroup {
	ret := make(map[string]*sync.WaitGroup)
	for after, constraints := range c.constraintMap {
		var wg sync.WaitGroup
		wg.Add(len(constraints))
		ret[after] = &wg
	}
	return ret
}

//runHooks runs all the hooks, failing on the first error
func (c *coordinator) runHooks() error {
	for name, source := range c.sources {
		for _, hook := range c.sourceHooks {
			if err := hook(name, source); err != nil {
				return err
			}
		}
	}

	for name, transform := range c.transformations {
		for _, hook := range c.transformHooks {
			if err := hook(name, transform); err != nil {
				return err
			}
		}
	}

	for name, destination := range c.destinations {
		for _, hook := range c.destHooks {
			if err := hook(name, destination); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *coordinator) Compile() error {
	if err := c.checkConstraints(); err != nil {
		return err
	}

	scc := topo.TarjanSCC(c.g)

	if len(scc) > len(c.nodes) {
		return fmt.Errorf("cannot compile: the dag for the job has cycles")
	}

	for name, nv := range c.nodes {

		switch nv.(type) {
		case *sourceNode:
			if len(c.g.From(c.nodeIds[name])) == 0 {
				return fmt.Errorf("a source cannot be a termination point of the task, but %s is", name)
			}
		case *destinationNode:
			if len(c.g.From(c.nodeIds[name])) > 0 {
				return fmt.Errorf("a destination is not allowed to have further destinations, but %s does", name)
			}
		case *transformNode:
			if len(c.g.From(c.nodeIds[name])) == 0 {
				return fmt.Errorf("a transform cannot be a termination point of the task, but %s is", name)
			}
			for _, dNode := range c.g.From(c.nodeIds[name]) {
				dnv := c.nodeIdsRev[dNode.ID()]
				switch (dnv).(type) {
				case *sourceNode:
					return fmt.Errorf("a source cannot be a destination, but %s is", name)
				}
			}
		default:
			panic(fmt.Sprintf("Unknown node type %T for node %s", nv, name))
		}
	}

	return c.runHooks()
}

func (c *coordinator) RegisterHooks(hooks ...interface{}) {
	for i := range hooks {
		switch v := hooks[i].(type) {
		case SourceHook:
			c.sourceHooks = append(c.sourceHooks, v)
		case TransformHook:
			c.transformHooks = append(c.transformHooks, v)
		case DestinationHook:
			c.destHooks = append(c.destHooks, v)
		default:
			panic(fmt.Errorf("unknown hook type %T %v", v, v))
		}
	}
}

func (c *coordinator) Execute() error {
	var wg sync.WaitGroup
	executionOrder, err := topo.Sort(c.g)
	if err != nil {
		panic(err) //this should be unreachable as we checked for cycles in Compile()
	}
	constraints := c.makeConstraints()
	for _, node := range executionOrder {
		var upstream string
		nv := c.nodeIdsRev[node.ID()]
		switch n := nv.(type) {
		case *transformNode:
			//don't do anything, it should have been invoked by source/transform
			upstream = n.name
		case *destinationNode:
			//don't do anything, it should have been invoked by source/transform
			upstream = n.name
		case *sourceNode:
			if err := n.s.Ping(); err != nil {
				return err
			}
			wg.Add(1)
			go func(name string) {
				if constraints[name] != nil {
					constraints[name].Wait()
				}
				n.s.Open(c.streams[name], c.l, c.s)
				for _, after := range c.constraintMapRev[name] {
					constraints[after].Done()
				}
				wg.Done()
			}(n.name)
			upstream = n.name
		default:
			panic(fmt.Sprintf("unknown node type %T: %v", nv, nv))
		}
		neighbors := c.g.From(node)

		multiplex := newMultiplexer(c.getNodeName(node), c.getAliases(neighbors), DefaultBufferSize)
		var testedParentStream Stream

		if c.tests[upstream] == nil {
			testedParentStream = c.streams[upstream]
		} else {
			testedParentStream = c.testStreams[upstream]
			// interpose test node so that
			// upstream -> downstream(s)
			// becomes
			// upstream -> test node -> downstream(s)
			c.tests[upstream].Open(c.streams[upstream], c.testStreams[upstream], c.l, c.s)
		}

		if len(neighbors) > 0 {
			wg.Add(1)
			go func(parentStream Stream) {
				multiplex.Open(parentStream, c.l, c.s)
				wg.Done()
			}(testedParentStream)
		}

		for _, dNode := range neighbors {
			dnv := c.nodeIdsRev[dNode.ID()]
			switch d := dnv.(type) {
			case *transformNode:
				wg.Add(1)
				go func(name string) {
					if constraints[name] != nil {
						constraints[name].Wait()
					}
					d.t.Open(multiplex, c.streams[name], c.l, c.s)
					for _, after := range c.constraintMapRev[name] {
						constraints[after].Done()
					}
					wg.Done()
				}(d.name)
			case *destinationNode:
				if err := d.d.Ping(); err != nil {
					return err
				}
				wg.Add(1)
				go func(name string) {
					if constraints[name] != nil {
						constraints[name].Wait()
					}
					d.d.Open(multiplex, c.l, c.s)
					for _, after := range c.constraintMapRev[name] {
						constraints[after].Done()
					}
					wg.Done()
				}(d.name)
			default:
				panic(fmt.Sprintf("unknown node type %T: %v", dnv, dnv))
			}
		}
	}
	wg.Wait()
	close(c.l.Chan())
	return nil
}

func (c *coordinator) getNodeName(node graph.Node) string {
	n := c.nodeIdsRev[node.ID()]
	switch d := n.(type) {
	case *sourceNode:
		return d.name
	case *transformNode:
		return d.name
	case *destinationNode:
		return d.name
	default:
		panic(fmt.Sprintf("unknown node type %T: %v", n, node.ID()))
	}
}

func (c *coordinator) getAliases(nodes []graph.Node) []string {
	var aliases []string
	for i := range nodes {
		node := c.nodeIdsRev[nodes[i].ID()]
		switch d := node.(type) {
		case *transformNode:
			aliases = append(aliases, d.alias)
		case *destinationNode:
			aliases = append(aliases, d.alias)
		default:
			panic(fmt.Sprintf("unknown node type %T: %v", node, node))
		}
	}
	return aliases
}

func NewCoordinator(logger Logger) Coordinator {
	return &coordinator{
		s:                &stopper{},
		l:                logger,
		g:                simple.NewDirectedGraph(0, 0),
		nodes:            make(map[string]interface{}),
		nodeIds:          make(map[string]graph.Node),
		nodeIdsRev:       make(map[int]interface{}),
		sources:          make(map[string]Source),
		destinations:     make(map[string]Destination),
		transformations:  make(map[string]Transform),
		streams:          make(map[string]Stream),
		tests:            make(map[string]*testNode),
		testStreams:      make(map[string]Stream),
		constraintMap:    make(map[string][]string),
		constraintMapRev: make(map[string][]string),
	}
}

func (c *coordinator) addNode(name string, val interface{}) error {
	if _, ok := c.nodes[name]; ok {
		return fmt.Errorf("name already exists %s", name)
	}
	id := c.g.NewNodeID()
	node := simple.Node(id)

	c.g.AddNode(node)
	c.nodes[name] = val
	c.nodeIds[name] = node
	c.nodeIdsRev[id] = val
	return nil
}

func (c *coordinator) AddSource(name string, alias string, s Source) error {
	if err := c.addNode(name, &sourceNode{name, alias, s}); err != nil {
		return err
	}
	c.sources[name] = s
	c.streams[name] = NewStream(nil, DefaultBufferSize)
	return nil
}

func (c *coordinator) AddDestination(name string, alias string, d Destination) error {
	if err := c.addNode(name, &destinationNode{name, alias, d}); err != nil {
		return err
	}
	c.destinations[name] = d
	return nil
}

func (c *coordinator) AddConstraint(before, after string) error {
	if _, ok := c.nodes[before]; !ok {
		return fmt.Errorf("name does not exist %s", before)
	}
	if _, ok := c.nodes[after]; !ok {
		return fmt.Errorf("name does not exist %s", after)
	}

	c.constraints = append(c.constraints, constraint{before, after})
	c.constraintMap[after] = append(c.constraintMap[after], before)
	c.constraintMapRev[before] = append(c.constraintMapRev[before], after)

	//Add additional constraint between 'before' destinations and 'after, to ensure
	//that all destinations will complete before the 'after' node Open()s
	for _, node := range c.g.From(c.nodeIds[before]) {
		dnv := c.nodeIdsRev[node.ID()]
		if dest, ok := dnv.(*destinationNode); ok {
			if _, ok := c.nodes[dest.name]; !ok {
				panic(fmt.Errorf("destination constraint does not exist %s", dest.name))
			}
			c.constraints = append(c.constraints, constraint{dest.name, after})
			c.constraintMap[after] = append(c.constraintMap[after], dest.name)
			c.constraintMapRev[dest.name] = append(c.constraintMapRev[dest.name], after)
		}
	}

	return nil
}

func (c *coordinator) AddTransform(name string, alias string, t Transform) error {
	if err := c.addNode(name, &transformNode{name, alias, t}); err != nil {
		return err
	}
	c.transformations[name] = t
	c.streams[name] = NewStream(nil, DefaultBufferSize)
	return nil
}

//AddTest is a shortcut that adds a test destination
func (c *coordinator) AddTest(node string, name string, desc string, co Condition) error {
	if tn, ok := c.tests[node]; ok {
		tn.Add(name, desc, co)
		return nil
	}

	if _, ok := c.nodes[node]; !ok {
		return fmt.Errorf("name does not exist %s", node)
	}

	tn := testNode{}
	tn.Add(name, desc, co)
	c.tests[node] = &tn
	c.testStreams[node] = NewStream(nil, DefaultBufferSize)
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
	c.g.SetEdge(simple.Edge{c.nodeIds[from], c.nodeIds[to], 1})
	return nil
}
