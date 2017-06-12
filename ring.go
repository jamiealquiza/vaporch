// Package vaporch is a fast
// consistent hashing implementation.
package vaporch

import (
	"errors"
	"math"
	"sort"
	"sync"
)

var (
	ErrNodeExists    = errors.New("Node already exists")
	ErrNodeNotExists = errors.New("Node does not exist")
)

// Ring is a vaporCH
// consistent hash ring.
type Ring struct {
	sync.RWMutex
	nodes   NodeList
	nodeMap map[string]*Node
	vnodes  int
}

// NodeList holdes a list
// of *Nodes.
type NodeList []*Node

// Node represents a node
// in the hash ring.
type Node struct {
	Name string
}

// Config holds vaporCH
// initialization parameters.
type Config struct {
	Nodes  []string
	VNodes int
}

// New takes a *Config and initializes
// a *Ring.
func New(c *Config) (*Ring, error) {
	if c.VNodes == 0 {
		c.VNodes = 3
	}

	r := &Ring{
		nodes:   NodeList{},
		nodeMap: make(map[string]*Node),
		vnodes:  c.VNodes,
	}

	// Check if the ring is
	// being supplied a node name
	// list at initialization.
	if c.Nodes == nil {
		return r, nil
	}

	r.AddNodes(c.Nodes)

	return r, nil
}

// Satisfy the sort interface
// for NodeList.
func (n NodeList) Len() int           { return len(n) }
func (n NodeList) Less(i, j int) bool { return n[i].Name < n[j].Name }
func (n NodeList) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

// Names returns a []string of node names
// from a NodeList.
func (n NodeList) Names() []string {
	s := []string{}
	for _, n := range n {
		s = append(s, n.Name)
	}

	return s
}

// Size returns the number of real nodes.
func (r *Ring) Size() int {
	switch len(r.nodes) {
	case 0:
		return 0
	default:
		return len(r.nodes) / r.vnodes
	}
}

// AddNode adds a node by name
// to the hash ring.
func (r *Ring) AddNode(n string) error {
	r.Lock()
	defer r.Unlock()

	// Check if the node already exists.
	if _, exists := r.nodeMap[n]; exists {
		return ErrNodeExists
	}

	// Add the node.
	node := &Node{Name: n}
	r.nodeMap[n] = node

	// Build a node list.
	nodes := NodeList{}
	for k := range r.nodeMap {
		nodes = append(nodes, r.nodeMap[k])
	}

	sort.Sort(nodes)

	// Populate by the configured VNodes factor.
	r.nodes = NodeList{}
	for i := 0; i < r.vnodes; i++ {
		r.nodes = append(r.nodes, nodes...)
	}

	return nil
}

// AddNodes adds multiple nodes at once.
func (r *Ring) AddNodes(ns []string) {
	for _, n := range ns {
		r.AddNode(n)
	}
}

// RemoveNode removes a node n from the hash ring.
func (r *Ring) RemoveNode(n string) error {
	r.Lock()
	defer r.Unlock()

	if _, exists := r.nodeMap[n]; !exists {
		return ErrNodeNotExists
	}

	// Remove node meta.
	delete(r.nodeMap, n)
	newNl := NodeList{}
	for _, nd := range r.nodes {
		if nd.Name != n {
			newNl = append(newNl, nd)
		}
	}

	r.nodes = newNl

	return nil
}

// Members returns all nodes
// in the *Ring as a NodeList.
func (r *Ring) Members() NodeList {
	r.RLock()
	m := make(NodeList, r.Size())
	copy(m, r.nodes)
	r.RUnlock()

	return m
}

// Get takes a key k and returns the node name
// that owns the key hash ID on the ring keyspace.
func (r *Ring) Get(k string) string {
	r.RLock()
	n := r.nodes[idxFromKey(k, len(r.nodes))].Name
	r.RUnlock()
	return n
}

// GetN takes a key k and replicas n and
// returns up to [n]string sequential nodes; each
// node considered a replica. The first node returned
// is what would be returned in a normal Get lookup,
// followed by the next n-1 nodes as positioned on
// the hash ring.
func (r *Ring) GetN(k string, n int) []string {
	r.RLock()
	l := r.Size()
	idx := idxFromKey(k, l)
	ns := []string{}

	// If n is > than the number of
	// nodes, only return up to the
	// number of nodes.
	if n > l {
		n = l
	}

	// Walk the keyspace and fetch
	// n sequential nodes.
	for i := 0; i < n; i++ {
		ns = append(ns, r.nodes[(idx+i)%l].Name)
	}

	r.RUnlock()

	return ns
}

// idxFromKey takes a key k and NodeList length
// l. The index is determined by scaling the FNV-1a
// 64 bit key hash to the range 0.0..len(r.NodeList)
// then rounding to the nearest int.
func idxFromKey(k string, l int) int {
	n := float64(l - 1)
	sf := scale(float64(hash(k)), 0, math.MaxUint64, 0, n)

	return int(math.Floor(sf + 0.5))
}

// hash takes a key k and returns
// the FNV-1a 64 bit hash.
func hash(k string) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range []byte(k) {
		h ^= uint64(c)
		h *= 1099511628211
	}

	return h
}

// Scale scales the input x with the input-min a0,
// input-max a1, output-min b0, and output-max b1.
func scale(x float64, a0, a1, b0, b1 float64) float64 {
	return (x-a0)/(a1-a0)*(b1-b0) + b0
}
