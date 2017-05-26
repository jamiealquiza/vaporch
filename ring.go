// Package vaporch is a fast, serch-free
// consistent-hashing implementation.
package vaporch

import (
	"errors"
	"math"
	"sort"
	"sync"
)

var (
	ErrNodeExists = errors.New("Node already exists")
)

// Ring is a vaporCH
// consistent-hashing ring.
type Ring struct {
	sync.RWMutex
	nodes   NodeList
	nodeMap map[string]*Node
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
	Nodes []*Node
}

// New takes a *Config and initializes
// a *Ring.
func New(c *Config) (*Ring, error) {
	r := &Ring{
		nodes:   NodeList{},
		nodeMap: make(map[string]*Node),
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
	r.nodes = append(r.nodes, node)
	// Sort, update meta.
	sort.Sort(r.nodes)
	r.nodeMap[n] = node

	return nil
}

// func (r *Ring) RemoveNode(n string) error {}

// AddNodes adds multiple nodes at once.
func (r *Ring) AddNodes(ns []*Node) {
	_ = ns
}

// Members returns all nodes
// in the *Ring as a NodeList.
func (r *Ring) Members() NodeList {
	r.RLock()
	m := make(NodeList, len(r.nodes))
	copy(m, r.nodes)
	r.RUnlock()

	return m
}

// Get takes a key k and returns the node name
// that owns the key hash ID on the ring keyspace.
func (r *Ring) Get(k string) string {
	return r.nodes[idxFromKey(k, len(r.nodes))].Name
}

// func (r *Ring) GetN(k string, n int) []string {}

// idxFromKey takes a key k and NodeList length
// l. The index is determined by scaling the FNV-1a
// 64 bit key hash to the range 0.0..len(r.NodeList)
// then rounding to the nearest int.
func idxFromKey(k string, l int) int {
	n := float64(l-1)
	sf := scale(float64(hash(k)), 0, math.MaxUint64, 0, n)

	return int(math.Floor(sf+0.5))
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

// Scale normalizes the input x with the input-min a0,
// input-max a1, output-min b0, and output-max b1.
func scale(x float64, a0, a1, b0, b1 float64) float64 {
	return (x-a0)/(a1-a0)*(b1-b0) + b0
}
