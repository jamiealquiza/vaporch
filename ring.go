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

type Ring struct {
	sync.RWMutex
	nodes   nodeList
	nodeMap map[string]*Node
}

type nodeList []*Node

type Node struct {
	Name string
}

type Config struct {
	Nodes []*Node
}

func New(c *Config) (*Ring, error) {
	r := &Ring{
		nodes:   nodeList{},
		nodeMap: make(map[string]*Node),
	}

	if c.Nodes == nil {
		return r, nil
	}

	r.AddNodes(c.Nodes)

	return r, nil
}

func (n nodeList) Len() int           { return len(n) }
func (n nodeList) Less(i, j int) bool { return n[i].Name < n[j].Name }
func (n nodeList) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

func (n nodeList) Names() []string {
	s := []string{}
	for _, n := range n {
		s = append(s, n.Name)
	}

	return s
}

// Hash node name and add to sorted list.
func (r *Ring) AddNode(n string) error {
	r.Lock()
	defer r.Unlock()
	if _, exists := r.nodeMap[n]; exists {
		return ErrNodeExists
	}

	node := &Node{Name: n}
	r.nodes = append(r.nodes, node)
	sort.Sort(r.nodes)
	r.nodeMap[n] = node

	return nil
}

func (r *Ring) AddNodes(ns []*Node) {
	_ = ns
}

func (r *Ring) Members() nodeList {
	r.RLock()
	m := make(nodeList, len(r.nodes))
	copy(m, r.nodes)
	r.RUnlock()

	return m
}

func (r *Ring) Get(k string) string {
	return r.nodes[idxFromKey(k, len(r.nodes))].Name
}

func idxFromKey(k string, l int) int {
	n := float64(l)
	return int(scale(float64(hash(k)), 0, math.MaxUint64, 0, n))
}

// FNV-1a 64 bit.
func hash(k string) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range []byte(k) {
		h ^= uint64(c)
		h *= 1099511628211
	}

	return h
}

// Input, InputMin, InputMax, OutputMin, OutputMax
func scale(x float64, a0, a1, b0, b1 float64) float64 {
	return (x-a0)/(a1-a0)*(b1-b0) + b0
}
