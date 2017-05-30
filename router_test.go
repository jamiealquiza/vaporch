package vaporhr_test

import (
	"testing"

	"github.com/jamiealquiza/vaporhr"
)

func TestAddNode(t *testing.T) {
	r, _ := vaporhr.New(&vaporhr.Config{})

	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	// Ensure dupes are prevented.
	err := r.AddNode("node-a")
	if err != vaporhr.ErrNodeExists {
		t.Error("Expected vaporhr.ErrNodeExists")
	}
}

func TestAddNodes(t *testing.T) {
	r, _ := vaporhr.New(&vaporhr.Config{Nodes: []string{"node-a", "node-b", "node-c"}})

	expected := []string{"node-a", "node-b", "node-c"}
	got := r.Members().Names()

	for n := range got {
		if got[n] != expected[n] {
			t.Errorf("Expected member %s, got %s", expected[n], got[n])
		}
	}
}

func TestRemoveNode(t *testing.T) {
	r, _ := vaporhr.New(&vaporhr.Config{})

	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	r.RemoveNode("node-c")

	members := r.Members().Names()

	// Ensure dupes are prevented.
	for _, n := range members {
		if n == "node-c" {
			t.Error("Unexpected node member node-c")
		}
	}
}

func TestGet(t *testing.T) {
	r, _ := vaporhr.New(&vaporhr.Config{})

	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	if r.Get("someRandomKey") != "node-d" {
		t.Errorf("Expected node-d, got %s\n", r.Get("someRandomKey"))
	}
}

func TestGetN(t *testing.T) {
	r, _ := vaporhr.New(&vaporhr.Config{})

	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	expected := []string{"node-c", "node-d", "node-e"}
	got := r.GetN("someRandomKey", 3)

	for n := range got {
		if got[n] != expected[n] {
			t.Errorf("Expected %s, got %s\n", expected[n], got[n])
		}
	}

	got = r.GetN("someRandomKey", 8)
	if len(got) > len(r.Members()) {
		t.Error("Unexpected number of nodes returned")
	}
}

func TestMembers(t *testing.T) {
	r, _ := vaporhr.New(&vaporhr.Config{})

	r.AddNode("node-a")
	r.AddNode("node-e")
	r.AddNode("node-d")
	r.AddNode("node-c")
	r.AddNode("node-b")

	// The router should sort the node names
	// lexicographically.
	members := r.Members().Names()
	expected := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	for n := range members {
		if members[n] != expected[n] {
			t.Error("Unexpected node member list or list order")
		}
	}
}
