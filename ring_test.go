package vaporch_test

import (
	"fmt"
	"testing"

	"github.com/jamiealquiza/vaporch"
)

var keys = []string{"Baby", "Baby", "Backpack", "Balloon", "Banana", "Bank", "Barbecue", "Bathroom", "Bathtub", "Bed", "Bed", "Bee", "Bible", "Bible", "Bird", "Bomb", "Book", "Boss", "Bottle", "Bowl", "Box", "Boy", "Brain", "Bridge", "Butterfly", "Button", "Cappuccino", "Car", "Car-race", "Carpet", "Carrot", "Cave", "Chair", "Chess", "Board", "Chief", "Child", "Chisel", "Chocolates", "Church", "Church", "Circle", "Circus", "Circus", "Clock", "Clown", "Coffee", "Coffee-shop"}

func TestAdd(t *testing.T) {
	r, _ := vaporch.New(&vaporch.Config{})

	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	err := r.AddNode("node-a")
	if err != vaporch.ErrNodeExists {
		t.Error("Expected vaporch.ErrNodeExists")
	}
}

func TestMembers(t *testing.T) {
	r, _ := vaporch.New(&vaporch.Config{})

	r.AddNode("node-a")
	r.AddNode("node-e")
	r.AddNode("node-d")
	r.AddNode("node-c")
	r.AddNode("node-b")

	err := r.AddNode("node-a")
	if err != vaporch.ErrNodeExists {
		t.Error("Expected vaporch.ErrNodeExists")
	}

	members := r.Members().Names()
	expected := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	for n := range members {
		if members[n] != expected[n] {
			t.Error("Unexpected node member list or list order")
		}
	}
}

func TestDistribution(t *testing.T) {
	r, _ := vaporch.New(&vaporch.Config{})

	nodes := map[string]uint64{
		"node-a": 0,
		"node-b": 0,
		"node-c": 0,
		"node-d": 0,
		"node-e": 0,
	}

	for n := range nodes {
		r.AddNode(n)
	}

	for _, k := range keys {
		nodes[r.Get(k)] += 1
	}

	fmt.Println(nodes)

}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()

	r, _ := vaporch.New(&vaporch.Config{})

	r.AddNode("node-a")
	r.AddNode("node-e")
	r.AddNode("node-d")
	r.AddNode("node-c")
	r.AddNode("node-b")

	mod := len(keys)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		r.Get(keys[i%mod])
	}
}
