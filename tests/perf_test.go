package vaporch_test

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/jamiealquiza/vaporch"
)

var (
	filePath string = "./words.txt"
)

// Words via https://raw.githubusercontent.com/dwyl/english-words/master/words.txt.

func TestDistribution(t *testing.T) {
	nodes := map[string]uint64{
		"node-a": 0,
		"node-b": 0,
		"node-c": 0,
		"node-d": 0,
		"node-e": 0,
	}

	r, _ := vaporch.New(&vaporch.Config{})
	for n := range nodes {
		r.AddNode(n)
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		nodes[r.Get(scanner.Text())]++
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("\t[Node:key-count]\t%v\n", nodes)

	var total float64
	var counts []float64
	for k := range nodes {
		v := float64(nodes[k])
		total += v
		counts = append(counts, v)
	}

	sort.Float64s(counts)
	rng := counts[len(counts)-1] - counts[0]
	imbp := rng / total * 100
	imbr := counts[len(counts)-1] / counts[0]

	fmt.Printf("\t[Greatest imbalance]\tportion of keys: %.2f%% / ratio: %.2fx\n\n",
		imbp, imbr)

}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()

	keyCount := 200000

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(file)
	r, _ := vaporch.New(&vaporch.Config{
		Nodes: []string{"node-a", "node-b", "node-c", "node-d", "node-e"},
	})

	keys := []string{}
	for scanner.Scan() {
		k := scanner.Text()
		keys = append(keys, k)
		if len(keys) == keyCount {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		r.Get(keys[i%keyCount])
	}
}
