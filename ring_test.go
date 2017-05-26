package vaporch_test

import (
	"fmt"
	"testing"

	"github.com/jamiealquiza/vaporch"
)

var keys = []string{"illuminate", "reindicating", "monosymmetrical", "consultatory", "wroth", "tyrannosaur", "unsimmered", "resolvent", "jaycee", "sheathbill", "sievelike", "arena", "hemicyclic", "behead", "propensity", "arene", "sadducean", "venule", "hyposthenic", "leukorrheal", "nonpalpability", "juttingly", "preenjoyment", "flexuosely", "lumpen", "hold", "unilluminated", "hexamethylenetetramine", "ashen", "debtor", "vitamin", "dove", "verdigrisy", "uredial", "bipod", "cinematheque", "defaming", "bluestem", "porthole", "psalterium", "arianistic", "parathion", "realterable", "adorability", "gurgitation", "phenomenologically", "fowliang", "ghastily", "rutaceous", "broch", "nonemotional", "unfended", "cool", "counterturn", "jurat", "tabourer", "vaccinization", "circumfluous", "rackett", "basswood", "communism", "archdeaconate", "pyrochemical", "familism", "stravaiger", "orient", "asunci", "euphoriant", "phanerogamy", "proscribed", "hydroquinone", "preplotted", "drain", "unpioneering", "multitoned", "aegirine", "convenance", "wash", "crossbencher", "bethesda", "notion", "unsuccessive", "significativeness", "destituting", "bearishly", "simn", "repromised", "unridiculed", "watercress", "hagberry", "fangless", "slav", "thiopental", "indissolubility", "oculomotor", "calefaction", "barbless", "rewon", "manganophyllite", "albuminize", "vogie", "hospitable", "ducker", "snakelike", "attila", "cumulonimbus", "wordy", "apogamously", "constantinople", "invincibly", "preannounced", "holiday", "unchannelled", "inappreciativeness", "nonreflector", "unalacritous", "diptych", "vineries", "kenneled", "oversocialize", "cathomycin", "uncinctured", "donsy", "tathagata", "organza", "tipstaff", "retroaction", "noncontemporary", "metagenesis", "calorizer", "declive", "hall", "semirussian", "fiord", "cryophilic", "vestige", "hydrocarbonaceous", "tracklayer", "conicoid", "damp", "unhurtful", "experienced", "hither", "hortatorily", "serapeums", "institutes", "dvina", "undefrayed", "buckeen", "rheotropism", "superfeminine", "alden", "simile", "supernation", "dossil", "glossotomy", "inundation", "lonny", "mauby", "bat", "outblow", "dyestuff", "buttony", "naturopathy", "nantucket", "mandi", "oxyhydrogen", "whetstone", "sidereally", "logier", "belly", "uncouple", "subsidence", "puseyism", "lebbek", "fraternized", "stephen", "ucca", "discommodity", "pinny", "unmeritable", "prophetical", "ribbentrop", "nonassessability", "molt", "azotos", "plasmalogen", "overridden", "stylish", "undercurl", "reanimation", "tetrameter", "tincture", "tingle", "scorpioid", "nonelopement", "heterolysis", "day", "bibliographer"}

func TestAdd(t *testing.T) {
	r, _ := vaporch.New(&vaporch.Config{})

	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	// Ensure dupes are prevented.
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

	// The ring should sort the node names
	// lexicographically.
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
	r.AddNode("node-b")
	r.AddNode("node-c")
	r.AddNode("node-d")
	r.AddNode("node-e")

	mod := len(keys)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		r.Get(keys[i%mod])
	}
}
