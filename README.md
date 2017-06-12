[![GoDoc](https://godoc.org/github.com/jamiealquiza/vaporhr?status.svg)](https://godoc.org/github.com/jamiealquiza/vaporch)

# vaporCH

Is a fast, general purpose, consistent hashing implementation for Go.

```
BenchmarkGet-8          30000000                39.9 ns/op             0 B/op          0 allocs/op
```

### Example
```golang
package main

import (
        "fmt"

        "github.com/jamiealquiza/vaporch"
)

func main() {
        r, _ := vaporch.New(&vaporhr.Config{
                Nodes: []string{"node-a", "node-b", "node-c", "node-d", "node-e"},
        })

        for _, key := range []string{"apple", "pear", "lemon", "pepper"} {
                fmt.Printf("Node for %s: %s\n", key, r.Get(key))
        }
}
```
Output:
```
Node for apple: node-a
Node for pear: node-e
Node for lemon: node-b
Node for pepper: node-a
```