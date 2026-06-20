package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"

	"distributed-kv-store/internal/config"
)

type HashRing struct {
	virtualNodes int
	keys         []uint32
	nodes        map[uint32]config.Node
}

func NewHashRing(virtualNodes int, nodes []config.Node) *HashRing {
	ring := &HashRing{
		virtualNodes: virtualNodes,
		nodes:        map[uint32]config.Node{},
	}
	for _, node := range nodes {
		ring.Add(node)
	}
	return ring
}

func (r *HashRing) Add(node config.Node) {
	for i := 0; i < r.virtualNodes; i++ {
		key := hashKey(node.ID + "#" + strconv.Itoa(i))
		r.keys = append(r.keys, key)
		r.nodes[key] = node
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *HashRing) Get(key string) (config.Node, bool) {
	nodes := r.GetReplicas(key, 1)
	if len(nodes) == 0 {
		return config.Node{}, false
	}
	return nodes[0], true
}

func (r *HashRing) GetReplicas(key string, count int) []config.Node {
	if len(r.keys) == 0 || count <= 0 {
		return nil
	}
	if count > len(r.uniqueNodes()) {
		count = len(r.uniqueNodes())
	}

	start := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hashKey(key)
	})
	if start == len(r.keys) {
		start = 0
	}

	result := make([]config.Node, 0, count)
	seen := map[string]bool{}
	for i := 0; len(result) < count && i < len(r.keys)*2; i++ {
		node := r.nodes[r.keys[(start+i)%len(r.keys)]]
		if seen[node.ID] {
			continue
		}
		seen[node.ID] = true
		result = append(result, node)
	}
	return result
}

func (r *HashRing) uniqueNodes() map[string]config.Node {
	nodes := map[string]config.Node{}
	for _, node := range r.nodes {
		nodes[node.ID] = node
	}
	return nodes
}

func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
