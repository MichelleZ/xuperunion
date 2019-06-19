/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"

	"github.com/xuperchain/log15"
)

const (
	// DefaultReplicas node default nums
	DefaultReplicas = 10
)

// KetamaConsistentHash keta struct
type KetamaConsistentHash struct {
	mu           sync.Mutex
	replicas     int
	hashMapNodes map[int]string // grpc.Address.Addr+weight_flag+replica_flag
	keysRing     HashRing
	resources    map[string]bool // grpc.Address.Addr string
	xlog         log.Logger
}

// NewKetamaConsistentHash New ketama struct
func NewKetamaConsistentHash(replicas int, xlog log.Logger) *KetamaConsistentHash {
	kch := &KetamaConsistentHash{
		replicas:     replicas,
		hashMapNodes: make(map[int]string),
		keysRing:     HashRing{},
		resources:    make(map[string]bool),
		xlog:         xlog,
	}

	if kch.replicas <= 0 {
		kch.replicas = DefaultReplicas
	}

	return kch
}

// Add add node to ketama
func (kch *KetamaConsistentHash) Add(node string) bool {
	kch.mu.Lock()
	defer kch.mu.Unlock()

	if _, ok := kch.resources[node]; ok {
		return false
	}

	for i := 0; i < kch.replicas; i++ {
		convertKey := kch.combineKey(i, node)
		key := int(crc32.ChecksumIEEE([]byte(convertKey)))
		kch.hashMapNodes[key] = convertKey
	}

	kch.resources[node] = true

	kch.sortKeysRing()
	return true
}

func (kch *KetamaConsistentHash) combineKey(i int, node string) string {
	return node + "-" + strconv.Itoa(i)
}

func (kch *KetamaConsistentHash) sortKeysRing() {
	kch.keysRing = HashRing{}
	for k := range kch.hashMapNodes {
		kch.keysRing = append(kch.keysRing, k)
	}
	sort.Sort(kch.keysRing)
}

// Delete delete node from keta
func (kch *KetamaConsistentHash) Delete(node string) {
	kch.mu.Lock()
	defer kch.mu.Unlock()

	if _, ok := kch.resources[node]; !ok {
		return
	}

	delete(kch.resources, node)

	for i := 0; i < kch.replicas; i++ {
		convertKey := kch.combineKey(i, node)
		key := int(crc32.ChecksumIEEE([]byte(convertKey)))
		delete(kch.hashMapNodes, key)
	}

	kch.sortKeysRing()
}

// Get get distributed node from key
func (kch *KetamaConsistentHash) Get(key string) (string, bool) {
	if kch.IsEmpty() {
		return "", false
	}

	hash := int(crc32.ChecksumIEEE([]byte(key)))
	kch.xlog.Info("keyRing for key", "key", hash)

	kch.mu.Lock()
	defer kch.mu.Unlock()

	idx := sort.Search(len(kch.keysRing), func(i int) bool {
		return kch.keysRing[i] >= hash
	})

	if idx == len(kch.keysRing) {
		idx = 0
	}

	node, ok := kch.hashMapNodes[kch.keysRing[idx]]
	return node, ok
}

// IsEmpty if keta is empty
func (kch *KetamaConsistentHash) IsEmpty() bool {
	kch.mu.Lock()
	defer kch.mu.Unlock()

	return len(kch.keysRing) == 0
}
