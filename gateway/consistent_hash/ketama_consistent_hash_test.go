/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package consistenthash

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/xuperchain/xuperunion/common/log"
	loggw "github.com/xuperchain/xuperunion/gateway/log"
)

const (
	node1 = "10.38.87.38:8989"
	node2 = "10.38.87.60:8989"
	node3 = "10.38.87.38:8967"
	node4 = "10.38.87.45:8967"
	node5 = "10.38.87.45:8988"
)

var (
	xlog, _ = log.OpenLog(loggw.CreateLog())
)

func TestNewKetamaConsistentHash(t *testing.T) {
	kch := NewKetamaConsistentHash(8, xlog)

	if kch.replicas != 8 {
		t.Errorf("Replicas is not equal intialized value.")
	}

	if len(kch.keysRing) != 0 {
		t.Errorf("KeysRing is not empty.")
	}

	if len(kch.resources) != 0 {
		t.Errorf("Resources is not empty.")
	}

	if len(kch.hashMapNodes) != 0 {
		t.Errorf("HashMapNodes is not empty.")
	}
	t.Log(kch.replicas)
	t.Log(kch.keysRing)
	t.Log(kch.resources)
	t.Log(kch.hashMapNodes)
}

func TestDefaultReplicas(t *testing.T) {
	kch := NewKetamaConsistentHash(0, xlog)

	if kch.replicas != 10 {
		t.Errorf("Replicas is not equal intialized value.")
	}
}

func TestAdd(t *testing.T) {
	kch := NewKetamaConsistentHash(1, xlog)
	kch.Add(node1)

	for _, ring := range kch.keysRing {
		if ring != 2860987738 {
			t.Errorf("Computed ring is %v.", ring)
		}
		t.Log(ring)
	}

	for k, v := range kch.resources {
		if k != "10.38.87.38:8989" || !v {
			t.Errorf("Resoures k: %v, v: %v.", k, v)
		}
		t.Logf("Key: %v, value: %v.", k, v)
	}

	for k, v := range kch.hashMapNodes {
		if k != 2860987738 || v != "10.38.87.38:8989-0" {
			t.Errorf("HashMapNodes k: %v, v: %v.", k, v)
		}
		t.Logf("Key: %v, value: %v.", k, v)
	}
}

func TestRepeatedAdd(t *testing.T) {
	kch := NewKetamaConsistentHash(1, xlog)
	kch.Add(node1)

	err := kch.Add(node1)
	if err {
		t.Errorf("Add repeated node error.")
	}
}

func TestDelete(t *testing.T) {
	kch := NewKetamaConsistentHash(1, xlog)
	kch.Add(node1)

	kch.Delete(node1)

	if len(kch.keysRing) != 0 {
		t.Errorf("KeysRing is not empty.")
	}

	if len(kch.resources) != 0 {
		t.Errorf("Resources is not empty.")
	}

	if len(kch.hashMapNodes) != 0 {
		t.Errorf("HashMapNodes is not empty.")
	}
}

func TestSortKeysRing(t *testing.T) {
	kch := NewKetamaConsistentHash(2, xlog)
	kch.Add(node1)
	kch.Add(node2)

	ring0 := 0
	for i, ring := range kch.keysRing {
		if i == 0 {
			ring0 = ring
		}
		if ring < ring0 {
			t.Errorf("SortKeysRing error.")
		}
		t.Log(ring)
	}
}

func TestIsEmpty(t *testing.T) {
	kch := NewKetamaConsistentHash(2, xlog)
	if !kch.IsEmpty() {
		t.Errorf("KeysRing should be empty.")
	}
}

func TestGet(t *testing.T) {
	kch := NewKetamaConsistentHash(2, xlog)
	kch.Add(node1)
	kch.Add(node2)
	kch.Add(node3)

	node, err := kch.Get("a")
	if node != "10.38.87.60:8989-0" || !err {
		t.Error("Distributed addr by Get() method is wrong.")
	}
	t.Logf("Distributed addr by Get() method is %v", node)
}

func TestKetamaConsistentHash(t *testing.T) {
	kch := NewKetamaConsistentHash(2, xlog)
	node, err := kch.Get("a")
	if node != "" || err {
		t.Error("Distributed addr by Get() method is wrong.")
	}

	t.Log("Intialize env")
	kch.Add(node1)
	kch.Add(node2)
	kch.Add(node3)
	kch.Add(node4)
	kch.Add(node5)
	kch.Add(node5)
	for _, ring := range kch.keysRing {
		t.Log(ring)
	}
	node, err = kch.Get("a")
	t.Log(node)
	t.Log(err)

	kch.Add(node2)
	kch.Delete(node2)
	kch.Add(node2)
	node, err = kch.Get("a")
	t.Log(node)
	t.Log(err)

	cnt := make(map[string]int)
	rand.Seed(time.Now().Unix())
	for i := 0; i < 20; i++ {
		rnd := rand.Intn(10000)
		key := fmt.Sprintf("abcd%v ", rnd)
		node, _ := kch.Get(key)
		cnt[node]++
	}

	t.Log(cnt)
}
