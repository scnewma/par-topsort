package main

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestParWalk(t *testing.T) {
	g := Graph[int]{
		v:     make(map[int]struct{}),
		edges: make(map[int][]int),
	}
	for i := 0; i <= 5; i++ {
		g.Add(i)
	}

	g.Connect(5, 2)
	g.Connect(5, 0)
	g.Connect(4, 0)
	g.Connect(4, 1)
	g.Connect(2, 3)
	g.Connect(3, 1)
	rand.Seed(time.Now().Unix())

	recv := []int{}
	var mu sync.Mutex
	err := g.ParTopSortWalk(5, func(v int) {
		mu.Lock()
		recv = append(recv, v)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	seen := map[int]bool{}
	for _, v := range recv {
		for _, e := range g.edges[v] {
			if seen[e] {
				t.Errorf("saw child vertex before parent: list=%+v", recv)
			}
		}
		seen[v] = true
	}
}

func TestParWalkCycle(t *testing.T) {
	g := Graph[int]{
		v:     make(map[int]struct{}),
		edges: make(map[int][]int),
	}
	for i := 0; i <= 5; i++ {
		g.Add(i)
	}

	g.Connect(5, 2)
	g.Connect(5, 0)
	g.Connect(4, 0)
	g.Connect(4, 1)
	g.Connect(2, 3)
	g.Connect(3, 1)
	g.Connect(1, 5)

	rand.Seed(time.Now().Unix())
	recv := []int{}
	var mu sync.Mutex
	err := g.ParTopSortWalk(5, func(v int) {
		mu.Lock()
		recv = append(recv, v)
		mu.Unlock()
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	})
	if err == nil {
		t.Errorf("should have had cycle error")
	}
}
