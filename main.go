package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
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
	err := g.ParTopSortWalk(5, func(v int) {
		fmt.Printf("%d ", v)
		time.Sleep(1 * time.Second)
	})
	if err != nil {
		panic(err)
	}
}

type Graph[T comparable] struct {
	v     map[T]struct{}
	edges map[T][]T
}

func (g *Graph[T]) Add(v T) {
	g.v[v] = struct{}{}
}

func (g *Graph[T]) Connect(u, v T) {
	g.edges[u] = append(g.edges[u], v)
}

// Kahn’s algorithm
func (g *Graph[T]) TopSort() []T {
	indegree := map[T]int{}

	for _, deps := range g.edges {
		for _, u := range deps {
			indegree[u]++
		}
	}

	q := []T{}
	for v := range g.edges {
		if indegree[v] == 0 {
			q = append(q, v)
		}
	}

	cnt := 0
	sorted := []T{}
	for len(q) > 0 {
		v := q[0]
		q = q[1:]
		sorted = append(sorted, v)

		for _, u := range g.edges[v] {
			indegree[u]--
			if indegree[u] == 0 {
				q = append(q, u)
			}
		}
		cnt++
	}

	if cnt != len(g.v) {
		panic("cycle detected")
	}

	return sorted
}

func (g *Graph[T]) TopSortWalk(fn func(v T) error) {
	indegree := map[T]int{}

	for _, deps := range g.edges {
		for _, u := range deps {
			indegree[u]++
		}
	}

	q := []T{}
	for v := range g.edges {
		if indegree[v] == 0 {
			q = append(q, v)
		}
	}

	cnt := 0
	for len(q) > 0 {
		v := q[0]
		q = q[1:]
		// exit early
		if err := fn(v); err != nil {
			return
		}

		for _, u := range g.edges[v] {
			indegree[u]--
			if indegree[u] == 0 {
				q = append(q, u)
			}
		}
		cnt++
	}

	if cnt != len(g.v) {
		panic("cycle detected")
	}
}

// ParTopSortWalk walks the graph in topologically sorted order calling fn in
// parallel. Control goroutine concurrency through the "c" parameter.
// Implements Kahn’s algorithm.
func (g *Graph[T]) ParTopSortWalk(c int, fn func(v T)) error {
	if len(g.v) == 0 {
		return nil
	}

	indegree := map[T]int{}

	for _, deps := range g.edges {
		for _, u := range deps {
			indegree[u]++
		}
	}

	// this must be buffered since the main goroutine and executor goroutines
	// do not deadlock since they are communicating back and forth over the
	// following two channels
	readyCh := make(chan T, len(g.v))
	done := make(chan T)

	var wg sync.WaitGroup
	wg.Add(c)
	for i := 0; i < c; i++ {
		go func() {
			defer wg.Done()
			for v := range readyCh {
				fn(v)
				done <- v
			}
		}()
	}

	outstanding := 0
	// add all source nodes (nodes that have no inbound edges)
	for v := range g.edges {
		if indegree[v] == 0 {
			outstanding++
			readyCh <- v
		}
	}

	// if we get to this point we know there are verticies in the graph so if
	// we didn't enqueue any verticies for processing then the graph must be
	// made of no source edges (i.e. all verticies form one large cycle)
	if outstanding == 0 {
		return errors.New("cycle detected")
	}

	completed := 0
	// processing of v has finished
	for v := range done {
		// notify all of v's dependents that it has finished
		for _, u := range g.edges[v] {
			indegree[u]--
			if indegree[u] == 0 {
				// u has no more requirements, it is now a source vertex,
				// process it
				outstanding++
				readyCh <- u
			}
		}

		completed++
		outstanding--
		// this indicates that the "queue" is empty
		if outstanding == 0 {
			break
		}
	}
	close(done)
	close(readyCh)
	wg.Wait()

	// if we didn't process all verticies then there must have been a cycle
	if completed != len(g.v) {
		return errors.New("cycle detected")
	}

	return nil
}

type Iter[T comparable] struct {
	g        *Graph[T]
	indegree map[T]int
	q        []T
}

func NewIter[T comparable](g *Graph[T]) *Iter[T] {
	indegree := map[T]int{}

	for _, deps := range g.edges {
		for _, u := range deps {
			indegree[u]++
		}
	}

	q := []T{}
	for v := range g.edges {
		if indegree[v] == 0 {
			q = append(q, v)
		}
	}

	return &Iter[T]{
		g:        g,
		indegree: indegree,
		q:        q,
	}
}

func (i *Iter[T]) HasNext() bool {
	return len(i.q) > 0
}

func (i *Iter[T]) Next() T {
	v := i.q[0]
	i.q = i.q[1:]

	for _, u := range i.g.edges[v] {
		i.indegree[u]--
		if i.indegree[u] == 0 {
			i.q = append(i.q, u)
		}
	}

	return v
}
