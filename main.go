package main

import (
	"fmt"
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

	fmt.Printf("%+v\n", g.edges)

	for _, v := range g.TopSort() {
		fmt.Printf("%d ", v)
	}
	fmt.Println()

	iter := NewIter(&g)
	for iter.HasNext() {
		fmt.Printf("%d ", iter.Next())
	}
	fmt.Println()

	pariter := NewParIter(&g)
	readyCh := pariter.Ready()
	fmt.Printf("%d ", <-readyCh)
	fmt.Printf("%d ", <-readyCh)
	pariter.Complete(2)
	fmt.Printf("%d ", <-readyCh)
	pariter.Complete(3)
	pariter.Complete(4)
	fmt.Printf("%d ", <-readyCh)
	pariter.Complete(5)
	fmt.Printf("%d ", <-readyCh)
	fmt.Printf("%d ", <-readyCh)
	pariter.Complete(0)
	pariter.Complete(1)
	// chan already closed
	<-readyCh
	fmt.Println()
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

type ParIter[T comparable] struct {
	g        *Graph[T]
	indegree map[T]int
	q        []T
	ready    chan T
	msg      chan T
}

func NewParIter[T comparable](g *Graph[T]) *ParIter[T] {
	indegree := map[T]int{}

	for _, deps := range g.edges {
		for _, u := range deps {
			indegree[u]++
		}
	}

	readyCh := make(chan T, len(g.edges))
	for v := range g.edges {
		if indegree[v] == 0 {
			readyCh <- v
		}
	}

	iter := &ParIter[T]{
		g:        g,
		indegree: indegree,
		ready:    readyCh,
		msg:      make(chan T),
	}
	go iter.run()
	return iter
}

func (i *ParIter[T]) Ready() <-chan T {
	return i.ready
}

func (i *ParIter[T]) Complete(v T) {
	i.msg <- v
}

func (i *ParIter[T]) run() {
	cnt := 0
Loop:
	for {
		select {
		case v := <-i.msg:
			for _, u := range i.g.edges[v] {
				i.indegree[u]--
				if i.indegree[u] == 0 {
					i.ready <- u
				}
			}
			cnt++

			if cnt == len(i.g.v) {
				close(i.ready)
				break Loop
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}
