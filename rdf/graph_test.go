package rdf

import (
	"bytes"
	"sort"
	"strings"
	"testing"
)

type triples []Triple

func (t triples) Len() int           { return len(t) }
func (t triples) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t triples) Less(i, j int) bool { return t[i].String() < t[j].String() }

func eqTriples(a, b []Triple) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	sort.Sort(triples(a))
	sort.Sort(triples(b))
	for i, tr := range a {
		if b[i] != tr {
			return false
		}
	}
	return true
}

func TestGraphInsert(t *testing.T) {
	g := NewGraph()

	trs := []Triple{
		{NewURI("s"), NewURI("p"), NewLiteral("a")},
		{NewURI("s"), NewURI("p"), NewLiteral(int32(100))},
		{NewURI("s"), NewURI("p"), NewLiteral("a")},
	}

	if n := g.Insert(trs...); n != 2 {
		t.Errorf("Graph.Insert(<2 triples>) => %d ; want 2", n)
	}

	if g.Size() != 2 {
		t.Errorf("Graph.Size() => %d; want 2", g.Size())
	}

	if !eqTriples(trs[:2], g.Triples()) {
		t.Errorf("Graph.Triples() => %v; want %v", g.Triples(), trs[:2])
	}

	if n := g.Insert(trs[0]); n != 0 {
		t.Errorf("Graph.Insert(%v) => %d; want 0", trs[0], n)
	}

	tests := []struct {
		tr   Triple
		want bool
	}{
		{trs[0], true},
		{trs[1], true},
		{Triple{NewURI("s"), NewURI("p"), NewLiteral("A")}, false},
		{Triple{NewURI("s"), NewURI("p"), NewLiteral(" a")}, false},
		{Triple{NewURI("s"), NewURI("p2"), NewLiteral("a")}, false},
		{Triple{NewURI("s"), NewURI("p"), NewLangLiteral("a", "en")}, false},
		{Triple{NewURI("s"), NewURI("p"), NewTypedLiteral("a", NewURI("mytype"))}, false},
		{Triple{NewURI("s"), NewURI("p"), NewLiteral(int64(100))}, false},
	}

	for _, test := range tests {
		if ok := g.Has(test.tr); ok != test.want {
			t.Errorf("Graph.Has(%v) => %v; want %v", test.tr, ok, test.want)
		}
	}
}

func TestGraphDelete(t *testing.T) {
	g := NewGraph()

	trs := []Triple{
		{NewURI("s"), NewURI("p"), NewLiteral("a")},
		{NewURI("s"), NewURI("p"), NewLiteral("b")},
		{NewURI("s"), NewURI("p"), NewLiteral("c")},
	}

	g.Insert(trs...)

	if g.Size() != 3 {
		t.Errorf("Graph.Size() => %d; want 3", g.Size())
	}

	if n := g.Delete(trs[0]); n != 1 {
		t.Errorf("Graph.Delete(%v) => %d; want 1", trs[0], n)
	}

	if g.Has(trs[0]) {
		t.Errorf("Graph.Delete(%v) didn't delete triple", trs[0])
	}

	if g.Size() != 2 {
		t.Errorf("Graph.Size() => %d; want 2", g.Size())
	}

	if n := g.Delete(trs...); n != 2 {
		t.Errorf("Graph.Delete(%v) => %d; want 1", trs, n)
	}

	if g.Size() != 0 {
		t.Errorf("Graph.Size() => %d; want 0", g.Size())
	}
}

func TestGraphEq(t *testing.T) {
	a := NewGraph()
	a.Insert(
		Triple{NewURI("s"), NewURI("p"), NewLiteral("a")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("b")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("c")},
		Triple{NewURI("s2"), NewURI("p2"), NewURI("s")},
	)
	b := NewGraph()
	b.Insert(
		Triple{NewURI("s2"), NewURI("p2"), NewURI("s")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("b")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("c")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("a")},
	)
	c := NewGraph()
	c.Insert(
		Triple{NewURI("s"), NewURI("p"), NewLiteral("a")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("b")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("c")},
		Triple{NewURI("s2"), NewURI("p2"), NewURI("s")},
		Triple{NewURI("s"), NewURI("p2"), NewURI("s2")},
	)
	d := NewGraph()
	d.Insert(
		Triple{NewURI("s"), NewURI("p"), NewLiteral("a")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("b")},
		Triple{NewURI("s"), NewURI("p"), NewLiteral("c")},
	)

	tests := []struct {
		a, b *Graph
		want bool
	}{
		{a, b, true},
		{a, c, false},
		{a, d, false},
	}

	for _, test := range tests {
		if got := test.a.Eq(test.b); got != test.want {
			t.Errorf("%v\nEq\n%v => %v; want %v", test.a.Serialize(Turtle), test.b.Serialize(Turtle), got, test.want)
		}
	}
}

func TestGraphNTriples(t *testing.T) {
	g := NewGraph()
	trs := []Triple{
		{NewURI("s"), NewURI("p"), NewLangLiteral("a", "en")},
		{NewURI("s2"), NewURI("p2"), NewLiteral(int32(100))},
		{NewURI("s"), NewURI("p"), NewLiteral("x\ny\nz")},
		{NewURI("s3"), NewURI("p3"), NewURI("s")},
	}
	g.Insert(trs...)

	want := `<s> <p> "a"@en .
<s2> <p2> "100"^^<http://www.w3.org/2001/XMLSchema#int> .
<s> <p> "x\ny\nz" .
<s3> <p3> <s> .
`
	wantLines := strings.Split(want, "\n")
	sort.Strings(wantLines)
	nt := g.Serialize(NTriples)
	ntLines := strings.Split(nt, "\n")
	sort.Strings(ntLines)
	if len(ntLines) != len(wantLines) {
		t.Fatalf("Graph.Serialize(%v, NTriples) => \n%s\nwant:\n%v", trs, nt, want)
	}
	for i, l := range wantLines {
		if l != ntLines[i] {
			t.Fatalf("Graph.Serialize(%v, NTriples) => \n%s\nwant:\n%v", trs, nt, want)
		}
	}
}

func TestGraphTurtle(t *testing.T) {
	g := NewGraph()
	trs := []Triple{
		{NewURI("s"), NewURI("p"), NewLangLiteral("a", "en")},
		{NewURI("s2"), NewURI("p2"), NewLiteral(int32(100))},
		{NewURI("s"), NewURI("p99"), NewLiteral("x\ny\nz")},
		{NewURI("s"), NewURI("p99"), NewLiteral("æøå")},
		{NewURI("s3"), NewURI("p3"), NewURI("s")},
	}
	g.Insert(trs...)

	want := `<s> <p> "a"@en ;
	<p99> "x\ny\nz", "æøå" .
<s2> <p2> "100"^^<http://www.w3.org/2001/XMLSchema#int> .
<s3> <p3> <s> .
`

	dec := NewDecoder(bytes.NewBufferString(want))
	wantGraph, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}
	if !g.Eq(wantGraph) {
		t.Errorf("Graph.Serialize(%v) => \n%s\nwant:\n%s",
			trs, g.Serialize(Turtle), wantGraph.Serialize(Turtle))
	}
}
