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
			t.Errorf("%v\nEq\n%v => %v; want %v", test.a.Serialize(Turtle, ""), test.b.Serialize(Turtle, ""), got, test.want)
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
	nt := g.Serialize(NTriples, "")
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
			trs, g.Serialize(Turtle, ""), wantGraph.Serialize(Turtle, ""))
	}
}

func TestGraphDot(t *testing.T) {
	t.Skip()
	g := NewGraph()
	trs := []Triple{
		{NewURI("http://example.org/person/2"), NewURI("http://example.org/ontology#name"), NewLiteral("Kurt Vonnegut")},
		{NewURI("http://example.org/person/2"), NewURI("http://example.org/ontology#born"), NewLiteral(1922)},
		{NewURI("http://example.org/person/2"), NewURI("http://example.org/ontology#dead"), NewLiteral(2007)},
		{NewURI("http://example.org/person/2"), NewURI("http://example.org/ontology#birthplace"), NewURI("http://example.org/place/1")},
		{NewURI("http://example.org/place/1"), NewURI("http://example.org/ontology#name"), NewLiteral("Indianapolis")},
		{NewURI("http://example.org/work/1"), NewURI("http://example.org/ontology#written_by"), NewURI("http://example.org/person/2")},
		{NewURI("http://example.org/work/2"), NewURI("http://example.org/ontology#written_by"), NewURI("http://example.org/person/2")},
		{NewURI("http://example.org/work/1"), NewURI("http://example.org/ontology#title"), NewLiteral("Cat's cradle")},
		{NewURI("http://example.org/work/1"), NewURI("http://example.org/ontology#first_published"), NewLiteral(1963)},
		{NewURI("http://example.org/work/2"), NewURI("http://example.org/ontology#title"), NewLiteral("Galápagos")},
		{NewURI("http://example.org/work/2"), NewURI("http://example.org/ontology#first_published"), NewLiteral(1985)},
	}
	g.Insert(trs...)

	want := `digraph G {
	node [shape=plaintext];

	"http://example.org/person/2"[label=<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='5'>
	<TR><TD BGCOLOR='#a0ffa0' COLSPAN='2'><FONT POINT-SIZE='12' FACE='monospace'>&lt;person/2&gt;</FONT></TD></TR>
	<TR>
		<TD ALIGN='RIGHT'><B>name</B> </TD>
		<TD ALIGN='LEFT'>Kurt Vonnegut</TD>
	</TR>
	<TR>
		<TD ALIGN='RIGHT'><B>born</B> </TD>
		<TD ALIGN='LEFT'>1922</TD>
	</TR>
	<TR>
		<TD ALIGN='RIGHT'><B>dead</B> </TD>
		<TD ALIGN='LEFT'>2007</TD>
	</TR>
	</TABLE>>];

	"http://example.org/place/1"[label=<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='5'>
	<TR><TD HREF='http://example.org/place/1.svg' BGCOLOR='#e0e0e0' COLSPAN='2'><FONT COLOR='blue' POINT-SIZE='12' FACE='monospace'>&lt;place/1&gt;</FONT></TD></TR>
	<TR>
		<TD ALIGN='RIGHT'><B>name</B> </TD>
		<TD ALIGN='LEFT'>Indianapolis</TD>
	</TR>
	</TABLE>>];

	"http://example.org/work/1"[label=<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='5'>
	<TR><TD HREF='http://example.org/work/1.svg' BGCOLOR='#e0e0e0' COLSPAN='2'><FONT COLOR='blue' POINT-SIZE='12' FACE='monospace'>&lt;work/1&gt;</FONT></TD></TR>
	<TR>
		<TD ALIGN='RIGHT'><B>first_published</B> </TD>
		<TD ALIGN='LEFT'>1963</TD>
	</TR>
	<TR>
		<TD ALIGN='RIGHT'><B>title</B> </TD>
		<TD ALIGN='LEFT'>Cat's cradle</TD>
	</TR>
	</TABLE>>];

	"http://example.org/work/2"[label=<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='5'>
	<TR><TD HREF='http://example.org/work/2.svg' BGCOLOR='#e0e0e0' COLSPAN='2'><FONT COLOR='blue' POINT-SIZE='12' FACE='monospace'>&lt;work/2&gt;</FONT></TD></TR>
	<TR>
		<TD ALIGN='RIGHT'><B>title</B> </TD>
		<TD ALIGN='LEFT'>Galápagos</TD>
	</TR>
	<TR>
		<TD ALIGN='RIGHT'><B>first_published</B> </TD>
		<TD ALIGN='LEFT'>1985</TD>
	</TR>
	</TABLE>>];

	"http://example.org/person/2"->"http://example.org/place/1"[label="birthplace"];
	"http://example.org/work/1"->"http://example.org/person/2"[label="written_by"];
	"http://example.org/work/2"->"http://example.org/person/2"[label="written_by"];
}`
	// TODO split lines, sort and compare
	if got := g.Dot("http://example.org/", []string{"http://example.org/person/2"}); got != want {
		t.Errorf("got:\n%s\nwant:\n%v", got, want)
	}

}

func TestDescribe(t *testing.T) {
	input := `
<s1> <p> "a" ;
     <p2> "b" ;
     <p3> <s2> ;
     <p4> "x", "y" .
<s3> <p5> <s1> ;
     <p> "zz" .
<s2> <p> "xx" .`

	want1 := `
<s1> <p> "a" ;
     <p2> "b" ;
     <p3> <s2> ;
     <p4> "x", "y" .`

	want2 := `
<s1> <p> "a" ;
     <p2> "b" ;
     <p3> <s2> ;
     <p4> "x", "y" .
<s3> <p5> <s1> .`

	dec := NewDecoder(bytes.NewBufferString(input))
	g, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}
	dec = NewDecoder(bytes.NewBufferString(want1))
	wantG1, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}

	dec = NewDecoder(bytes.NewBufferString(want2))
	wantG2, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}

	got := g.Describe(NewURI("s1"), false)

	if !got.Eq(wantG1) {
		t.Errorf("Describe(<s1>, false) => \n%s\nwant:\n%s", got.Serialize(Turtle, ""), wantG1.Serialize(Turtle, ""))
	}

	got = g.Describe(NewURI("s1"), true)

	if !got.Eq(wantG2) {
		t.Errorf("Describe(<s1>, false) => \n%s\nwant:\n%s", got.Serialize(Turtle, ""), wantG2.Serialize(Turtle, ""))
	}
}

func TestGraphMerge(t *testing.T) {
	a := `<s> <p> "a" .
<s2> <p> <o> . `

	b := `<s> <p> "b" .
<s2> <p> <o> .
<s3> <p> <o> .`

	want := `<s> <p> "a", "b" .
<s2> <p> <o> .
<s3> <p> <o> .`

	dec := NewDecoder(bytes.NewBufferString(a))
	ga, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}
	dec = NewDecoder(bytes.NewBufferString(b))
	gb, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}
	dec = NewDecoder(bytes.NewBufferString(want))
	wantg, err := dec.DecodeGraph()
	if err != nil {
		t.Fatal(err)
	}

	if !ga.Merge(gb).Eq(wantg) {
		t.Errorf("merging\n%s\nwith\n%s\ngot:\n%s\nwant:\n%s",
			a, b, ga.Serialize(Turtle, ""), want)
	}

}
