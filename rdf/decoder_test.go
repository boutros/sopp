package rdf

import (
	"bytes"
	"io"
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		input string
		want  []Triple
	}{
		{"", nil},
		{"<s> <p> <o> .", []Triple{Triple{NewURI("s"), NewURI("p"), NewURI("o")}}},
		{`<s> <p> "abc" .`, []Triple{Triple{NewURI("s"), NewURI("p"), NewLiteral("abc")}}},
		{`<s> <p> "hi"@en .`, []Triple{Triple{NewURI("s"), NewURI("p"), NewLangLiteral("hi", "en")}}},
		{`<s> <p> "hi"@en ; <p2> "a", "b" .`, []Triple{
			Triple{NewURI("s"), NewURI("p"), NewLangLiteral("hi", "en")},
			Triple{NewURI("s"), NewURI("p2"), NewLiteral("a")},
			Triple{NewURI("s"), NewURI("p2"), NewLiteral("b")}}},
		{`<s> <p> "1"^^<int> .`, []Triple{Triple{NewURI("s"), NewURI("p"), NewTypedLiteral("1", NewURI("int"))}}},
		{"<s> <p> \"x\", \"y\" ; <p2> \"z\" .\n <s2> <p3> <s> .\n", []Triple{
			Triple{NewURI("s"), NewURI("p"), NewLiteral("x")},
			Triple{NewURI("s"), NewURI("p"), NewLiteral("y")},
			Triple{NewURI("s"), NewURI("p2"), NewLiteral("z")},
			Triple{NewURI("s2"), NewURI("p3"), NewURI("s")}}},
		{"<s> <p> <o>\n\t;<p2> <o2>\n\t;<p3> <o3> .\n <s2> <p4> <o4> .\n", []Triple{
			Triple{NewURI("s"), NewURI("p"), NewURI("o")},
			Triple{NewURI("s"), NewURI("p2"), NewURI("o2")},
			Triple{NewURI("s"), NewURI("p3"), NewURI("o3")},
			Triple{NewURI("s2"), NewURI("p4"), NewURI("o4")}}},
		{`<s> <p> "a" ; <p2> "b" ; <p3>  "c" .`, []Triple{
			Triple{NewURI("s"), NewURI("p"), NewLiteral("a")},
			Triple{NewURI("s"), NewURI("p2"), NewLiteral("b")},
			Triple{NewURI("s"), NewURI("p3"), NewLiteral("c")}}},
		{"@base <http://example.org> .\n<r1> <p1> <o1> .", []Triple{
			Triple{
				NewURI("http://example.org/r1"),
				NewURI("http://example.org/p1"),
				NewURI("http://example.org/o1"),
			}}},
		{"<s> a <Something> .", []Triple{
			Triple{NewURI("s"),
				RDFtype,
				NewURI("Something"),
			}}},
		{"@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n<s> <p> \"9912534\"^^xsd:long .", []Triple{
			Triple{
				NewURI("s"),
				NewURI("p"),
				NewLiteral(int64(9912534)),
			}}},
		{"<s> <p> true .\n <s2> <p2> false .", []Triple{
			Triple{NewURI("s"), NewURI("p"), NewLiteral(true)},
			Triple{NewURI("s2"), NewURI("p2"), NewLiteral(false)}}},
	}

	for _, test := range tests {
		dec := NewDecoder(bytes.NewBufferString(test.input))
		got := NewGraph()
		for tr, err := dec.Decode(); err != io.EOF; tr, err = dec.Decode() {
			if err != nil {
				t.Fatal(err)
			}
			got.Insert(tr)
		}
		want := NewGraph()
		want.Insert(test.want...)

		if !got.Eq(want) {
			t.Errorf("decoding:\n%q\ngot:\n%v\nwant:\n%v",
				test.input, got.Serialize(Turtle, ""), want.Serialize(Turtle, ""))
		}
	}
}

func TestSkolemizeBnode(t *testing.T) {
	input := `
	_:a <p> "o" .
	<s> <p> _:a ;
    <p> <o> .`
	dec := NewDecoder(bytes.NewBufferString(input))
	dec.Skolemize = func(s string) URI { return NewURI("base/" + s) }
	got, err := dec.DecodeGraph()
	if err != nil {
		t.Fatalf("decoding:\n%q\ngot error: ", input, err)
	}
	want := NewGraph()
	want.Insert(
		Triple{NewURI("s"), NewURI("p"), NewURI("o")},
		Triple{NewURI("base/a"), NewURI("p"), NewLiteral("o")},
		Triple{NewURI("s"), NewURI("p"), NewURI("base/a")},
	)
	if !got.Eq(want) {
		t.Errorf("got:\n%v\nwant:\n%v", got.Triples(), want.Triples())
	}
	// TODO test if dec.Skolemize == nil
}

func TestDecodeErrors(t *testing.T) {}
