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
		{`<s> <p> "1"^^<int> .`, []Triple{Triple{NewURI("s"), NewURI("p"), NewTypedLiteral("1", NewURI("int"))}}},
		{`<s> <p> "x", "y" .`, []Triple{
			Triple{NewURI("s"), NewURI("p"), NewLiteral("x")},
			Triple{NewURI("s"), NewURI("p"), NewLiteral("y")}}},
		{`<s> <p> "a" ; <p2> "b" ; <p3>  "c" .`, []Triple{
			Triple{NewURI("s"), NewURI("p"), NewLiteral("a")},
			Triple{NewURI("s"), NewURI("p2"), NewLiteral("b")},
			Triple{NewURI("s"), NewURI("p3"), NewLiteral("c")}}},
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
				test.input, got.Serialize(Turtle), want.Serialize(Turtle))
		}
	}
}

func TestDecodeErrors(t *testing.T) {}
