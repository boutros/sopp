package rdf

import "testing"

func TestPrefixMap(t *testing.T) {
	p := NewPrefixMap()

	if _, err := p.Resolve("a:a"); err == nil {
		t.Errorf("PrefixMap.Resolve(a:a) => %v, want \"cannot resolve: a:a\"", err)
	}

	p.Set("foaf", NewURI("http://xmlns.com/foaf/0.1/"))
	if u, err := p.Resolve("foaf:name"); err != nil || u != NewURI("http://xmlns.com/foaf/0.1/name") {
		t.Errorf("PrefixMap.Resolve(foaf:name) => %v; want %v",
			u, NewURI("http://xmlns.com/foaf/0.1/name"))
	}

	p.Set("foaf", NewURI("http://xmlns.com/foaf/2/"))
	if u, err := p.Resolve("foaf:name"); err != nil || u != NewURI("http://xmlns.com/foaf/2/name") {
		t.Errorf("PrefixMap.Resolve(foaf:name) => %v; want %v",
			u, NewURI("http://xmlns.com/foaf/2/name"))
	}

	want := "<http://purl.org/dc/terms/title>"
	if s := p.Shrink(NewURI("http://purl.org/dc/terms/title")); s != want {
		t.Errorf("PrefixMap.Shrink(http://purl.org/dc/terms/title) => %s; want %s", s, want)
	}

	if s := p.Shrink(NewURI("http://xmlns.com/foaf/0.1/knows")); s != "foaf:knows" {
		t.Errorf("PrefixMap.Shrink(http://purl.org/dc/terms/title) => %s; want foaf:knows", s)
	}

	if r := p.Shrink(NewURI("http://ex.org/book/1")); r != "<http://ex.org/book/1>" {
		t.Errorf("PrefixMap.Shrink(http://ex.org/book/1) => %v; want <http://ex.org/book/1>", r)
	}

	p.Base = NewURI("http://ex.org/")
	if r := p.Shrink(NewURI("http://ex.org/book/1")); r != "<book/1>" {
		t.Errorf("PrefixMap.Shrink(http://ex.org/book/1) => %v; want <book/1>", r)
	}
}
