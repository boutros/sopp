package rdf

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type PrefixMap struct {
	p2uri map[string]URI
	uri2p map[URI]string
	Base  URI
}

func NewPrefixMap() *PrefixMap {
	return &PrefixMap{
		p2uri: make(map[string]URI),
		uri2p: make(map[URI]string),
		Base:  URI(""),
	}
}
func (p *PrefixMap) Set(prefix string, u URI) {
	p.p2uri[prefix] = u
	p.uri2p[u] = prefix
}

func (p *PrefixMap) Resolve(s string) (URI, error) {
	if i := strings.Index(s, ":"); i > 0 {
		prefix, path := s[:i], s[i+1:]
		if u, ok := p.p2uri[prefix]; ok {
			return NewURI(string(u) + path), nil
		}
	}

	return URI(""), fmt.Errorf("cannot resolve: %s", s)
}

func (p *PrefixMap) Shrink(u URI) string {
	if p.Base != "" && strings.HasPrefix(string(u), string(p.Base)) {
		return "<" + strings.TrimPrefix(string(u), string(p.Base)) + ">"
	}
	ns, path := split(string(u))
	if prefix, ok := p.uri2p[URI(ns)]; ok {
		return prefix + ":" + path
	}
	return "<" + string(u) + ">"
}

func split(uri string) (string, string) {
	i := len(uri)
	for i > 0 {
		r, w := utf8.DecodeLastRuneInString(uri[:i])
		if r == '/' || r == '#' {
			return uri[:i], uri[i:]
		}
		i -= w
	}
	return uri, uri
}

func (u URI) resolve(s string) URI {
	r, _ := utf8.DecodeLastRuneInString(s)
	switch r {
	case '/':
		return NewURI(strings.TrimSuffix(string(u), "/") + s)
	case '#':
		return NewURI(strings.TrimSuffix(string(u), "#") + s)
	default:
		r2, _ := utf8.DecodeLastRuneInString(string(u))
		switch r2 {
		case '/', '#':
			return NewURI(string(u) + s)
		default:
			return NewURI(string(u) + "/" + s)
		}
	}
}
