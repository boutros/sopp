package rdf

import (
	"bytes"
	"fmt"
	"sort"
)

// Format represents a RDF graph serialization format
type Format int

// Available serialization formats.
const (
	NTriples Format = iota
	Turtle
	TurtleTransit
)

// Triple represents a RDF Triple, also known as a RDF Statement.
type Triple struct {
	// Subj is the subject of the Triple
	Subj URI
	// Pred is the predicate of the Triple
	Pred URI
	// Obj is the object of the triple.
	Obj Term
}

// String returns a N-Triples serialization of the Triple.
func (tr Triple) String() string {
	return fmt.Sprintf("<%s> <%s> %s .", tr.Subj, tr.Pred, tr.Obj)
}

// Graph represents an RDF graph.
type Graph struct {
	nodes map[URI]map[URI]terms
}

// NewGraph returns a new Graph.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[URI]map[URI]terms),
	}
}

// Size returns the number of triples in the Graph.
func (g *Graph) Size() (n int) {
	for _, props := range g.nodes {
		for _, vals := range props {
			n += len(vals)
		}
	}
	return n
}

// Triples returns all the triples in the Graph.
func (g *Graph) Triples() []Triple {
	trs := make([]Triple, 0, len(g.nodes))

	for subj, props := range g.nodes {
		for pred, terms := range props {
			for _, term := range terms {
				trs = append(trs, Triple{Subj: subj, Pred: pred, Obj: term})
			}
		}
	}

	return trs
}

// Eq tests for equality between graphs, meaning that they contain
// the same triples, and no graph has triples not in the other graph.
func (g *Graph) Eq(other *Graph) bool {
	if len(g.nodes) != len(other.nodes) {
		return false
	}
	for subj, props := range g.nodes {
		if _, ok := other.nodes[subj]; !ok {
			return false
		}
		for pred, terms := range props {
			if _, ok := other.nodes[subj][pred]; !ok {
				return false
			}
			if !eqTerms(terms, other.nodes[subj][pred]) {
				return false
			}
		}
	}
	for subj, props := range other.nodes {
		if _, ok := g.nodes[subj]; !ok {
			return false
		}
		for pred, terms := range props {
			if _, ok := g.nodes[subj][pred]; !ok {
				return false
			}
			if !eqTerms(terms, g.nodes[subj][pred]) {
				return false
			}
		}
	}
	return true
}

// eqTerms checks if two Terms contains the same triples.
// Will panics if a & b is of different length.
func eqTerms(a, b terms) bool {
	sort.Sort(a)
	sort.Sort(b)
	for i, t := range a {
		if t != b[i] {
			return false
		}
	}
	return true
}

// Insert adds one or more triples to the Graph. It returns the number
// of triples inserted which where not allready present.
func (g *Graph) Insert(trs ...Triple) (n int) {
outer:
	for _, t := range trs {
		if _, ok := g.nodes[t.Subj]; ok {
			// subject exists
			if trms, ok := g.nodes[t.Subj][t.Pred]; ok {
				// predicate exists
				for _, term := range trms {
					if term == t.Obj {
						// triple already in graph
						continue outer
					}
				}
				// add object
				g.nodes[t.Subj][t.Pred] = append(g.nodes[t.Subj][t.Pred], t.Obj)
				n++
			} else {
				// new predicate for subject
				g.nodes[t.Subj][t.Pred] = make(terms, 0, 1)
				// add object
				g.nodes[t.Subj][t.Pred] = append(g.nodes[t.Subj][t.Pred], t.Obj)
				n++
			}
		} else {
			// new subject
			g.nodes[t.Subj] = make(map[URI]terms)
			// add predicate
			g.nodes[t.Subj][t.Pred] = make(terms, 0, 1)
			// add object
			g.nodes[t.Subj][t.Pred] = append(g.nodes[t.Subj][t.Pred], t.Obj)
			n++
		}
	}
	return
}

// Has checks if given triple is present in Graph
func (g *Graph) Has(tr Triple) bool {
	if subj, ok := g.nodes[tr.Subj]; ok {
		if trms, ok := subj[tr.Pred]; ok {
			for _, term := range trms {
				if term == tr.Obj {
					return true
				}
			}
		}
	}
	return false
}

// Delete deletes one or more triples from the Graph. It returns the
// number of triples deleted.
func (g *Graph) Delete(trs ...Triple) (n int) {
outer:
	for _, tr := range trs {
		if subj, ok := g.nodes[tr.Subj]; ok {
			if trms, ok := subj[tr.Pred]; ok {
				for i, term := range trms {
					if term == tr.Obj {
						g.nodes[tr.Subj][tr.Pred] = append(trms[:i], trms[i+1:]...)
						n++
						continue outer
					}
				}
			}
		}
	}
	return
}

func (g *Graph) Serialize(f Format) string {
	var b bytes.Buffer

	if f == Turtle {
		for subj, props := range g.nodes {
			fmt.Fprintf(&b, "<%s> ", subj)
			p := 0
			for pred, terms := range props {
				if p > 0 {
					b.WriteString(" ;\n\t")
				}

				for i, term := range terms {
					if i == 0 {
						fmt.Fprintf(&b, "<%s> ", pred)
					}
					switch t := term.(type) {
					case URI:
						fmt.Fprintf(&b, "<%s>", t)
					case Literal:
						switch t.DataType() {
						case RDFlangString:
							fmt.Fprintf(&b, "%q@%s", t.String(), t.Lang())
						case XSDstring:
							fmt.Fprintf(&b, "%q", t.String())
						default:
							fmt.Fprintf(&b, "%q^^<%s>", t.String(), t.DataType())
						}
					}
					if i+1 < len(terms) {
						b.WriteString(", ")
					}
				}
				p++
			}
			b.WriteString(" .\n")
		}
		return b.String()
	}

	for subj, props := range g.nodes {
		for pred, terms := range props {
			for _, term := range terms {
				fmt.Fprintf(&b, "<%s> <%s> ", subj, pred)
				switch t := term.(type) {
				case URI:
					fmt.Fprintf(&b, "<%s> .\n", t)
				case Literal:
					switch t.DataType() {
					case RDFlangString:
						fmt.Fprintf(&b, "%q@%s .\n", t.String(), t.Lang())
					case XSDstring:
						fmt.Fprintf(&b, "%q .\n", t.String())
					default:
						fmt.Fprintf(&b, "%q^^<%s> .\n", t.String(), t.DataType())
					}
				}
			}
		}
	}

	return b.String()
}

/*

type PrefixMap struct {
	p2uri map[string]URI
	uri2p map[string]URI
	//base  URI
}

//func (p *PrefixMap) SetBase(u URI)            {}
func (p *PrefixMap) Set(prefix string, u URI) {}
func (p *PrefixMap) Resolve(s string) URI     {}
func (p *PrefixAmp) Shrink(u URI) string      {}

func (g *Graph) Update(q UpdateQuery)
func (g *Graph) Construct(q ConstructQuery) => *Graph

type UpdateQuery struct {
	insert    []Pattern
	delete    []Pattern
	where     []Pattern
}

type ConstructQuery struct {
	construct []Pattern
	where     []Pattern
}
*/
