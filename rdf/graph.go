package rdf

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
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
	switch obj := tr.Obj.(type) {
	case URI:
		return fmt.Sprintf("<%s> <%s> <%s> .\n", tr.Subj, tr.Pred, obj)
	case Literal:
		switch obj.DataType() {
		case XSDstring:
			return fmt.Sprintf("<%s> <%s> %q .\n", tr.Subj, tr.Pred, obj.value)
		case RDFlangString:
			return fmt.Sprintf("<%s> <%s> %q@%s .\n", tr.Subj, tr.Pred, obj.value, obj.language)
		case XSDboolean:
			return fmt.Sprintf("<%s> <%s> %s .\n", tr.Subj, tr.Pred, obj.value)
		default:
			return fmt.Sprintf("<%s> <%s> %q^^<%s> .\n", tr.Subj, tr.Pred, obj.value, obj.datatype)
		}
	}
	panic("unreachable")
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

// Nodes return the graph as a map which node URI's as key,
// and a map of the subject's predicate URI's to Terms as value.
func (g *Graph) Nodes() map[URI]map[URI]terms {
	return g.nodes
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
func eqTerms(a, b terms) bool {
	if len(a) != len(b) {
		return false
	}
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

func (g *Graph) Serialize(f Format, base string) string {
	var b bytes.Buffer

	if f == Turtle {
		if base != "" {
			fmt.Fprintf(&b, "@base <%s> .\n", base)
		}
		for subj, props := range g.nodes {
			fmt.Fprintf(&b, "<%s> ", strings.TrimPrefix(string(subj), base))
			p := 0
			for pred, terms := range props {
				if p > 0 {
					b.WriteString(" ;\n\t")
				}

				for i, term := range terms {
					if i == 0 {
						fmt.Fprintf(&b, "<%s> ", strings.TrimPrefix(string(pred), base))
					}
					switch t := term.(type) {
					case URI:
						fmt.Fprintf(&b, "<%s>", strings.TrimPrefix(string(t), base))
					case Literal:
						switch t.DataType() {
						case RDFlangString:
							fmt.Fprintf(&b, "%q@%s", t.String(), t.Lang())
						case XSDstring:
							fmt.Fprintf(&b, "%q", t.String())
						default:
							fmt.Fprintf(&b, "%q^^<%s>", t.String(), strings.TrimPrefix(string(t.DataType()), base))
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

// Describe returns a graph with all the triples where the given node
// is subject. If asObject is true, it also includes the triples where
// the node is object.
func (g *Graph) Describe(node URI, asObject bool) *Graph {
	res := NewGraph()
	for subj, props := range g.nodes {
		for pred, terms := range props {
			for _, term := range terms {
				if subj == node || (asObject && term == node) {
					res.Insert(Triple{Subj: subj, Pred: pred, Obj: term})
				}
			}
		}
	}
	return res
}

// Merge merges the other graph into current graph.
func (g *Graph) Merge(other *Graph) *Graph {
	for subj, props := range other.nodes {
		// ensure subject exist
		if _, ok := g.nodes[subj]; !ok {
			g.nodes[subj] = make(map[URI]terms)
		}
		for pred, termsOther := range props {
			// ensure predicate exist
			termsCur, ok := g.nodes[subj][pred]
			if !ok {
				g.nodes[subj][pred] = make(terms, 0, 1)
			}
			// TODO sort terms to do binary search;
			// faster if there are a lot, but maybe slower if just a few.
		eachTerm:
			for _, to := range termsOther {
				for _, t := range termsCur {
					if t == to {
						continue eachTerm
					}
				}
				g.nodes[subj][pred] = append(g.nodes[subj][pred], to)
			}
		}
	}
	return g
}

// Dot returns a representation of the graph in graphviz' dot format.
func (g *Graph) Dot(base string, focus []string) string {
	// TODO This is getting messy - consider generating using text/template.
	var b bytes.Buffer
	b.WriteString("digraph \"")
	b.WriteString(strings.Join(focus, "+"))
	b.WriteString("\" {\n\tnode [shape=plaintext];\n\n")

	type link struct {
		from, to URI
		label    string
	}

	var links []link

	for node, props := range g.nodes {
		fmt.Fprintf(&b, "\t%q[label=<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='5'>\n", node)
		isFocus := false
		for _, c := range focus {
			if node.String() == c {
				isFocus = true
				break
			}
		}
		if isFocus {
			b.WriteString("\t<TR><TD ALIGN='RIGHT' BGCOLOR='#e0e0e0'>")
			// Print class membership (rdf:type)
			if _, ok := props[RDFtype]; ok {
				b.WriteString("<FONT POINT-SIZE='10'>")
				for i, term := range props[RDFtype] {
					_, shortObj := split(term.String())
					b.WriteString(shortObj)
					if i+1 < len(props[RDFtype]) {
						b.WriteString("<BR/>")
					}
				}
				b.WriteString("</FONT>")
			}
			b.WriteString("</TD><TD ALIGN='LEFT' BGCOLOR='#a0ffa0'><FONT POINT-SIZE='12' FACE='monospace'>&lt;")
			b.WriteString(strings.TrimPrefix(node.String(), base))
			b.WriteString("&gt;</FONT></TD></TR>\n")
		} else {
			b.WriteString("\t<TR><TD ALIGN='RIGHT' BGCOLOR='#e0e0e0'>")
			if _, ok := props[RDFtype]; ok {
				b.WriteString("<FONT POINT-SIZE='10'>")
				for i, term := range props[RDFtype] {
					_, shortObj := split(term.String())
					b.WriteString(shortObj)
					if i+1 < len(props[RDFtype]) {
						b.WriteString("<BR/>")
					}
				}
				b.WriteString("</FONT>")
			}
			b.WriteString("</TD><TD ALIGN='LEFT' BGCOLOR='#e0e0e0' HREF='/")
			b.WriteString(node.String())
			b.WriteString("' TITLE='")
			b.WriteString(node.String())
			b.WriteString("'><FONT COLOR='blue' POINT-SIZE='12' FACE='monospace'>&lt;")
			b.WriteString(strings.TrimPrefix(node.String(), base))
			b.WriteString("&gt;</FONT></TD>")
			b.WriteString("<TD TITLE='add to graph' BGCOLOR='#e0e0e0' HREF='/")
			b.WriteString(strings.Join(focus, "+/") + "+/" + node.String())
			b.WriteString("'><FONT COLOR='blue'><B>+</B></FONT></TD></TR>\n")
		}

		for pred, terms := range props {
			if pred == RDFtype {
				// rdf:type triples allready handeled above
				continue
			}
			_, shortPred := split(pred.String())
			for _, term := range terms {
				switch t := term.(type) {
				case URI:
					if _, ok := g.nodes[term.(URI)]; ok {
						links = append(links, link{node, t, shortPred})
						break
					}
					// Inline nodes witch does not appear as subject (i.e has no attributes)
					b.WriteString("\t<TR>\n\t\t<TD ALIGN='RIGHT'><B>")
					b.WriteString(shortPred)
					b.WriteString("</B> </TD>\n\t\t<TD ALIGN='LEFT' HREF='")
					if !strings.HasPrefix(t.String(), "http://") {
						b.WriteRune('/')
					}
					b.WriteString(t.String())
					b.WriteString("' TITLE='")
					b.WriteString(t.String())
					b.WriteString("'><FONT COLOR='blue'>")
					b.WriteString(t.String())
					if strings.HasPrefix(t.String(), "http://") {
						b.WriteString("</FONT></TD>\n\t</TR>\n")
					} else {
						b.WriteString("</FONT></TD>\n\t\t")
						b.WriteString("<TD TITLE='add to graph' BGCOLOR='#e0e0e0' HREF='/")
						b.WriteString(strings.Join(focus, "+/") + "+/" + t.String())
						b.WriteString("'><FONT COLOR='blue'><B>+</B></FONT></TD>\n\t</TR>\n")
					}
				case Literal:
					b.WriteString("\t<TR>\n\t\t<TD ALIGN='RIGHT'><B>")
					b.WriteString(shortPred)
					b.WriteString("</B> </TD>\n\t\t<TD ALIGN='LEFT'>")
					b.WriteString(t.String())
					b.WriteString("</TD>\n\t</TR>\n")
				}
			}
		}
		b.WriteString("\t</TABLE>>];\n\n")
	}

	for _, l := range links {
		fmt.Fprintf(&b, "\t%q->%q[label=%q];\n", l.from, l.to, l.label)
	}
	b.WriteString("}")
	return b.String()
}

type Pattern struct {
	Subj, Pred, Obj QVar
}

type QVar interface {
	validAsQVar()
}

type any struct{}

func (a any) validAsQVar() {}

var Any = any{}

type matchPattern struct {
	s, p, o bool
}

var keepTriple matchPattern = matchPattern{true, true, true}

func (g *Graph) Construct(p Pattern) *Graph {
	res := NewGraph()

	for _, tr := range g.Triples() {
		var m matchPattern

		switch subj := p.Subj.(type) {
		case URI:
			if subj == tr.Subj {
				m.s = true
			}
		case any:
			m.s = true
		}

		switch pred := p.Pred.(type) {
		case URI:
			if pred == tr.Pred {
				m.p = true
			}
		case any:
			m.p = true
		}

		switch obj := p.Obj.(type) {
		case Term:
			if obj == tr.Obj {
				m.o = true
			}
		case any:
			m.o = true
		}

		if m == keepTriple {
			res.Insert(tr)
		}

	}

	return res
}
