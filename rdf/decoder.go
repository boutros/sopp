package rdf

import (
	"fmt"
	"io"
)

// Decoder is a streaming decoder for RDF turtle/n-triples.
type Decoder struct {
	scanner *scanner

	// state
	ns       *PrefixMap // prefixes
	tr       Triple     // parsed triple to be returned
	keepSubj bool       // triple ended in ';' - keep subject in next call to Decode()
	keepPred bool       // triple ended in ',' - keep predicate (and subject) in next call to Decode()
	//skip     bool       // skip triple (because it has blank nodes and Skolemize==nil)

	// Skolemize creates an URI given a blank node identifier. If not set, the triples with
	// blank nodes will be silently discarded.
	Skolemize func(s string) URI

	// Base is the initial base URI. It will be changed by any
	// base directives in the stream.
	Base URI
}

// NewDecoder returns a new Decoder over the given stream.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{scanner: newScanner(r), ns: NewPrefixMap()}
}

// Decode returns the next Triple in the input stream, or an error. The error
// io.EOF signifies the end of the stream.
func (d *Decoder) Decode() (Triple, error) {

	var (
		tok    token
		next   token
		prefix string
	)

start:
	tok = d.scanner.Scan()
	switch tok.Type {
	case tokenEOF:
		return d.tr, io.EOF
	case tokenEOL:
		goto start
	case tokenBaseDirective:
		goto scanAndStoreBase
	case tokenPrefixDirective:
		goto scanAndStorePrefix
	case tokenURI, tokenURIshrinked:
		if d.keepSubj {
			if d.keepPred {
				goto storeObjURI
			}
			goto storePred
		}
		goto storeSubj
	case tokenLiteral:
		if !d.keepPred {
			return d.errorExpected("Directive|URI", tok)
		}
		goto storeObjLiteral
	case tokenBNode:
		if d.Skolemize == nil {
			goto scanUntilNextStatement
		}
		if d.keepPred {
			goto storeObjBNode
		}
		d.tr.Subj = d.Skolemize(tok.Text)
		goto scanPred
	default:
		panic("TODO default")
	}

scanAndStoreBase:
	tok = d.scanner.Scan()
	if tok.Type != tokenURI {
		return d.errorExpected("URI", tok)
	}
	d.Base = URI(tok.Text)
	goto scanDirectiveTermination

scanAndStorePrefix:
	if tok = d.scanner.Scan(); tok.Type != tokenPrefix {
		return d.errorExpected("prefix", tok)
	}
	prefix = tok.Text
	if tok = d.scanner.Scan(); tok.Type != tokenURI {
		return d.errorExpected("URI", tok)
	}
	d.ns.Set(prefix, URI(tok.Text))
	goto scanDirectiveTermination

storeSubj:
	if tok.Type == tokenURIshrinked {
		d.tr.Subj = d.unshrinkURI(tok.Text)
	} else { // tok.Type == tokenURI
		d.tr.Subj = d.resolveURI(tok.Text)
	}

scanPred:
	tok = d.scanner.Scan()
	if tok.Type != tokenURI && tok.Type != tokenURIshrinked {
		return d.errorExpected("URI", tok)
	}

storePred:
	if tok.Type == tokenURIshrinked {
		d.tr.Pred = d.unshrinkURI(tok.Text)
	} else { // tok.Type == tokenURI
		d.tr.Pred = d.resolveURI(tok.Text)
	}

	//scanObj:
	tok = d.scanner.Scan()

	//storeObj:
	switch tok.Type {
	case tokenURI, tokenURIshrinked:
		goto storeObjURI
	case tokenBNode:
		goto storeObjBNode
	case tokenLiteral:
		goto storeObjLiteral
	default:
		return d.errorExpected("URI|Literal", tok)
	}

storeObjBNode:
	if d.Skolemize == nil {
		goto scanUntilNextStatement
	}
	d.tr.Obj = d.Skolemize(tok.Text)
	goto scanTripleTermination

storeObjURI:
	if tok.Type == tokenURIshrinked {
		d.tr.Obj = d.unshrinkURI(tok.Text)
	} else { // tok.Type == tokenURI
		d.tr.Obj = d.resolveURI(tok.Text)
	}
	goto scanTripleTermination

storeObjLiteral:
	next = d.scanner.Scan()
	switch next.Type {
	case tokenLangTag:
		d.tr.Obj = NewLangLiteral(tok.Text, next.Text)
		goto scanTripleTermination
	case tokenDot, tokenSemicolon, tokenComma:
		d.tr.Obj = NewLiteral(tok.Text)
		tok = next // actOnTripleTermination checks tok
		goto actOnTripleTermination
	case tokenTypeMarker:
		goto scanLiteralDatatype
	default:
		return d.errorExpected("Dot|Language tag|Datatype marker", next)
	}

scanLiteralDatatype:
	next = d.scanner.Scan()
	switch next.Type {
	case tokenURIshrinked:
		d.tr.Obj = Literal{value: tok.Text, datatype: d.unshrinkURI(next.Text)}
	case tokenURI:
		d.tr.Obj = Literal{value: tok.Text, datatype: d.resolveURI(next.Text)}
	default:
		return d.errorExpected("URI", next)
	}
	goto scanTripleTermination

scanDirectiveTermination:
	tok = d.scanner.Scan()
	if tok.Type != tokenDot {
		return d.errorExpected("Dot", tok)
	}
	goto start // continue scanning for triples

scanUntilNextStatement:
	for {
		tok = d.scanner.Scan()
		switch tok.Type {
		case tokenSemicolon, tokenComma, tokenDot:
			goto start
		case tokenEOF:
			return d.tr, io.EOF
		}
	}

scanTripleTermination:
	tok = d.scanner.Scan()

actOnTripleTermination:
	switch tok.Type {
	case tokenSemicolon:
		d.keepSubj = true
		d.keepPred = false
		// continue to done
	case tokenComma:
		d.keepSubj = true
		d.keepPred = true
		// continue to done
	case tokenDot:
		d.keepSubj = false
		d.keepPred = false
		// continue to done
	case tokenEOL:
		goto scanTripleTermination
	default:
		return d.errorExpected("Dot|Semicolon|Comma", tok)
	}

	//done:
	return d.tr, nil
}

func (d *Decoder) errorExpected(expected string, tok token) (Triple, error) {
	switch tok.Type {
	case tokenEOF:
		return d.tr, io.EOF
	case tokenIllegal:
		return d.tr, fmt.Errorf("%d:%d expected %s, found %q (%s: %s)",
			d.scanner.Row, d.scanner.Col, expected, tok.Text, tok.Type, d.scanner.Error)
	default:
		return d.tr, fmt.Errorf("%d:%d expected %s, found %q (%s)",
			d.scanner.Row, d.scanner.Col, expected, tok.Text, tok.Type)
	}
}

func (d *Decoder) resolveURI(s string) URI {
	return URI(s).Resolve(d.Base)
}

func (d *Decoder) unshrinkURI(s string) URI {
	uri, err := d.ns.Resolve(s)
	if err != nil {
		return URI(s)
	}
	return uri
}

// DecodeGraph parses the entire stream and returns the triples as a Graph.
func (d *Decoder) DecodeGraph() (*Graph, error) {
	g := NewGraph()
	for tr, err := d.Decode(); err != io.EOF; tr, err = d.Decode() {
		if err != nil {
			fmt.Println(err)
			fmt.Println(string(d.scanner.line))
			panic("TODO DecodeGraph with errors")
		}
		g.Insert(tr)
	}
	return g, nil
}
