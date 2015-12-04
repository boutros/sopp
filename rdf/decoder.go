package rdf

import (
	"fmt"
	"io"
)

// Decoder is a streaming decoder for RDF turtle/n-triples.
type Decoder struct {
	scanner *scanner

	// state
	base     string         // base URI
	ns       map[string]URI // prefixes
	tr       Triple         // parsed triple to be returned
	keepSubj bool           // keep subject in next call to Decode()
	keepPred bool           // keep predicate in next call to Decode()

	// Skolemize creates an URI given a blank node identifier
	Skolemize func(s string) URI

	// Base is the initial base URI. It will be changed by any
	// base directives in the stream.
	Base URI
}

// NewDecoder returns a new Decoder over the given stream.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{scanner: newScanner(r)}
}

// Decode returns the next Triple in the input stream, or an error. The error
// io.EOF signifies the end of the stream.
func (d *Decoder) Decode() (Triple, error) {

	if !d.keepSubj {
		if err := d.parseSubject(); err != nil {
			return d.tr, err
		}
	}

	if !d.keepPred {
		if err := d.parsePredicate(); err != nil {
			return d.tr, err
		}
	}

	if err := d.parseObject(); err != nil {
		return d.tr, err
	}

	return d.tr, nil
}

func (d *Decoder) parseSubject() (err error) {
	d.tr.Subj, err = d.parseURI()
	return err
}

func (d *Decoder) parsePredicate() (err error) {
	d.tr.Pred, err = d.parseURI()
	return err
}

func (d *Decoder) parseURI() (uri URI, err error) {
	tok := d.scanner.Scan()
	switch tok.Type {
	case tokenURI:
		uri = NewURI(tok.Text)
	case tokenEOF:
		err = io.EOF
	}
	return uri, err
}

func (d *Decoder) parseObject() error {
	tok := d.scanner.Scan()
	switch tok.Type {
	case tokenURI:
		d.tr.Obj = NewURI(tok.Text)
		break
	case tokenLiteral:
		next := d.scanner.Scan()
		switch next.Type {
		case tokenDot:
			d.tr.Obj = NewLiteral(tok.Text)
			d.keepPred = false
			d.keepPred = false
			return nil
		case tokenTypeMarker:
			next = d.scanner.Scan()
			switch next.Type {
			case tokenURI:
				d.tr.Obj = NewTypedLiteral(tok.Text, NewURI(next.Text))
				return nil
			case tokenEOF:
				return io.EOF
			}
		case tokenSemicolon:
			d.keepSubj = true
			d.keepPred = false
			d.tr.Obj = NewLiteral(tok.Text)
			return nil
		case tokenComma:
			d.keepSubj = true
			d.keepPred = true
			d.tr.Obj = NewLiteral(tok.Text)
			return nil
		default:
			return fmt.Errorf("%d:%d expected datatype, dot, semicolon or comma, got %q (%s)",
				d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
		}
	case tokenEOF:
		return io.EOF
	}

	tok = d.scanner.Scan()
	switch tok.Type {
	case tokenDot:
		// we got a full valid triple
		d.keepPred = false
		d.keepPred = false
		return nil
	default:
		return fmt.Errorf("expected dot, semicolon or comma, got %v", tok.Type)
	}

	return nil
}

// DecodeAll parses the entire stream and returns the triples as a Graph.
func (d *Decoder) DecodeAll() (*Graph, error) {
	return NewGraph(), nil
}
