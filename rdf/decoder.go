package rdf

import (
	"fmt"
	"io"
)

// Decoder is a streaming decoder for RDF turtle/n-triples.
type Decoder struct {
	scanner *scanner

	// state
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
	tok := d.scanner.Scan()
	switch tok.Type {
	case tokenBase:
		tok = d.scanner.Scan()
		if tok.Type != tokenURI {
			err = fmt.Errorf("%d:%d expected URI after base directive, got %q (%s)",
				d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
			break
		}

		d.Base = URI(tok.Text)

		tok = d.scanner.Scan()
		if tok.Type != tokenDot {
			err = fmt.Errorf("%d:%d expected Dot to close base directive, got %q (%s)",
				d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
			break
		}
		return d.parseSubject()
	case tokenURI:
		d.tr.Subj = d.newURI(tok.Text)
	case tokenEOL:
		if d.tr.Subj, err = d.parseURI(); err != nil {
			return err
		}
	case tokenEOF:
		err = io.EOF
	default:
		err = fmt.Errorf("%d:%d expected URI or directive, got %q (%s)",
			d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
	}
	return err
}

func (d *Decoder) parsePredicate() (err error) {
	d.tr.Pred, err = d.parseURI()
	return err
}

func (d *Decoder) newURI(s string) URI {
	return URI(s).Resolve(d.Base)
}

func (d *Decoder) parseURI() (uri URI, err error) {
	tok := d.scanner.Scan()
	switch tok.Type {
	case tokenURI:
		uri = d.newURI(tok.Text)
	case tokenEOL:
		return d.parseURI()
	case tokenEOF:
		err = io.EOF
	default:
		err = fmt.Errorf("%d:%d expected URI, got %q (%s)",
			d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
	}
	return uri, err
}

func (d *Decoder) parseObject() error {
	tok := d.scanner.Scan()
	switch tok.Type {
	case tokenURI:
		d.tr.Obj = d.newURI(tok.Text)
		break
	case tokenLiteral:
		next := d.scanner.Scan()
		switch next.Type {
		case tokenDot:
			d.tr.Obj = NewLiteral(tok.Text)
			d.keepSubj = false
			d.keepPred = false
			return nil
		case tokenTypeMarker:
			next = d.scanner.Scan()
			switch next.Type {
			case tokenURI:
				d.tr.Obj = Literal{value: tok.Text, datatype: d.newURI(next.Text)}
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
		case tokenLangTag:
			d.tr.Obj = NewLangLiteral(tok.Text, next.Text)
		case tokenIllegal:
			return fmt.Errorf("%d:%d expected Datatype|Dot|Semicolon|Comma, got %q (%s: %s)",
				d.scanner.Row, d.scanner.Col, tok.Text, tok.Type, d.scanner.Error)
		default:
			return fmt.Errorf("%d:%d expected Datatype|Dot|Semicolon|Comma, got %q (%s)",
				d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
		}
	case tokenEOF:
		return io.EOF
	}

	// We got a full triple, check for termination
	tok = d.scanner.Scan()
	switch tok.Type {
	case tokenDot:
		d.keepSubj = false
		d.keepPred = false
	case tokenSemicolon:
		d.keepSubj = true
		d.keepPred = false
	case tokenComma:
		d.keepSubj = true
		d.keepPred = true
	case tokenEOF:
		return io.EOF
	case tokenIllegal:
		return fmt.Errorf("%d:%d expected Dot|Semicolon|Comma, got %q (%s: %s)",
			d.scanner.Row, d.scanner.Col, tok.Text, tok.Type, d.scanner.Error)
	default:
		return fmt.Errorf("%d:%d expected Dot|Semicolon|Comma, got %q (%s)",
			d.scanner.Row, d.scanner.Col, tok.Text, tok.Type)
	}

	return nil
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
