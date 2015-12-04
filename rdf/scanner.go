package rdf

import (
	"bufio"
	"bytes"
	"io"
	"unicode/utf8"
)

type tokenType int

const (
	// Special tokens
	tokenIllegal tokenType = iota
	tokenEOF
	tokenEOL

	// Turtle tokens
	tokenPrefix
	tokenBase
	tokenURI
	tokenBNode
	tokenLiteral
	tokenLangTag
	tokenTypeMarker
	tokenComma
	tokenSemicolon
	tokenDot
)

const eof = rune(-1)

type token struct {
	Type tokenType
	Text string
}

func (t tokenType) String() string {
	switch t {
	case tokenIllegal:
		return "Illegal"
	case tokenEOF:
		return "EOF"
	case tokenEOL:
		return "EOL"
	case tokenPrefix:
		return "Prefix"
	case tokenBase:
		return "Base"
	case tokenBNode:
		return "Blank Node"
	case tokenURI:
		return "URI"
	case tokenLiteral:
		return "Literal"
	case tokenLangTag:
		return "Language tag"
	case tokenTypeMarker:
		return "DataType marker"
	case tokenComma:
		return "Comma"
	case tokenSemicolon:
		return "Semicolon"
	case tokenDot:
		return "Dot"
	default:
		return "token String() TODO"
	}
}

type scanner struct {
	r    *bufio.Reader
	line []byte // line being scanned

	pos      int  // positon in line
	start    int  // start of current token
	unescape bool // true when token needs unescaping

	// Keep track of position in stream for error reporting:
	Row int // line number
	Col int // position in line (in runes, not bytes)

	// Error holds the last encountered error explanation-
	// It is invalidated on next call to Scan()
	Error string
}

func newScanner(r io.Reader) *scanner {
	return &scanner{r: bufio.NewReader(r), Row: 1}
}

func (s *scanner) Scan() token {
	s.start = s.pos
	s.Error = ""
	addStart, addEnd := 0, 0
	r := s.next()

	for isWhitespace(r) {
		s.ignore()
		r = s.next()
	}

	var tok tokenType
runeSwitch:
	switch r {
	case '\n':
		s.Row++
		s.ignore()
		tok = tokenEOL
	case '<':
		if !s.scanTo('>') {
			s.Error = "unterminated URI"
			break
		}
		addStart = 1
		addEnd = -1
		tok = tokenURI
	case '.':
		s.ignore()
		tok = tokenDot
	case '#':
		if !s.scanTo('\n') {
			tok = tokenEOF
		}
		s.ignore()
		tok = tokenEOL
	case ',':
		tok = tokenComma
		s.ignore()
	case ';':
		tok = tokenSemicolon
		s.ignore()
	case '"':
		s.ignore()
		for {
			end := s.scanTo('"')
			if !end {
				s.start-- // we want starting quote in error message
				s.Error = "unterminated Literal"
				break runeSwitch
			}
			if s.pos > 1 && s.line[s.pos-2] != '\\' {
				break
			}
		}
		if s.pos > s.start {
			addEnd = -1
		}
		tok = tokenLiteral
	case '^':
		if s.peek() != '^' {
			s.Error = "unexpected token"
			s.scanUntilNextToken()
			break runeSwitch
		}
		s.pos++ // consume ^^
		s.ignore()
		tok = tokenTypeMarker
	case eof:
		tok = tokenEOF
	case utf8.RuneError:
		s.Error = "illegal UTF-8 encoding"
	default:
		s.Error = "unexpected token"
		s.scanUntilNextToken()
	}

	if s.Error != "" {
		tok = tokenIllegal
	}
	if s.unescape {
		s.unescape = false
		return s.unescaped(tok, s.start+addStart, s.pos+addEnd)
	}
	return token{tok, string(s.line[s.start+addStart : s.pos+addEnd])}
}

func (s *scanner) ignore() {
	s.start = s.pos
}

func (s *scanner) next() rune {
	if s.pos == len(s.line) {
		line, err := s.r.ReadBytes('\n')
		if err != nil && len(line) == 0 {
			return eof
		}
		s.line = line
		s.start = 0
		s.pos = 0
		s.Col++
	}

	r, w := utf8.DecodeRune(s.line[s.pos:])
	s.pos += w
	s.Col++

	return r
}

func (s *scanner) peek() rune {
	r, _ := utf8.DecodeRune(s.line[s.pos:])
	return r
}

func (s *scanner) scanTo(stop rune) bool {
	for r := s.next(); r != stop; r = s.next() {
		switch r {
		case eof:
			return false
		case utf8.RuneError:
			s.Error = "illegal UTF-8 encoding"
			return false
		case '\\':
			s.unescape = true
		}
	}
	return true
}

func (s *scanner) scanUntilNextToken() {
	for {
		r := s.peek()
		switch r {
		case '<', '"', '.', ';', ',', '\n', ' ', eof:
			return
		default:
			s.next()
		}
	}
}

func (s *scanner) unescaped(typ tokenType, from, to int) token {
	i := from
	buf := bytes.NewBuffer(make([]byte, 0, to-i))
	for i < to {
		r, w := utf8.DecodeRune(s.line[i:])
		if w == 0 {
			break
		}
		i += w
		if r != '\\' {
			buf.WriteRune(r)
			continue
		}
		var c byte
		switch s.line[i] {
		case 't':
			c = '\t'
		case 'b':
			c = '\b'
		case 'n':
			c = '\n'
		case 'r':
			c = '\r'
		case 'f':
			c = '\f'
		case '"':
			c = '"'
		case '\'':
			c = '\''
		case '\\':
			c = '\\'
		case 'u', 'U':
			d := uint64(0)
			start := i
			digits := 4
			if s.line[i] == 'U' {
				digits = 8
			}
			for i < start+digits {
				i++
				if i == len(s.line) {
					s.Error = "illegal escape sequence"
					return token{tokenIllegal, string(s.line[start-1 : i])}
				}
				x := uint64(s.line[i])
				if x >= 'a' {
					x -= 'a' - 'A'
				}
				d1 := x - '0'
				if d1 > 9 {
					d1 = 10 + d1 - ('A' - '0')
				}
				if 0 > d1 || d1 > 15 {
					j := i
					for !utf8.FullRune(s.line[j:i]) {
						i++
					}
					s.Error = "illegal escape sequence"
					return token{tokenIllegal, string(s.line[start-1 : i-1])}
				}
				d = (16 * d) + d1
			}
			buf.WriteRune(rune(d))
			i++
			continue
		default:
			s.Error = "illegal escape sequence"
			return token{tokenIllegal, string(s.line[i-1 : i+1])}
		}
		buf.WriteByte(c)
		i++
	}

	return token{typ, buf.String()}
}

func isWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r'
}