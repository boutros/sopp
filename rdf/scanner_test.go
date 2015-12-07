package rdf

import (
	"bytes"
	"testing"
)

func collectTokens(s *scanner) []token {
	tokens := []token{}
	for {
		tk := s.Scan()
		if tk.Type == tokenEOF {
			break
		}
		tokens = append(tokens, token{tk.Type, tk.Text})

	}
	return tokens
}

func equalTokens(a, b []token) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if a[k].Type != b[k].Type {
			return false
		}
		if a[k].Text != b[k].Text {
			return false
		}
	}
	return true
}

func TestScanTokens(t *testing.T) {
	tests := []struct {
		input string
		want  []token
	}{
		{"", nil},
		{" \t ", nil},
		{"\n", []token{{tokenEOL, ""}}},
		{"<a>", []token{{tokenURI, "a"}}},
		{" <http://xyz/æøå.123> ", []token{{tokenURI, "http://xyz/æøå.123"}}},
		{`""`, []token{{tokenLiteral, ""}}},
		{`"a"`, []token{{tokenLiteral, "a"}}},
		{`"a"`, []token{{tokenLiteral, "a"}}},
		{`"hei"@nb-No `, []token{{tokenLiteral, "hei"}, {tokenLangTag, "nb-No"}}},
		{`"\""`, []token{{tokenLiteral, "\""}}},
		{`"\t\r\n\f\b\\\u00b7\u00B7\U000000b7\U000000B7"`, []token{{tokenLiteral, "\t\r\n\f\b\\····"}}},
		{`"abc\tæøå"`, []token{{tokenLiteral, "abc\tæøå"}}},
		{`"line #1\nline #2"`, []token{{tokenLiteral, "line #1\nline #2"}}},
		{"<a>^^<f>", []token{{tokenURI, "a"}, {tokenTypeMarker, ""}, {tokenURI, "f"}}},
		{`"a", "b"`, []token{{tokenLiteral, "a"}, {tokenComma, ""}, {tokenLiteral, "b"}}},
		{`"a"; "b"`, []token{{tokenLiteral, "a"}, {tokenSemicolon, ""}, {tokenLiteral, "b"}}},
		{"<a><b> <c> .", []token{
			{tokenURI, "a"},
			{tokenURI, "b"},
			{tokenURI, "c"},
			{tokenDot, ""}}},
		{"<a> # a comment <b>\n<c>", []token{
			{tokenURI, "a"},
			{tokenEOL, ""},
			{tokenURI, "c"}}},
		{`<http://example/æøå> <http://example/禅> "\"\\\r\n Здра́вствуйте	☺" .`, []token{
			{tokenURI, "http://example/æøå"},
			{tokenURI, "http://example/禅"},
			{tokenLiteral, "\"\\\r\n Здра́вствуйте\t☺"},
			{tokenDot, ""}}},
	}

	for _, test := range tests {
		scanner := newScanner(bytes.NewBufferString(test.input))
		if tokens := collectTokens(scanner); !equalTokens(tokens, test.want) {
			t.Errorf("scanning %q got %v; want %v", test.input, tokens, test.want)
		}
	}
}

func TestScanErrors(t *testing.T) {
	tests := []struct {
		input    string
		errWant  string
		textWant string
	}{
		{"<a", "unterminated URI", "<a"},
		{`"xy\z"`, "illegal escape sequence", `\z`},
		{`"\u00F ij"`, "illegal escape sequence", `\u00F`},
		{`"\u123"`, "illegal escape sequence", `\u123`},
		{`"\u123ø.a"`, "illegal escape sequence", "\\u123\xc3"},
		{`"hei`, "unterminated Literal", `"hei`},
		{`abc <a>`, "unexpected token", "abc"},
		{`^a b`, "unexpected token", "^a"},
		{`@ <a>`, "invalid language tag", ""},
	}

	for _, test := range tests {
		scanner := newScanner(bytes.NewBufferString(test.input))
		tok := scanner.Scan()
		if tok.Type != tokenIllegal {
			t.Errorf("scanning %q got %v; want %v", test.input, tok.Type, tokenIllegal)
		}
		if scanner.Error != test.errWant || tok.Text != test.textWant {
			t.Errorf("scanning %q got %q, %q; want %q, %q",
				test.input, scanner.Error, tok.Text, test.errWant, test.textWant)
		}
	}
}
