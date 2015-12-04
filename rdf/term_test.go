package rdf

import (
	"strconv"
	"testing"
	"time"
)

func TestNewURI(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"<>\"{}|^`\\", ""},
		{"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F", ""},
		{"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x20", ""},
		{"æøå", "æøå"},
		{" http://example.org/resorce#123 ", "http://example.org/resorce#123"},
	}

	for _, test := range tests {
		if NewURI(test.in).String() != test.want {
			t.Errorf("NewURI(%q) => %q; want %q", test.in, NewURI(test.in), test.want)
		}
	}
}

func TestURISplit(t *testing.T) {
	//TODO
}

func TestNewLiteral(t *testing.T) {
	tests := []struct {
		in interface{}
		dt URI
	}{
		{false, XSDboolean},
		{true, XSDboolean},
		{"a string", XSDstring},
		{int8(1), XSDbyte},
		{int16(-32768), XSDshort},
		{int32(2147483647), XSDint},
		{'æ', XSDint},
		{rune('\xef'), XSDint},
		{int64(11), XSDlong},
		{uint8(0), XSDunsignedByte},
		{byte('\xff'), XSDunsignedByte},
		{uint16(5), XSDunsignedShort},
		{uint32(999), XSDunsignedInt},
		{uint64(18446744073709551615), XSDunsignedLong},
		{float32(3.14), XSDfloat},
		{float64(0.99999), XSDdouble},
		{time.Date(1999, 12, 24, 12, 45, 0, 123, time.UTC), XSDdateTimeStamp},
	}
	for _, test := range tests {
		l := NewLiteral(test.in)
		if l.DataType() != test.dt {
			t.Errorf("NewLiteral(%v).DataType() => %q; want %q", test.in, l.DataType(), test.dt)
		}
		if l.Value() != test.in {
			t.Errorf("NewLiteral(%v).Value() = %v; want  %v", test.in, l.Value(), test.in)
		}
	}
}

func TestNewLiteralArchDependent(t *testing.T) {
	// Test that float and int types get the corresponding 32/64-bit datatypes
	// Note that the type returned by Value() will not be typed as uint/int/float,
	// but as uint32/uint64, int32/int64 or float32/float64.

	intType := XSDlong
	uintType := XSDunsignedLong
	floatType := XSDdouble
	if strconv.IntSize == 32 {
		intType = XSDint
		uintType = XSDunsignedInt
		floatType = XSDfloat
	}

	tests := []struct {
		in interface{}
		dt URI
	}{
		{0, intType},
		{1234567, intType},
		{uint(99), uintType},
		{3.14, floatType},
	}

	for _, test := range tests {
		l := NewLiteral(test.in)
		if l.DataType() != test.dt {
			t.Errorf("NewLiteral(%v).DataType() => %q; want %q", test.in, l.DataType(), test.dt)
		}
	}
}

func TestNewLiteralCustomeType(t *testing.T) {
	v := struct{ a, b string }{"hei", "hå"}
	l := NewLiteral(v)
	if l.DataType() != XSDstring {
		t.Errorf("NewLiteral(%v).DataType() => %s ; want %s ", v, l.DataType(), XSDstring)
	}
	want := `struct { a string; b string }{a:"hei", b:"hå"}`
	if l.Value() != want {
		t.Errorf("NewLiteral(%v).Value() => %s ; want %s ", v, l.Value(), want)
	}
}

func TestNewLangLiteral(t *testing.T) {
	l := NewLangLiteral("hei", "no")
	if l.Value() != "hei" {
		t.Errorf("NewLangLiteral(\"hei\", \"no\").Value() => %v ; want \"hei\"", l.Value())
	}
	if l.Lang() != "no" {
		t.Errorf("NewLangLiteral(\"hei\", \"no\").Lang() => %v ; want \"no\"", l.Lang())
	}
	if l.DataType() != RDFlangString {
		t.Errorf("NewLangLiteral(\"hei\", \"no\").DataType() => %v ; want %v", l.DataType(), RDFlangString)
	}
}

func TestNewTypedLiteral(t *testing.T) {
	v := struct{ x, y int }{1, 2}
	dt := NewURI("http://example.org/class/Point")
	l := NewTypedLiteral(v, dt)
	if l.DataType() != dt {
		t.Errorf("NewTypeLiteral(%v, %v).DataType() => %s ; want %s ", v, dt, l.DataType(), dt)
	}
	want := `struct { x int; y int }{x:1, y:2}`
	if l.Value() != want {
		t.Errorf("NewTypedLiteral(%v, %v).Value() => %s ; want %s ", v, dt, l.Value(), want)
	}
}
