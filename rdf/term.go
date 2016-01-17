package rdf

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Commonly used datatype URIs (and the ones used by this package internally):
var (
	RDFtype          = URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
	RDFlangString    = URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
	XSDboolean       = URI("http://www.w3.org/2001/XMLSchema#boolean")
	XSDbyte          = URI("http://www.w3.org/2001/XMLSchema#byte")
	XSDint           = URI("http://www.w3.org/2001/XMLSchema#int")
	XSDshort         = URI("http://www.w3.org/2001/XMLSchema#short")
	XSDlong          = URI("http://www.w3.org/2001/XMLSchema#long")
	XSDinteger       = URI("http://www.w3.org/2001/XMLSchema#integer")
	XSDstring        = URI("http://www.w3.org/2001/XMLSchema#string")
	XSDunsignedShort = URI("http://www.w3.org/2001/XMLSchema#unsignedShort")
	XSDunsignedInt   = URI("http://www.w3.org/2001/XMLSchema#unsignedInt")
	XSDunsignedLong  = URI("http://www.w3.org/2001/XMLSchema#unsignedLong")
	XSDunsignedByte  = URI("http://www.w3.org/2001/XMLSchema#unsignedByte")
	XSDfloat         = URI("http://www.w3.org/2001/XMLSchema#float")
	XSDdouble        = URI("http://www.w3.org/2001/XMLSchema#double")
	XSDdateTimeStamp = URI("http://www.w3.org/2001/XMLSchema#dateTimeStamp")
)

// URI represents an URI node in a RDF graph.
type URI string

// NewURI returns a new URI. The following characters will be stripped:
// <>"{}|^`\ - as well as characters in the range 0x00-0x20. No other
// validations are performed.
func NewURI(s string) URI {
	b := bytes.NewBuffer(make([]byte, 0, len(s)))
	for _, ch := range s {
		switch ch {
		case '<', '>', '"', '{', '}', '|', '^', '`', '\\':
		default:
			if ch > '\x20' {
				b.WriteRune(ch)
			}
		}
	}
	return URI(b.String())
}

// String returns the URI as a string.
func (u URI) String() string {
	return string(u)
}

// Resolve resolves the URI against the given base URI, and return
// the new, absolute URI. If the URI is no relative, it is returned umonified.
func (u URI) Resolve(base URI) URI {
	// Return early if the URI is absolute
	if strings.HasPrefix(string(u), "http://") || base == "" {
		// TODO: An URI can have other schemas than http
		return u
	}
	r, _ := utf8.DecodeRuneInString(string(u))
	switch r {
	case '/':
		return URI(strings.TrimSuffix(string(base), "/") + string(u))
	case '#':
		return URI(strings.TrimSuffix(string(base), "#") + string(u))
	default:
		r, _ := utf8.DecodeLastRuneInString(string(base))
		switch r {
		case '/', '#':
			return URI(string(base) + string(u))
		default:
			return URI(string(base) + "/" + string(u))
		}
	}
}

// validAsTerm satiesfies the Term interface for URI.
func (u URI) validAsTerm() {}

func (u URI) validAsQVar() {}

// Literal represents a literal value node in a RDF graph. A Literal has
// a value and a datatype. If the datatype is rdf:langString, it also
// has a language tag.
type Literal struct {
	value    string
	language string
	datatype URI
}

// NewLiteral returns a new Literal with a datatype inferred from the type
// of the given value, according to the following table:
//
//   Go type       | Literal datatype
//   --------------|-----------------
//   bool          | xsd:boolean
//   int           | xsd:int/xsd:long
//   int8          | xsd:byte
//   int16         | xsd:short
//   int32/rune    | xsd:int
//   int64         | xsd:long
//   uint          | xsd:unsignedInt/xsd:unsignedLong
//   uint8/byte    | xsd:unsignedByte
//   uint16        | xsd:unsignedShort
//   uint32        | xsd:unsignedInt
//   uint64        | xsd:unsignedLong
//   float         | xsd:float/xsd:double
//   float32       | xsd:float
//   float64       | xsd:double
//   string        | xsd:string
//   time.Time     | xsd:dateTimeStamp
//
// Any other type will be given the type xsd:string,
// and the value of fmt.Sprintf("%#v", v).
func NewLiteral(v interface{}) Literal {
	switch t := v.(type) {
	case bool:
		return Literal{
			value:    strconv.FormatBool(t),
			datatype: XSDboolean,
		}
	case int:
		if strconv.IntSize == 32 {
			return Literal{
				value:    strconv.FormatInt(int64(t), 10),
				datatype: XSDint,
			}
		}
		return Literal{
			value:    strconv.FormatInt(int64(t), 10),
			datatype: XSDlong,
		}
	case int8:
		return Literal{
			value:    strconv.FormatInt(int64(t), 10),
			datatype: XSDbyte,
		}
	case int16:
		return Literal{
			value:    strconv.FormatInt(int64(t), 10),
			datatype: XSDshort,
		}
	case int32:
		return Literal{
			value:    strconv.FormatInt(int64(t), 10),
			datatype: XSDint,
		}
	case int64:
		return Literal{
			value:    strconv.FormatInt(int64(t), 10),
			datatype: XSDlong,
		}
	case uint:
		if strconv.IntSize == 32 {
			return Literal{
				value:    strconv.FormatUint(uint64(t), 10),
				datatype: XSDunsignedInt,
			}
		}
		return Literal{
			value:    strconv.FormatUint(uint64(t), 10),
			datatype: XSDunsignedLong,
		}
	case uint8:
		return Literal{
			value:    strconv.FormatUint(uint64(t), 10),
			datatype: XSDunsignedByte,
		}
	case uint16:
		return Literal{
			value:    strconv.FormatUint(uint64(t), 10),
			datatype: XSDunsignedShort,
		}
	case uint32:
		return Literal{
			value:    strconv.FormatUint(uint64(t), 10),
			datatype: XSDunsignedInt,
		}
	case uint64:
		return Literal{
			value:    strconv.FormatUint(uint64(t), 10),
			datatype: XSDunsignedLong,
		}
	case float32:
		return Literal{
			value:    strconv.FormatFloat(float64(t), 'E', -1, 32),
			datatype: XSDfloat,
		}
	case float64:
		return Literal{
			value:    strconv.FormatFloat(float64(t), 'E', -1, 64),
			datatype: XSDdouble,
		}
	case string:
		return Literal{value: t, datatype: XSDstring}
	case time.Time:
		return Literal{
			value:    t.UTC().Format(time.RFC3339Nano),
			datatype: XSDdateTimeStamp,
		}
	default:
		return Literal{
			value:    fmt.Sprintf("%#v", t),
			datatype: XSDstring,
		}
	}
}

// Value returns the Literal's typed value in theS corresponding Go type.
func (l Literal) Value() interface{} {
	switch l.datatype {
	case XSDboolean:
		v, _ := strconv.ParseBool(l.value)
		return v
	case XSDstring:
		return l.value
	case XSDint:
		v, _ := strconv.ParseInt(l.value, 10, 32)
		return int32(v)
	case XSDlong:
		v, _ := strconv.ParseInt(l.value, 10, 64)
		return v
	case XSDbyte:
		v, _ := strconv.ParseInt(l.value, 10, 8)
		return int8(v)
	case XSDshort:
		v, _ := strconv.ParseInt(l.value, 10, 16)
		return int16(v)
	case XSDunsignedByte:
		v, _ := strconv.ParseUint(l.value, 10, 8)
		return byte(v)
	case XSDunsignedShort:
		v, _ := strconv.ParseUint(l.value, 10, 16)
		return uint16(v)
	case XSDunsignedInt:
		v, _ := strconv.ParseUint(l.value, 10, 32)
		return uint32(v)
	case XSDunsignedLong:
		v, _ := strconv.ParseUint(l.value, 10, 64)
		return v
	case XSDfloat:
		v, _ := strconv.ParseFloat(l.value, 32)
		return float32(v)
	case XSDdouble:
		v, _ := strconv.ParseFloat(l.value, 64)
		return v
	case XSDdateTimeStamp:
		v, _ := time.Parse(time.RFC3339Nano, l.value)
		return v.UTC()
	default:
		// return as string
		return l.value
	}
}

// String returns the Literal's value as a string.
func (l Literal) String() string {
	return l.value
}

// DataType returns the DataType URI of the Literal.
func (l Literal) DataType() URI {
	return l.datatype
}

// Lang returns a Literal's language tag, if present.
func (l Literal) Lang() string {
	return l.language
}

// validAsTerm satiesfies the Term interface for Literal.
func (l Literal) validAsTerm() {}

func (l Literal) validAsQVar() {}

// NewLangLiteral returns a new, language-tagged Literal.
func NewLangLiteral(v string, lang string) Literal {
	return Literal{
		value:    v,
		language: lang,
		datatype: RDFlangString,
	}
}

// NewTypedLiteral returns a new Literal with the given DataType.
func NewTypedLiteral(v string, dt URI) Literal {
	return Literal{
		value:    v,
		datatype: dt,
	}
}

// Term represents a RDF Term: the combination of URI and Literal.
type Term interface {
	// String returns a string represenation of a Term
	String() string

	// method is not exported to hinder interface implementations outside this package:
	validAsTerm()
}

// terms is a slice of Term. (Necessary to make it sortable)
type terms []Term

// Len satisfies the Sort interface for Terms.
func (t terms) Len() int { return len(t) }

// Swap satisfies the Sort interface for Terms.
func (t terms) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

// Less satisfies the Sort interface for Terms.
func (t terms) Less(i, j int) bool { return t[i].String() < t[j].String() }

// DecodeTerm decodes a slice of byte into a RDF Term.
func DecodeTerm(b []byte) (Term, error) {
	return nil, nil
}
