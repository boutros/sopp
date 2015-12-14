package sopp

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing/quick"
	"time"

	"github.com/boutros/sopp/rdf"
)

// testing/quick defaults to 5 iterations and a random seed.
// You can override these settings from the command line:
//
//   -quick.count     The number of iterations to perform.
//   -quick.seed      The seed to use for randomizing.
//   -quick.maxnodes  The maximum number of nodes in the generated RDF graph to be insert into a DB.

var (
	qcount, qseed, qmaxnodes int
	rnd                      *rand.Rand
)

func init() {
	flag.IntVar(&qcount, "quick.count", 5, "")
	flag.IntVar(&qseed, "quick.seed", int(time.Now().UnixNano())%100000, "")
	flag.IntVar(&qmaxnodes, "quick.maxnodes", 10, "")
	flag.Parse()
	fmt.Fprintln(os.Stderr, "random seed:", qseed)
	fmt.Fprintf(os.Stderr, "quick settings: count=%v, maxnodes=%v\n", qcount, qmaxnodes)
	rnd = rand.New(rand.NewSource(int64(qseed)))
}

func qconfig() *quick.Config {
	return &quick.Config{
		MaxCount: qcount,
		Rand:     rand.New(rand.NewSource(int64(qseed))),
	}
}

type testdata []testdataitem

type testdataitem struct {
	rdf.Triple
}

func (t testdata) Len() int           { return len(t) }
func (t testdata) Less(i, j int) bool { return t[i].String() < t[j].String() }
func (t testdata) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func (t testdata) Generate(rand *rand.Rand, size int) reflect.Value {

	// Generate a random graph
	// based on qmaxnodes = max subjects

	base := "http://test.org/"

	// Create a pool of 10-100 random URIs to choose from as predicates
	n := rand.Intn(90) + 10
	preds := make([]rdf.URI, n)
	for i := range preds {
		preds[i] = randURI(base)
	}

	n = rand.Intn(qmaxnodes-1) + 1
	nodes := make([]rdf.URI, n)
	for i := range nodes {
		nodes[i] = randURI(base)
	}

	graph := make(testdata, 0, n)
	for _, subj := range nodes {
		var tr testdataitem
		tr.Subj = subj

		// Generate 1-10 predicates per node
		n := rand.Intn(10) + 1
		for i := 0; i < n; i++ {
			tr.Pred = preds[rand.Intn(len(preds))]

			r := rnd.Intn(100)
			switch {
			case r < 20:
				// 20% object is another node in graph
				tr.Obj = nodes[rand.Intn(len(nodes))]
			case r < 25:
				// 5% object is an URI not present in graph
				tr.Obj = randURI("")
			default:
				// 75% object is a Literal
				tr.Obj = randLiteral()
			}
			graph = append(graph, testdataitem(tr))
		}

	}

	return reflect.ValueOf(graph)
}

func randURI(base string) rdf.URI {
	n := rnd.Intn(100)
	if n > 70 {
		// 70% using base uri
		base = ""
	}

	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-///..")
	l := rnd.Intn(100) + 1
	r := make([]rune, l)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return rdf.NewURI(base + string(r))
}

func randLiteral() rdf.Literal {
	r := rnd.Intn(100)
	switch {
	case r < 60: // 60% strings
		v, _ := quick.Value(reflect.TypeOf(""), rnd)
		return rdf.NewLiteral(v.String())
	case r < 70: // 10% language tagged strings
		v, _ := quick.Value(reflect.TypeOf(""), rnd)
		return rdf.NewLangLiteral(v.String(), randLang())
	case r < 75: // 5% int64
		v, _ := quick.Value(reflect.TypeOf(1), rnd)
		return rdf.NewLiteral(v.Int())
	case r < 80: // 5% int32
		v, _ := quick.Value(reflect.TypeOf(int32(1)), rnd)
		return rdf.NewLiteral(int32(v.Int()))
	case r < 82: // 2% float64
		v, _ := quick.Value(reflect.TypeOf(3.14), rnd)
		return rdf.NewLiteral(v.Float())
	case r < 84: // 2% float64
		v, _ := quick.Value(reflect.TypeOf(float32(3.14)), rnd)
		return rdf.NewLiteral(float32(v.Float()))
	case r < 86: // 2% boolean
		v, _ := quick.Value(reflect.TypeOf(true), rnd)
		return rdf.NewLiteral(v.Bool())
	case r < 87: // 1% int8
		v, _ := quick.Value(reflect.TypeOf(int8(0)), rnd)
		return rdf.NewLiteral(int8(v.Int()))
	case r < 88: // 1% int16
		v, _ := quick.Value(reflect.TypeOf(int16(0)), rnd)
		return rdf.NewLiteral(int16(v.Int()))
	case r < 88: // 1% uint8
		v, _ := quick.Value(reflect.TypeOf(uint8(0)), rnd)
		return rdf.NewLiteral(uint8(v.Uint()))
	case r < 89: // 1% uint16
		v, _ := quick.Value(reflect.TypeOf(uint16(0)), rnd)
		return rdf.NewLiteral(uint16(v.Uint()))
	case r < 91: // 2% uint32
		v, _ := quick.Value(reflect.TypeOf(uint32(0)), rnd)
		return rdf.NewLiteral(uint32(v.Uint()))
	case r < 93: // 2% uint64
		v, _ := quick.Value(reflect.TypeOf(uint64(0)), rnd)
		return rdf.NewLiteral(v.Uint())
	default: // 6% time.Time
		s := rand.Int63()
		n := rand.Int63()
		return rdf.NewLiteral(time.Unix(s, n))
	}
}

func randLang() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-")
	l := rnd.Intn(8) + 1
	r := make([]rune, l)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return string(r)
}
