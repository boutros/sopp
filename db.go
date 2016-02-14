package sopp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/boltdb/bolt"
	"github.com/boutros/sopp/rdf"
)

// Exported errors
var (
	// ErrNotFound is an error signifying that the resource (Triple/Term)
	// is not present in the database.
	ErrNotFound = errors.New("not found")

	// ErrDBFull is returned when the database cannot store more terms.
	// Note: this will most likely be lower than MaxTerms, since the ID's of
	// deleted terms are not reclaimed.
	ErrDBFull = errors.New("database full: term limit reached")
)

const (
	// MaxTerms is the maximum number of unique RDF terms that can be stored.
	MaxTerms = 4294967295
)

// Buckets in the key-value store:
var (
	// RDF Terms
	bucketTerms    = []byte("terms")  // uint32 -> term
	bucketIdxTerms = []byte("iterms") // term -> uint32

	// Triple indices       composite key         bitmap
	bucketSPO = []byte("spo") // Subect + Predicate -> Object
	bucketOSP = []byte("osp") // Object + Subject   -> Predicate
	bucketPOS = []byte("pos") // Predicate + Object -> Subject
)

// DB is a RDF triple store backed by a key-value store.
type DB struct {
	// kv is the key-value database (BoltDB) backing the triple store
	kv *bolt.DB

	// The majority of the URIs in a RDF graph are typically made up
	// using the same base URI, so we optimize storage for those by
	// making it the default case, and only storing the absolute part
	// of an URI when it's not the default one.
	//
	// The base should include the scheme and hostname, ex: http://example.org/
	//
	// It must not be changed as long as the database is open, but may
	// be set in the call to Open() when opening a database.
	base string

	// TODOs >>

	// muPred protects the bimap of predicates
	//muPred sync.RWMutex

	// The number of predicates used in a RDF is usually quite low, so we
	// maintain a cache of those in a bi-directional map
	//pred bimap.URI2uint32
}

// Stats holds some statistics of the triple store.
type Stats struct {
	NumTerms int
	//NumTriples    int
	File        string
	SizeInBytes int
}

// Stats return statistics about the triple store.
func (db *DB) Stats() (Stats, error) {
	st := Stats{}
	if err := db.kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketTerms)
		st.NumTerms = bkt.Stats().KeyN
		//st.NumTriples = int(atomic.LoadInt64(&db.numTr))
		st.File = db.kv.Path()
		s, err := os.Stat(st.File)
		if err != nil {
			return err
		}
		st.SizeInBytes = int(s.Size())
		return nil
	}); err != nil {
		return st, err
	}
	return st, nil
}

// Open creates and opens a database at the given path.
// If the file does not exist it will be created.
// Only one process can have access to the file at a time.
func Open(path string, base string) (*DB, error) {
	kv, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	db := &DB{
		kv:   kv,
		base: base,
	}
	return db.setup()
}

// Close closes the database, relasing the lock on the database file.
func (db *DB) Close() error {
	return db.kv.Close()
}

// setup makes sure the database has all the required buckets.
func (db *DB) setup() (*DB, error) {
	err := db.kv.Update(func(tx *bolt.Tx) error {
		// Make sure all the required buckets are present
		for _, b := range [][]byte{bucketTerms, bucketIdxTerms, bucketSPO, bucketPOS, bucketOSP} {
			_, err := tx.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}

		/*
			// Count number of triples
			bkt = tx.Bucket(bSPO)
			cur = bkt.Cursor()

			var n uint64
			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				if v != nil {
					bitmap := roaring.NewBitmap()
					_, err := bitmap.ReadFrom(bytes.NewReader(v))
					if err != nil {
						return err
					}
					n += bitmap.GetCardinality()
				} // else ?
			}
			db.numTr = int64(n)
		*/

		return nil
	})
	return db, err
}

// Insert stores the given Triple.
func (db *DB) Insert(tr rdf.Triple) error {
	err := db.kv.Update(func(tx *bolt.Tx) error {
		sID, err := db.addTerm(tx, tr.Subj)
		if err != nil {
			return err
		}

		// TODO get pID from cache
		pID, err := db.addTerm(tx, tr.Pred)
		if err != nil {
			return err
		}

		oID, err := db.addTerm(tx, tr.Obj)
		if err != nil {
			return err
		}

		return db.storeTriple(tx, sID, pID, oID)
	})
	return err
}

// Delete removes the given Triple from the indices. It also removes
// any Term unique to that Triple from the store.
// It return ErrNotFound if the Triple is not stored
func (db *DB) Delete(tr rdf.Triple) error {
	err := db.kv.Update(func(tx *bolt.Tx) error {
		sID, err := db.getID(tx, tr.Subj)
		if err != nil {
			return err
		}

		// TODO get pID from cache
		pID, err := db.getID(tx, tr.Pred)
		if err != nil {
			return err
		}

		oID, err := db.getID(tx, tr.Obj)
		if err != nil {
			return err
		}

		return db.removeTriple(tx, sID, pID, oID)
	})
	return err
}

// Has checks if the given Triple is stored.
func (db *DB) Has(tr rdf.Triple) (exists bool, err error) {
	err = db.kv.View(func(tx *bolt.Tx) error {
		sID, err := db.getID(tx, tr.Subj)
		if err == ErrNotFound {
			return nil
		} else if err != nil {
			return err
		}
		// TODO get pID from cache, and move to top before sID
		pID, err := db.getID(tx, tr.Pred)
		if err == ErrNotFound {
			return nil
		} else if err != nil {
			return err
		}
		oID, err := db.getID(tx, tr.Obj)
		if err == ErrNotFound {
			return nil
		} else if err != nil {
			return err
		}

		bkt := tx.Bucket(bucketSPO)

		sp := make([]byte, 8)
		copy(sp, u32tob(sID))
		copy(sp[4:], u32tob(pID))

		bitmap := roaring.NewBitmap()
		bo := bkt.Get(sp)
		if bo == nil {
			return nil
		}

		_, err = bitmap.ReadFrom(bytes.NewReader(bo))
		if err != nil {
			return err
		}

		exists = bitmap.Contains(oID)
		return nil
	})
	return exists, err
}

// Describe returns a graph with all the triples where the given node
// is subject. If asObject is true, it also includes the triples where
// the node is object.
func (db *DB) Describe(node rdf.URI, asObject bool) (*rdf.Graph, error) {
	g := rdf.NewGraph()
	err := db.kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketIdxTerms)
		bt := db.encode(node)
		bs := bkt.Get(bt)
		if bs == nil {
			return nil
		}
		// seek in SPO index:
		// WHERE { <node> ?p ?o }
		sid := btou32(bs)
		cur := tx.Bucket(bucketSPO).Cursor()
	outerSPO:
		for k, v := cur.Seek(u32tob(sid - 1)); k != nil; k, v = cur.Next() {
			switch bytes.Compare(k[:4], bs) {
			case 0:
				bkt = tx.Bucket(bucketTerms)
				b := bkt.Get(k[4:])
				if b == nil {
					return errors.New("bug: term ID in index, but not stored")
				}

				// TODO get pred from cache
				pred, err := db.decode(b)
				if err != nil {
					return err
				}
				bitmap := roaring.NewBitmap()
				_, err = bitmap.ReadFrom(bytes.NewReader(v))
				if err != nil {
					return err
				}
				it := bitmap.Iterator()
				for it.HasNext() {
					o := it.Next()
					b = bkt.Get(u32tob(o))
					if b == nil {
						return errors.New("bug: term ID in index, but not stored")
					}

					obj, err := db.decode(b)
					if err != nil {
						return err
					}
					g.Insert(rdf.Triple{Subj: node, Pred: pred.(rdf.URI), Obj: obj})
				}
			case 1:
				break outerSPO
			}
		}

		if !asObject {
			return nil
		}
		// seek in OSP index:
		// WHERE { ?s ?p <node> }
		cur = tx.Bucket(bucketOSP).Cursor()
	outerOSP:
		for k, v := cur.Seek(u32tob(sid - 1)); k != nil; k, v = cur.Next() {
			switch bytes.Compare(k[:4], bs) {
			case 0:
				bkt = tx.Bucket(bucketTerms)
				b := bkt.Get(k[4:])
				if b == nil {
					return errors.New("bug: term ID in index, but not stored")
				}
				subj, err := db.decode(b)
				if err != nil {
					return err
				}
				bitmap := roaring.NewBitmap()
				_, err = bitmap.ReadFrom(bytes.NewReader(v))
				if err != nil {
					return err
				}
				it := bitmap.Iterator()
				for it.HasNext() {
					o := it.Next()
					b = bkt.Get(u32tob(o))
					if b == nil {
						return errors.New("bug: term ID in index, but not stored")
					}
					// TODO get pred from cache
					pred, err := db.decode(b)
					if err != nil {
						return err
					}
					g.Insert(rdf.Triple{Subj: subj.(rdf.URI), Pred: pred.(rdf.URI), Obj: node})
				}
			case 1:
				break outerOSP
			}
		}

		return nil
	})

	return g, err
}

// Import imports triples from an Turtle stream, in batches of given size.
// It will ignore triples with blank nodes and errors.
// It returns the total number of triples imported.
func (db *DB) Import(r io.Reader, batchSize int) (int, error) {
	dec := rdf.NewDecoder(r)
	g := rdf.NewGraph()
	c := 0 // totalt count
	i := 0 // current batch count
	for tr, err := dec.Decode(); err != io.EOF; tr, err = dec.Decode() {
		if err != nil {
			// log.Println(err.Error())
			continue
		}
		g.Insert(tr)
		i++
		if i == batchSize {
			err = db.ImportGraph(g)
			if err != nil {
				return c, err
			}
			c += i
			i = 0
			g = rdf.NewGraph()
		}
	}
	if len(g.Nodes()) > 0 {
		err := db.ImportGraph(g)
		if err != nil {
			return c, err
		}
		c += i
	}
	return c, nil
}

func (db *DB) ImportGraph(g *rdf.Graph) error {
	return db.kv.Update(func(tx *bolt.Tx) error {
		for subj, props := range g.Nodes() {

			sID, err := db.addTerm(tx, subj)
			if err != nil {
				return err
			}

			for pred, terms := range props {
				pID, err := db.addTerm(tx, pred)
				if err != nil {
					return err
				}

				for _, obj := range terms {
					// TODO batch bitmap operations for all obj in terms
					oID, err := db.addTerm(tx, obj)
					if err != nil {
						return err
					}

					err = db.storeTriple(tx, sID, pID, oID)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

// Dump writes the entire database as a Turtle serialization to the given writer.
func (db *DB) Dump(to io.Writer) error {
	// TODO getTerm without expanding base URI?
	// base is prefixed added, but then stripped again here
	w := bufio.NewWriter(to)
	w.WriteString("@base <")
	w.WriteString(db.base)
	w.WriteString(">")
	return db.kv.View(func(tx *bolt.Tx) error {
		defer w.Flush()

		var curSubj uint32
		var subj, pred, obj rdf.Term

		bkt := tx.Bucket(bucketSPO)
		if err := bkt.ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				panic("len(SPO key) != 8")
			}
			var err error
			sID := btou32(k[:4])
			if sID != curSubj {
				// end previous statement
				w.WriteString(" .\n")
				curSubj = sID

				if subj, err = db.getTerm(tx, sID); err != nil {
					return err
				}
				w.WriteRune('<')
				w.WriteString(strings.TrimPrefix(subj.String(), db.base))
				w.WriteString("> ")
			} else {
				// continue with same subject
				w.WriteString(" ;\n\t")
			}

			pID := btou32(k[4:])

			if pred, err = db.getTerm(tx, pID); err != nil {
				return err
			}
			if pred == rdf.RDFtype {
				w.WriteString("a ")
			} else {
				w.WriteRune('<')
				w.WriteString(strings.TrimPrefix(pred.String(), db.base))
				w.WriteString("> ")
			}

			bitmap := roaring.NewBitmap()
			if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
				return err
			}

			i := bitmap.Iterator()
			c := 0
			for i.HasNext() {
				if obj, err = db.getTerm(tx, i.Next()); err != nil {
					return err
				}
				if c > 0 {
					w.WriteString(", ")
				}
				switch t := obj.(type) {
				case rdf.URI:
					w.WriteRune('<')
					w.WriteString(strings.TrimPrefix(obj.String(), db.base))
					w.WriteRune('>')
				case rdf.Literal:
					// TODO bench & optimize
					switch t.DataType() {
					case rdf.RDFlangString:
						fmt.Fprintf(w, "%q@%s", t.String(), t.Lang())
					case rdf.XSDstring:
						fmt.Fprintf(w, "%q", t.String())
					default:
						fmt.Fprintf(w, "%q^^<%s>", t.String(), strings.TrimPrefix(t.DataType().String(), db.base))
					}
				}
				c++
			}

			return nil
		}); err != nil {
			return err
		}

		w.WriteString(" .\n")
		return nil
	})
}

func (db *DB) forEach(fn func(rdf.Triple) error) error {
	return db.kv.View(func(tx *bolt.Tx) error {

		bkt := tx.Bucket(bucketSPO)
		// iterate over each each triple in SPO index
		if err := bkt.ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				panic("len(SPO key) != 8")
			}
			sID := btou32(k[:4])
			pID := btou32(k[4:])

			var tr rdf.Triple
			var err error
			var term rdf.Term

			if term, err = db.getTerm(tx, sID); err != nil {
				return err
			}
			tr.Subj = term.(rdf.URI)
			if term, err = db.getTerm(tx, pID); err != nil {
				return err
			}
			tr.Pred = term.(rdf.URI)

			bitmap := roaring.NewBitmap()
			if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
				return err
			}

			// Iterate over each term in object bitmap
			i := bitmap.Iterator()
			for i.HasNext() {
				if tr.Obj, err = db.getTerm(tx, i.Next()); err != nil {
					return err
				}
				if err := fn(tr); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}

		return nil
	})
}

func (db *DB) addTerm(tx *bolt.Tx, term rdf.Term) (id uint32, err error) {
	bt := db.encode(term)

	if id, err = db.getIDb(tx, bt); err == nil {
		// Term is allready in database
		return id, nil
	} else if err != ErrNotFound {
		// Some other IO error occured
		return 0, err
	}

	// get a new ID
	bkt := tx.Bucket(bucketTerms)
	n, err := bkt.NextSequence()
	if err != nil {
		return 0, err
	}
	if n > MaxTerms {
		return 0, ErrDBFull
	}

	id = uint32(n)
	idb := u32tob(uint32(n))

	// store term and index it
	err = bkt.Put(idb, bt)
	if err != nil {
		return 0, err
	}
	bkt = tx.Bucket(bucketIdxTerms)
	err = bkt.Put(bt, idb)
	return id, err
}

func (db *DB) storeTriple(tx *bolt.Tx, s, p, o uint32) error {
	indices := []struct {
		k1 uint32
		k2 uint32
		v  uint32
		bk []byte
	}{
		{s, p, o, bucketSPO},
		{o, s, p, bucketOSP},
		{p, o, s, bucketPOS},
	}

	key := make([]byte, 8)

	for _, i := range indices {
		bkt := tx.Bucket(i.bk)
		copy(key, u32tob(i.k1))
		copy(key[4:], u32tob(i.k2))
		bitmap := roaring.NewBitmap()

		bo := bkt.Get(key)
		if bo != nil {
			_, err := bitmap.ReadFrom(bytes.NewReader(bo))
			if err != nil {
				return err
			}
		}

		newTriple := bitmap.CheckedAdd(i.v)
		if !newTriple {
			// Triple is allready stored
			return nil
		}
		var b bytes.Buffer
		_, err := bitmap.WriteTo(&b)
		if err != nil {
			return err
		}
		err = bkt.Put(key, b.Bytes())
		if err != nil {
			return err
		}
	}

	//atomic.AddInt64(&db.numTr, 1)

	return nil
}

// removeTriple removes a triple from the indices. If the triple
// contains any terms unique to that triple, they will also be removed.
func (db *DB) removeTriple(tx *bolt.Tx, s, p, o uint32) error {
	// TODO think about what to do if present in one index but
	// not in another: maybe panic? Cause It's a bug that should be fixed.

	indices := []struct {
		k1 uint32
		k2 uint32
		v  uint32
		bk []byte
	}{
		{s, p, o, bucketSPO},
		{o, s, p, bucketOSP},
		{p, o, s, bucketPOS},
	}

	key := make([]byte, 8)
	for _, i := range indices {
		bkt := tx.Bucket(i.bk)
		copy(key, u32tob(i.k1))
		copy(key[4:], u32tob(i.k2))

		bo := bkt.Get(key)
		if bo == nil {
			// TODO should never happen, return bug error?
			return ErrNotFound
		}

		bitmap := roaring.NewBitmap()
		_, err := bitmap.ReadFrom(bytes.NewReader(bo))
		if err != nil {
			return err
		}
		hasTriple := bitmap.CheckedRemove(i.v)
		if !hasTriple {
			// TODO should never happen, return bug error?
			return ErrNotFound
		}
		// Remove from index if bitmap is empty
		if bitmap.GetCardinality() == 0 {
			err = bkt.Delete(key)
			if err != nil {
				return err
			}
		} else {
			var b bytes.Buffer
			_, err = bitmap.WriteTo(&b)
			if err != nil {
				return err
			}
			err = bkt.Put(key, b.Bytes())
			if err != nil {
				return err
			}
		}
	}

	//atomic.AddInt64(&db.numTr, -1)

	return db.removeOrphanedTerms(tx, s, p, o)
}

func (db *DB) removeTerm(tx *bolt.Tx, termID uint32) error {
	bkt := tx.Bucket(bucketTerms)
	term := bkt.Get(u32tob(termID))
	if term == nil {
		// removeTerm should never be called on a allready deleted Term
		return errors.New("bug: removeTerm: Term does not exist")
	}
	err := bkt.Delete(u32tob(termID))
	if err != nil {
		return err
	}
	bkt = tx.Bucket(bucketIdxTerms)
	err = bkt.Delete(term)
	if err != nil {
		return err
	}
	return nil
}

// removeOrphanedTerms removes any of the given Terms if they are no longer
// part of any triple.
func (db *DB) removeOrphanedTerms(tx *bolt.Tx, s, p, o uint32) error {
	// TODO by now we don't know whether object is a Literal or and URI.
	// If we knew it to be a Literal, checking the OSP index would suffice.

	for _, id := range unique(s, p, o) {
		if notInIndex(tx, id, bucketSPO) && notInIndex(tx, id, bucketOSP) && notInIndex(tx, id, bucketPOS) {
			err := db.removeTerm(tx, id)
			if err != nil {
				if err == ErrNotFound {
					return errors.New("bug: removeOrphanedTerms removing Term allready gone")
				}
				return err
			}
		}
	}
	return nil
}

// unique removes any duplicates of the s,p,o IDs.
func unique(s, p, o uint32) []uint32 {
	// TODO revise this function and its usage
	res := make([]uint32, 0, 3)
	res = append(res, s)
	if p != s {
		res = append(res, p)
	}
	if o != s && o != p {
		res = append(res, o)
	}
	return res
}

func notInIndex(tx *bolt.Tx, id uint32, idx []byte) bool {
	cur := tx.Bucket(idx).Cursor()
	for k, _ := cur.Seek(u32tob(id - 1)); k != nil; k, _ = cur.Next() {
		switch bytes.Compare(k[:4], u32tob(id)) {
		case 0:
			return false
		case 1:
			return true
		}
	}
	return true
}

func (db *DB) getID(tx *bolt.Tx, term rdf.Term) (id uint32, err error) {
	bkt := tx.Bucket(bucketIdxTerms)
	bt := db.encode(term)
	b := bkt.Get(bt)
	if b == nil {
		err = ErrNotFound
	} else {
		id = btou32(b)
	}
	return id, err
}

func (db *DB) getIDb(tx *bolt.Tx, term []byte) (id uint32, err error) {
	bkt := tx.Bucket(bucketIdxTerms)
	b := bkt.Get(term)
	if b == nil {
		err = ErrNotFound
	} else {
		id = btou32(b)
	}
	return id, err
}

// getTerm returns the term for a given ID.
func (db *DB) getTerm(tx *bolt.Tx, id uint32) (rdf.Term, error) {
	bkt := tx.Bucket(bucketTerms)
	b := bkt.Get(u32tob(id))
	if b == nil {
		return nil, ErrNotFound
	}
	return db.decode(b)
}

func (db *DB) encode(t rdf.Term) []byte {
	switch term := t.(type) {
	case rdf.URI:
		if strings.HasPrefix(string(term), db.base) {
			l := len(db.base)
			b := make([]byte, len(term)-l+1)
			copy(b[1:], string(term)[l:])
			return b
		}
		b := make([]byte, len(term)+1)
		b[0] = 0x01
		copy(b[1:], string(term))
		return b
	case rdf.Literal:
		var dt byte
		switch term.DataType() {
		case rdf.XSDstring:
			dt = 0x02
		case rdf.RDFlangString:
			ll := len(term.Lang())
			b := make([]byte, len(term.String())+ll+2)
			b[0] = 0x03
			b[1] = uint8(ll)
			copy(b[2:], []byte(term.Lang()))
			copy(b[2+ll:], []byte(term.String()))
			return b
		case rdf.XSDboolean:
			dt = 0x04
		case rdf.XSDbyte:
			dt = 0x05
		case rdf.XSDint:
			dt = 0x06
		case rdf.XSDshort:
			dt = 0x07
		case rdf.XSDlong:
			dt = 0x08
		case rdf.XSDinteger:
			dt = 0x09
		case rdf.XSDunsignedShort:
			dt = 0x0A
		case rdf.XSDunsignedInt:
			dt = 0x0B
		case rdf.XSDunsignedLong:
			dt = 0x0C
		case rdf.XSDunsignedByte:
			dt = 0x0D
		case rdf.XSDfloat:
			dt = 0x0E
		case rdf.XSDdouble:
			dt = 0x0F
		case rdf.XSDdateTimeStamp:
			dt = 0x10
		default:
			ll := len(term.DataType())
			b := make([]byte, len(term.String())+ll+2)
			b[0] = 0xFF
			b[1] = uint8(ll)
			copy(b[2:], []byte(term.DataType()))
			copy(b[2+ll:], []byte(term.String()))
			return b

		}
		b := make([]byte, len(term.String())+1)
		b[0] = dt
		copy(b[1:], string(term.String()))
		return b
	}

	panic("unreachable")
}

func (db *DB) decode(b []byte) (rdf.Term, error) {
	// We control the encoding, so the only way for this method to fail to decode
	// into a RDF term is if the underlying stoarge has been corrupted on the file system level.
	if len(b) == 0 {
		return nil, errors.New("cannot decode empty byte slice into RDF term")
	}

	var dt rdf.URI
	switch b[0] {
	case 0x00:
		return rdf.URI(db.base + string(b[1:])), nil
	case 0x01:
		return rdf.URI(string(b[1:])), nil
	case 0x02:
		return rdf.NewTypedLiteral(string(b[1:]), rdf.XSDstring), nil
	case 0x03:
		if len(b) < 2 {
			return nil, fmt.Errorf("cannot decode as rdf:langString: %v", b)
		}
		ll := int(b[1])
		if len(b) < ll+2 {
			return nil, fmt.Errorf("cannot decode as rdf:langString: %v", b)
		}
		return rdf.NewLangLiteral(string(b[ll+2:]), string(b[2:2+ll])), nil
	case 0x04:
		dt = rdf.XSDboolean
	case 0x05:
		dt = rdf.XSDbyte
	case 0x06:
		dt = rdf.XSDint
	case 0x07:
		dt = rdf.XSDshort
	case 0x08:
		dt = rdf.XSDlong
	case 0x09:
		dt = rdf.XSDinteger
	case 0x0A:
		dt = rdf.XSDunsignedShort
	case 0x0B:
		dt = rdf.XSDunsignedInt
	case 0x0C:
		dt = rdf.XSDunsignedLong
	case 0x0D:
		dt = rdf.XSDunsignedByte
	case 0x0E:
		dt = rdf.XSDfloat
	case 0x0F:
		dt = rdf.XSDdouble
	case 0x10:
		dt = rdf.XSDdateTimeStamp
	case 0xFF:
		if len(b) < 2 {
			return nil, fmt.Errorf("cannot decode as literal: %v", b)
		}
		ll := int(b[1])
		if len(b) < ll {
			return nil, fmt.Errorf("cannot decode as literal: %v", b)
		}
		return rdf.NewTypedLiteral(string(b[ll+2:]), rdf.NewURI(string(b[2:2+ll]))), nil
	default:
		return nil, fmt.Errorf("cannot decode RDF term: %v", b)
	}

	return rdf.NewTypedLiteral(string(b[1:]), dt), nil
}

// u32tob converts a uint32 into a 4-byte slice.
func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// btou32 converts a 4-byte slice into an uint32.
func btou32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
