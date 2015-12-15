package sopp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/boutros/sopp/rdf"
	"github.com/tgruben/roaring"
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
					bitmap := roaring.NewRoaringBitmap()
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

		bitmap := roaring.NewRoaringBitmap()
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

func (db *DB) forEach(fn func(rdf.Triple) error) error {
	return db.kv.View(func(tx *bolt.Tx) error {

		bkt := tx.Bucket(bucketSPO)
		// iterate over each each triple in SPO index
		if err := bkt.ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				return nil
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

			bitmap := roaring.NewRoaringBitmap()
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
		bitmap := roaring.NewRoaringBitmap()

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
