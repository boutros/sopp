package sopp

import (
	"io/ioutil"
	"os"
	"testing"
	"testing/quick"
)

type testDB struct {
	*DB
}

func newTestDB() *testDB {
	db, err := Open(tempfile(), "http://test.org/")
	if err != nil {
		panic("cannot open db: " + err.Error())
	}
	return &testDB{db}
}

func (db *testDB) Close() {
	defer os.Remove(db.kv.Path())
	db.DB.Close()
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "sopp-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// Verify that triples can be stored in DB, and reported as stored.
func TestInsert_Quick(t *testing.T) {
	f := func(items testdata) bool {
		db := newTestDB()
		defer db.Close()

		/*// in-memory graph used as an reference implementation
		ref := rdf.NewGraph()*/
		for _, item := range items {
			// Verify that triple is not present in graph
			// (Technically it is possible that the same triple was generated twice, but
			// so unlikely that we can forget about it)
			/*if ref.Has(item.Triple) {
				panic("bug in rdf.Graph.Has")
			}*/

			present, err := db.Has(item.Triple)
			if present {
				t.Logf("DB.Has(%v) => true before insert", item.Triple)
				t.FailNow()
			}
			if err != nil {
				t.Logf("DB.Has(%v) failed: ", item.Triple, err)
				t.FailNow()
			}

			/*ref.Insert(item.Triple)*/
			if err := db.Insert(item.Triple); err != nil {
				t.Logf("DB.Insert(%v) failed: %v", item.Triple, err)
				t.FailNow()
			}
		}

		print(".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
}
