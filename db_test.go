package sopp

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"testing/quick"

	"github.com/boutros/sopp/rdf"
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

		// Remove any duplicates from the testdata set
		sort.Sort(items)
		c := 0
		for i, item := range items {
			if i > 0 && item == items[i-1] {
				items[i] = items[c]
				c++
			}
		}
		items = items[c:]

		for _, item := range items {
			// Verify that triple is not present in graph
			// (Technically it is possible that the same triple was generated twice, but
			// so unlikely that we can forget about it)

			present, err := db.Has(item.Triple)
			if present {
				t.Logf("DB.Has(%v) => true before insert", item.Triple)
				t.FailNow()
			}
			if err != nil {
				t.Logf("DB.Has(%v) failed: ", item.Triple, err)
				t.FailNow()
			}

			if err := db.Insert(item.Triple); err != nil {
				t.Logf("DB.Insert(%v) failed: %v", item.Triple, err)
				t.FailNow()
			}

			// Verify triple is now present in graph
			present, err = db.Has(item.Triple)
			if !present {
				t.Logf("DB.Has(%v) => false after insert", item.Triple)
				t.FailNow()
			}
			if err != nil {
				t.Logf("DB.Has(%v) failed: ", item.Triple, err)
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

// Verify that triples can be deleted and reported as not stored.
func TestDelete_Quick(t *testing.T) {
	f := func(items testdata) bool {
		db := newTestDB()
		defer db.Close()

		for _, item := range items {
			if err := db.Insert(item.Triple); err != nil {
				t.Logf("DB.Insert(%v) failed: %v", item.Triple, err)
				t.FailNow()
			}

			if err := db.Delete(item.Triple); err != nil {
				t.Logf("DB.Delete(%v) failed: %v", item.Triple, err)
				t.FailNow()
			}

			// Verify triple is not present in graph
			present, err := db.Has(item.Triple)
			if present {
				t.Logf("DB.Has(%v) => true before insert", item.Triple)
				t.FailNow()
			}
			if err != nil {
				t.Logf("DB.Has(%v) failed: ", item.Triple, err)
				t.FailNow()
			}

			// Verify that triple can't be deleted agin
			if err := db.Delete(item.Triple); err != ErrNotFound {
				t.Logf("DB.Delete(%v) => %v; want ErrNotFound", item.Triple, err)
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

// Verify the encode/decode roundtrip of a triple.
func TestEncodeDecode_Quick(t *testing.T) {
	f := func(items testdata) bool {
		db := newTestDB()
		defer db.Close()

		sort.Sort(items)

		for _, item := range items {
			if err := db.Insert(item.Triple); err != nil {
				t.Logf("DB.Insert(%v) failed: %v", item.Triple, err)
				t.FailNow()
			}
		}

		result := make(testdata, 0, len(items))
		if err := db.forEach(func(tr rdf.Triple) error {
			var item testdataitem
			item.Triple = tr
			result = append(result, item)
			return nil
		}); err != nil {
			t.Logf("DB.forEach() failed: %v", err)
			t.FailNow()
		}

		sort.Sort(result)

		if len(items) != len(result) {
			t.Logf("inserted %d triples, got %d back from DB", len(items), len(result))
			t.FailNow()
		}
		for i, item := range items {
			if result[i].Triple != item.Triple {
				t.Logf("got %v; wanted %v ", result[i].Triple, item.Triple)
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
