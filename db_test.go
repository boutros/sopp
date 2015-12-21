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

		// Remove any duplicates from the testdata
		sort.Sort(items)
		c := 0
		for i, item := range items {
			if i > 0 && item == items[i-1] {
				items[i] = items[c] // don't need to preserve order
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
				t.Logf("DB.Has(%v) => true after delete", item.Triple)
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

		// Verify that all Terms are now deleted (by removeOrphanedTerms)
		stats, err := db.Stats()
		if err != nil {
			t.Logf("DB.Stats() failed: %v", err)
			t.FailNow()
		}
		if stats.NumTerms != 0 {
			t.Logf("Terms in DB after deleting all triples: %d (removeOrhpanTerms fail)", stats.NumTerms)
			t.FailNow()
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

		// Remove any duplicates from the testdata
		sort.Sort(items)
		c := 0
		for i, item := range items {
			if i > 0 && item == items[i-1] {
				// we want to preserve order
				copy(items[i-1:], items[i:])
			}
		}
		items = items[:c]

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

		if len(items) != len(result) {
			t.Logf("inserted %d triples, got %d back from DB", len(items), len(result))
			t.FailNow()
		}

		sort.Sort(result)
		for i, item := range items {
			if item.Triple != result[i].Triple {
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

// Verify that Describe returns the same graph as rdf rdf.Graph reference implementation.
func TestDescribe_Quick(t *testing.T) {
	f := func(items testdata) bool {
		db := newTestDB()
		defer db.Close()

		// test against in-memory reference implementation
		ref := rdf.NewGraph()

		for _, item := range items {
			if err := db.Insert(item.Triple); err != nil {
				t.Logf("DB.Insert(%v) failed: %v", item.Triple, err)
				t.FailNow()
			}
			ref.Insert(item.Triple)
		}

		for _, item := range items {
			want := ref.Describe(item.Triple.Subj, false)

			got, err := db.Describe(item.Triple.Subj, false)
			if err != nil {
				t.Logf("DB.Describe(%v, false) failed: %v", err)
				t.FailNow()
			}

			if !got.Eq(want) {
				t.Logf("DB.Describe(%v, false) =>\n%s\nwant:\n%s",
					item.Triple.Subj, got.Serialize(rdf.Turtle), want.Serialize(rdf.Turtle))
				t.FailNow()
			}

			want = ref.Describe(item.Triple.Subj, true)
			got, err = db.Describe(item.Triple.Subj, true)
			if err != nil {
				t.Logf("DB.Describe(%v, true) failed: %v", err)
				t.FailNow()
			}

			if !got.Eq(want) {
				t.Logf("DB.Describe(%v, true) =>\n%s\nwant:\n%s",
					item.Triple.Subj, got.Serialize(rdf.Turtle), want.Serialize(rdf.Turtle))
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
