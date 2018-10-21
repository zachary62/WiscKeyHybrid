package badger

import (
	"testing"
	"io/ioutil"
	"time"
	"fmt"
)

func TestInitiate(t *testing.T) {
	t.Log("Testing creating a new manager. Should have no error...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal director %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	defer db.Close()
}

//Test Manually
//should print "move from d to f whether to LSM true"
func TestAddSeg1(t *testing.T) {
	t.Log("Testing adding segment...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal director %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	db.mgr.AddSeg([]byte{100},[]byte{102},4)
	defer db.Close()
}

//Test Manually
//should print "move from d to f whether to LSM true"
//should print "move from d to f whether to LSM false"
func TestAddSeg2(t *testing.T) {
	t.Log("Testing adding segments...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal director %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	db.mgr.AddSeg([]byte{100},[]byte{102},4)
	//need to wait for some time because move is a background process
	time.Sleep(1000 * time.Millisecond)
	db.mgr.AddSeg([]byte{100},[]byte{102},-4)
	defer db.Close()
}

//Test Manually
//should print "move from d to f whether to LSM true"
//should print "move from g to i whether to LSM true"
//should print "move from d to f whether to LSM false"
//should print "move from g to i whether to LSM false"
//may appear in different order
func TestCooling(t *testing.T) {
	t.Log("Testing cooling system...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal director %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	db.mgr.AddSeg([]byte{100},[]byte{102},4)
	db.mgr.AddSeg([]byte{103},[]byte{105},4)
	time.Sleep(1000 * time.Millisecond)
	db.mgr.Cooling(0.5)
	defer db.Close()
}

//Test Manually
//should print "move from e to g whether to LSM true"
func TestInterator1(t *testing.T) {
	t.Log("Testing receiving segment from iterator...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal directory %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	defer db.Close()

	// Start a writable transaction.
	txn := db.NewTransaction(true)
	txn.Set([]byte{101}, []byte{1})
	txn.Set([]byte{103}, []byte{3})
	// Commit the transaction and check for error.
	if err := txn.Commit(nil); err != nil {
		t.Errorf("Unexpected error while committing %s.", err)
	}

	// Start a non-writable transaction.
  txn = db.NewTransaction(false)
	itopts := DefaultIteratorOptions
  it := txn.NewIterator(itopts)

	for it.Rewind(); it.Valid(); it.Next() {
	}
	it.Close()
	txn.Discard()
}

//Test Manually
//should print "move from d to f whether to LSM true"
//should print "move from d to f whether to LSM false"
func TestRandomSearch(t *testing.T) {
	t.Log("Testing cooling system after random search...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal director %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	defer db.Close()
	db.mgr.AddSeg([]byte{100},[]byte{102},4)
	time.Sleep(1000 * time.Millisecond)
	// Start a non-writable transaction.
	txn := db.NewTransaction(false)
	for i := 0; i < 10; i++{
		txn.Get([]byte{0})
	}
	txn.Discard()
}

//Test Manually
//should print "move from d to f whether to LSM true"
//should print "move from d to f whether to LSM false"
func TestHybridSet(t *testing.T) {
	t.Log("Testing HybridSet...")
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		t.Errorf("Unexpected error while using temporal director %s.", err)
	}
	opts := DefaultOptions
  opts.Dir = dir
  opts.ValueDir = dir
  db, err := Open(opts)
	defer db.Close()
	db.mgr.AddSeg([]byte{100},[]byte{102},4)
	time.Sleep(1000 * time.Millisecond)
	// Start a writable transaction.
	txn := db.NewTransaction(true)
	txn.HybridSet([]byte{101}, []byte{3})
	txn.HybridSet([]byte{102}, []byte(fmt.Sprintf("%*d", 1024*64, 1)))
	txn.HybridSet([]byte{105}, []byte{5})
	if err := txn.Commit(nil); err != nil {
		t.Errorf("Unexpected error while committing %s.", err)
	}
	// Start a non-writable transaction.
	txn = db.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get([]byte{101})
	if err != nil {
		t.Errorf("Unexpected error while getting %s.", err)
	}
	if !item.IsInLSM(){
		t.Errorf("Should in LSM because segment is hot.")
	}
	item, err = txn.Get([]byte{102})
	if err != nil {
		t.Errorf("Unexpected error while getting %s.", err)
	}
	if item.IsInLSM(){
		t.Errorf("Should not in LSM because value size too large.")
	}
	item, err = txn.Get([]byte{105})
	if err != nil {
		t.Errorf("Unexpected error while getting %s.", err)
	}
	if !item.IsInLSM(){
		t.Errorf("Should not in LSM because segment is not hot.")
	}
}
