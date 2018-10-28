package badger

import (
	"log"
	"sync"
	"sync/atomic"
	"github.com/dgraph-io/badger/stat"
	"github.com/dgraph-io/badger/y"
	"os"
)

type segcache struct {
	sync.RWMutex
	start []byte
	end 	[]byte
	inLSM bool
	valid bool
}

//TODO let GC collect moved data
type manager struct {
	boilingp	int32	//when data moves to LSM
	freezingp int32 //when data moves to vlog
	coolingp	int32 //when to begin a cooling
	segc			*segcache //one cache only for now
	db        *DB
	segt 			*stat.SegTable
}

func newManager(database *DB, len uint32, bp int32, fp int32, cp int32) *manager {
	return &manager{
		segt:  		 stat.NewSegTable(len,bp,fp),
		boilingp:	 bp,
	  freezingp: fp,
		coolingp:	 cp,
		db:				 database,
		segc:			 &segcache{valid: false},
	}
}

//return whether to store in LSM
//notice that you should first test the value size first!!!
func (mgr *manager) StoreInLSM(key []byte) bool {
	//consults cache first
	mgr.segc.RLock()
	if mgr.segc.valid && y.ComparePureKeys(key,mgr.segc.start) >= 0 && y.ComparePureKeys(key,mgr.segc.end) <= 0 {
		mgr.segc.RUnlock()
		log.Println("in cache")
		return mgr.segc.inLSM
	}
	mgr.segc.RUnlock()
	heat,end,_,find := mgr.segt.FindSeg(key)
	//store data in Vlog in default
	if !find {
		return false
	}
	//update cache
	mgr.segc.Lock()
	defer mgr.segc.Unlock()
	mgr.segc.start = key
	mgr.segc.end = end
	mgr.segc.inLSM = (heat >= mgr.boilingp)
	return mgr.segc.inLSM
}

//add heat to a segment
//do a move if necessary
//Notice that iterator do prefetch, which will send a larger range
//Currently, I just keep the larger range because even if prefetch is unnecessary at the end
//It will promote performance during iterator and should not be turned off
func (mgr *manager) AddSeg(start []byte, end []byte, heat int32) {
	if y.ComparePureKeys(start,end) > 0 {
		start, end = end, start
	}
	move := mgr.segt.StoreSeg(start, end, heat)
	if move && heat > 0 {
		mgr.db.SendTomoveCh(start,end,true)
	} else if move && heat < 0 {
		mgr.db.SendTomoveCh(start,end,false)
	}
}

func (mgr *manager) SetInLSM(start []byte, end []byte, inLSM bool) {
	if y.ComparePureKeys(start,end) > 0 {
		start, end = end, start
	}
	mgr.segt.ChngeSegInLSM(start, end, inLSM)
}

func (mgr *manager) Cooling(cool float32) {
	slice := mgr.segt.Cooling(cool)
	for i := 0; i < len(slice); i = i + 2 {
		mgr.db.SendTomoveCh(slice[i],slice[i+1],false)
	}
}

//just for testing
//delete Test in iterator.go for normal use
func (mgr *manager) AddSegTest(start []byte, end []byte, heat int32) {
	log.Printf("start: %v, end: %v, heat: %d\n", start, end, heat)
}

func (mgr *manager) AddCool() {
	retry:
		coolingp := atomic.AddInt32(&mgr.coolingp, -1)
		if coolingp == 0 {
			atomic.AddInt32(&mgr.coolingp, 10)
			mgr.Cooling(0.1)
		} else if coolingp < 0{
			//a conflict occurs!
			//cool is deducted more than once and should retry
			//otherwise, may deducted for lots of times and won't become positive even after add 10
			atomic.AddInt32(&mgr.coolingp, 1)
			goto retry
		}
}

//for debug
func (mgr *manager) PrintAll() {
	mgr.segt.PrintAll()
}

func (mgr *manager) WriteSeg() {
	mgr.segt.SaveTable(mgr.db.opt.Dir + string(os.PathSeparator) + "Segtable.data")
}

func (mgr *manager) LoadSeg() {
	mgr.segt.LoadTable(mgr.db.opt.Dir + string(os.PathSeparator) + "Segtable.data")
}
