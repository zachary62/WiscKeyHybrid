package stat

import (
	"log"
	"sync"
	"sync/atomic"
	"github.com/dgraph-io/badger/y"
	"io/ioutil"
  "os"
)

type segstat struct {
	heat	 int32 //how likely the data will be range queried
	inLSM  bool	//whether it is in the LSM
	valid  bool //whether this segstat is valid
	start 	 []byte
	end 	 	 []byte
}

type SegTable struct {
	sync.RWMutex
	length    uint32
	curr			uint32
	boilingp	int32	//when data moves to LSM
  freezingp int32 //when data moves to vlog
	segarray 	[]segstat
}

//avoid overflow
//for now 250 max
func (seg *segstat) addheat(delta int32) int32 {
	n := atomic.AddInt32(&seg.heat, delta)
	if n > 250 {
		n = atomic.AddInt32(&seg.heat, -delta)
	}
	return n
}

func NewSegTable(len uint32, bp int32, fp int32) *SegTable {
	y.AssertTruef(bp>=fp, "Boiling point must be higher than or equal to freezing point")
	return &SegTable{
		segarray:  make([]segstat, len),
		length:		 len,
		boilingp:	 bp,
	  freezingp: fp,
		curr:			 0,
	}
}

//storeSeg returns whether segment is above boilingp if heat is positive only after change and segment is not in LSM
//or whether segment is below freezingp if heat is negative only after change and segment is in LSM
//Segment is coalesced if it is included by a segment above boiling point, or is above boiling point and overlaps with a segment above boiling point
//TODO: There are some problems about overlap coalesce, because it is half "in the LSM/vlog"
//What should we do if a segment goes below freezing point from boiling point
//a segment with heat 0 should set valid to be false
func (segt *SegTable) StoreSeg(start []byte, end []byte, heat int32) bool {
	y.AssertTruef(y.ComparePureKeys(start,end) <= 0, "start must be higher than or equal to end")
	//store the coldest heat and index
	//if table is full, expand table!
	//never replace any existed segments now
	sheat := segt.boilingp
	index := -1
	segt.Lock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid == false && index == -1{
			sheat = 0
			index = int(i)
		//inclusive coalesce
		} else if segt.curr <= segt.length {
				if identical(start,end,segt.segarray[i].start,segt.segarray[i].end) ||
			  (segt.segarray[i].heat >= segt.boilingp && include(segt.segarray[i].start,segt.segarray[i].end,start,end)){
					haft := segt.segarray[i].addheat(heat)
					segt.Unlock()
					y.AssertTruef(heat!=0, "Old Segment must have a non-zero heat")
					if segt.segarray[i].heat >= segt.boilingp{
						segt.OverlapCoalesce()
					}
					return (heat > 0 && haft >= segt.boilingp && haft - heat < segt.boilingp && !segt.segarray[i].inLSM)||
					(heat < 0 && haft <= segt.freezingp && haft - heat > segt.freezingp && segt.segarray[i].inLSM)
		  	//overlap coalesce
			} else if heat >= segt.boilingp && segt.segarray[i].heat >= segt.boilingp && overlap(start,end,segt.segarray[i].start,segt.segarray[i].end){
					segt.segarray[i].addheat(heat)
					if y.ComparePureKeys(start,segt.segarray[i].start) < 0 {
						segt.segarray[i].start = start
					}
					if y.ComparePureKeys(end,segt.segarray[i].end) > 0 {
						segt.segarray[i].end = end
					}
					segt.Unlock()
					segt.OverlapCoalesce()
					y.AssertTruef(heat!=0, "Old Segment must have a non-zero heat")
					//always return true if it is doing an Overlap Coalesce
					return true
			}
		} else if segt.curr > segt.length {
				if identical(start,end,segt.segarray[i].start,segt.segarray[i].end) ||
				 	 include(segt.segarray[i].start,segt.segarray[i].end,start,end){
					haft := segt.segarray[i].addheat(heat)
					segt.Unlock()
					y.AssertTruef(heat!=0, "Old Segment must have a non-zero heat")
					if segt.segarray[i].heat >= segt.boilingp{
						segt.OverlapCoalesce()
					}
					return (heat > 0 && haft >= segt.boilingp && haft - heat < segt.boilingp && !segt.segarray[i].inLSM)||
					(heat < 0 && haft <= segt.freezingp && haft - heat > segt.freezingp && segt.segarray[i].inLSM)
				//overlap coalesce
			} else if include(start,end,segt.segarray[i].start,segt.segarray[i].end) ||
			 		overlap(start,end,segt.segarray[i].start,segt.segarray[i].end){
					segt.segarray[i].addheat(heat)
					if y.ComparePureKeys(start,segt.segarray[i].start) < 0 {
						segt.segarray[i].start = start
					}
					if y.ComparePureKeys(end,segt.segarray[i].end) > 0 {
						segt.segarray[i].end = end
					}
					segt.Unlock()
					segt.OverlapCoalesce()
					y.AssertTruef(heat!=0, "Old Segment must have a non-zero heat")
					//always return true if it is doing an Overlap Coalesce
					return true
			}
		}
	}
	//table is full
	//replace the invalid segment
	if sheat < heat && index != -1{
		segt.segarray[index].valid = true
		segt.segarray[index].inLSM = false
		segt.segarray[index].start = start
		segt.segarray[index].end = end
		segt.segarray[index].heat = heat
		segt.curr ++
		segt.Unlock()
		y.AssertTruef(heat>=0, "New Segment must have a positive or zero heat")
		return heat >= segt.boilingp
	}
	ss := segstat{valid:true,heat:heat,inLSM:false,start:start,end:end}
	segt.segarray = append(segt.segarray,ss)
	segt.curr ++
	segt.Unlock()
	return heat >= segt.boilingp
}

//FindSeg returns the hottest segment includes start with its heat, end and inLSM
//For a range, don't call FindSeg for every key. Call keys after end.
func (segt *SegTable) FindSeg(start []byte) (int32, []byte, bool, bool){
	var heat int32
	var end []byte
	var inLSM bool
	find := false
	//find the hottest seg which includes start
	segt.RLock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid == true && y.ComparePureKeys(start,segt.segarray[i].start) >=0 &&
		y.ComparePureKeys(start,segt.segarray[i].end) <=0 && heat < segt.segarray[i].heat {
			heat = segt.segarray[i].heat
			inLSM = segt.segarray[i].inLSM
			end = segt.segarray[i].end
			find = true
		}
	}
	segt.RUnlock()
	return heat, end, inLSM, find
}

func (segt *SegTable) ChngeSegInLSM(start []byte, end []byte, inLSM bool) {
	segt.Lock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid == true && y.ComparePureKeys(start,segt.segarray[i].start) >=0 &&
		y.ComparePureKeys(end,segt.segarray[i].end) <=0{
			segt.segarray[i].inLSM = inLSM
		}
	}
	segt.Unlock()
}

//Cooling multiply cool to each heat
//and return an array of start/end which is below freezingp only after change and in LSM tree
func (segt *SegTable) Cooling(cool float32) [][]byte {
	y.AssertTruef(cool > 0 && cool < 1, "Cool should be between 0 and 1 exclusively")
	segt.Lock()
	var freeze [][]byte
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid == true {
			oldheat := segt.segarray[i].heat
			segt.segarray[i].heat = int32(float32(segt.segarray[i].heat) * cool)
			if oldheat >= segt.freezingp && segt.segarray[i].heat <= segt.freezingp && segt.segarray[i].inLSM{
				freeze = append(freeze,segt.segarray[i].start)
				freeze = append(freeze,segt.segarray[i].end)
			}
		}
	}
	segt.Unlock()
	return freeze
}

//returns whether two segments overlap (inclusive also overlap)
func overlap(start1 []byte, end1 []byte, start2 []byte, end2 []byte) bool{
	y.AssertTruef(y.ComparePureKeys(start1,end1) <= 0 && y.ComparePureKeys(start2,end2) <= 0, "start must be higher than or equal to end")
	return !(y.ComparePureKeys(end1,start2) < 0 || y.ComparePureKeys(end2,start1) < 0)
}

//returns whether two segments identical
func identical(start1 []byte, end1 []byte, start2 []byte, end2 []byte) bool{
	y.AssertTruef(y.ComparePureKeys(start1,end1) <= 0 && y.ComparePureKeys(start2,end2) <= 0, "start must be higher than or equal to end")
	return (y.ComparePureKeys(end1,end2) == 0 && y.ComparePureKeys(start1,start2) == 0)
}


//returns whether the first segment includes the second
func include(start1 []byte, end1 []byte, start2 []byte, end2 []byte) bool{
	y.AssertTruef(y.ComparePureKeys(start1,end1) <= 0 && y.ComparePureKeys(start2,end2) <= 0, "start must be higher than or equal to end")
	return (y.ComparePureKeys(start2,start1) >= 0 && y.ComparePureKeys(end2,end1) <= 0)
}
//for debug only
func (segt *SegTable) PrintAll() {
	segt.RLock()
	log.Println(segt.curr)
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid{
		  //log.Println(segt.segarray[i].inLSM)
			log.Println(segt.segarray[i].start)
			log.Println(segt.segarray[i].end)
			log.Println(segt.segarray[i].heat)

		}
	}
	segt.RUnlock()
}

//Coalesce overlapping Segments which are above boiling point
func (segt *SegTable) OverlapCoalesce() {
	segt.Lock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid == true && segt.segarray[i].heat >= segt.boilingp {
			segt.overlapCoalesceStart(i)
		}
	}
	segt.Unlock()
}

//recursive function call
//Lock before call
func (segt *SegTable) overlapCoalesceStart(index uint32) {
	for j := index + 1; j < uint32(len(segt.segarray)); j++ {
		if segt.segarray[j].valid && segt.segarray[j].heat >= segt.boilingp &&
		overlap(segt.segarray[index].start,segt.segarray[index].end,segt.segarray[j].start,segt.segarray[j].end){
			segt.segarray[index].addheat(segt.segarray[j].heat)
			segt.segarray[j].valid = false
			segt.curr --
			if y.ComparePureKeys(segt.segarray[j].start,segt.segarray[index].start) < 0 {
				segt.segarray[index].start = segt.segarray[j].start
			}
			if y.ComparePureKeys(segt.segarray[j].end,segt.segarray[index].end) > 0 {
				segt.segarray[index].end = segt.segarray[j].end
			}
			segt.overlapCoalesceStart(index)
			break;
		}
	}
}

//for every valid seg
//first byte heat, second byte inLSM
//start and end always begins with their lengths
func (segt *SegTable) SaveTable(dir string) {
	f, err := os.Create(dir)
	y.Check(err)
	defer f.Close()

	segt.RLock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid{
			var k byte
			if segt.segarray[i].inLSM == true {
				k = 1
			} else if segt.segarray[i].inLSM == false{
					k = 0
			}
			f.Write([]byte{byte(segt.segarray[i].heat),k,byte(len(segt.segarray[i].start))})
			f.Write(segt.segarray[i].start)
			f.Write([]byte{byte(len(segt.segarray[i].end))})
			f.Write(segt.segarray[i].end)
		}
	}
	segt.RUnlock()
	f.Sync()
}

//temporal seg file
//TODO: a lot to consider:
//make it more lightweight like other files in badger
//let manager detect whether file exists
//deal with error in a mild way
//make heat larger, current 250 max because of byte restriction
//how to process when heat larger than 250?
//it will discard extra data
//how to process when length is 0?
func (segt *SegTable) LoadTable(dir string) {
	segt.Lock()
	b, err := ioutil.ReadFile(dir) // just pass the file name
	y.Check(err)
	cur := 0
	for i := uint32(0); len(b) != cur; i++ {
		segt.segarray[i].heat = int32(b[cur])
		y.AssertTruef(b[cur+1] == 1 || b[cur+1] == 0, "error inLSM while loading")
		if b[cur+1] == 1{
			segt.segarray[i].inLSM = true
		} else if b[cur+1] == 0{
			segt.segarray[i].inLSM = false
		}
		len1 := int(b[cur+2])
		segt.segarray[i].start = b[cur+3:cur+3+len1]
		len2 := int(b[cur+3+len1])
		segt.segarray[i].end = b[cur+4+len1:cur+4+len1+len2]
		cur = cur+4+len1+len2
		segt.segarray[i].valid = true
	}
	segt.Unlock()
}

//for debug only
func (segt *SegTable) FlushAll() [2][][]byte{
	var s [2][][]byte
	segt.RLock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid{
			if segt.segarray[i].heat <= segt.freezingp && segt.segarray[i].inLSM == true {
				s[0] = append(s[0],segt.segarray[i].start,segt.segarray[i].end)
			}
			if segt.segarray[i].heat >= segt.boilingp && segt.segarray[i].inLSM == false {
				s[1] = append(s[1],segt.segarray[i].start,segt.segarray[i].end)
			}
		}
	}
	segt.RUnlock()
	return s
}
