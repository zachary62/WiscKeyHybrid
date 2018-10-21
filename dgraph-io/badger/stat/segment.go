package stat

import (
	"log"
	"sync"
	"sync/atomic"
	"github.com/dgraph-io/badger/y"
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
	boilingp	int32	//when data moves to LSM
  freezingp int32 //when data moves to vlog
	segarray 	[]segstat
}

func (seg *segstat) addheat(delta int32) int32 {
	return atomic.AddInt32(&seg.heat, delta)
}

func NewSegTable(len uint32, bp int32, fp int32) *SegTable {
	y.AssertTruef(bp>=fp, "Boiling point must be higher than or equal to freezing point")
	return &SegTable{
		segarray:  make([]segstat, len),
		length:		 len,
		boilingp:	 bp,
	  freezingp: fp,
	}
}

//storeSeg returns whether segment is above boilingp if heat is positive only after change and segment is not in LSM
//or whether segment is below freezingp if heat is negative only after change and segment is in LSM
//Segment is coalesced if it is included by a segment above boiling point, or is above boiling point and overlaps with a segment above boiling point
//TODO: There are some problems about overlap coalesce, because it is half "in the LSM/vlog"
//What should we do if a segment goes below freezing point from boiling point
func (segt *SegTable) StoreSeg(start []byte, end []byte, heat int32) bool {
	y.AssertTruef(y.ComparePureKeys(start,end) <= 0, "start must be higher than or equal to end")
	//store the coldest heat and index
	//if table is full, use coldest replacement
	//Notice that it doesn't replace any segment above boiling point
	sheat := segt.boilingp
	index := -1
	segt.Lock()
	for i := uint32(0); i < segt.length; i++ {
		if segt.segarray[i].valid == false{
			segt.segarray[i].valid = true
			segt.segarray[i].inLSM = false
			segt.segarray[i].start = start
			segt.segarray[i].end = end
			segt.segarray[i].heat = heat
			y.AssertTruef(heat>=0, "New Segment must have a positive or zero heat")
			segt.Unlock()
			return heat >= segt.boilingp
		//inclusive coalesce
		} else if (y.ComparePureKeys(start,segt.segarray[i].start) == 0 && y.ComparePureKeys(end,segt.segarray[i].end) == 0) ||
		 (segt.segarray[i].heat >= segt.boilingp && y.ComparePureKeys(start,segt.segarray[i].start) >= 0 && y.ComparePureKeys(end,segt.segarray[i].end) <= 0){
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
		} else if sheat > segt.segarray[i].heat {
			sheat = segt.segarray[i].heat
			index = int(i)
		}
	}
	//table is full
	//replace the coldest one if qualify
	if sheat < heat && index >= 0{
		segt.segarray[index].inLSM = false
		segt.segarray[index].start = start
		segt.segarray[index].end = end
		segt.segarray[index].heat = heat
		segt.Unlock()
		y.AssertTruef(heat>=0, "New Segment must have a positive or zero heat")
		return heat >= segt.boilingp
	}
	segt.Unlock()
	return false
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
	for i := uint32(0); i < segt.length; i++ {
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
	for i := uint32(0); i < segt.length; i++ {
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
	for i := uint32(0); i < segt.length; i++ {
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

//returns whether two segments overlap
func overlap(start1 []byte, end1 []byte, start2 []byte, end2 []byte) bool{
	y.AssertTruef(y.ComparePureKeys(start1,end1) <= 0 && y.ComparePureKeys(start2,end2) <= 0, "start must be higher than or equal to end")
	return !(y.ComparePureKeys(end1,start2) < 0 || y.ComparePureKeys(end2,start1) < 0)
}

//for debug only
func (segt *SegTable) PrintAll() {
	segt.RLock()
	for i := uint32(0); i < segt.length; i++ {
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
	for i := uint32(0); i < segt.length; i++ {
		if segt.segarray[i].valid == true && segt.segarray[i].heat >= segt.boilingp {
			segt.overlapCoalesceStart(i)
		}
	}
	segt.Unlock()
}

//recursive function call
//Lock before call
func (segt *SegTable) overlapCoalesceStart(index uint32) {
	for j := index + 1; j < segt.length; j++ {
		if segt.segarray[j].valid && segt.segarray[j].heat >= segt.boilingp &&
		overlap(segt.segarray[index].start,segt.segarray[index].end,segt.segarray[j].start,segt.segarray[j].end){
			segt.segarray[index].addheat(segt.segarray[j].heat)
			segt.segarray[j].valid = false
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
