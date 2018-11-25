package stat

import (
	"log"
	"sync"
	"sync/atomic"
	"github.com/dgraph-io/badger/y"
	"io/ioutil"
  "os"
	"unsafe"
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
	length    uint32 //target length of SegTable
	curr			uint32 //actual valid segments in SegTable
	size			uint32 //size of segment array
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
//TODO: There are some problems about overlap coalesce, because it is half "in the LSM/vlog"
//What should we do if a segment goes below freezing point from boiling point
//a segment with heat 0 should set valid to be false
//Notice to use safe copy of slices before storing them
func (segt *SegTable) StoreSeg(start []byte, end []byte, heat int32) bool {
	y.AssertTruef(y.ComparePureKeys(start,end) <= 0, "start must be higher than or equal to end")
	//store the coldest heat and index
	//if table is full, expand table!
	//never replace any existed segments now
	sheat := segt.boilingp
	index := -1
	// start and end is used to denote the result of expand
	// this is to choose the smallest segment
	var tstart []byte
	var tend []byte
	// whether include
	// this is to tell from overlap
	incl := false
	segt.Lock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid == false && index == -1{
			sheat = 0
			index = int(i)
		//curr smaller than target
		} else if segt.curr <= segt.length {
				//identical to or included by an existing segment
				if identical(start,end,segt.segarray[i].start,segt.segarray[i].end) {
					haft := segt.segarray[i].addheat(heat)
					y.AssertTruef(heat!=0, "Old Segment must have a non-zero heat")
					if haft >= segt.boilingp && haft - heat < segt.boilingp {
						segt.DivideOrCoalesce(i)
					}
					segt.Unlock()
					return (heat > 0 && haft >= segt.boilingp && haft - heat < segt.boilingp && !segt.segarray[i].inLSM)||
					(heat < 0 && haft <= segt.freezingp && haft - heat > segt.freezingp && segt.segarray[i].inLSM)
		  	//overlap coalesce
			}
		//curr larget than target
		} else if segt.curr > segt.length {
				if identical(start,end,segt.segarray[i].start,segt.segarray[i].end) ||
				 	 (include(segt.segarray[i].start,segt.segarray[i].end,start,end) && segt.segarray[i].heat >= segt.boilingp){
					haft := segt.segarray[i].addheat(heat)
					y.AssertTruef(heat!=0, "Old Segment must have a non-zero heat")
					if haft >= segt.boilingp && haft - heat < segt.boilingp {
						segt.DivideOrCoalesce(i)
					}
					segt.Unlock()
					return (heat > 0 && haft >= segt.boilingp && haft - heat < segt.boilingp && !segt.segarray[i].inLSM)||
					(heat < 0 && haft <= segt.freezingp && haft - heat > segt.freezingp && segt.segarray[i].inLSM)
				//overlap coalesce
			} else if include(start,end,segt.segarray[i].start,segt.segarray[i].end) && segt.segarray[i].heat <= segt.boilingp && heat <= segt.boilingp {
					if !incl || (y.ComparePureKeys(segt.segarray[i].start,segt.segarray[index].start) <= 0 && y.ComparePureKeys(segt.segarray[i].end,segt.segarray[index].end)>= 0) {
						incl = true
						index = int(i)
						tstart = start
						tend = end
					}
			} else if overlap(start,end,segt.segarray[i].start,segt.segarray[i].end) && segt.segarray[i].heat <= segt.boilingp && heat <= segt.boilingp {
				if (len(tstart) == 0 && len(tend) == 0) || (y.ComparePureKeys(segt.segarray[i].start,tstart) >= 0 && y.ComparePureKeys(segt.segarray[i].end,tend)<= 0) {
					index = int(i)
					tstart = segt.segarray[i].start
					tend = segt.segarray[i].end
					if y.ComparePureKeys(start,segt.segarray[i].start) < 0 {
						tstart = start
					}
					if y.ComparePureKeys(end,segt.segarray[i].end) > 0 {
						tend = end
					}
				}
			}
		}
	}

	//combine old cold segment when size is above target
	if len(tstart) != 0 && len(tend)!=0 && index != -1 {
		segt.segarray[index].start = tstart
		segt.segarray[index].end = tend
		// if heat > segt.segarray[index].heat {
		// 	segt.segarray[index].heat = heat
		// }
		segt.segarray[index].addheat(heat)

		segt.Unlock()
		return false
	}

	// there is an invalid
	if sheat <= heat && index != -1{
		segt.segarray[index].valid = true
		segt.segarray[index].inLSM = false
		segt.segarray[index].start = start
		segt.segarray[index].end = end
		segt.segarray[index].heat = heat
		segt.curr ++
		y.AssertTruef(heat>=0, "New Segment must have a positive or zero heat")
		if heat >= segt.boilingp {
				segt.DivideOrCoalesce(uint32(index))
		}
		segt.Unlock()
		return heat >= segt.boilingp
	}
	// there is not invalid
	ss := segstat{valid:true,heat:heat,inLSM:false,start:start,end:end}
	segt.segarray = append(segt.segarray,ss)
	segt.curr ++
	if heat >= segt.boilingp {
			segt.DivideOrCoalesce(uint32(len(segt.segarray)-1))
	}
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
			if segt.segarray[i].heat == 0{
				segt.segarray[i].valid = false
				segt.curr --
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

// Divide Or Coalesce new hot segment
// index provides the position of new hot segment
// If there’s only one segment and the table size is smaller than target size, divide them.
// Otherwise, expand these two segments and sum up their heats.
// Notice: it doesn't lock the table, so call it when table has already been locked!
func (segt *SegTable) DivideOrCoalesce(index uint32) {
	// the index of segment table that may be divided
	// choose its initial value to be index because index should never be chosen as divided index
	var dindex uint32 = index
	// onlyone decide whether we should divide two segments
	// we will devide index and dindex if onlyone is still true in the end and dindex is not index (so that there is indeed an overlapping index)
	var onlyone bool = true
	// if the table size is larger than target size, don't divide to make table size smaller
	if segt.curr > segt.length {
		onlyone = false
	}
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		// skip if this segment is not hot or not valid or is the index
		if i == index || segt.segarray[i].valid == false || segt.segarray[i].heat < segt.boilingp {
			continue
		}
		if overlap(segt.segarray[index].start,segt.segarray[index].end,segt.segarray[i].start,segt.segarray[i].end){
			// the first one, mark and may divide
			if dindex == index && onlyone {
				dindex = i
				continue
			}
			onlyone = false
			// may need to expnd first one
			if dindex != index {
				segt.expand(index,dindex)
				dindex = index
			}
			segt.expand(index,i)
		}
	}
	if onlyone && dindex != index{
		segt.divide(index,dindex)
	}
}

// expand two segments in the segment table
// it will expand index 1 and invalidate index 2
// the start key will be the smaller of two segments, end be the larger
// heat will be the sum
// inLSM will be true only if two segments' inLSM are both true
// Notice: it doesn't lock the table, so call it when table has already been locked!
func (segt *SegTable) expand(index1 uint32,index2 uint32){
	segt.segarray[index2].valid = false
	segt.curr --
	if y.ComparePureKeys(segt.segarray[index1].start,segt.segarray[index2].start) >= 0 {
		segt.segarray[index1].start = segt.segarray[index2].start
	}
	if y.ComparePureKeys(segt.segarray[index1].end,segt.segarray[index2].end) <= 0 {
		segt.segarray[index1].end = segt.segarray[index2].end
	}
	segt.segarray[index1].addheat(segt.segarray[index2].heat)
	if segt.segarray[index2].inLSM == false {
		segt.segarray[index1].inLSM = false
	}
}

// divide two segments
// check two segments overlap before calling
// my implementation is to list all the situations because there are only two segments
// need to make it recursive if more
// Notice: it doesn't lock the table, so call it when table has already been locked!
func (segt *SegTable) divide(index1 uint32,index2 uint32){
	segt.curr ++
	if y.ComparePureKeys(segt.segarray[index1].start,segt.segarray[index2].start) <= 0 && y.ComparePureKeys(segt.segarray[index1].end,segt.segarray[index2].end) <= 0 {
		heat := segt.segarray[index1].heat + segt.segarray[index2].heat
		if heat > 250{
			heat = 250
		}
		inLSM := false
		if segt.segarray[index1].inLSM && segt.segarray[index2].inLSM{
			inLSM = true
		}
		segt.segarray = append(segt.segarray,segstat{valid:true,heat:heat,inLSM:inLSM,start:segt.segarray[index2].start,end:segt.segarray[index1].end})
		tmp := segt.segarray[index1].end
		segt.segarray[index1].end = segt.segarray[index2].start
		segt.segarray[index2].start = tmp
	} else if y.ComparePureKeys(segt.segarray[index1].start,segt.segarray[index2].start) <= 0 && y.ComparePureKeys(segt.segarray[index1].end,segt.segarray[index2].end) >= 0 {
		heat := segt.segarray[index1].heat + segt.segarray[index2].heat
		if heat > 250{
			heat = 250
		}
		inLSM := false
		if segt.segarray[index1].inLSM && segt.segarray[index2].inLSM{
			inLSM = true
		}
		segt.segarray = append(segt.segarray,segstat{valid:true,heat:heat,inLSM:inLSM,start:segt.segarray[index2].start,end:segt.segarray[index2].end})
		tmp := segt.segarray[index1].end
		segt.segarray[index1].end = segt.segarray[index2].start
		segt.segarray[index2].start = segt.segarray[index2].end
		segt.segarray[index2].end = tmp
		segt.segarray[index2].inLSM = segt.segarray[index1].inLSM
		segt.segarray[index2].heat = segt.segarray[index1].heat
	} else if y.ComparePureKeys(segt.segarray[index1].start,segt.segarray[index2].start) >= 0 && y.ComparePureKeys(segt.segarray[index1].end,segt.segarray[index2].end) >= 0 {
		heat := segt.segarray[index1].heat + segt.segarray[index2].heat
		if heat > 250{
			heat = 250
		}
		inLSM := false
		if segt.segarray[index1].inLSM && segt.segarray[index2].inLSM{
			inLSM = true
		}
		segt.segarray = append(segt.segarray,segstat{valid:true,heat:heat,inLSM:inLSM,start:segt.segarray[index1].start,end:segt.segarray[index2].end})
		tmp := segt.segarray[index2].end
		segt.segarray[index2].end = segt.segarray[index1].start
		segt.segarray[index1].start = tmp
	} else if y.ComparePureKeys(segt.segarray[index1].start,segt.segarray[index2].start) >= 0 && y.ComparePureKeys(segt.segarray[index1].end,segt.segarray[index2].end) <= 0 {
		heat := segt.segarray[index1].heat + segt.segarray[index2].heat
		if heat > 250{
			heat = 250
		}
		inLSM := false
		if segt.segarray[index1].inLSM && segt.segarray[index2].inLSM{
			inLSM = true
		}
		segt.segarray = append(segt.segarray,segstat{valid:true,heat:heat,inLSM:inLSM,start:segt.segarray[index1].start,end:segt.segarray[index1].end})
		tmp := segt.segarray[index2].end
		segt.segarray[index2].end = segt.segarray[index1].start
		segt.segarray[index1].start = segt.segarray[index1].end
		segt.segarray[index1].end = tmp
		segt.segarray[index1].inLSM = segt.segarray[index2].inLSM
		segt.segarray[index1].heat = segt.segarray[index2].heat
	}
}

// obsolete for now because we ensure that hot segments don't overlap
// Coalesce overlapping Segments which are above boiling point
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
	var i uint32
	for i = uint32(0); len(b) != cur; i++ {
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
	segt.curr = uint32(i)
	segt.Unlock()
}

// return value [0][][] stores keys needed to move from LSM to vLog
// [1][][] stores keys needed to move from vLog to LSM
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

// return the size of segarray by iterating through all segments and sum up
// for now, the size of an single array should be 56 + cap(start) + cap(end)
// we use cap instead of len because cap is the real length of memory allocated
// 56 is because 4(heat int32) + 1 (inLSM bool) + 1 (valid bool) + 2 (padding) + 24(start slice) + 24(end slice)
// slice is 24 because 8(pointer) + 8(int) + 8(end) on my 64 bit machine
// for a 32 bit machine slice size should be 12
// more details: https://blog.golang.org/go-slices-usage-and-internals
func (segt *SegTable) ArraySize() int{
	var size int
	segt.RLock()
	for i := uint32(0); i < uint32(len(segt.segarray)); i++ {
		if segt.segarray[i].valid{
			size += int(unsafe.Sizeof(segt.segarray[i])) + cap(segt.segarray[i].start) + cap(segt.segarray[i].end)
		}
	}
	segt.RUnlock()
	return size
}
