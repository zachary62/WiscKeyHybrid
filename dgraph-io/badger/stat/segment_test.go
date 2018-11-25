package stat

import (
	"testing"
	"sync"
	"github.com/dgraph-io/badger/y"
	"io/ioutil"
	"os"
)

func TestAddheat(t *testing.T) {
	t.Log("Testing adding heat to segstat with 1000... (expected heat: 1000)")
	ss := &segstat{heat: 0, inLSM: false}
	if heat := ss.addheat(250); heat != 250 {
		t.Errorf("Expected heat of 1000, but it was %d instead.", heat)
	}
}

func TestAddheatMulti(t *testing.T) {
	t.Log("Testing adding heat to segstat with 1000 threads... (expected heat: 1000)")
	ss := &segstat{heat: 0, inLSM: false}
	var wg sync.WaitGroup
	wg.Add(250)
	for i := 0; i < 250; i++ {
	    go func() {
	        defer wg.Done()
	        ss.addheat(1)
	    }()
	}
	wg.Wait()
	if heat := ss.addheat(1); heat != 250 {
		t.Errorf("Expected heat of 1000, but it was %d instead.", heat)
	}
}

func TestAddSeg(t *testing.T) {
	t.Log("Testing adding one Segment to SegTable with 123 heat... ")
	segt := NewSegTable(10,10,3)
	segt.StoreSeg([]byte{1,1}, []byte{1,3}, 123)
	if heat,end,_,_ := segt.FindSeg([]byte{1,2}); heat != 123 || y.ComparePureKeys(end,[]byte{1,3}) != 0{
		t.Errorf("Expected end at {1,3}, heat of 123, but the heat was %d and ends is %v instead.", heat, end)
	}
}

func TestAddSegSmall(t *testing.T) {
	t.Log("Testing adding ten Segments to SegTable ... ")
	segt := NewSegTable(10,10,3)

	for i := byte(1); i < 11; i++ {
		segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
	}

	for i := byte(1); i < 11; i++ {
		if heat,end,_,_ := segt.FindSeg([]byte{10*i}); heat != int32(i) || y.ComparePureKeys(end,[]byte{10*(i+1)-1}) != 0{
			t.Errorf("Expected heat of %d, end at {1,3}, but the heat was %d and ends is %v instead.",int(i), heat, end)
		}
	}
}

func TestAddSegMulti(t *testing.T) {
	t.Log("Testing adding Segments to SegTable with 10 threads ... ")
	segt := NewSegTable(10,10,3)

	var wg sync.WaitGroup
	wg.Add(10)
	for i := byte(1); i < 11; i++ {
			go func(i byte) {
					defer wg.Done()
					segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
			}(i)
	}
	wg.Wait()
	//segt.PrintAll()
	wg.Add(10)
	for i := byte(1); i < 11; i++ {
			go func(i byte) {
					defer wg.Done()
					if heat,end,_,_ := segt.FindSeg([]byte{10*i}); heat != int32(i) || y.ComparePureKeys(end,[]byte{10*(i+1)-1}) != 0{
						t.Errorf("Expected heat of %d, end at %v, but the heat was %d and ends is %v instead.",int32(i),[]byte{10*(i+1)-1},heat,end)
					}
			}(i)
	}
	wg.Wait()
}

func TestAddSegSame(t *testing.T) {
	t.Log("Testing adding ten same Segments to SegTable ... ")
	segt := NewSegTable(10,10,3)

	for i := 0; i < 10; i++ {
		segt.StoreSeg([]byte{10}, []byte{20}, 1)
	}

	if heat,end,_,_ := segt.FindSeg([]byte{10}); heat != 10 || y.ComparePureKeys(end,[]byte{20}) != 0{
		t.Errorf("Expected heat of 10, end at {20}, but the heat was %d and ends is %v instead.", heat, end)
	}
}

func TestAddSegSameMulti(t *testing.T) {
	t.Log("Testing adding ten same Segments to SegTable with ten threads ... ")
	segt := NewSegTable(10,10,3)

	var wg sync.WaitGroup
	wg.Add(10)
	for i := byte(1); i < 11; i++ {
			go func(i byte) {
					defer wg.Done()
					segt.StoreSeg([]byte{10}, []byte{20}, 1)
			}(i)
	}
	wg.Wait()

	if heat,end,_,find := segt.FindSeg([]byte{10}); !find || heat != 10 || y.ComparePureKeys(end,[]byte{20}) != 0{
		t.Errorf("Expected heat of 10, end at {20}, but the heat was %d and ends is %v instead.", heat, end)
	}
}

func TestChangeLSM(t *testing.T) {
	t.Log("Testing changing inLSM ... ")
	segt := NewSegTable(10,10,3)
	segt.StoreSeg([]byte{10}, []byte{20}, 1)
	segt.ChngeSegInLSM([]byte{10}, []byte{20},true)
	if _,_,inLSM,find := segt.FindSeg([]byte{10});!find || inLSM!= true{
		t.Errorf("Expected inLSM true, but inLSM is false instead.")
	}
	segt.ChngeSegInLSM([]byte{10}, []byte{20},false)
	if _,_,inLSM,find := segt.FindSeg([]byte{10});!find || inLSM!= false{
		t.Errorf("Expected inLSM false, but inLSM is true instead.")
	}
}

func TestStoreReturn(t *testing.T) {
	t.Log("Testing return value of StoreSeg ... ")
	segt := NewSegTable(10,10,5)
	if change := segt.StoreSeg([]byte{10}, []byte{20}, 7); change != false{
		t.Errorf("Change heat from 0 to 7 with boiling point 10, should return false.")
	}
	if change := segt.StoreSeg([]byte{10}, []byte{20}, 7); change != true{
		t.Errorf("Change heat from 7 to 14 with boiling point 10, should return true.")
	}
	if change := segt.StoreSeg([]byte{10}, []byte{20}, 7); change != false{
		t.Errorf("Change heat from 14 to 21 with boiling point 10, should return false.")
	}
	segt.ChngeSegInLSM([]byte{10}, []byte{20},true)
	if change := segt.StoreSeg([]byte{10}, []byte{20}, -7); change != false{
		t.Errorf("Change heat from 21 to 14 with freezing point 5, should return false.")
	}
	if change := segt.StoreSeg([]byte{10}, []byte{20}, -7); change != false{
		t.Errorf("Change heat from 14 to 7 with freezing point 5, should return false.")
	}
	if change := segt.StoreSeg([]byte{10}, []byte{20}, -7); change != true{
		t.Errorf("Change heat from 7 to 0 with freezing point 5, should return true.")
	}
	if change := segt.StoreSeg([]byte{10}, []byte{20}, -7); change != false{
		t.Errorf("Change heat from 0 to -7 with freezing point 5, should return false.")
	}
}

func TestCooling(t *testing.T) {
	t.Log("Testing cooling system of SegTable ... ")
	segt := NewSegTable(10,10,3)
	segt.StoreSeg([]byte{10}, []byte{19}, 4)
	segt.StoreSeg([]byte{20}, []byte{29}, 4)
	segt.StoreSeg([]byte{30}, []byte{39}, 4)
	segt.StoreSeg([]byte{40}, []byte{49}, 8)
	segt.StoreSeg([]byte{50}, []byte{59}, 8)
	segt.ChngeSegInLSM([]byte{10}, []byte{19}, true)
	segt.ChngeSegInLSM([]byte{20}, []byte{29}, true)
	segt.ChngeSegInLSM([]byte{40}, []byte{49}, true)
	segt.ChngeSegInLSM([]byte{50}, []byte{59}, true)
	slice1 := segt.Cooling(0.5)
	if len(slice1) != 4{
		t.Errorf("Expected 4 byte array returned after cooling, but it returns %d byte arrays instead.", len(slice1))
	}
	if y.ComparePureKeys(slice1[0],[]byte{10}) != 0 || y.ComparePureKeys(slice1[1],[]byte{19}) != 0 ||
		 y.ComparePureKeys(slice1[2],[]byte{20}) != 0 || y.ComparePureKeys(slice1[3],[]byte{29}) != 0 {
			 t.Errorf("Wrong return byte slices from Cooling: %v.",slice1)
	}
	slice2 := segt.Cooling(0.5)
	if len(slice2) != 4{
		t.Errorf("Expected 4 byte array returned after cooling, but it returns %d byte arrays instead.", len(slice2))
	}
	if y.ComparePureKeys(slice2[0],[]byte{40}) != 0 || y.ComparePureKeys(slice2[1],[]byte{49}) != 0 ||
		 y.ComparePureKeys(slice2[2],[]byte{50}) != 0 || y.ComparePureKeys(slice2[3],[]byte{59}) != 0 {
			 t.Errorf("Wrong return byte slices from Cooling: %v.",slice2)
	}
	slice3 := segt.Cooling(0.5)
	if len(slice3) != 0{
		t.Errorf("Expected 0 byte array returned after cooling, but it returns %d byte arrays instead.", len(slice3))
	}
}

func TestReplacement(t *testing.T) {
	t.Log("Testing coldest replacement in StoreSeg ... ")
	segt := NewSegTable(10,2,2)
	for i := byte(1); i < 11; i++ {
		segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
	}
	segt.StoreSeg([]byte{10,10}, []byte{20,20}, 4)
	//They can be found now because we always include segments
	// if _,_,_,find := segt.FindSeg([]byte{10}); find{
	// 	t.Errorf("Expected be replaced, but is found.")
	// }
	if heat,end,_,find := segt.FindSeg([]byte{10,10}); !find || heat != 4 || y.ComparePureKeys(end,[]byte{20,20}) != 0{
		t.Errorf("Expected find seg with heat of 4, end at {20,20}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
	}
	segt.StoreSeg([]byte{30,30}, []byte{40,40}, 4)
	// if _,_,_,find := segt.FindSeg([]byte{30,30}); find{
	// 	t.Errorf("Expected be not found because all is above boiling point, but is found.")
	// }
}

func TestInclusiveCoalesce(t *testing.T) {
	t.Log("Testing inclusive coalesce in StoreSeg ... ")
	segt := NewSegTable(10,2,2)
	for i := byte(1); i < 11; i++ {
		segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
	}
	segt.StoreSeg([]byte{35}, []byte{38}, 4)
	if heat,end,_,find := segt.FindSeg([]byte{35}); !find || heat != 7 || y.ComparePureKeys(end,[]byte{39}) != 0{
		t.Errorf("Expected find seg with heat of 7, end at {39}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
	}
	segt.StoreSeg([]byte{15}, []byte{18}, 1)
	if heat,_,_,_ := segt.FindSeg([]byte{15}); heat!= 1 {
		t.Errorf("Expected be not coalesced because table is full and segment that includes it is not above boiling point.")
	}
}

func TestOverlap(t *testing.T) {
	t.Log("Testing Overlap ... ")
	if overlap([]byte{1},[]byte{2},[]byte{3},[]byte{4}) == true{
		t.Errorf("Overlap error for [1,2] [3,4].")
	}
	if overlap([]byte{3},[]byte{4},[]byte{1},[]byte{2}) == true{
		t.Errorf("Overlap error for [1,2] [3,4].")
	}
	if overlap([]byte{1},[]byte{3},[]byte{2},[]byte{4}) == false{
		t.Errorf("Overlap error for [1,3] [2,4].")
	}
	if overlap([]byte{2},[]byte{4},[]byte{1},[]byte{3}) == false{
		t.Errorf("Overlap error for [1,3] [2,4].")
	}
	if overlap([]byte{1},[]byte{2},[]byte{1},[]byte{2}) == false{
		t.Errorf("Overlap error for [1,2] [1,2].")
	}
	if overlap([]byte{1},[]byte{2},[]byte{2},[]byte{3}) == false{
		t.Errorf("Overlap error for [1,2] [2,3].")
	}
	if overlap([]byte{2},[]byte{3},[]byte{1},[]byte{2}) == false{
		t.Errorf("Overlap error for [1,2] [2,3].")
	}
}

func TestOverlapCoalesce1(t *testing.T) {
	t.Log("Testing Overlap Coalesce 1 in StoreSeg ... ")
	segt := NewSegTable(10,2,2)
	for i := byte(1); i < 11; i++ {
		segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
	}
	segt.StoreSeg([]byte{15}, []byte{55,55}, 4)

	if heat,end,_,find := segt.FindSeg([]byte{15}); !find || heat != 58 || y.ComparePureKeys(end,[]byte{55,55}) != 0{
		t.Errorf("Expected find seg with heat of 58, end at {55,55}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
	}

	for i := byte(1); i < 10; i++ {
		segt.StoreSeg([]byte{10*i,0,0}, []byte{10*(i+1)-1,0,0}, 2)
	}

	for i := byte(1); i < 10; i++ {
		if heat,end,_,_ := segt.FindSeg([]byte{10*i,0,0}); heat != 2 || y.ComparePureKeys(end,[]byte{10*(i+1)-1,0,0}) != 0{
			t.Errorf("Expected heat of 2, end at %v, but the heat was %d and ends is %v instead.",[]byte{10*(i+1)-1,0,0},heat,end)
		}
	}
	if _,_,_,find := segt.FindSeg([]byte{10*10,0,0}); find{
		t.Errorf("Should not be found because Seg Table is full")
	}
}

func TestOverlapCoalesce2(t *testing.T) {
	t.Log("Testing Overlap Coalesce 2 in StoreSeg ... ")
	segt := NewSegTable(10,2,2)
	for i := byte(10); i > 0; i-- {
		segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
	}
	segt.StoreSeg([]byte{15}, []byte{55,55}, 4)

	if heat,end,_,find := segt.FindSeg([]byte{15}); !find || heat != 58 || y.ComparePureKeys(end,[]byte{55,55}) != 0{
		t.Errorf("Expected find seg with heat of 58, end at {55,55}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
	}

	for i := byte(1); i < 10; i++ {
		segt.StoreSeg([]byte{10*i,0,0}, []byte{10*(i+1)-1,0,0}, 2)
	}

	for i := byte(1); i < 10; i++ {
		if heat,end,_,_ := segt.FindSeg([]byte{10*i,0,0}); heat != 2 || y.ComparePureKeys(end,[]byte{10*(i+1)-1,0,0}) != 0{
			t.Errorf("Expected heat of 2, end at %v, but the heat was %d and ends is %v instead.",[]byte{10*(i+1)-1,0,0}, heat, end)
		}
	}
	if _,_,_,find := segt.FindSeg([]byte{10*10,0,0}); find{
		t.Errorf("Should not be found because Seg Table is full")
	}
}

func TestOverlapCoalesce3(t *testing.T) {
	t.Log("Testing Overlap Coalesce 3 in StoreSeg ... ")
	segt := NewSegTable(10,2,2)
	for i := byte(9); i > 0; i-- {
		segt.StoreSeg([]byte{10*i}, []byte{10*(i+1)-1}, int32(i))
	}
	segt.StoreSeg([]byte{15}, []byte{55,55}, 1)
	segt.StoreSeg([]byte{15}, []byte{55,55}, 1)
	if heat,end,_,find := segt.FindSeg([]byte{15}); !find || heat != 46 || y.ComparePureKeys(end,[]byte{55,55}) != 0{
		t.Errorf("Expected find seg with heat of 48, end at {55,55}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
	}

	for i := byte(1); i < 10; i++ {
		segt.StoreSeg([]byte{10*i,0,0}, []byte{10*(i+1)-1,0,0}, 2)
	}

	for i := byte(1); i < 10; i++ {
		if heat,end,_,_ := segt.FindSeg([]byte{10*i,0,0}); heat != 2 || y.ComparePureKeys(end,[]byte{10*(i+1)-1,0,0}) != 0{
			t.Errorf("Expected heat of 2, end at %v, but the heat was %d and ends is %v instead.",[]byte{10*(i+1)-1,0,0}, heat, end)
		}
	}
	if _,_,_,find := segt.FindSeg([]byte{10*10,0,0}); find{
		t.Errorf("Should not be found because Seg Table is full")
	}
}

func TestWriteLoad(t *testing.T) {
	t.Log("Testing writing and loading table ... ")
	dir, err := ioutil.TempDir("", "badgerwl")
	defer os.RemoveAll(dir)
	if err != nil {
		t.Errorf("Unexpected error while using temporal directory %s.", err)
	}
	dir = dir + string(os.PathSeparator) + "table"
	ss1 := NewSegTable(10,0,0)
	ss1.segarray[0] = segstat{5,true,true,[]byte{5,2},[]byte{7,8}}
  ss1.segarray[1] = segstat{119,false,true,[]byte{9},[]byte{4,8,10}}
	ss1.segarray[2] = segstat{119,false,false,[]byte{9},[]byte{4,8,10}}
	ss1.SaveTable(dir)
	ss2 := NewSegTable(10,0,0)
	ss2.LoadTable(dir)
	for i := 0; i < 3; i++{
		if ss1.segarray[i].valid != ss2.segarray[i].valid{
			t.Errorf("The %d segments have different valid status in different tables, first table %t when second table %t.",i,ss1.segarray[i].valid,ss2.segarray[i].valid)
		}
		if !ss1.segarray[i].valid{
			continue
		}
		if y.ComparePureKeys(ss1.segarray[i].start,ss2.segarray[i].start) != 0{
			t.Errorf("The %d segments have different starts, the first is %v, the sond is %v.", i,ss1.segarray[i].start,ss2.segarray[i].start)
		}
		if y.ComparePureKeys(ss1.segarray[i].end,ss2.segarray[i].end) != 0{
			t.Errorf("The %d segments have different starts, the first is %v, the sond is %v.", i,ss1.segarray[i].end,ss2.segarray[i].end)
		}
	}

	if ss2.curr != 2{
		t.Errorf("Segments have different curr, the first is 2, the second is %d.", ss2.curr)
	}
}

func TestFlush(t *testing.T) {
	t.Log("Testing flushing of SegTable ... ")
	segt := NewSegTable(10,10,3)
	segt.StoreSeg([]byte{10}, []byte{19}, 2)
	segt.StoreSeg([]byte{20}, []byte{29}, 2)
	segt.StoreSeg([]byte{30}, []byte{39}, 4)
	segt.StoreSeg([]byte{40}, []byte{49}, 4)
	segt.StoreSeg([]byte{50}, []byte{59}, 12)
	segt.StoreSeg([]byte{60}, []byte{69}, 12)
	segt.ChngeSegInLSM([]byte{10}, []byte{19}, true)
	segt.ChngeSegInLSM([]byte{20}, []byte{29}, false)
	segt.ChngeSegInLSM([]byte{30}, []byte{39}, true)
	segt.ChngeSegInLSM([]byte{40}, []byte{49}, false)
	segt.ChngeSegInLSM([]byte{50}, []byte{59}, true)
	segt.ChngeSegInLSM([]byte{60}, []byte{69}, false)
	s := segt.FlushAll()
	if len(s[0]) != 2 {
		t.Errorf("The flush segment 0 should have 2 length, but %d instead.", len(s[0]))
	}
	if y.ComparePureKeys(s[0][0],[]byte{10}) != 0 || y.ComparePureKeys(s[0][1],[]byte{19}) != 0{
		t.Errorf("The flush segment should be [10, 19], but %v, %v instead.",s[0][0],s[0][1])
	}
	if len(s[1]) != 2 {
		t.Errorf("The flush segment 1 should have 2 length, but %d instead.", len(s[1]))
	}
	if y.ComparePureKeys(s[1][0],[]byte{60}) != 0 || y.ComparePureKeys(s[1][1],[]byte{69}) != 0{
		t.Errorf("The flush segment should be [60, 69], but %v, %v instead.",s[1][0],s[1][1])
	}
}

// Test curr denotes right number of segments in the table
func TestCurr(t *testing.T) {
	t.Log("Testing writing and loading table ... ")
	dir, err := ioutil.TempDir("", "badgerwl")
	defer os.RemoveAll(dir)
	if err != nil {
		t.Errorf("Unexpected error while using temporal directory %s.", err)
	}
	dir = dir + string(os.PathSeparator) + "table"
	ss1 := NewSegTable(10,10,5)
	ss1.StoreSeg([]byte{50}, []byte{69}, 5)
	ss1.StoreSeg([]byte{10}, []byte{29}, 15)
	ss1.Cooling(0.1)
	ss1.StoreSeg([]byte{5}, []byte{29}, 10)
	ss1.StoreSeg([]byte{30}, []byte{35}, 5)
	ss1.Cooling(0.5)
	ss1.StoreSeg([]byte{10}, []byte{29}, 5)
	ss1.StoreSeg([]byte{5}, []byte{29}, 10)
	ss1.StoreSeg([]byte{30}, []byte{35}, 5)
	ss1.Cooling(0.8)
	ss1.SaveTable(dir)
	ss2 := NewSegTable(10,10,5)
	ss2.LoadTable(dir)

	if ss1.curr != ss2.curr{
		t.Errorf("Segments have different curr, the first is %d, the second is %d.",ss1.curr, ss2.curr)
	}
}

// Test Coalesce when Table size is larger than target
func TestCoalesce1(t *testing.T) {
		t.Log("Testing Coalesce when Table size is larger than target ... ")
		segt := NewSegTable(1,10,3)
		segt.StoreSeg([]byte{5}, []byte{10}, 2)
	  segt.StoreSeg([]byte{15}, []byte{40}, 2)
	  segt.StoreSeg([]byte{8}, []byte{30}, 2)
		// should coalesce because 5,10 is smaller than 15,40
		if heat,end,_,find := segt.FindSeg([]byte{5}); !find || heat != 2 || y.ComparePureKeys(end,[]byte{30}) != 0{
			t.Errorf("Expected find seg with heat of 2, end at {30}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}

// Test Coalesce when Table size is larger than target
// This should not pass because current implementation compares both ends of key
// should find a way to compare length
// func TestCoalesce2(t *testing.T) {
// 		t.Log("Testing Coalesce when Table size is larger than target ... ")
// 		segt := NewSegTable(1,10,3)
// 		segt.StoreSeg([]byte{15}, []byte{40}, 2)
// 		segt.StoreSeg([]byte{5}, []byte{10}, 2)
// 	  segt.StoreSeg([]byte{8}, []byte{30}, 2)
// 		// should coalesce because 5,10 is smaller than 15,40
// 		if heat,end,_,find := segt.FindSeg([]byte{5}); !find || heat != 2 || y.ComparePureKeys(end,[]byte{30}) != 0{
// 			t.Errorf("Expected find seg with heat of 2, end at {30}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
// 		}
// }

// Test Coalesce when Table size is larger than target
func TestCoalesce3(t *testing.T) {
		t.Log("Testing Coalesce when Table size is larger than target ... ")
		segt := NewSegTable(1,10,3)
		segt.StoreSeg([]byte{5}, []byte{30}, 2)
	  segt.StoreSeg([]byte{15}, []byte{25}, 2)
	  segt.StoreSeg([]byte{5}, []byte{40}, 2)
		// should coalesce because 5,10 is smaller than 15,40
		if heat,end,_,find := segt.FindSeg([]byte{5}); !find || heat != 2 || y.ComparePureKeys(end,[]byte{40}) != 0{
			t.Errorf("Expected find seg with heat of 2, end at {40}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}

// Test Coalesce when Table size is larger than target
func TestCoalesce4(t *testing.T) {
		t.Log("Testing Coalesce when Table size is larger than target ... ")
		segt := NewSegTable(1,10,3)
	  segt.StoreSeg([]byte{15}, []byte{25}, 2)
		segt.StoreSeg([]byte{5}, []byte{30}, 2)
	  segt.StoreSeg([]byte{5}, []byte{40}, 2)
		// should coalesce because 5,10 is smaller than 15,40
		if heat,end,_,find := segt.FindSeg([]byte{5}); !find || heat != 2 || y.ComparePureKeys(end,[]byte{40}) != 0{
			t.Errorf("Expected find seg with heat of 2, end at {40}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}

// Test divide of hot segments
func TestDivide1 (t *testing.T) {
		t.Log("Testing divide of hot segments ... ")
		segt := NewSegTable(10,10,3)
	  segt.StoreSeg([]byte{0}, []byte{10}, 15)
	  segt.StoreSeg([]byte{2}, []byte{5}, 15)

		if heat,end,_,find := segt.FindSeg([]byte{1}); !find || heat != 15 || y.ComparePureKeys(end,[]byte{2}) != 0{
			t.Errorf("Expected find seg with heat of 15, end at {2}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
		if heat,end,_,find := segt.FindSeg([]byte{3}); !find || heat != 30 || y.ComparePureKeys(end,[]byte{5}) != 0{
			t.Errorf("Expected find seg with heat of 30, end at {5}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
		if heat,end,_,find := segt.FindSeg([]byte{6}); !find || heat != 15 || y.ComparePureKeys(end,[]byte{10}) != 0{
			t.Errorf("Expected find seg with heat of 15, end at {10}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}

// Test divide of hot segments
func TestDivide2 (t *testing.T) {
		t.Log("Testing divide of hot segments ... ")
		segt := NewSegTable(10,10,3)
	  segt.StoreSeg([]byte{0}, []byte{10}, 15)
	  segt.StoreSeg([]byte{2}, []byte{5}, 5)
		segt.StoreSeg([]byte{2}, []byte{5}, 10)

		if heat,end,_,find := segt.FindSeg([]byte{1}); !find || heat != 15 || y.ComparePureKeys(end,[]byte{2}) != 0{
			t.Errorf("Expected find seg with heat of 15, end at {2}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
		if heat,end,_,find := segt.FindSeg([]byte{3}); !find || heat != 30 || y.ComparePureKeys(end,[]byte{5}) != 0{
			t.Errorf("Expected find seg with heat of 30, end at {5}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
		if heat,end,_,find := segt.FindSeg([]byte{6}); !find || heat != 15 || y.ComparePureKeys(end,[]byte{10}) != 0{
			t.Errorf("Expected find seg with heat of 15, end at {10}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}

// Test expand of hot segments
func TestExpand1 (t *testing.T) {
		t.Log("Testing expand of hot segments ... ")
		segt := NewSegTable(10,10,3)
		segt.StoreSeg([]byte{6}, []byte{8}, 15)
		segt.StoreSeg([]byte{2}, []byte{5}, 15)
		segt.StoreSeg([]byte{1}, []byte{10}, 15)

		if heat,end,_,find := segt.FindSeg([]byte{1}); !find || heat != 45 || y.ComparePureKeys(end,[]byte{10}) != 0{
			t.Errorf("Expected find seg with heat of 45, end at {10}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}

// Test expand of hot segments
func TestExpand2 (t *testing.T) {
		t.Log("Testing expand of hot segments ... ")
		segt := NewSegTable(10,10,3)
		segt.StoreSeg([]byte{6}, []byte{8}, 15)
		segt.StoreSeg([]byte{2}, []byte{5}, 15)
		segt.StoreSeg([]byte{1}, []byte{10}, 5)
		segt.StoreSeg([]byte{1}, []byte{10}, 10)

		if heat,end,_,find := segt.FindSeg([]byte{1}); !find || heat != 45 || y.ComparePureKeys(end,[]byte{10}) != 0{
			t.Errorf("Expected find seg with heat of 45, end at {10}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
		}
}
