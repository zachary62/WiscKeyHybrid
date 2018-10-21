package stat

import (
	"testing"
	"sync"
	"github.com/dgraph-io/badger/y"
)

func TestAddheat(t *testing.T) {
	t.Log("Testing adding heat to segstat with 1000... (expected heat: 1000)")
	ss := &segstat{heat: 0, inLSM: false}
	if heat := ss.addheat(1000); heat != 1000 {
		t.Errorf("Expected heat of 1000, but it was %d instead.", heat)
	}
}

func TestAddheatMulti(t *testing.T) {
	t.Log("Testing adding heat to segstat with 1000 threads... (expected heat: 1000)")
	ss := &segstat{heat: 0, inLSM: false}
	var wg sync.WaitGroup
	wg.Add(999)
	for i := 0; i < 999; i++ {
	    go func() {
	        defer wg.Done()
	        ss.addheat(1)
	    }()
	}
	wg.Wait()
	if heat := ss.addheat(1); heat != 1000 {
		t.Errorf("Expected heat of 1000, but it was %d instead.", heat)
	}
}

func TestAddSeg(t *testing.T) {
	t.Log("Testing adding one Segment to SegTable with 123 heat... ")
	segt := NewSegTable(10,0,0)
	segt.StoreSeg([]byte{1,1}, []byte{1,3}, 123)
	if heat,end,_,_ := segt.FindSeg([]byte{1,2}); heat != 123 || y.ComparePureKeys(end,[]byte{1,3}) != 0{
		t.Errorf("Expected end at {1,3}, heat of 123, but the heat was %d and ends is %v instead.", heat, end)
	}
}

func TestAddSegSmall(t *testing.T) {
	t.Log("Testing adding ten Segments to SegTable ... ")
	segt := NewSegTable(10,0,0)

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
	segt := NewSegTable(10,0,0)

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
	segt := NewSegTable(10,0,0)

	for i := 0; i < 10; i++ {
		segt.StoreSeg([]byte{10}, []byte{20}, 1)
	}

	if heat,end,_,_ := segt.FindSeg([]byte{10}); heat != 10 || y.ComparePureKeys(end,[]byte{20}) != 0{
		t.Errorf("Expected heat of 10, end at {20}, but the heat was %d and ends is %v instead.", heat, end)
	}
}

func TestAddSegSameMulti(t *testing.T) {
	t.Log("Testing adding ten same Segments to SegTable with ten threads ... ")
	segt := NewSegTable(10,0,0)

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
	segt := NewSegTable(10,0,0)
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
			 t.Errorf("Wrong return byte slices from Cooling.")
	}
	slice2 := segt.Cooling(0.5)
	if len(slice2) != 4{
		t.Errorf("Expected 4 byte array returned after cooling, but it returns %d byte arrays instead.", len(slice2))
	}
	if y.ComparePureKeys(slice2[0],[]byte{40}) != 0 || y.ComparePureKeys(slice2[1],[]byte{49}) != 0 ||
		 y.ComparePureKeys(slice2[2],[]byte{50}) != 0 || y.ComparePureKeys(slice2[3],[]byte{59}) != 0 {
			 t.Errorf("Wrong return byte slices from Cooling.")
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
	if _,_,_,find := segt.FindSeg([]byte{10}); find{
		t.Errorf("Expected be replaced, but is found.")
	}
	if heat,end,_,find := segt.FindSeg([]byte{10,10}); !find || heat != 4 || y.ComparePureKeys(end,[]byte{20,20}) != 0{
		t.Errorf("Expected find seg with heat of 4, end at {20,20}, but find is %t, the heat was %d and end is %v instead.", find, heat, end)
	}
	segt.StoreSeg([]byte{30,30}, []byte{40,40}, 4)
	if _,_,_,find := segt.FindSeg([]byte{30,30}); find{
		t.Errorf("Expected be not found because all is above boiling point, but is found.")
	}
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
