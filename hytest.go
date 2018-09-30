package main

import (
	"log"
	"os"
	"github.com/dgraph-io/badger"
    "github.com/dgraph-io/badger/options"
	"time"
    "fmt"
    "math/rand"
)

//address of the test db, use slash(/) for Linux
var dir string = "\\tmp\\badger"

type TestS struct {
    yseql float64
    sseqq float64
    yranl float64
    rseqq float64
    rranq float64
    size  float64
}

func main() {

    //basic wisckey
    testbody(1)
    //wisckey uses ValueThreshold
    testbody(2)
    //wisckey uses levelDBset
    testbody(3)
    //wisckey uses levelDBset only for range search
    testbody(4)
}

func testbody(testnum int){

    //delete tmp directory for new db
    os.RemoveAll("\\tmp")

    // open output file
    fo, err := os.Create(fmt.Sprintf("load%d.data",testnum))
    if err != nil {
        panic(err)
    }

    // close fo on exit and check for its returned error
    defer func() {
        if err := fo.Close(); err != nil {
            panic(err)
        }
    }()

    //write the titles for the data
    if _, err := fo.Write([]byte(fmt.Sprintf("# x yseql sseqq yranl rseqq rranq size\n"))); err != nil {
            panic(err)
    }

    var TestR = make([]TestS, 7)

    //size of the value in Bytes
    bytes := []int{64, 256, 1024, 4*1024, 16*1024, 60*1024}
    //bytes := []int{60*1024}
    for i, bt := range bytes {
            //number of key-value pairs given the size of value
            num := 1024*1024*1024/ (bt + 16)
            d := 0.0
            //sequential-load performance
            switch testnum {
        	case 1:
        		d, _ = WiscTestInsert(bt,num,false,false,false,false)
        	case 2:
        		d, _ = WiscTestInsert(bt,num,false,false,false,true)
            case 3:
                d, _ = WiscTestInsert(bt,num,false,true,false,false)
            case 4:
                d, _ = WiscTestInsert(bt,num,false,false,true,false)
        	}
            TestR[i].yseql = 1024/d

            d = 0.0
            s := 0.0
            //random-load performance
            switch testnum {
        	case 1:
        		d, s = WiscTestInsert(bt,num,true,false,false,false)
        	case 2:
        		d, s = WiscTestInsert(bt,num,true,false,false,true)
            case 3:
                d, s = WiscTestInsert(bt,num,true,true,false,false)
            case 4:
                d, s = WiscTestInsert(bt,num,true,false,true,false)
        	}
            //write the random load performance and final size
            TestR[i].yranl = 1024/d
            TestR[i].size = s
    }

    for i, bt := range bytes {
            //number of key-value pairs given the size of value
            num := 1024*1024*1024/ (bt + 16)
            d := getRangeQuery(num/4, bt, true)
            //write the range query of random loaded data
            TestR[i].rseqq = 256/d
    }

    for i, bt := range bytes {
            //number of key-value pairs given the size of value
            num := 1*1024*1024*1024/ (bt + 16)
            d := getRangeQuery(num/4, bt, false)
            //write the range query of sequential loaded data
            TestR[i].sseqq = 256/d
    }

    for i, bt := range bytes {
            //number of key-value pairs given the size of value
            num := 1*1024*1024*1024/ (bt + 16)
            d := getQuery(num, bt)
            //write the random lookip of sequential loaded data
            TestR[i].rranq = float64(bt+16)*1000/(d*1024*1024)
    }

    for i, _ := range bytes {
            //write the range query of sequential loaded data
            if _, err := fo.Write([]byte(fmt.Sprintf("%d %f %f %f %f %f %f\n",
                    i+1,TestR[i].yseql,TestR[i].sseqq,TestR[i].yranl,
                    TestR[i].rseqq,TestR[i].rranq,TestR[i].size)));
            err != nil {
                     panic(err)
            }
    }
}

//bt is the size of value in byte, num is the number of key-value
//ran is whether random insert, leveldb is whether levelDB set
//hybrid is whether only first 1/4 is levelDB set
//valueset sets ValueThreshold to math.MaxUint16-16
//return time spent in second, and size of db iff ran
func WiscTestInsert(bt int, num int, ran bool, leveldb bool, hybrid bool, valueset bool) (float64, float64){

    //prepare the temporal db
    err := os.MkdirAll(fmt.Sprintf("%s%d%t",dir,bt,ran), 777)
    if err != nil {
        log.Fatal(err)
    }

    // Open the Badger database located in the /tmp/badger directory.
    // It will be created if it doesn't exist.
    opts := badger.DefaultOptions
    opts.Dir = fmt.Sprintf("%s%d%t",dir,bt,ran)
    opts.ValueDir = fmt.Sprintf("%s%d%t",dir,bt,ran)
    opts.TableLoadingMode = options.MemoryMap
    if valueset {
        opts.ValueThreshold = 65000
    }
    db, err := badger.Open(opts)
    if err != nil {
        log.Fatal(err)
    }

    // Start a writable transaction.
	txn := db.NewTransaction(true)
	defer txn.Discard()

    key := 0
    rankey := rand.Perm(num)
    now := time.Now()

	// Use the transaction...
    for i := 0; i < num; {
        //check whether random access
        if ran {
            key = rankey[i]
        } else {
            key = i
        }

        //pad key to 16 bytes and value to given bytes
        //use a new transaction if it is full
        if key <= num/4 && hybrid{
            err = txn.LevelDBSet([]byte(fmt.Sprintf("%16d", key)), []byte(fmt.Sprintf("%*d", bt, key)));
        } else if leveldb {
            err = txn.LevelDBSet([]byte(fmt.Sprintf("%16d", key)), []byte(fmt.Sprintf("%*d", bt, key)));
        } else {
            err = txn.Set([]byte(fmt.Sprintf("%16d", key)), []byte(fmt.Sprintf("%*d", bt, key)));
        }

        if  err != nil {
            if err = txn.Commit(nil); err != nil {
        	    log.Fatal(err)
        	}
        	txn = db.NewTransaction(true)
        } else {
        	i++
        }
    }

    // Commit the transaction and check for error.
    if err = txn.Commit(nil); err != nil {
        log.Fatal(err)
    }
    d := time.Since(now).Seconds()
    db.Close()

    //get the size of database iff ran
    var sz float64
    if ran {
        opts := badger.DefaultOptions
        opts.Dir = fmt.Sprintf("%s%d%t",dir,bt,ran)
        opts.ValueDir = fmt.Sprintf("%s%d%t",dir,bt,ran)

        db, err := badger.Open(opts)
        if err != nil {
            log.Fatal(err)
        }

        lsm, vlog := db.Size()
        db.Close()
        sz = float64(lsm + vlog)/ (1024*1024)
    }

    return d,sz
}

func getRangeQuery(num int, bt int, ran bool) float64{
    opts := badger.DefaultOptions
    opts.Dir = fmt.Sprintf("%s%d%t",dir,bt,ran)
    opts.ValueDir = fmt.Sprintf("%s%d%t",dir,bt,ran)
    opts.TableLoadingMode = options.MemoryMap
    db, err := badger.Open(opts)
    if err != nil {
        log.Fatal(err)
    }

    now2 := time.Now()

    // Start a writable transaction.
    txn := db.NewTransaction(false)

    itopts := badger.DefaultIteratorOptions
    itopts.PrefetchSize = num
    it := txn.NewIterator(itopts)

    it.Rewind()


    // Use the transaction...
    for i := 0; i < num; i++ {

        item := it.Item()
        if !it.Valid(){
            log.Fatal(i)
        }
        _ = item.Key()
        _, err := item.Value()
        if err != nil {
            log.Fatal(err)
        }
        it.Next()
    }
    it.Close()

    // Commit the transaction and check for error.
    if err = txn.Commit(nil); err != nil {
        log.Fatal(err)
    }
    d2 := time.Since(now2).Seconds()
    txn.Discard()
    db.Close()

    return d2
}

func getQuery(num int, bt int) float64{
    opts := badger.DefaultOptions
    opts.Dir = fmt.Sprintf("%s%d%t",dir,bt,true)
    opts.ValueDir = fmt.Sprintf("%s%d%t",dir,bt,true)
    opts.TableLoadingMode = options.MemoryMap
    db, err := badger.Open(opts)
    if err != nil {
        log.Fatal(err)
    }

    now2 := time.Now()

    // Start a writable transaction.
    txn := db.NewTransaction(false)
    defer txn.Discard()

    rankey := rand.Perm(num)

    // Use the transaction...
    for i := 0; i < 1000; i++ {

        //pad key to 16 bytes and value to given bytes
        item, err := txn.Get([]byte(fmt.Sprintf("%16d", rankey[i])))
        if err != nil {
            log.Fatal(err)
        }
        val, err := item.Value()
        if err != nil {
            log.Fatal(err)
        }
        if val == nil{
            log.Fatal(err)
        }
    }

    // Commit the transaction and check for error.
    if err = txn.Commit(nil); err != nil {
        log.Fatal(err)
    }

    d2 := time.Since(now2).Seconds()
    db.Close()
    return d2
}
