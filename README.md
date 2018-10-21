# WiscKeyHybrid
This project aims at making a hybrid version of Wisckey with LevelDB.
This project is based on https://github.com/dgraph-io/badger.
After installing the badger, replace the draph-io in $GOPATH.
To load key-value pair directly into LSM-tree as LevelDB, you can use the txn.LevelDBSet() method:

```go
err := txn.LevelDBSet([]byte("key"), []byte("value"))
if err != nil {
	return err
}
```

A more smart Set method is under development! You can use txn.HybridSet() method:
```go
err := txn.HybridSet([]byte("key"), []byte("value"))
if err != nil {
	return err
}
```
This HybridSet will store the data according to your usage patterns.

The graphs of the results are made using zplot http://pages.cs.wisc.edu/~remzi/Zplot/z-plot/docs/index.html.
