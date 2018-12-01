# Managed Storage Hierarchy in WiscKey
## Introduction
[WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) exploits the high parallel random-read performance of SSD by seperating value from key. While WiscKey has an overall better performance than LevelDB, it has a poor range query performance when value size is small. Therefore, we design a managed storage hierarchy to combine the performance advantages of LevelDB with Wisckey by storing the frequently range queried data into LSM tree directly. The range query performance of data from LSM tree and vLog is shown below.

![illustration](image/wisckey.png)

Managed storage hierarchy includes three parts: Migration Process, Statistics and Manager. 

During the data load, Manager decide where the values will be inserted. For value size smaller than 4 bytes, it is directly inserted into LSM tree because it is smaller than the pointer size. For value size larger than 64 KB, it is directly inserted into vLog because WiscKey has a better range query performance when value size is larger than 64KB . If value size is between 4 bytes and 64 KB, the choice is based on the statistics of current storage and range query pattern to achieve the following goals:

- The size of LSM tree should not be too large so that random lookup performance is not affected too much. For example, at most 30% values are loaded directly into LSM tree. Otherwise, the manager should move values from the LSM tree to vLog to decrease its size.

- Values that are more likely to be range queried in the future should stay at LSM tree. The prediction is based on the previous range query history in the statistics.

![illustration](image/manage.png)

In order to better predict the range query pattern, we utilize temporal and spatial locality: the key ranges that are range queried before are more likely to the range queried in the future. This principle is from the design of cache. However, cache deals with units of data while we need to consider ranges of data. We would always receive statistics in form of [start key of range query, end key of range query] after user uses iterator. Besides, the overlap between two ranges of keys should not be neglected. For example, if user frequently range query [1,3] and [2,4], then values with keys between [2,3] are more likely to be stored in LSM tree than [1,2] and [3,4]. The problem is more similar to the memory management, in which we need to deal with the statistics of a huge range.

Statistics is implemented through Segment Table in \badger\stat\segment.go and Manager is implemented in \badger\manager.go. The  \badger\stat\segment_test.go are the segment table’s  test codes. The \badger\manager_test.go are the manager’s implementation and test codes. To run the test codes, type go test in the command line under each directory. Segment Table and Manager are designed to be thread safe.

## Project Status
This project is based on https://github.com/dgraph-io/badger.
After installing the badger, replace the /draph-io directory in $GOPATH with /draph-io in our project.
To load key-value pair directly into LSM-tree like LevelDB, you can use the txn.LevelDBSet() method:

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
This HybridSet will store the data according to your usage patterns in Statistics.

The graphs of the results are made using zplot http://pages.cs.wisc.edu/~remzi/Zplot/z-plot/docs/index.html.
