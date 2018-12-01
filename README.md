# Managed Storage Hierarchy in WiscKey
## Introduction
[WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) exploits the high parallel random-read performance of SSD by seperating value from key. While WiscKey has an overall better performance than LevelDB, it has a poor range query performance when value size is small. Therefore, we design a managed storage hierarchy to combine the performance advantages of LevelDB with Wisckey by storing frequently range queried data into LSM tree directly. The range query and random lookup performance of data from LSM tree and vLog in WiscKey is shown below.

|![illustration](image/wisckey.png)|![illustration](image/wisckeyq.png)|
| ----|----|
| <sub>**Range Query Performance.** This figure shows range query performance. 256 MB of data is range queried from a 1-GB database with values loaded to LSM or vLog.</sub>|<sub>**Random Lookup Performance.** This figure shows the random lookup performance from a 1-GB database with values loaded to LSM or vLog.</sub>|

Managed storage hierarchy includes three parts: Migration Process, Statistics and Manager. 

During the data load, Manager decides where the values should be inserted. For value with size smaller than 4 bytes, it is directly inserted into LSM tree because it is smaller than the pointer. For value with size larger than 64 KB, it is directly inserted into vLog because vLog provides a better range query performance when value size is larger than 64KB . If value size is between 4 bytes and 64 KB, the choice is based on the statistics of current storage and range query pattern to achieve the following goals:

- The size of LSM tree should not be too large. As shown above, random loopkup performance is affected largely when data are all loaded to LSM tree. We need to maintain a balance between data in LSM tree and data in vLog so that random lookup performance is not affected too much. For example, at most 30% values are loaded directly into LSM tree. Otherwise, the manager should call migration process to migrate values from the LSM tree to vLog.

- Values that are more likely to be range queried should stay at LSM tree. Because the proportion of data in LSM tree should not be too large, we only store the hottest data. In one sense, LSM tree is like a cache storing only frequently or recently range queried data. 

- Data in LSM tree should be migrated back to vLog gradually as users do a large number of random lookups. There is no need to keep data in LSM tree if users only use random lookup. 

![illustration](image/manage.png)

In order to better predict the range query pattern, we utilize temporal and spatial locality: the key ranges that are range queried before are more likely to the range queried in the future. This principle is from the design of cache. However, while cache deals with units of data, we need to consider ranges of data. We would always receive statistics in form of [start key of range query, end key of range query] after user uses iterator. Besides, the overlap between two ranges of keys should not be neglected. For example, if user frequently range query [1,3] and [2,4], then values with keys between [2,3] are more likely to be stored in LSM tree than [1,2] and [3,4]. The problem is more similar to the memory management, in which we need to deal with the statistics of a huge range.

Statistics is implemented using Segment Table in \badger\stat\segment.go, Migration is implemented in \badger\db.go and Manager is implemented in \badger\manager.go. They are all designed to be thread safe.

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

Current, we are trying to find the best tuning. 

The graphs of the results are made using zplot http://pages.cs.wisc.edu/~remzi/Zplot/z-plot/docs/index.html.
