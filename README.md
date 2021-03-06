# OLTPBench-WriteBack-ClientSideCache
This is the implementation of TPC-C using a client-side cache with the write-back policy.  It depends on two other projects available from scdblab github:
1.  Version of memcached that supports S and X leases along with the concept of pinning buffered writes, https://github.com/scdblab/IQ-WhalinTwemcache
2. The cache library used by TPC-C implementation, https://github.com/scdblab/cafe

This project was used to gather performance numbers reported in:  Shahram Ghandeharizadeh and Hieu Nguyen.  [Design, Implementation, and Evaluation of Write-Back Policy with Cache Augmented Data Stores.](http://dblab.usc.edu/Users/papers/writeback.pdf)  USC Database Laboratory Technical Report Number 2018-07.

It is important to read the above DBLAB technical report prior to reading the rest of this readme file.

TPC-C consists of 5 transactions.  For each transaction, we provide a table that details each SQL statement of that transaction, whether it reads or writes a cache entry, key of the cache entry that it reads or writes, the mapping it looks up in case of a read cache miss, and the mapping that is updated by one of its writes. There are 5 tables, one per transaction, as follows:

New-Order Transaction:
![image](https://drive.google.com/uc?export=view&id=112x2cAreEMNzXLBDqTdi3AifHAuPHyMk)

Payment Transaction:
![image](https://drive.google.com/uc?export=view&id=1bUJvmrDxS51UsutJoa0krZwZKwJ_4FR2)

Delivery Transaction:
![image](https://drive.google.com/uc?export=view&id=1j9RU_kfVv7LwVWfLVEcR3kGsv4IF_os9)

Order-Status Transaction:
![image](https://drive.google.com/uc?export=view&id=1e7aSxOjILdLETaM8U5fvUKgw48bUVgCJ)

Stock-Level Transaction:
![image](https://drive.google.com/uc?export=view&id=1_eBQtHH8M4rp1pcfNt3baKmqnDGkXNy4)
