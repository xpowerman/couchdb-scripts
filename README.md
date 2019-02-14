# couchdb-scripts
Useful scripts for **[couchdb](http://couchdb.apache.org)** administrator.

## Description
Here is my self-written collection of useful script which help me to manage **[couchdb](http://couchdb.apache.org)** instances and clusters. All scripts writed on python3. 

## Dependencies

To run this scripts your need at least this packages installed:

- **`Python3`**
- **`Python3-pip`**

And install some additional non-standard libraries, which is used in scripts (pip3 install *`<library>`*): 

- **`lockfile`**
- **`CouchDB`**
- **`requests`**
- **`urllib3`**

## Curent scripts description:

- **`couchdb_benchmark->couchdb_benchmark.py`** - simple couchdb perfomance test script (for example to compare perfomance of two instances)
- **`couchdb_replication->couchdb_replication.py`** - couchdb databases replication script from source instance to tartget instance
- **`couchdb_misc->couchdb_db_sizes.py`** - script to list file sizes of couchdb databases sorted by size 
- **`couchdb_misc->couchdb_db_compare.py`** - compare databases of two couchdb instances by names, and show difference
- **`couchdb_misc->couchdb_db_remove.py`** - remove database from instance listed in file

Hope this scripts help somebody! ;)
