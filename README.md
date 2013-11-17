ggdata
======

Simple implementation of iterator-based relational query executor. 
Intended to integrate with a fine-grained provenance system

Tables can

* parse arrays of object
* infer data schema
* load from postgres database (soon)

Implements the following operators

* filter
* aggregate
* group by
* partition
* orderby
* limit
* offset
* crossproduct
* project

Column and Row oriented table implementations



