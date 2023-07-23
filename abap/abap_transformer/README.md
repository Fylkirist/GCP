ABAP Data Transformer
=====================

With the ABAP Data Transformer operator you will have an out-of-box `data` cleanser for several ABAP datatypes. 

> Note: This operator requires pyarrow >= 12.0.1 and hence tagged. 
>       Also 'require' google libraries since it is meant to provide data for a bigquery streaming operator, 
>       not requiring two different images to be built - and hence serialization between processes.

> Note: each callback registered in the script runs in a **different thread**.
> As the script developer, you should handle potential concurrency issues such as race
> conditions. You can, for instance, use primitives from the
> Python [threading module](https://docs.python.org/3/library/threading.html)
> to get protected against said issues.


Configuration parameters
------------

* **optimize for bigquery** (mandatory): options are `passthrough` or `optimize`  


Input
------------
* **input** a message object from an ABAP Gen 1 operator

Output
------------
* **output** a message object forwarded to next operator (transformed)
* **info** a string output used to send information about the operations of the operator

