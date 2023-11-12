ABAP Data Transformer
=====================

With the ABAP Data Transformer operator you will have an out-of-box `data` cleanser for several ABAP datatypes. 

> Note: This operator requires pyarrow >= 12.0.1 and hence tagged. 
>       Also 'require' google libraries since it is meant to provide data for a bigquery streaming operator, 
>       not requiring two different images to be built - and hence serialization between processes.
>
>       Requires ABAPMeta.py in Dockerfile

Configuration parameters
------------

* **optimize for bigquery** (mandatory): options are `passthrough`, `optimize` or `optimize_with_NULLS`
*     
* **insert Timestamp** (mandatory): if True - additional INSERT_TS field added which contains inserted Timestamp
* **alpha conversion** (mandatory): if True - remove rightpadded zeros from fields of type MD_DOMNAME = /SCWM/DO_TU_NUM
* **alpha conversion fields**: additional fields to remove rightpadded zeros, delimited with comma


Input
------------
* **input** a message object from an ABAP Gen 1 operator

Output
------------
* **output** a message object forwarded to next operator (transformed)
* **info** a string output used to send information about the operations of the operator

