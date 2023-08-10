## Custom operator; Google BigQuery CDC Writer

The Google BigQuery CDC writer enables table replication and streaming continuous row requests to Google BigQuery. 

#### The following dependencies are required for this operator:

* Message compiler in pipeline to get protobuf definition
* google-cloud-bigquery
* google-cloud-bigquery-storage
* google-cloud-storage

### Usage

Put this operator as an endpoint after the protobuf message compiler and change the configuration to use your connection id and target dataset + table for your pipeline.