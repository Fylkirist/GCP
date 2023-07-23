## Custom operator; Google BigQuery CDC Writer

Set precompiled to false if you are using protocompiler operator in pipeline, if you are manually compiling protobufs, use the name TUNIT in the .proto file and substitute buffer_dict entries with the bytestring + start and end indices from the pb2.py script.

Sourcetable is only read to select the protobuf from the buffer_dict given that precompiled is set to True.