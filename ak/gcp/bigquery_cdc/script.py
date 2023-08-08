## 
## Custom operator to write to Google Bigquery table using the new CDC streaming functionality
## BK: 2023-07-05 - Initial framework
## AK: 2023-07-13 - V1 of the operator
##

from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()

# Load necessary high-level APIs
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery_storage_v1.services.big_query_write.async_client import (
    BigQueryWriteAsyncClient,
)
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    ProtoRows,
    ProtoSchema,
)
from typing import AsyncIterable, AsyncIterator, Optional, Sequence, Union
from io import StringIO
from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.message import Message

import json, csv, sys
import asyncio

#Credentials for Google Cloud platform
def gcp_creds( keyfile_content : dict, ):
    gcp_credentials = service_account.Credentials.from_service_account_info(json.loads(keyfile_content))
    return gcp_credentials

# Global variables
table_struct = None

# Printing useful input about the environment to the "info" port -> str
def print_info():
    
    # What version do we use for Gen1
    api.send("info", f'Python version = {sys.version}')
    api.send("info", 'AK (2023-07-13): Version 1.0 - Have message dict for 5 tables - can also get message desc through protocompiler')
    api.send("info","AK (2023-07-24): Version 1.1 - Added dependency to understand if protocompiler is present in pipeline")
    api.send("info","AK (2023-08-08): Version 1.2 - Removed unnecessary globals, removed buffer dict, timestamping is now handled by data transformer, message definition now only happens once")
    
    # Connection properties
    conn_props = api.config.bigquery['connectionProperties']
    api.send("info", f'')
    api.send("info", f'projectID = "{conn_props["projectId"]}"')
    api.send("info", f'rootPath = "{conn_props["rootPath"] if "rootPath" in conn_props else None}"')
    api.send("info", f'target = "{conn_props["projectId"]}.{api.config.targetdataset}.{api.config.targettable}"')
        

# Create an async iterator where each message batch is less than the input size, bigquery default stream has a 10MB limit per stream.
async def generate_message_batches(messages: Sequence[Message], max_size_mb: int) -> AsyncIterator[Sequence[Message]]:
    current_batch = []
    current_size = 0

    for message in messages:
        message_size = message.ByteSize()
        if current_size + message_size <= max_size_mb * 1024 * 1024:
            current_batch.append(message)
            current_size += message_size
        else:
            yield current_batch
            current_batch = [message]
            current_size = message_size
    
    if current_batch:
        yield current_batch


def _build_append_rows_request(
    messages: Sequence[Message], stream_name: Union[str,None],
) -> AppendRowsRequest:
   
    assert messages
    rows = ProtoRows(
        serialized_rows=[message.SerializeToString() for message in messages]
    )

    request = AppendRowsRequest()
    if stream_name is None:
        request.proto_rows = AppendRowsRequest.ProtoData(rows=rows)
    else:
        first_message = messages[0]
        proto_descriptor = DescriptorProto()
        first_message.DESCRIPTOR.CopyToProto(proto_descriptor)
        request.write_stream = stream_name
        request.proto_rows = AppendRowsRequest.ProtoData(
            writer_schema=ProtoSchema(proto_descriptor=proto_descriptor),
            rows=rows
        )
    return request
        
async def _stream_of_append_row_requests(
    write_stream_name: str, messages: AsyncIterable[Sequence[Message]]
) -> AsyncIterator[AppendRowsRequest]:
    stream_name_for_first_message: Optional[str] = write_stream_name
    
    try:
        async for messages_chunk in messages:
            yield _build_append_rows_request(
                messages_chunk, stream_name_for_first_message
            )
            stream_name_for_first_message = None
    except Exception:
        api.send("info","Unexpected exception while streaming messages.")


async def default_stream_to_bq(table,messages,client):
    table_ref = TableReference.from_string(table)
    stream_name = f"projects/{table_ref.project}/datasets/{table_ref.dataset_id}/tables/{table_ref.table_id}/streams/_default"
    api.send("info", f"Stream started: {stream_name}")
    
    requests = _stream_of_append_row_requests(
        stream_name,
        generate_message_batches(messages,9)
    )

    api.send("info","Requests memed")
    response = await client.append_rows(
        requests=requests
    )
    
    api.send("info","Requests sent")
    async for _ in response:
        api.send("info",str(_.error))
        api.send("info",str(_.append_result))
        pass
    
    await client.transport.close()

# Parse the CSV and schema to return a list of message objects that conform to the proto schema
def parse_input(data):
    global table_struct
    # 2023-07-27 check for presence of Data Transform
    optimize_method = data.attributes.get("ak.abap.data_transformer")  # Get Optimize method if any
    api.send("info", f'Optimize method = "{optimize_method}"')

    body = list(csv.reader(StringIO(data.body)))
    if data.attributes["ABAP"]["Kind"] == "Element":
        return []
    
    if table_struct == None:
        abap = data.attributes["ABAP"]["Fields"]
        parsed_abap = {}
        for field in abap:
            field["Name"] = field["Name"].replace('/','_')
            parsed_abap[field["Name"]] = field
        table_struct = parsed_abap
    
    output = []
    for row in body:
        new_msg = TUNIT()
        count = 0
        try:
            for col,field in table_struct.items():
                value = row[count]
                # if the data has been through the abap transformer, we can skip typing for the message.
                if optimize_method == "Optimize":
                    pass
                elif field["Kind"] == 'C': # BK - remove checking the opther options
                    pass
                elif field["Kind"] in ['s', 'I']:
                    value = int(value)
                elif field["Kind"] in ['P']:
                    value = float(value)
                elif field["Kind"] in ['D'] and value == "9999-99-99":  # BK check if date is out of range
                    value = None  # 2023-07-27 changed to None (old "9999-12-31" )  
                if value is not None and len(str(value)) > 0:  # TODO - 2023-07-27 add logic for "Optimize with NULLS" 
                    setattr(new_msg,field["Name"],value)
                if "IUUC_OPERATION" in col:
                    setattr(new_msg,"_CHANGE_TYPE","DELETE" if value == "D" else "UPSERT")
                count+=1
        except TypeError:
            api.send("info", f"TypeError exception raised for '{str(col)}'!")
        output.append(new_msg)    
    return output

def select_proto(data):
    if not data.attributes['proto_struct']:
        api.logger.error("No message struct detected in input: Proto compiler missing in pipeline.")

    _globals = globals()
    if "TUNIT" in _globals:
        return
    DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(eval(data.attributes['proto_struct']['bytes']))
    _builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
    _builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'TUNIT_pb2', _globals)
    if _descriptor._USE_C_DESCRIPTORS == False:
        DESCRIPTOR._options = None
        _globals['_TUNIT']._serialized_start = data.attributes['proto_struct']['start']
        _globals['_TUNIT']._serialized_end = data.attributes['proto_struct']['end']


## This is the main input operator - getting data from the input port
def on_input(data):
    select_proto(data)
    messages = parse_input(data)
    if len(messages) == 0:
        return
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        default_stream_to_bq(
            f"{api.config.bigquery['connectionProperties']['projectId']}.{api.config.targetdataset}.{api.config.targettable}",
            messages,
            BigQueryWriteAsyncClient(credentials=gcp_creds(api.config.bigquery['connectionProperties']['keyFile']))  
        )
    )
    api.send("info", f"{len(messages)} messages sent.")


#### MAIN ####
api.add_generator(print_info)
api.set_port_callback("input", on_input)