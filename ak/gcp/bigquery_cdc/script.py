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

buffer_dict = {}


# Load necessary high-level APIs
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import json, csv, sys
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
import asyncio
# Load necessary high-level APIs
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import json, csv, sys

#Credentials for Google Cloud platform
def gcp_creds( keyfile_content : dict, ):
    gcp_credentials = service_account.Credentials.from_service_account_info(json.loads(keyfile_content))
    return gcp_credentials

# Global variables
gs_client = None
gs_project_id = None
gs_sourcetable = None
gs_dataset = None
gs_targettable = None
gs_rootpath = None
gs_selected_proto = None
table_struct = None
optimize_method = None
# AK: Use internal buffer_dict for message structure - if False - get structure from "Google BigQuery Protocompiler" 
use_buffer_dict = False

# Get the required propertiers from the environment ands store into global variables for reuse
def get_properties():
    global gs_client, gs_rootpath, gs_project_id, gs_dataset, gs_targettable, gs_sourcetable
        
    # TODO - may use a try catch
    
    # Examine the ConnectionProperties object
    # ---------------------------------------
    # GCS connection config: projectId, keyFile, rootPath - NB! rootPath may be empty
    # GCP_BIGQUERY connection config: projectId, keyFile,  additionalRegions (not used by this operator)
    # https://vsystem.ingress.dh-zqghmzcj.dhaas-live.shoot.live.k8s-hana.ondemand.com/app/datahub-app-connection/connections?connectionTypes=GCS
    conn_props = api.config.bigquery['connectionProperties']
    gs_project_id = conn_props['projectId']
    gs_rootpath = conn_props['rootPath'] if 'rootPath' in conn_props else None 
    gs_sourcetable = api.config.sourcetable
    gs_dataset = api.config.targetdataset
    gs_targettable = api.config.targettable

# Printing useful input about the environment to the "info" port -> str
def print_info():
    global gs_client, gs_rootpath, gs_project_id, gs_dataset, gs_targettable, gs_sourcetable
    
    # What version do we use for Gen1
    api.send("info", f'Python version = {sys.version}')
    api.send("info", 'AK (2023-07-13): Version 1.0 - Have message dict for 5 tables - can also get message desc through protocompiler')
    api.send("info","AK (2023-07-24): Version 1.1 - Added dependency to understand if protocompiler is present in pipeline")
    
    # Fill in properties into global variables
    get_properties()
    
    # Connection properties
    api.send("info", f'')
    api.send("info", f'projectID = "{gs_project_id}"')
    api.send("info", f'rootPath = "{gs_rootpath}"')
    api.send("info", f'sourcetable = "{gs_sourcetable}"')
    api.send("info", f'target = "{gs_project_id}.{gs_dataset}.{gs_targettable}"')
    #api.send("info", f'Optimize method = "{optimize_method}"')

    # Connect
    keyfile = api.config.bigquery['connectionProperties']['keyFile']
    gs_client = gs_client or BigQueryWriteAsyncClient(credentials=gcp_creds(keyfile))

    if gs_client : 
        api.send("info", f'Connected successfully to {gs_project_id}')
    else :
        api.send("info", 'Connect not successful.....')
        

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
    global table_struct, optimize_method
    #optimize_method = api.config["ak.abap.data_transformer"] if "ak.abap.data_transformer" in api.config else None  # 2023-07-27 check for presence of Data Transform
    optimize_method = data.attributes.get("ak.abap.data_transformer")  # Not checked if this is possible
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
                if data.attributes["ak.abap.abap_transformer"] and data.attributes["ak.abap.abap_transformer"] == "Optimize":
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
    global gs_selected_proto, use_buffer_dict
    gs_selected_proto = api.config.sourcetable
    if gs_selected_proto == "":
        return
    # Check if protocompiler is in the pipeline
    if "ak.gcp.protocompiler" in data.attributes:
        use_buffer_dict = False
    else:
        use_buffer_dict = True
    
    if use_buffer_dict:
        DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(buffer_dict[gs_selected_proto]['bytes'])

        _globals = globals()
        _builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
        _builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'TUNIT_pb2', _globals)
        if _descriptor._USE_C_DESCRIPTORS == False:
            DESCRIPTOR._options = buffer_dict[gs_sourcetable]['options']
            _globals['_TUNIT']._serialized_start = buffer_dict[gs_selected_proto]['start']
            _globals['_TUNIT']._serialized_end = buffer_dict[gs_selected_proto]['end']
    else:
        DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(eval(data.attributes['proto_struct']['bytes']))
        _globals = globals()
        _builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
        _builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'TUNIT_pb2', _globals)
        if _descriptor._USE_C_DESCRIPTORS == False:
            DESCRIPTOR._options = None
            _globals['_TUNIT']._serialized_start = data.attributes['proto_struct']['start']
            _globals['_TUNIT']._serialized_end = data.attributes['proto_struct']['end']
    api.send("info",gs_selected_proto)


## This is the main input operator - getting data from the input port
def on_input(data):
    select_proto(data)
    messages = parse_input(data)
    if len(messages) == 0:
        return
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        default_stream_to_bq(
            f"{gs_project_id}.{gs_dataset}.{gs_targettable}",
            messages,
            BigQueryWriteAsyncClient(credentials=gcp_creds(api.config.bigquery['connectionProperties']['keyFile']))  
        )
    )
    api.send("info", f"{len(messages)} messages sent.")


#### MAIN ####
api.add_generator(print_info)
api.set_port_callback("input", on_input)
