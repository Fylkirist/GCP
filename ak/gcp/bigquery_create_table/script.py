## 
## Custom operator to create a table in Google Bigquery table
## using SLT ABAP format
## BK: 2023-07-06 - Initial framework
##                - added logic for bq_client, only reads forst event - skipping other events
##                - added proto content as well
## BK: 2023-07-14 - Updated meatdata structure - changed data types and added (max_length)
## BK: 2023-07-19 - Updated table structure to use DATE end TIME for Kind:"D" and "T"
## BK: 2023-07-24 - Updated with new Kind=Z - create timestamp column in BigQuery
## AK: 2023-07-24 - Operator now checks if table exists and does alter sql upon table creation
## BK: 2023-08-07 - When Numeric scale is larger than 9, or precision larger than 38, use BIGNUMERIC
## AK: 2023-08-08 - Removed unnecessary code
##

# Load necessary high-level APIs
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import json, sys

#Credentials for Google Cloud platform
def gcp_creds( keyfile_content : dict, ):
    gcp_credentials = service_account.Credentials.from_service_account_info(json.loads(keyfile_content))
    return gcp_credentials

# Once flag
once_flag = True

def check_table_exists(table,client):
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False

# Printing useful input about the environment to the "info" port -> str
def print_info():
    # What version do we use for Gen1
    api.send('info', "Google BigQuery Table Producer v0.95")
    api.send('info', "-----------------------------------")
    api.send("info", f"Python version = {sys.version}")
    api.send("info", "AK (2023-07-14): First version")
    api.send("info", "BK (2023-07-19): TIME datatype added")
    api.send("info", "BK (2023-07-24): TIMESTAMP datatype added")
    api.send("info", "BK (2023-08-07): BIGNUMERIC datatype added (precision >38 and scale>9)")
    api.send("info", "AK (2023-08-08): Cleanup")
    
    # Connection properties
    conn_props = api.config.bigquery['connectionProperties']
    table_id = f"{api.config.bigquery['connectionProperties']['projectId']}.{api.config.targetdataset}.{api.config.targettable}"
    api.send("info", f'')
    api.send("info", f'projectID = "{conn_props["projectId"]}"')
    api.send("info", f'rootPath = "{conn_props["rootPath"] if "rootPath" in conn_props else None }"')
    api.send("info", f'target = "{table_id}"\n' )


def create_table(mydict,client):
    # Create a BigQuery Table based on metadata

    # DONE
    # The SAP metadata required to understand data structure and build external artifacts
    meta = []  # Colnames from dict object
    #ABAPKEY = mydict['ABAP']
    abapmeta = iter(mydict['metadata'])
    
    # Iterate through ABAP fields and metadata fields - need to know columnname, key field, data type and lengths
    # Need to align ABAP Fields with metdata Fields - ABAP has the output structure, but do not keep key info
    # However - metadata may contain object info not materialized like ".INCLUDE" and ".APPEND"
    for row in mydict['ABAP']['Fields']:
        colname = row['Name']
        if colname not in ['TABLE_NAME', 'IUUC_OPERATION', "INSERT_TS"]:
            r = next(abapmeta)
            #print(f"colname = {colname}, {r['Field']['COLUMNNAME']}")
            while r['Field']['COLUMNNAME'] != colname:
                r = next(abapmeta)
                #print(r['Field']['COLUMNNAME'])
            r_colname = r['Field']['COLUMNNAME']
            r_key = r['Field']['KEY']
            r_abaptype = r['Field']['ABAPTYPE']
            r_abaplen = int(r['Field']['ABAPLEN'])
            r_outputlen = int(r['Field']['OUTPUTLEN'])
        else: # TABLE_NAME and IUUC_OPERATION do not have metadata
            r_colname = colname
            r_key = ''
            r_abaplen = r_outputlen = row['Length']
            r_outputlen = 0 # Only for non-char (RAW, DATE and NUMERIC may have formatting or is packed)
        l = [colname, row['Kind'], int(row['Length']), int(row['Decimals']), r_colname,
             r_key, r_abaptype, r_abaplen, r_outputlen]
        meta.append(l)
    
    # Creating the physical object    
    schema = []
    for field in meta :
        # Bigquery doesn't allow / in field names, replace to underscore (default behaviour of GBQ)
        bq_colname = field[0].replace('/','_')
    
        if field[1] in ['C', 'X', 'N'] :
            # Output may be expanded - need outputlen instead
            gbq_field = bigquery.SchemaField(bq_colname, 'STRING', mode = 'NULLABLE', max_length = field[8] )
        elif field[1] in ['s', 'I', ] :
            gbq_field = bigquery.SchemaField(bq_colname, 'INTEGER', mode = 'NULLABLE' )
        elif field[1] in ['P', ] :
            # Packed numeric, precision and scale from metadata, not ABAP - this is a NUMERIC, BIGNUMERIC or FLOAT type
            _datatype = "NUMERIC" if (field[7] <=38 and field[3] <= 9) else "BIGNUMERIC"
            gbq_field = bigquery.SchemaField(bq_colname, _datatype, mode = 'NULLABLE', precision = field[7], scale = field[3] )
        elif field[1] in ['D', ] :
            gbq_field = bigquery.SchemaField(bq_colname, 'DATE', mode = 'NULLABLE' )
        elif field[1] in ['T'] :
            gbq_field = bigquery.SchemaField(bq_colname, 'TIME', mode = 'NULLABLE' )
        elif field[1] in ['Z'] : # Added Z as a special internal datatype
            gbq_field = bigquery.SchemaField(bq_colname, 'TIMESTAMP', mode = 'NULLABLE' )
        else :
            # Anything else has to be a string
            gbq_field = bigquery.SchemaField(bq_colname, 'STRING', mode = 'NULLABLE' )
        
        schema.append(gbq_field)
    
    #print(schema)
    
    table_id = f"{api.config.bigquery['connectionProperties']['projectId']}.{api.config.targetdataset}.{api.config.targettable}"
    table = bigquery.Table(table_id, schema=schema)
    
    #table.clustering_fields = ["MANDT", "TU_NUM"]
    # INfo about key is in column 6 og meta structure, name in column 1 (max 4 columns clustered)
    table.clustering_fields = [ a[0].replace('/','_')   for a in meta if a[5] == 'X'][:4]
    
    # Create table object
    table = client.create_table(table)  # Make an API request.
    api.send( "info",
        "Created clustered table {}.{}.{}".format(table.project, table.dataset_id, table.table_id),
    )
    
    # Creating SQL to ALTER table for CDC support
    # BK (2023-07-25) - need to quote objects with backtick(`) - some project ids may contain special chars
    sql1 = f"ALTER TABLE `{table_id}` SET OPTIONS ( max_staleness = INTERVAL 15 MINUTE);"
    #sql1 = f"ALTER TABLE {table.dataset_id}.{table.table_id} SET OPTIONS ( max_staleness = INTERVAL 15 MINUTE);"
    
    # Find keys
    keys = [ a[0]   for a in meta if a[5] == 'X']
    # BK (2023-07-25) - need to quote objects with backtick(`) - some project ids may contain special chars
    sql2 = f"ALTER TABLE `{table_id}` ADD PRIMARY KEY ( { ','.join(keys)} ) NOT ENFORCED;"
    #sql2 = f"ALTER TABLE {table.dataset_id}.{table.table_id} ADD PRIMARY KEY ( { ','.join(keys)} ) NOT ENFORCED;"

    query = f"{sql1}\n{sql2}"
    request = client.query(query)

    for thing in request:
        api.send("info",str(thing))


##
## This is the main input handler - getting SLT data
##
def on_input(data):
    global once_flag
    # TODO - check if we get the right message in with the required fields
    # Check if we have a valid ABAP and metadata structure
    bq_client = bigquery.Client(credentials=gcp_creds(api.config.bigquery['connectionProperties']['keyFile']))

    if not once_flag:
        batch_index = data.attributes['message.batchIndex']
        api.send("info", f"Event skipped - message.batchIndex = {batch_index}")
        return
    
    if check_table_exists(
        table=f"{api.config.bigquery['connectionProperties']['projectId']}.{api.config.targetdataset}.{api.config.targettable}",
        client=bq_client
        ):
        table=f"{api.config.bigquery['connectionProperties']['projectId']}.{api.config.targetdataset}.{api.config.targettable}"
        api.send("info",f"{table} already exists.")
        once_flag = False

    elif data.attributes.get("ABAP") and data.attributes.get("metadata") :
        # I can create table
        create_table(data.attributes,client= bq_client)
        once_flag = False
        

##
## MAIN
##
api.add_generator(print_info)
api.set_port_callback("input", on_input)
