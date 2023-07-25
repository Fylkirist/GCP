## 
## Custom operator to create a table in Google Bigquery table
## using SLT ABAP format
## BK: 2023-07-06 - Initial framework
##                - added logic for bq_client, only reads forst event - skipping other events
##                - added proto content as well
## BK: 2023-07-14 - Updated meatdata structure - changed data types and added (max_length)
## BK: 2023-07-19 - Updated table structure to use DATE end TIME for Kind:"D" and "T"
## BK: 2023-07-24 - Updated with new Kind=Z - create timestamp column in BigQuery
##
## AK: 2023-07-24 - Operator now checks if table exists and does alter sql upon table creation
##

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
bq_client = None
gs_project_id = None
gs_sourcetable = None
gs_dataset = None
gs_targettable = None
gs_rootpath = None
gs_table_id = None
# Once flag
once_flag = True

def check_table_exists(table,client):
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False

# Get the required properties from the environment and store into global variables for reuse
def get_properties():
    global bq_client, gs_rootpath, gs_project_id, gs_dataset, gs_targettable, gs_sourcetable, gs_table_id
        
    # TODO - may use a try catch
    
    # Examine the ConnectionProperties object
    # ---------------------------------------
    # GCS connection config: projectId, keyFile, rootPath - NB! rootPath may be empty
    # GCP_BIGQUERY connection config: projectId, keyFile,  additionalRegions (not used by this operator)
    # https://vsystem.ingress.dh-zqghmzcj.dhaas-live.shoot.live.k8s-hana.ondemand.com/app/datahub-app-connection/connections?connectionTypes=GCS
    conn_props = api.config.bigquery['connectionProperties']
    gs_project_id = conn_props['projectId']
    gs_rootpath = conn_props['rootPath'] if 'rootPath' in conn_props else None 
    #gs_sourcetable = api.config.sourcetable
    gs_dataset = api.config.targetdataset
    gs_targettable = api.config.targettable
    gs_table_id = f'{gs_project_id}.{gs_dataset}.{gs_targettable}'
    

# Printing useful input about the environment to the "info" port -> str
def print_info():
    global bq_client, gs_rootpath, gs_project_id, gs_dataset, gs_targettable, gs_table_id
    
    # What version do we use for Gen1
    api.send('info', 'Google BigQuery Table Producer v0.9')
    api.send('info', '-----------------------------------')
    api.send("info", f'Python version = {sys.version}')
    api.send("info", 'AK (2023-07-14): Work in progress')
    api.send("info", "BK (2023-07-19): TIME datatype added")
    api.send("info", "BK (2023-07-24): TIMESTAMP datatype added")
    
    
    # Fill in properties into global variables
    get_properties()
    
    # Connection properties
    api.send("info", f'')
    api.send("info", f'projectID = "{gs_project_id}"')
    api.send("info", f'rootPath = "{gs_rootpath}"')
    api.send("info", f'target = "{gs_table_id}"\n' )

    # Connect
    keyfile = api.config.bigquery['connectionProperties']['keyFile']
    bq_client = bq_client or bigquery.Client(credentials=gcp_creds(keyfile))

    if bq_client : 
        api.send("info", f'Connected successfully to {gs_project_id}')
    else :
        api.send("info", 'Connect not successful.....')
        

def create_table(mydict):
    global bq_client, gs_table_id
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
        if colname not in ['TABLE_NAME', 'IUUC_OPERATION']:
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
            # Pakced numeric, precision and scale from metadata, not ABAP
            gbq_field = bigquery.SchemaField(bq_colname, 'NUMERIC', mode = 'NULLABLE', precision = field[7], scale = field[3] )
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
    
    table = bigquery.Table(gs_table_id, schema=schema)
    
    #table.clustering_fields = ["MANDT", "TU_NUM"]
    # INfo about key is in column 6 og meta structure, name in column 1 (max 4 columns clustered)
    table.clustering_fields = [ a[0].replace('/','_')   for a in meta if a[5] == 'X'][:4]
    
    # Create table object
    table = bq_client.create_table(table)  # Make an API request.
    api.send( "info",
        "Created clustered table {}.{}.{}".format(table.project, table.dataset_id, table.table_id),
    )
    
    # Creating SQL to ALTER table for CDC support
    sql1 = f"ALTER TABLE {gs_table_id} SET OPTIONS ( max_staleness = INTERVAL 15 MINUTE);"
    
    # Find keys
    keys = [ a[0]   for a in meta if a[5] == 'X']
    sql2 = f"ALTER TABLE {gs_table_id } ADD PRIMARY KEY ( { ','.join(keys)} ) NOT ENFORCED;"
    
    query = f"{sql1}\n{sql2}"
    request = bq_client.query(query)

    for thing in request:
        api.send("info",str(thing))

##
## This is the main input handler - getting SLT data
##
def on_input(data):
    global once_flag
    # TODO - check if we get the right message in with the required fields
    # Check if we have a valid ABAP and metadata structure

    if check_table_exists(table=gs_table_id,client=bq_client):
        once_flag = False
        api.send("info",f"{gs_table_id} already exists.")
    if data.attributes.get("ABAP") and data.attributes.get("metadata") and once_flag :
        # I can create table
        create_table(data.attributes)
        once_flag = False
    else :
        batch_index = data.attributes['message.batchIndex']
        api.send("info", f"Event skipped - message.batchIndex = {batch_index}")



##
## MAIN
##
api.add_generator(print_info)
api.set_port_callback("input", on_input)
