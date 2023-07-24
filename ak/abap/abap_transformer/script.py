##
## Module to use pyarrow to otpimze data in the body - make ABAP data compliant with target system
##
import io
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import json, sys

# parameter to pass through message.attrribtutes { "ak.abap.abap_transformer" : "Optimize ^ Passthrough"
optimize_method = None

# Standard class for handling ABAP metadata - included in operator folder
#import ABAPmeta.py
##
## Creating a ABAP Metadata class
##
class ABAPmeta:
    col_names = [] # Ordered list of column names - same order as the CSV in the body
    col_meta = {} # metadata about the column
    col_types = {} # abap metadata about the column
    lastBatch = False
    is_valid_ABAP = False
    def __init__(self, attributes):
            # Check if EOF message (Initial Load scenario)
        if attributes["ABAP"]["Kind"] == "Element":
            self.lastBatch = True
            #api.send("output", data)
            #api.send("info", "Received lastBatch=True message from SLT")
            return
        if self.is_valid_ABAP == True:
            return
        ABAPKEY = attributes['ABAP']
        METAKEY = attributes['metadata']
        self.col_names = []
        for columnname in ABAPKEY['Fields']:
            self.col_names.append(columnname['Name'])
            #col_types[columnname['Name']] = columnname['Kind']
            self.col_types[columnname['Name']] = columnname # add the abap struct to the columnname for easier lookup
            #abapdesc = [item for item in ABAPKEY["Fields"] if item["Name"] == columnname['Name']][0]
            #print(abapdesc)
            desc = {"MD_DOMNAME": "SAP_DI", "ABAPTYPE": "CHAR" } if columnname['Name'] in [ "TABLE_NAME", "IUUC_OPERATION", ] else [item for item in METAKEY if item["Field"]["COLUMNNAME"] == columnname['Name']][0]["Field"]
            self.col_meta[columnname['Name']] = desc
            #print(desc)
            self.is_valid_ABAP = True
        return
    
    # Methods to get values out of the ABAPmeta class
    @classmethod
    def meta_value( cls, colname, metaname): # ABAP struct
        return cls.col_meta[colname][metaname]
    @classmethod
    def abap_value( cls, colname, abapname): # metadata struct
        return cls.col_types[colname][abapname]
    @classmethod
    def meta( cls, colname) :
        return cls.col_meta[colname]
    @classmethod
    def set_abap_kind(cls, colname, kind):
        if colname in cls.col_types:
            cls.col_types[colname]["Kind"] = kind
        else:
            raise Exception( f"ABAPmeta: Could not set {colname} to {type}, the column name do not exist in structure!")


# INPUT handler - getting "message" data with ABAP info
def on_input(data):

    m = ABAPmeta(data.attributes) # reorg of attributes
    # Check if EOF message (Initial Load scenario)
    if m.lastBatch == True:
        api.send("output", data)
        api.send("info", "Received lastBatch=True message from SLT")
        return
        

    # TODO: Send this onfo once pr run, also add info about which columns are affected
    #       by tramsformations - building a infobuffer and esn all at once
    #
    api.send("info","Input data columns\n---------------------")
    for k, v in m.col_types.items():
        api.send("info",f"Name:{k}, Kind:{v['Kind']}")
    
    schema = {c: pa.string() for c in m.col_names}
    table = csv.read_csv(io.BytesIO(data.body.encode()), 
                            read_options=csv.ReadOptions( column_names=m.col_names,),
                            convert_options = csv.ConvertOptions(  column_types=schema),)
    api.send("info", f"Read data into table object numrows={table.num_rows}")
    
    # Loop through table and check metadata to do transformations
    # ABAP - MD_DOMNAME contains the formatting rules of the column
    #
    # BK: All fields are by default strings (managed by schema on read_csv()
    
    colnames = table.column_names # Already defined in variable 'colnames' above
    
    new_array = []
    for colname in colnames:
        
        desc = m.meta(colname) # Using ABAPmeta class object
        table_col = table[colname]
        if 1==2 and desc["MD_DOMNAME"] == "TZNTSTMPS":
            # Full timestamp - need to remove precision of .0000000 - future timestamp is not valid - all 9's
            # ["2016-11-08T11:05:27.0000000", "9999-99-99T99:99:99.9999999"]
            tz = pc.utf8_slice_codeunits(table_col,0,19)
            tz = pc.replace_substring( tz, "-99-99", "-12-31")
            tz = pc.replace_substring( tz, "99:99:99", "23:59:59")
            tz = pc.strptime(tz, "%Y-%m-%dT%H:%M:%S", "s")
            new_array.append(tz)
        elif desc["MD_DOMNAME"] == "TZNTSTMPS": # New implementation
            nc = pc.utf8_slice_codeunits(table_col,0,19)
            nc = pc.replace_substring( nc, "-99-99", "-12-31")
            nc = pc.replace_substring( nc, "99:99:99", "23:59:59")
            new_array.append(nc.cast(pa.timestamp("us")))
            # Change data type to Z for timestamp if "Optimize"
            if optimize_method == "Optimize" :
                m.set_abap_kind(colname, "Z")
                api.send("info", f"{colname} is optimized to kind Z")
            
        elif desc["MD_DOMNAME"] == "MEINS":
            # Unit domain uses undfeined when it is not configured - switch to empty string
            nc = pc.replace_substring( table_col, "undefined", "")
            new_array.append(nc)
        elif desc["MD_DOMNAME"] in ["/SCMB/TMDL_TZNTSTMPS","/SCWM/DO_TIMESTAMP_WH" ]:
            # Datetime datatype [20230703171513, 0] ->> ["20230703171513", "00000101000000"] 
            nc = pc.replace_substring_regex( table_col.cast(pa.string()), "^[0]$", "00000101000000")
            #nc = pc.cast(table_col, pa.string())
            #nc = pc.replace_substring_regex( nc, "^[0]$", "00000101000000")
            #nc = pc.strptime(nc, "%Y%m%d%H%M%S", "s")
            new_array.append(nc)
        elif desc["MD_DOMNAME"] in ["DATUM"]:
            nc = pc.replace_substring( table_col, "-99-99", "-12-31")
            new_array.append(nc)
        elif desc["ABAPTYPE"] in ["TIMS"]:
            nc =pc.utf8_slice_codeunits(table_col,0,8)
            new_array.append(nc)
        else:
            new_array.append(table_col)
            
    # Table is only a struct,  no copying happening here
    transformed_table = pa.Table.from_arrays(
        new_array,
        names=colnames,
        )
    
    with io.BytesIO() as newbody_b:
        csv.write_csv(
            transformed_table,
            newbody_b,
            write_options=csv.WriteOptions(
                include_header=False, delimiter=",", quoting_style="none"
                ),
            )
        data.attributes["ak.abap.abap_transformer"] = optimize_method 
        api.send("output", api.Message( attributes = data.attributes, body = newbody_b.getvalue().decode()))
        ## BK: ALternate way - may check wich is faster
        ##   data.body = newbody_b.getvalue().decode()
        ##   api.send("output", data)


# Initial info about the ABAP Transformer Operator
def gen():
    global optimize_method
    api.send("info", "ABAP Data Transformer version 0.6")
    api.send("info", "Provides cleansing of ABAP datatypes, especially dates and timestamps")
    api.send("info", f"Installed pyarrow versjon = {pa.__version__} >= 11.0.0")
    optimize_method = api.config.optimize_for_bigquery
    api.send("info", f'ak.abap.abap_transformer : "{optimize_method}"')
    api.send("info", "")


##
## MAIN
##
api.add_generator(gen)
api.set_port_callback("input", on_input)
