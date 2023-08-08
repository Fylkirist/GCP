##
## Module to use pyarrow to otpimze data in the body - make ABAP data compliant with target system
##
import io
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import json, sys
from datetime import datetime

VERSION = "0.86"  # Updated 2023-08-08
VERSION_UPDATE = [  "2023-07-26 - Updated handling of timestamps: 9999-99-99 -> '0000-01-01' ",
                    "2023-07-31 - csv.WriteOptions() changed quoting_style from 'none' to 'needed' ",
                    "2023-08-08 - added two now config items; alpha_conversion -> remove leading zeros, insert_timestamp -> timestamp when changed",
                    ]

ONCE_FLAG = True

# Standard class for handling ABAP metadata - included in operator folder
#from ABAPmeta import ABAPmeta
##
## Creating a ABAP Metadata class
##
class AbapMeta:
    # Constants
    PASSTHROUGH = "Passthrough"
    OPTIMIZE = "Optimize"
    OPTIMIZE_WITH_NULLS = "Optimize with NULLS"
    # Class variables
    # col_names = [] # Ordered list of column names - same order as the CSV in the body
    # col_meta = {} # metadata about the column
    # col_types = {} # abap metadata about the column
    # lastBatch = False
    # is_valid_ABAP = False
    # optimize_method = None

    def __init__(self, attributes):
        # Get attributes from the config environment
        self.optimize_method = attributes.get("ak.abap.data_transformer")
        self.alpha_conversion = attributes.get("ak.abap.alpha_conversion")
        self.insert_timestamp = attributes.get("ak.abap.insert_timestamp")
        # Check if EOF message (Initial Load scenario)
        if attributes["ABAP"]["Kind"] == "Element":
            self.lastBatch = True
            #self.is_valid_ABAP = False
            # api.send("output", data)
            # api.send("info", "Received lastBatch=True message from SLT")
            return
        self.lastBatch = False
        ABAPKEY = attributes["ABAP"]
        METAKEY = attributes["metadata"]
        self.col_names = []
        self.col_types = {}
        self.col_meta = {}

        if self.insert_timestamp:  # Add new column in ABAP description - not available in source
            ABAPKEY["Fields"].append(
                {
                "Name": "INSERT_TS",
                "Type": "",
                "Kind": "Z",
                "Length": 0.0,
                "Decimals": 0.0
            })
        for columnname in ABAPKEY["Fields"]:
            self.col_names.append(columnname["Name"])
            # col_types[columnname['Name']] = columnname['Kind']
            self.col_types[
                columnname["Name"]
            ] = columnname  # add the abap struct to the columnname for easier lookup
            # abapdesc = [item for item in ABAPKEY["Fields"] if item["Name"] == columnname['Name']][0]
            # print(abapdesc)
            desc = (
                {"MD_DOMNAME": "SAP_DI", "ABAPTYPE": "CHAR"}
                if columnname["Name"]
                in [
                    "TABLE_NAME",
                    "IUUC_OPERATION",
                    "INSERT_TS",
                ]
                else [
                    item
                    for item in METAKEY
                    if item["Field"]["COLUMNNAME"] == columnname["Name"]
                ][0]["Field"]
            )
            self.col_meta[columnname["Name"]] = desc
            # print(desc)
            self.is_valid_ABAP = True

            
            
        return

    # Methods to easily retreive values
    def meta_value(self, colname, metaname):  # ABAP struct
        return self.col_meta[colname][metaname]

    def abap_value(self, colname, abapname):  # metadata struct
        return self.col_types[colname][abapname]

    def meta(self, colname):
        return self.col_meta[colname]

    def set_abap_kind(self, colname, kind):
        if colname in self.col_types and len(kind) == 1:
            self.col_types[colname]["Kind"] = kind
        else:
            raise Exception(
                f"ABAPmeta: Could not set {colname} to {kind}, the column name do not exist in structure or length of type >1!"
            )

    def pyarrow_schema(self):
        print(f"opt_met={self.optimize_method}, const={self.OPTIMIZE_WITH_NULLS}")
        if self.optimize_method != self.OPTIMIZE_WITH_NULLS:
            print("Strings only")
            _schema = {c: pa.string() for c in self.col_names}
            return _schema
        _schema = {}
        print("Specific datatypes")
        for _col in self.col_names:
            if self.col_types[_col]["Kind"] in [
                "I",
                "N",
                "s",
            ]:  # "s" is an internal INT2 ABAP type
                _dt = pa.int64()
            elif self.col_types[_col]["Kind"] in [
                "F",
            ]:
                _dt = pa.float64()
            elif self.col_types[_col]["Kind"] in ["P"]:
                if self.col_meta[_col]["MD_DOMNAME"] in ["/SCMB/TMDL_TZNTSTMPS", "/SCWM/DO_TIMESTAMP_WH"]:
                    _dt = pa.int64()
                else:
                    _dt = pa.string()
            else:
                _dt = pa.string()
            _schema[_col] = _dt
        return _schema
        
    def pyarrow_strings_can_be_null(self):
        return True if self.optimize_method == self.OPTIMIZE_WITH_NULLS else False



# INPUT handler - getting "message" data with ABAP info
def on_input(data):
    global ONCE_FLAG
    # Infusing attributes with information acquired by this operator
    data.attributes["ak.abap.cleansed"] = api.config.optimize_for_bigquery  # pass through that we have used the ABAP Data Transform
    data.attributes["ak.abap.data_transformer"] = api.config.optimize_for_bigquery  # Used by AbapMeta class
    data.attributes["ak.abap.alpha_conversion"] = api.config.alpha_conversion  # Boolean value if Alpha is trimmed for leading zeros
    data.attributes["ak.abap.insert_timestamp"] = api.config.insert_timestamp  # Boolean value if we have added a timestamp
    m = AbapMeta(data.attributes) # reorg of attributes
    # Check if EOF message (Initial Load scenario)
    if m.lastBatch == True:
        api.send("output", data)
        api.send("info", "Received lastBatch=True message from ABAP source operator")
        return
        
    if ONCE_FLAG:
        col_info = "\nInput data columns\n---------------------"
        for k, v in m.col_types.items():
            col_info += "\n" + f"Name:{k}, Kind:{v['Kind']}"
        api.send("info",col_info)
        ONCE_FLAG = False
    #schema = {c: pa.string() for c in m.col_names}  # Replaced with code below
    schema = m.pyarrow_schema()
    strings_can_be_null = m.pyarrow_strings_can_be_null()
    table = csv.read_csv(io.BytesIO(data.body.encode()), 
                            read_options=csv.ReadOptions( column_names=m.col_names[:-1 if m.insert_timestamp else len(m.col_names)],),
                            convert_options = csv.ConvertOptions(  column_types=schema, 
                                                    strings_can_be_null=strings_can_be_null,
                                                    include_missing_columns = False, ))
    api.send("info", f"Read data into table object; numcols={table.num_columns}, numrows={table.num_rows}")
    
    # Loop through table and check metadata to do transformations
    # ABAP - MD_DOMNAME contains the formatting rules of the column
    #
    # BK: All fields are by default strings (managed by schema on read_csv()
    
    # colnames = table.column_names # Already defined in variable 'm.col_names' above
    
    new_array = []
    for colname in m.col_names:  # 2023-08-08 colnames do not include INSERT_TS, use m.col_names instead
        
        #desc = m.meta(colname) # Using meta class object
        desc = m.col_meta[colname] # Using meta class object
        table_col = table[colname] if colname != "INSERT_TS" else None  # TS_INSERT column is not in the data, will be created here
        if desc["MD_DOMNAME"] == "TZNTSTMPS":  # New implementation - 2023-07-27 and improved 2023-08-08
            # ["2016-11-08T11:05:27.0000000", "9999-99-99T99:99:99.9999999"] to # ["2016-11-08T11:05:27", null]
            # Alt. solution; since these are strings, pc_replace_substring(table_col,"9999-99-99T99:99:99.9999999","")
            py = pc.utf8_slice_codeunits(table_col,0,19).to_pylist()
            mask = pc.match_substring(table_col, "9999-99-99T",)
            nc = pa.array(py, type=pa.string(), mask=mask.combine_chunks()).cast(pa.timestamp("us"))  # Expect local TZ
            nc = pc.assume_timezone(nc, "UTC",ambiguous="earliest")
            new_array.append(nc.cast(pa.int64()))
            # Change data type to Z for timestamp if "Optimize"
            if m.optimize_method in [m.OPTIMIZE, m.OPTIMIZE_WITH_NULLS] :
                m.set_abap_kind(colname, "Z")
                api.send("info", f"{colname} is optimized to kind Z")
        elif desc["MD_DOMNAME"] == "MEINS":
            # Unit domain uses 'undefined' when it is not configured - switch to empty string
            nc = pc.replace_substring( table_col, "undefined", "")
            new_array.append(nc)
        elif desc["MD_DOMNAME"] in ["/SCMB/TMDL_TZNTSTMPS","/SCWM/DO_TIMESTAMP_WH" ]:  # these are timestamps ""
            # Datetime datatype [20230703171513, 0] ->> ["20230703171513", ""] 
            nc = pc.replace_substring_regex( table_col.cast(pa.string()), "^[0]$", "")
            # Change data type to Z for timestamp if "Optimize"
            if m.optimize_method in [m.OPTIMIZE, m.OPTIMIZE_WITH_NULLS] :
                mask = pc.match_substring_regex(nc, "^$",)
                nc = pa.array(nc.to_pylist(), type=pa.string(), mask=mask.combine_chunks())
                nc = pc.strptime(nc, "%Y%m%d%H%M%S", "us").cast(pa.int64())  # Google BigQuery internal storage is in microseconds
                m.set_abap_kind(colname, "Z")
                api.send("info", f"{colname} is optimized to kind Z")
            new_array.append(nc)
        elif desc["MD_DOMNAME"] in ["DATUM"]:
            nc = pc.replace_substring( table_col, "9999-99-99", "")  # Non-filled ABAP date is delivered as 9999-99-99
            new_array.append(nc)
        elif desc["ABAPTYPE"] in ["TIMS"]:
            nc =pc.utf8_slice_codeunits(table_col,0,8)
            new_array.append(nc)
        elif desc["MD_DOMNAME"] in ["/SCWM/DO_TU_NUM",] and m.alpha_conversion:   # ALPHA conversion
            # nc = table_col.cast(pa.int64()).cast(pa.string())  # Works only on numeric strings
            nc = pc.ascii_ltrim(table_col,"0")  # Remove leading character - if string only contains zeros, empty string will be returned
            new_array.append(nc)
        elif colname in ["INSERT_TS",]:
            #nc = pa.nulls(table.num_rows)  # Add a null column for timestamp - value gets inserted in CDC writer
            nc = pa.array([int(datetime.now().timestamp() * 1e6)]*len(table))  # Timestamp values inserted using microseconds
            new_array.append(nc)
        else:
            new_array.append(table_col)
            
    # Table is only a struct,  no copying happening here
    transformed_table = pa.Table.from_arrays(
        new_array,
        names=m.col_names,
        )
    
    with io.BytesIO() as newbody_b:
        csv.write_csv(
            transformed_table,
            newbody_b,
            write_options=csv.WriteOptions(
                include_header=False, delimiter=",", quoting_style="needed"  # 2023-07-31 changed from 'none' to 'needed'
                ),
            )
        api.send("output", api.Message( attributes = data.attributes, body = newbody_b.getvalue().decode()))
        ## BK: ALternate way - may check wich is faster
        ##   data.body = newbody_b.getvalue().decode()
        ##   api.send("output", data)


# Initial info about the ABAP Transformer Operator
def gen():
    api.send("info", f"ABAP Data Transformer version {VERSION}")
    api.send("info", "Provides cleansing of ABAP datatypes, especially dates and timestamps")
    for vu in VERSION_UPDATE:
        api.send("info", f"{vu}")
        
    api.send("info", f"Installed pyarrow versjon = {pa.__version__} >= 11.0.0")
    api.send("info", "")
    
    # Fetching which behavior to use in the ABAP Data Transformer
    api.send("info", f'ak.abap.abap_transformer : "{api.config.optimize_for_bigquery}"')
    api.send("info", f'ak.abap.alpha_conversion : "{api.config.alpha_conversion}"')
    api.send("info", f'ak.abap.insert_timestamp : "{api.config.insert_timestamp}"')
    api.send("info", "")


##
## MAIN
##
api.add_generator(gen)
api.set_port_callback("input", on_input)
