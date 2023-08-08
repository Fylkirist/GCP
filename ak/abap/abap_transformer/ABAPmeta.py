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
