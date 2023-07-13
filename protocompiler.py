import os
import subprocess

dir_path = os.path.abspath(os.path.curdir)
onceflag = False
buffer_struct = {}

def parse_and_compile(data):
    global onceflag
    global buffer_struct
    if onceflag:
        data.attributes["proto_struct"] = buffer_struct
        api.send("output",data)
        return
    mydict = data.attributes
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

    proto = []
    proto.append('syntax = "proto2";')
    proto.append('')
    proto.append('message TUNIT {')
    for idx, field in enumerate(meta) :
     # Bigquery doesn't allow / in field names, replace to underscore (default behaviour of GBQ)
        bq_colname = field[0].replace('/','_')
        if field[1] in ['C', 'X', 'D','N'] :
            bq_dt = "string"
        elif field[1] in ['s', 'I'] :
            bq_dt = "int64"
        elif field[1] in 'P' :
            bq_dt = "float"
        else :
        # Anything else has to be a string
            bq_dt = "string"
        bq_field = f"  optional {bq_dt} {bq_colname} = {idx+1};"
        proto.append(bq_field)

    proto.append( f"  optional string _CHANGE_TYPE = {idx+2};" )
    proto.append('}')

    f = open("TUNIT.proto","+a")    
    for line in proto:
        f.write(line + "\n")
        api.send("info",line)
    f.close()
    
    command = [f"{dir_path}/bin/protoc", f"-I={dir_path}", f"--python_out={dir_path}", f"{dir_path}/TUNIT.proto"]
    a = subprocess.run(command, stdout=subprocess.PIPE)
    api.send("info", a.stdout.decode("utf-8"))
    with open(f"{dir_path}/TUNIT_pb2.py","r") as f:
        lines = f.readlines()
        bytes_string = lines[15][58:-2]
        start = lines[23][39:-1]
        end = lines[24][37:-1]
        buffer_struct["bytes"] = bytes_string
        buffer_struct["start"] = start
        buffer_struct["end"] = end
        
    data.attributes["proto_struct"] = buffer_struct
    api.send("output",data)
    onceflag = True
    return

api.set_port_callback("input",parse_and_compile)