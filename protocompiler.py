import os
import subprocess

dir_path = os.path.curdir
onceflag = False

def parse_and_compile(data):
    global onceflag
    data.attributes["proto_file_path"] = dir_path+"/Protocol.proto"
    if onceflag:
        api.send("output",data)
    mydict = data.attributes
    meta = [] 
    abapmeta = iter(mydict['metadata'])
    for row in mydict['ABAP']['Fields']:
        colname = row['Name']
        if colname not in ['TABLE_NAME', 'IUUC_OPERATION']:
            r = next(abapmeta)
            while r['Field']['COLUMNNAME'] != colname:
                r = next(abapmeta)
                r_colname = r['Field']['COLUMNNAME']
                r_key = r['Field']['KEY']
                r_abaplen = int(r['Field']['ABAPLEN'])
        else:
            r_colname = ''
            r_key = ''
            r_abaplen = 0
            l = [colname, row['Kind'], int(row['Length']), int(row['Decimals']), r_colname,
             r_key, r_abaplen]
            meta.append(l)

    proto = []
    proto.append('syntax = "proto2";')
    proto.append('')
    proto.append('message Protocol {')
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

    f = open("Protocol.proto","+a")    
    for line in proto:
        f.write(line + "\n")
    f.close()

    command = [f"{dir_path}/protoc/bin/protoc", f"-I={dir_path}", f"--python_out={dir_path}", f"{dir_path}/Protocol.proto"]
    subprocess.run(command, shell=True)
    api.send("output",data)
    onceflag = True
    return

api.set_callback("input",parse_and_compile)