# Methods from Stack Overflow and Apache/Arrow Jira about parsing timestamps
# .%f (fraction of a second) is not supported in srtptime() 
# - however with a "clean canonical timestamp", a cast from string to timestamp works!!!
# Refs; https://issues.apache.org/jira/browse/ARROW-15883, https://github.com/apache/arrow/issues/20146, 

## Working example when reading the file - with canonical format including the fractions of seconds, a .cast() works perfectly!!
s = b"""a
2022-03-05T09:00:00.000000
2022-03-05T09:01:01.100000
9999-12-31 23:59:59.000000
"""

import io
from pyarrow import csv

# Scenario 1 , 2 and 3
scene = 3
print( f"Scene = {scene}")
if scene == 2 :
    schema = {"a:" : pa.timestamp("us") }
    pass
elif scene == 3:
    schema = {"a": pa.string() }

tst_table = csv.read_csv(io.BytesIO(s), convert_options = None if scene == 1 else csv.ConvertOptions(  column_types=schema),)

if scene == 3:
    print(tst_table["a"].cast(pa.timestamp("us")))
else:
    print(tst_table)
    
'''
    For reference : https://stackoverflow.com/questions/73780443/pyarrow-issue-with-timestamp-data
    As a workaround you could first read timestamps as strings, then process them by slicing off the 
    fractional part and add that as pa.duration to processed timestamps:
    ----code----
    import pyarrow as pa
    import pyarrow.compute as pc
    ts = pa.array(["1970-01-01T00:00:59.123456789", "2000-02-29T23:23:23.999999999"], pa.string())
    ts2 = pc.strptime(pc.utf8_slice_codeunits(ts, 0, 19), format="%Y-%m-%dT%H:%M:%S", unit="ns")
    d = pc.utf8_slice_codeunits(ts, 20, 99).cast(pa.int64()).cast(pa.duration("ns"))
    pc.add(ts2, d)
    --- code ---
'''
