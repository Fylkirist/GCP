##
## Receives a message and will encode the payload (data.body) into avro format
##

import fastavro as avro
import io

def init_func():
    pass

def on_input(data):
    pass


## MAIN ##
api.add_generator( init_func )
api.set_port_callback("input", on_input)

