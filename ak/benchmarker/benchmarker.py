import time

start = time.time()
time_tracker = 0
byte_tracker = 0

def increment_timer():
    global time_tracker
    elapsed = time.time()
    time_tracker = elapsed-start

def on_data_input(data):
    global byte_tracker
    byte_tracker += len(data.body.encode('utf-8'))
    api.send("dataout",data)

def on_cmd_input(command):
    data_info_string = "Command entered: " + command + " \n"
    if command.lower() == "hour":
        data_info_string += f"Throughput: {(byte_tracker/1024)/(time_tracker/3600):.1f%} kilobytes per hour"
    elif command.lower() == "second":
        data_info_string += f"Throughput: {(byte_tracker/1024)/time_tracker:.1f%} kilobytes per second"
    elif command.lower() == "total":
        data_info_string += f"Total throughput: {byte_tracker/1024:.1f%} kilobytes over {time_tracker:.0f%} seconds"
    else:
        data_info_string += "Invalid command: Supported commands are hour,second and total"
    api.send("info",data_info_string)

api.add_timer("5s",increment_timer)
api.set_port_callback("datain",on_data_input)
api.set_port_callback("cmd",on_cmd_input)
