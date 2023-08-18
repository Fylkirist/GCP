import time

start = time.time()
time_tracker = 0
byte_tracker = 0

def on_data_input(data):
    global time_tracker
    global byte_tracker
    elapsed = time.time()
    time_tracker = elapsed-start
    byte_tracker += len(data.body.encode('utf-8'))
    api.send("dataout",data)

def on_cmd_input(command):
    data_info_string = "Command entered: " + command + " \n"
    if command.lower() == "hour":
        data_info_string += f"Throughput: {(byte_tracker/1024)/(time_tracker/3600)} kilobytes per hour"
    elif command.lower() == "second":
        data_info_string += f"Throughput: {(byte_tracker/1024)/time_tracker} kilobytes per second"
    elif command.lower() == "total":
        data_info_string += f"Total throughput: {byte_tracker/1024} kilobytes over {time_tracker} seconds"
    else:
        data_info_string += "Invalid command: Supported commands are hour,second and total"
    api.send("info",data_info_string)

api.set_port_callback("datain",on_data_input)
api.set_port_callback("cmd",on_cmd_input)