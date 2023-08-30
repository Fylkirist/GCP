import time
import sys
import iperf3
import csv
import io

start = time.time()
time_tracker = 0
byte_tracker = 0
line_tracker = 0

def use_iperf_benchmark(host:str,port:int):
    client = iperf3.Client()
    client.server_hostname = host
    client.port = port
    result = client.run()
    if result.error:
        return f"\nFeil oppstod under test: {result.error}"
    else:
        return f"""\nAverage transmitted data in all sorts of networky formats:\n
        {"-"*40}\n
        Megabits per second  (Mbps)  {float(result.sent_Mbps):.0f} sent\n
        MegaBytes per second (MB/s)  {float(result.sent_MB_s):.1f} sent\n
        {"-"*40}\n
        Megabits per second  (Mbps)  {float(result.received_Mbps):.0f} received\n
        MegaBytes per second (MB/s)  {float(result.received_MB_s):.1f} received
        """

def increment_timer():
    global time_tracker
    elapsed = time.time()
    time_tracker = elapsed-start

def on_data_input(data):
    global byte_tracker
    global line_tracker
    if type(data.body) == list:
        line_tracker+=len(data.body)
    elif type(data.body) == dict:
        line_tracker+=1
    elif type(data.body) == str:
        line_tracker+=len(list(csv.reader(io.StringIO(data.body))))
    else:
        line_tracker+=1
    byte_tracker += sys.getsizeof(data.body)
    api.send("dataout",data)

def on_cmd_input(command):
    split_command_string = command.split(" ")
    data_info_string = "Command entered: " + command + " \n"
    if split_command_string[0].lower() == "lines":
        if split_command_string[1].lower() == "hour":
            data_info_string += f"Throughput: {(line_tracker)/(time_tracker/3600):.0f} rows per hour"
        elif split_command_string[1].lower() == "second":
            data_info_string += f"Throughput: {(line_tracker)/time_tracker:.0f} rows per second"
        elif split_command_string[1].lower() == "total":
            data_info_string += f"Total throughput: {line_tracker} rows over {time_tracker:.0f} seconds"
        else:
            data_info_string += "Invalid command: Supported commands for \"lines\" are hour,second and total"
    elif split_command_string[0].lower() == "bytes":
        if split_command_string[1].lower() == "hour":
            data_info_string += f"Throughput: {(byte_tracker/1024)/(time_tracker/3600):.2f} kilobytes per hour"
        elif split_command_string[1].lower() == "second":
            data_info_string += f"Throughput: {(byte_tracker/1024)/time_tracker:.2f} kilobytes per second"
        elif split_command_string[1].lower() == "total":
            data_info_string += f"Total throughput: {byte_tracker/1024:.2f} kilobytes over {time_tracker:.0f} seconds"
        else:
            data_info_string += "Invalid command: Supported commands for \"bytes\" are hour,second and total"
    elif split_command_string[0].lower() == "iperf":
        if len(split_command_string) == 3:
            data_info_string+=use_iperf_benchmark(split_command_string[1],int(split_command_string[2]))
        else:
            data_info_string += "Insufficient arguments for iperf command, usage: iperf [host_url] [port_number]"

    api.send("info",data_info_string)

api.add_timer("1s",increment_timer)
api.set_port_callback("datain",on_data_input)
api.set_port_callback("cmd",on_cmd_input)
