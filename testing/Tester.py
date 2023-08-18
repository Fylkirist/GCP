import random

letters_and_numbers = ["1","2","3","4","5","6","7","8","9","0","a","A","b","B","c","C","d","D","e","E","f","F"]

def send_test_string():
    message_len = random.randrange(1000,20000)
    csv_message = ""
    for i in range(message_len):
        for y in range(1,5):
            csv_message+=letters_and_numbers[random.randrange(0,len(letters_and_numbers)-1)]
        csv_message+=","
    
    output_msg = api.Message(body = csv_message, attributes = None)
    api.send("output",output_msg)

api.add_timer("5s", send_test_string)