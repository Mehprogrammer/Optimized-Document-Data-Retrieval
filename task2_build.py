import json
import sys
from pymongo import MongoClient
import time

def read_messages(s_data, m, filename = "messages.json"):
    print("\nReading messages.json")
    with open(filename, "r") as m_json: #read messages.json
        b_size = 5000
        b = []
        for line in m_json: #read each line from the json file
            if line == '[\n' or line == ']':
                continue

            line = line.strip(',\n')
            line = json.loads(line)
            b.append(line)

            if len(b) == b_size:    #insert all lines in batch
                embed_sender_info(b, s_data, m)
                b.clear()

        if len(b) >= 1: #insert remaining lines in batch
            embed_sender_info(b, s_data, m)

def embed_sender_info(messages_data, senders_data, messages):
    #start_time = time.time()

    # Create a dictionary from the senders data, faster search times
    senders_dict = {sender['sender_id']: sender for sender in senders_data}

    for message in messages_data:   #loop to find corresponding sender info from senders.json
        sender_info = senders_dict.get(message['sender'], None)
        if sender_info:
            message['sender_info'] = sender_info
        else:
            message['sender_info'] = {}  # Handle case where sender info is not found
    messages.insert_many(messages_data)  #insert the messages data with sender info into the messages collection
    #end_time = time.time()
    #print(f"Time taken to embed sender info: {end_time - start_time} seconds")
    

def main():
    # Get port number as a command line argument
    if len(sys.argv) > 1:
        port_number = sys.argv[1]
    else:
        print("Please provide the port number as a command line argument")
        sys.exit(1)

    # Connect to MongoDB
    try:
        client = MongoClient(f"mongodb://localhost:{port_number}")
        db = client["MP2Embd"]
        messages = db["messages"]
        print("Connected to MongoDB")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)

    doc_count_messages = messages.estimated_document_count()
    # Drop existing messages collection
    if doc_count_messages >= 1:
        messages.drop()

    # Read senders.json
    with open("senders.json", "r") as senders_file:  #no need to insert. just read the data for embedding later
        senders_data = json.load(senders_file)     

    start_time = time.time()
    read_messages(senders_data, messages)  #read messages.json and embed sender info
    end_time = time.time()

    total_time = end_time - start_time
    print(f"\nTime taken to embed sender info into messages collection: {total_time} seconds")

if __name__ == "__main__":
    main()
