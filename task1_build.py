import json
import sys
from pymongo import MongoClient
import pymongo
import time

def read_messages(filename = "messages.json"):
    # Read and insert data in batches into messages_collection
    with open(filename, "r") as messages_json:
        batch_size = 5000
        batch = []

        # read each line from the json file
        for line in messages_json:
            # Skip the opening and closing brackets of the json file
            if line == '[\n' or line == ']':
                continue

            # Format line properly so we can use json.loads on it then append to batch array
            line = line.strip(',\n')
            line = json.loads(line)
            batch.append(line)

            # Number of lines in batch = batch size so we insert all the lines and then clear
            if len(batch) == batch_size:
                messages.insert_many(batch)
                batch.clear()

        # Insert remaining number of lines in batch to the collection
        if len(batch) >= 1:
            messages.insert_many(batch)


def read_senders(filename = "senders.json"):
    # Read and insert data in batches into senders_collection
    with open("senders.json", "r") as senders_json:
        data = json.load(senders_json)
        for dic in data:
            senders.insert_one(dic)




# Get port number as a command line argument
if len(sys.argv) > 1:
    port_number = sys.argv[1]
else:
    print("Please provide the port number as a command line argument when you run the program: ")
    sys.exit(1)

# Try to connect
try:
    # Create the connection with the port_number recieved as a command line
    client = MongoClient(f"mongodb://localhost:{port_number}")
    db = client["MP2Norm"]
    messages = db["messages"]
    senders = db["senders"]
    print("Connected to MongoDB")
except pymongo.errors.ConnectionError:
    print(f"Failed to connect. possibly wrong Port number (port number entered: {port_number}) ")
    sys.exit(1)

doc_count_messages = messages.estimated_document_count()    #getting the count of documents in the collections
doc_count_sender = senders.estimated_document_count()

if doc_count_messages >= 1:
    messages.drop()

if doc_count_sender >= 1:
    senders.drop()

#call the read_messages
start_time = time.time()
read_messages()
end_time = time.time()
print(f"\nsuccessfully created messages collection, took {end_time - start_time} seconds")

#call the read_senders
start_time = time.time()
read_senders()
end_time = time.time()
print(f"\nsuccessfully created senders collection, took {end_time - start_time} seconds")
