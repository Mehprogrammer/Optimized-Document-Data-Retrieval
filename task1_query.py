import json
import sys
from pymongo import MongoClient
import pymongo
import time

def Q1():
    """
    QUERY 1: Returns the number of messages that have “ant” in their text
    """
    try:
        pipeline ={"text": {"$regex": ".*ant.*"}}

        start_time = time.time()
        ant_count = messages.count_documents(pipeline, maxTimeMS=120000)
        end_time = time.time()

        total_time = end_time - start_time
        print(f"\nNumber of messages that have “ant” in their text: {ant_count}")
        print(f"Query 1 execution time = {total_time} seconds")
    except pymongo.errors.ExecutionTimeout as e:
        print(f"\nError: {e}")
def Q2():
    """
    QUERY 2: Find the nick name/phone number of the sender who has sent the greatest number of messages.
    Return the nick name/phone number and the number of messages sent by that sender.
    You do not need to return the senders name or credit.
    """
    try:
        start_time = time.time()        #start the time
        pipeline = [{"$sortByCount": "$sender"}, {"$limit": 1}]

        most_messages = list(db.messages.aggregate(pipeline, maxTimeMS = 120000))
        end_time = time.time()
        total_time = end_time - start_time

        print(f"\n{most_messages[0]['_id']} sent the most messages, total messages: {most_messages[0]['count']}")
        print(f"Query 2 execution time = {total_time} seconds")

    except pymongo.errors.ExecutionTimeout as e:     #this error will only happen if the time exceeds
        print(f"\nError: {e}")
def Q3():
    """
    QUERY 3: Return the number of messages where the sender’s credit is 0
    """
    try:
        pipeline = [{"$match": {"credit": 0}},{"$lookup": {"from": "messages","localField": "sender_id","foreignField": "sender","as": "0_credit"}},{"$unwind": "$0_credit"},{"$count": "0"}]

        start_time = time.time()
        messages_count = list(senders.aggregate(pipeline, maxTimeMS=120000))
        end_time = time.time()

        total_time = end_time - start_time
        print(f"\nNumber of messages where the sender's credit: 0 is {messages_count[0]['0']}")
        print(f"Query 3 execution time = {total_time} seconds")

    except pymongo.errors.ExecutionTimeout as e:
        print(f"\nError: {e}")
def Q4():
    """
    QUERY 4: Double the credit of all senders whose credit is less than 100.
    """
    start_time = time.time()
    double = db.senders.update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}})

    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nSuceessfully doubled the credit of all senders whose credit is less than 100")
    print(f"Query 4 execution time = {end_time - start_time} seconds")




# Get port number as a command line argument from ALL files
if len(sys.argv) > 1:
    port_number = sys.argv[1]
else:
    print("Please provide the port number as a command line argument")
    sys.exit(1)

# Try to connect
try:
    # Create the connection with the port_number recieved as a command line
    client = MongoClient(f"mongodb://localhost:{port_number}")
    db = client["MP2Norm"]
    messages = db["messages"]
    senders = db["senders"]
    print("Connected to MongoDB")
except pymongo.errors.ConnectionError as e:
    print(f"error: {e} ")
    sys.exit(1)

#----------------------------------------------------------------------------------------------
# STEP 3: MongoDB query
#----------------------------------------------------------------------------------------------
#call to query 1
Q1()

#call to query 2
Q2()

#call to query 3
Q3()

#call to query 4
Q4()

#----------------------------------------------------------------------------------------------
# STEP 4: Create indices to make queries faster. overall, improve efficiency
#----------------------------------------------------------------------------------------------
try:
    #create the sender index from messages collecion field
    db.messages.create_index([("sender", 1)])

    #create the text index from messages collection field
    db.messages.create_index([("text", "text")])

    #create the sender_id index from senders collection field
    db.senders.create_index([("sender_id", 1)])

    print("\ncreated the indexes for the collections")
except pymongo.errors.OperationFailure as e:
    print(f"\nerror: {e}")
except Exception as e:
    print(f"\nerror: {e}")


#----------------------------------------------------------------------------------------------
# re-run queries 1, 2, 3 for runtime check:
#----------------------------------------------------------------------------------------------

print("\nrunning queries again with indexes:")

# re-run Q1
Q1()

#re-run Q2
Q2()

#re-run Q3
Q3()



try:
    #drop all indexes that were made for both collections
    db.senders.drop_indexes()
    db.messages.drop_indexes()
    print("\nsuccessfuly dropped the indexes")
except pymongo.errors.OperationFailure as e:
    print(f"\nerror: {e} ")
except Exception as e:
    print(f"\nerror: {e} ")
