import json
import sys
from pymongo import MongoClient
import pymongo
import time

# Initialize connection to MongoDB
if len(sys.argv) > 1:
    port_number = sys.argv[1]
else:
    print("Please provide the port number as a command line argument")
    sys.exit(1)

try:
    client = MongoClient(f"mongodb://localhost:{port_number}")
    db = client["MP2Embd"]  # Use the database for the embedded document store
    messages = db["messages"]  # The messages collection now includes embedded sender info
    print("Connected to MongoDB")
except pymongo.errors.ConnectionError as e:
    print(f"error: {e}")
    sys.exit(1)

def Q1():
    """
    QUERY 1: Return the number of messages that contain “ant” in their text.
    This query remains unchanged because it only depends on the 'text' field of the messages.
    """
    try:
        pipeline = {"text": {"$regex": ".*ant.*"}}
        start_time = time.time()
        ant_count = messages.count_documents(pipeline, maxTimeMS=120000)
        end_time = time.time()
        print(f"\nNumber of messages that have 'ant' in their text: {ant_count}")
        print(f"Query 1 execution time = {end_time - start_time} seconds")
    except pymongo.errors.ExecutionTimeout as e:
        print(f"\nError: {e}")

def Q2():
    """
    QUERY 2: Find the sender who has sent the most messages.
    """
    try:
        pipeline = [
            {"$group": {"_id": "$sender_info.sender_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        start_time = time.time()
        most_messages = list(messages.aggregate(pipeline, maxTimeMS=120000))
        end_time = time.time()
        if most_messages:
            print(f"\n{most_messages[0]['_id']} sent the most messages, total messages: {most_messages[0]['count']}")
        else:
            print("\nNo data found.")
        print(f"Query 2 execution time = {end_time - start_time} seconds")
    except pymongo.errors.ExecutionTimeout as e:
        print(f"\nError: {e}")

def Q3():
    """
    QUERY 3: Return the number of messages where the sender’s credit is 0.
    Adjusted to directly filter embedded sender information.
    """
    try:
        pipeline = {"sender_info.credit":  0}
        start_time = time.time()
        count_result = messages.count_documents(pipeline, maxTimeMS=120000)
        #count_result = list(messages.aggregate(pipeline, maxTimeMS=120000))
        end_time = time.time()
        #count = count_result[0]['count'] if count_result else 0
        print(f"\nNumber of messages where the sender's credit is 0: {count_result}")
        print(f"Query 3 execution time = {end_time - start_time} seconds")
    except pymongo.errors.ExecutionTimeout as e:
        print(f"\nError: {e}")

def Q4():
    """
    QUERY 4: Double the credit of all senders whose credit is less than 100.
    """
    try:

        start_time = time.time()
        result = db.messages.update_many({"sender_info.credit": {"$lt": 100}},{"$mul" : {"sender_info.credit" : 2}})
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\nSuceessfully doubled the credit of all senders whose credit is less than 100")
        print(f"Query 4 execution time = {total_time} seconds")
    except pymongo.errors.ExecutionTimeout as e:
        print(f"\nError: {e}")


# Execute queries
Q1()
Q2()
Q3()
Q4()
