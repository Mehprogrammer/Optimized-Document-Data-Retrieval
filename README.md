# Optimized Document Data Retireval - Winter 2024  
Group member names and github usernames(3-4 members)  
  yaqoobi, Mosa Yaqoobi  
  drae, Drae Yong  
  talal2, Talal Khan  
  mjmemon, Junaid Memon

# Group work break-down strategy
Split into two teams:

Team 1 - Worked on Task 1
- Drae    
- Mosa    

Team 2 - Worked on Task 2
- Junaid
- Talal
  


# Code execution guide
- The four main files must be run via python3 <the_file_name> <port_key>

Example: running task1_build.py where the port key for my database is 27017 -> the command in the terminal would be: "python3 task1_build.py 27017".
## Overview
This project demonstrates the use of indexing and data embedding techniques in MongoDB to optimize the performance of database queries. It provides insights into how different data models (normalized vs. embedded) and indexing strategies affect query execution times, especially in the context of large datasets.

## Files
- **task1_build.py**: Script for building and populating the MongoDB collections using a normalized data model (separate collections for messages and senders).
- **task1_query.py**: Script for executing queries and measuring their execution times on the normalized data model, both with and without indexing.
- **task2_build.py**: Script for building and populating the MongoDB collection using an embedded data model (sender information embedded within messages).
- **task2_query.py**: Script for executing queries and measuring their execution times on the embedded data model.

## Setup and Installation
1. Install MongoDB and ensure it is running on your local machine.
2. Clone the repository and navigate to the project directory.
3. Ensure you have Python installed, along with the necessary dependencies:
    ```bash
    pip install pymongo
    ```
4. To run the scripts, use the following command structure:
    ```bash
    python task1_build.py <PORT_NUMBER>
    python task1_query.py <PORT_NUMBER>
    python task2_build.py <PORT_NUMBER>
    python task2_query.py <PORT_NUMBER>
    ```

## How It Works

### Task 1: Normalized Data Model with Indexing
- The `task1_build.py` script reads data from `messages.json` and `senders.json`, inserts it into separate MongoDB collections, and then builds indices on specific fields.
- The `task1_query.py` script runs four queries on the data and logs the execution times both before and after indexing.
  
### Task 2: Embedded Data Model
- The `task2_build.py` script reads the same data and embeds the sender information directly into each message document before inserting it into MongoDB.
- The `task2_query.py` script runs the same set of queries as in Task 1 and compares the execution times against the normalized data model.

## Key Observations
1. **Indexing**: Indexing the sender and text fields in the normalized model significantly improved the performance of queries involving those fields, especially for lookups based on the sender's credit.
2. **Text Search**: Regex-based text searches did not benefit much from indexing due to limitations in MongoDB's text indexing capabilities.
3. **Embedded Model**: The embedded model showed slower performance for aggregation queries, such as finding the most active sender, due to the overhead of processing embedded documents.
4. **Query Performance**: The normalized model, with indexing, generally outperformed the embedded model for complex queries involving sorting, grouping, or joining data.



