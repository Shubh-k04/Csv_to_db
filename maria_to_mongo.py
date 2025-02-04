import mysql.connector
from pymongo import MongoClient

# MySQL (MariaDB) Connection Setup
mariadb_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rootpassword',
    'database': 'csv_dump',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_general_ci'
}

# MongoDB Connection Setup
mongo_client = MongoClient('mongodb://root:rootpassword@localhost:27017/')
mongo_db = mongo_client['csv_dump']  # MongoDB Database
mongo_collection = mongo_db['filtered_data']  # Target Collection in MongoDB

# Batch size for fetching MySQL data
batch_size = 10000  

try:
    print("Connecting to MySQL...")
    mydb = mysql.connector.connect(**mariadb_config)
    cursor = mydb.cursor(dictionary=True)  # Use dictionary cursor to get column names
    print("Connected to MySQL")

    # Get total number of rows
    cursor.execute("SELECT COUNT(*) AS total FROM filtered_data")
    total_rows = cursor.fetchone()['total']
    print(f"Total rows in MySQL: {total_rows}")

    # Fetch and insert in batches
    offset = 0
    while offset < total_rows:
        print(f"Fetching rows {offset} to {offset + batch_size} from MySQL...")
        
        # Fetch batch of data
        cursor.execute(f"SELECT * FROM filtered_data LIMIT {batch_size} OFFSET {offset}")
        rows = cursor.fetchall()
        
        if not rows:
            break  # Stop if no more rows

        # Insert into MongoDB
        mongo_collection.insert_many(rows)
        print(f"Inserted {len(rows)} rows into MongoDB.")

        offset += batch_size  # Update offset for next batch

    print("Data transfer from MySQL to MongoDB completed successfully!")

except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")
except Exception as e:
    print(f"Error: {e}")
finally:
    if cursor:
        cursor.close()
    if mydb.is_connected():
        mydb.close()
    mongo_client.close()
    print("Connections closed.")
