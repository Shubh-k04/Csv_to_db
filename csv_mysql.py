import os
import mysql.connector
import pandas as pd
from flask import Flask, request, jsonify

app = Flask(__name__)

# Hardcoded file path for standalone execution
FILE_PATH = r"C:\Users\SCG\Downloads\filtered_data.csv"

# MariaDB Database Credentials
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "csv_dump",
    "port": 3306,
    "charset": "utf8mb4",
    "use_unicode": True,
    "collation": "utf8mb4_general_ci"
}

def import_csv_to_maria(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return

        print("Connecting to MariaDB...")
        mydb = mysql.connector.connect(**DB_CONFIG)
        cursor = mydb.cursor()
        
        # Set proper character set and collation
        cursor.execute("SET NAMES utf8mb4")
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET character_set_connection=utf8mb4")
        print("Connected successfully!")

        print("Reading CSV file...")
        df = pd.read_csv(file_path)
        # Drop columns where all values are NaN
        df = df.dropna(axis=1, how='all')
        
        df.columns = [col.replace(" ", "_").replace("-", "_").replace(".", "_").replace("nan", "") 
                     for col in df.columns]

        print("\nColumns in CSV:", df.columns.tolist())
        print("\nChecking for NaN values:")
        print(df.isna().sum())
        
        table_name = "filtered_data"

        # Drop existing table
        print("\nDropping existing table...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        mydb.commit()

        # Improved type mapping based on your data
        type_mapping = {
            'object': 'VARCHAR(255)',
            'int64': 'BIGINT',
            'float64': 'DOUBLE',  # Changed from FLOAT to DOUBLE for better precision
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME',
            'timedelta64': 'TIME',
            'category': 'VARCHAR(255)'
        }

        print("\nCreating new table...")
        create_table_query = f"CREATE TABLE {table_name} ("
        
        # Print data types for debugging
        print("\nColumn data types:")
        for col in df.columns:
            col_type = df[col].dtype.name
            print(f"{col}: {col_type}")
            
            # Determine SQL type based on column content
            if col_type == 'float64' and df[col].isna().sum() > df.shape[0] * 0.5:
                # If more than 50% are NaN, use VARCHAR
                sql_type = 'VARCHAR(255)'
            else:
                sql_type = type_mapping.get(col_type, 'VARCHAR(255)')
            
            create_table_query += f"`{col}` {sql_type} NULL, "
            
        create_table_query = create_table_query.rstrip(', ') + ") CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"

        print("\nExecuting CREATE TABLE query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        mydb.commit()
        print("Table created successfully!")

        # Insert Data in Chunks
        chunksize = 25000  # Reduced chunk size
        total_rows = 0

        print("\nStarting data import...")
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk = chunk.dropna(axis=1, how='all')
            chunk.columns = [col.replace(" ", "_").replace("-", "_").replace(".", "_").replace("nan", "") 
                           for col in chunk.columns]

            # Prepare insert query with backticks
            columns = ", ".join([f"`{col}`" for col in chunk.columns])
            placeholders = ", ".join(['%s'] * len(chunk.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # Convert chunk to list of tuples with proper NULL handling
            data = [
                tuple(None if pd.isna(value) else 
                      str(value) if isinstance(value, float) and (abs(value) > 1e308 or pd.isna(value)) 
                      else value for value in row)
                for row in chunk.values
            ]

            try:
                cursor.executemany(sql, data)
                mydb.commit()
                total_rows += len(chunk)
                print(f"Inserted {total_rows} rows so far...")
            except mysql.connector.Error as err:
                print(f"Error inserting chunk: {err}")
                print(f"Query was: {sql}")
                print(f"First row of data: {data[0] if data else 'No data'}")
                mydb.rollback()
                raise

        print(f"\nImport completed successfully! Total rows imported: {total_rows}")
        return {"message": f"CSV data imported successfully! Total rows: {total_rows}"}

    except mysql.connector.Error as err:
        error_msg = f"Database error: {str(err)}"
        print(error_msg)
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        return {"error": error_msg}
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'mydb' in locals() and mydb.is_connected():
            mydb.close()
            print("Database connection closed.")

# Flask route for Docker deployment
@app.route('/import_csv_to_maria', methods=['POST'])
def import_csv_to_maria_route():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part in request"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        # Save the file temporarily
        file_path = os.path.join("/app/uploads", file.filename)
        os.makedirs("/app/uploads", exist_ok=True)
        file.save(file_path)

        # Import the file
        result = import_csv_to_maria(file_path)

        # Clean up
        os.remove(file_path)

        if "error" in result:
            return jsonify(result), 500
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print(f"Starting import process for file: {FILE_PATH}")
    import_csv_to_maria(FILE_PATH)
