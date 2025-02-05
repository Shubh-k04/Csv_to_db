from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId
import mysql.connector
from pymongo import MongoClient
from werkzeug.utils import secure_filename
import pandas as pd

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# MySQL (MariaDB) Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = (
    f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?charset=utf8mb4"
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# MongoDB Connection Setup
mongo_client = MongoClient(
    f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@"
    f"{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/"
)
mongo_db = mongo_client[os.getenv('MONGO_DB_NAME')]  # MongoDB Database
mongo_collection = mongo_db['filtered_data']  # Target Collection in MongoDB

# File upload configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Database configuration for mysql.connector
DB_CONFIG = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_NAME'),
    "port": int(os.getenv('DB_PORT')),
    "charset": 'utf8mb4',
    "collation": 'utf8mb4_general_ci'
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def import_csv_to_maria(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return {"error": "File not found"}

        print("Connecting to MariaDB...")
        mydb = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            port=int(os.getenv('DB_PORT')),
            charset='utf8mb4',
            use_unicode=True,
            collation='utf8mb4_general_ci'
        )
        cursor = mydb.cursor(buffered=True)
        
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

        # Improved type mapping
        type_mapping = {
            'object': 'VARCHAR(255)',
            'int64': 'BIGINT',
            'float64': 'DOUBLE',
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
        create_table_query = create_table_query.rstrip(', ') + ") CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"

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

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # Save file in chunks
            chunk_size = 8192  # 8KB chunks
            with open(filepath, 'wb') as f:
                while True:
                    chunk = file.stream.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
            
            return jsonify({
                'message': 'File uploaded successfully',
                'filename': filename,
                'options': [
                    {'route': '/import_csv_to_maria', 'description': 'Import to MariaDB'},
                    {'route': '/import_csv_to_mongo', 'description': 'Import to MongoDB'}
                ]
            }), 200
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def hello():
    return "Flask server is running!"

@app.route('/import_csv_to_mongo', methods=['POST'])
def import_csv_to_mongo():
    try:
        filename = request.json.get('filename')
        if not filename:
            return jsonify({"error": "Filename not provided"}), 400
            
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(filename))
        if not os.path.exists(filepath):
            return jsonify({"error": "File not found"}), 404

        # Import the data using pandas and insert into MongoDB in chunks
        df_iter = pd.read_csv(filepath, chunksize=1000)
        total_rows = 0
        for chunk in df_iter:
            records = chunk.to_dict('records')
            mongo_collection.insert_many(records)
            total_rows += len(records)
        
        # Clean up the uploaded file
        os.remove(filepath)
        
        return jsonify({
            "message": "CSV data imported to MongoDB successfully!",
            "total_rows": total_rows
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/import_csv_to_maria', methods=['POST'])
def import_csv_to_maria_route():
    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            # Check if filename was provided in JSON
            if not request.json or 'filename' not in request.json:
                return jsonify({"error": "No file uploaded and no filename provided"}), 400
            
            # Use existing file from upload folder
            filename = request.json.get('filename')
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(filename))
            
            if not os.path.exists(file_path):
                return jsonify({"error": "File not found"}), 404
        else:
            # Handle direct file upload
            file = request.files['file']
            if file.filename == '':
                return jsonify({"error": "No selected file"}), 400
                
            # Save the uploaded file
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            file.save(file_path)

        try:
            # Import the file using the function above
            result = import_csv_to_maria(file_path)
            
            # Clean up the file after processing
            if os.path.exists(file_path):
                os.remove(file_path)
                
            if "error" in result:
                return jsonify(result), 500
            return jsonify(result), 200

        except Exception as e:
            # Clean up file if there's an error during processing
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_mongo_data', methods=['GET'])
def get_mongo_data():
    try:
        # Get page and limit from query params (default to page 1 and limit 1000)
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 1000))

        # Skip rows based on the page number
        skip = (page - 1) * limit

        # Fetch the data from MongoDB with pagination
        data = mongo_collection.find().skip(skip).limit(limit)

        # Convert ObjectId to string and format the response
        result = []
        for doc in data:
            doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
            result.append(doc)

        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_maria_data', methods=['GET'])
def get_maria_data():
    mydb = None
    cursor = None
    try:
        # Get pagination parameters from query string
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        
        # Calculate offset
        offset = (page - 1) * per_page

        # Connect to MariaDB
        mydb = mysql.connector.connect(**DB_CONFIG)
        cursor = mydb.cursor(dictionary=True, buffered=True)

        # Get total count of records
        cursor.execute("SELECT COUNT(*) as total FROM filtered_data")
        total_records = cursor.fetchone()['total']

        # Get paginated data
        cursor.execute(
            "SELECT * FROM filtered_data LIMIT %s OFFSET %s",
            (per_page, offset)
        )
        data = cursor.fetchall()

        # Calculate total pages
        total_pages = (total_records + per_page - 1) // per_page

        # Prepare response
        response = {
            "data": data,
            "pagination": {
                "current_page": page,
                "per_page": per_page,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "links": {
                "self": f"/get_maria_data?page={page}&per_page={per_page}",
                "first": f"/get_maria_data?page=1&per_page={per_page}",
                "last": f"/get_maria_data?page={total_pages}&per_page={per_page}",
                "next": f"/get_maria_data?page={page+1}&per_page={per_page}" if page < total_pages else None,
                "prev": f"/get_maria_data?page={page-1}&per_page={per_page}" if page > 1 else None
            }
        }

        return jsonify(response), 200

    except mysql.connector.Error as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if cursor is not None:
            cursor.close()
        if mydb is not None and mydb.is_connected():
            mydb.close()

@app.route('/update_mongo_data/<string:id>', methods=['PUT'])
def update_mongo_data(id):
    try:
        data = request.get_json()
        mongo_collection.update_one({'_id': ObjectId(id)}, {"$set": data})
        return jsonify({"message": "MongoDB data updated successfully!"}), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/delete_mongo_data/<string:id>', methods=['DELETE'])
def delete_mongo_data(id):
    try:
        # Validate ObjectId format
        if not ObjectId.is_valid(id):
            return jsonify({"error": "Invalid ObjectId format"}), 400
        
        # Attempt deletion
        result = mongo_collection.delete_one({'_id': ObjectId(id)})
        
        if result.deleted_count == 0:
            return jsonify({"message": "MongoDB document not found"}), 404
        
        return jsonify({"message": "MongoDB data deleted successfully!"}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update_maria_data', methods=['PUT'])
def update_maria_data():
    mydb = None
    try:
        # Expecting a JSON body with keys: where_column, where_value, update_column, update_value
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body is missing"}), 400

        where_column = data.get('where_column')
        where_value = data.get('where_value')
        update_column = data.get('update_column')
        update_value = data.get('update_value')

        if not where_column or where_value is None:
            return jsonify({"error": "Missing where_column or where_value in request body"}), 400
        if not update_column or update_value is None:
            return jsonify({"error": "Missing update_column or update_value in request body"}), 400

        # Connect to MariaDB
        mydb = mysql.connector.connect(**DB_CONFIG)

        # Check if where_column exists
        with mydb.cursor(dictionary=True, buffered=True) as schema_cursor:
            schema_cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'filtered_data' AND COLUMN_NAME = %s",
                (os.getenv('DB_NAME'), where_column)
            )
            if not schema_cursor.fetchone():
                return jsonify({"error": f"Unknown column in where clause: {where_column}"}), 400

        # Check if update_column exists and get its data type
        with mydb.cursor(dictionary=True, buffered=True) as schema_cursor:
            schema_cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'filtered_data' AND COLUMN_NAME = %s",
                (os.getenv('DB_NAME'), update_column)
            )
            column_info = schema_cursor.fetchone()
            if not column_info:
                return jsonify({"error": f"Unknown column: {update_column}"}), 400

            col_type = column_info['DATA_TYPE'].lower()
            try:
                if col_type in ('decimal', 'int', 'bigint', 'float', 'double'):
                    if isinstance(update_value, str) and update_value.lower() == 'null':
                        processed_value = None
                    else:
                        processed_value = float(update_value) if col_type in ('decimal', 'float', 'double') else int(update_value)
                else:
                    processed_value = update_value
            except ValueError:
                return jsonify({"error": f"Invalid value for column {update_column}: {update_value}"}), 400

        # First validate that a record with the given where_value exists
        with mydb.cursor(dictionary=True, buffered=True) as check_cursor:
            check_query = f"SELECT * FROM filtered_data WHERE `{where_column}` = %s"
            check_cursor.execute(check_query, (where_value,))
            existing_record = check_cursor.fetchone()
            if not existing_record:
                return jsonify({"error": "Record not found"}), 404

        # Perform update
        with mydb.cursor(buffered=True) as update_cursor:
            update_query = f"UPDATE filtered_data SET `{update_column}` = %s WHERE `{where_column}` = %s"
            update_cursor.execute(update_query, (processed_value, where_value))
            mydb.commit()

        # Fetch updated record
        with mydb.cursor(dictionary=True, buffered=True) as fetch_cursor:
            fetch_cursor.execute(check_query, (where_value,))
            updated_record = fetch_cursor.fetchone()

        return jsonify({
            "message": "Record updated successfully",
            "updated_record": updated_record
        }), 200

    except mysql.connector.Error as err:
        if mydb:
            mydb.rollback()
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except Exception as e:
        if mydb:
            mydb.rollback()
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if mydb:
            mydb.close()

@app.route('/delete_maria_data', methods=['DELETE'])
def delete_maria_data():
    mydb = None
    cursor = None
    try:
        # Expecting a JSON body with keys: unique_column, unique_value
        data = request.get_json()
        if not data or not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON delete data provided"}), 400

        unique_column = data.get('unique_column')
        unique_value = data.get('unique_value')

        if not unique_column:
            return jsonify({"error": "No unique column specified"}), 400
        if unique_value is None:
            return jsonify({"error": "No unique value provided"}), 400

        # Connect to MariaDB
        mydb = mysql.connector.connect(**DB_CONFIG)
        cursor = mydb.cursor(dictionary=True, buffered=True)

        # Check if unique_column exists
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'filtered_data' AND COLUMN_NAME = %s",
            (os.getenv('DB_NAME'), unique_column)
        )
        if not cursor.fetchone():
            return jsonify({"error": f"Unknown column: {unique_column}"}), 400

        # First check if record exists
        check_query = f"SELECT * FROM filtered_data WHERE `{unique_column}` = %s"
        cursor.execute(check_query, (unique_value,))
        if not cursor.fetchone():
            return jsonify({"error": "Record not found"}), 404

        # Delete query
        delete_query = f"DELETE FROM filtered_data WHERE `{unique_column}` = %s"
        cursor.execute(delete_query, (unique_value,))
        mydb.commit()

        return jsonify({
            "message": "Record deleted successfully",
            "rows_affected": cursor.rowcount
        }), 200

    except mysql.connector.Error as err:
        if mydb:
            mydb.rollback()
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except Exception as e:
        if mydb:
            mydb.rollback()
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if mydb and mydb.is_connected():
            mydb.close()

@app.route('/test', methods=['GET'])
def test():
    return "Server is running!"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
