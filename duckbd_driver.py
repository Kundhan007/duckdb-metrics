import duckdb
import time
from datetime import datetime
from tabulate import tabulate
import atexit

# 🌐 Global connection variable, initially None
con = None

def get_connection():
    """Gets the existing DuckDB connection or creates a new one."""
    global con
    if con is None:
        try:
            print("Attempting to connect to DuckDB...")
            con = duckdb.connect(database='function_metrics.db', read_only=False)
            print("DuckDB connection successful. Setting up database...")
            # Ensure the table exists when the connection is first made
            setup_database(con) 
        except duckdb.IOException as e:
            print(f"Failed to connect to DuckDB due to lock: {e}")
            # In a multi-process scenario like Flask reloader, 
            # returning None might be better than raising immediately.
            # The calling function should handle the None case.
            return None 
        except Exception as e:
            print(f"An unexpected error occurred connecting to DuckDB: {e}")
            return None
    return con

def close_connection():
    """Closes the DuckDB connection if it exists."""
    global con
    if con is not None:
        try:
            print("Closing DuckDB connection...")
            con.close()
            con = None
            print("DuckDB connection closed.")
        except Exception as e:
            print(f"Error closing DuckDB connection: {e}")

# Register cleanup on program exit
atexit.register(close_connection)

def setup_database(db_conn):
    """Sets up the necessary table in the database."""
    if db_conn is None:
        print("Cannot setup database, connection is None.")
        return
    try:
        db_conn.execute("""
        CREATE TABLE IF NOT EXISTS function_metrics (
            sno INTEGER PRIMARY KEY,  -- Auto-incrementing serial number
            function_name VARCHAR,
            start_time VARCHAR,  -- Store as formatted string
            duration_ms INTEGER,
            status VARCHAR,
            error VARCHAR
        );
        """)
        print("✅ Metrics database table verified/created.")
    except Exception as e:
        print(f"Error setting up database table: {e}")
        # Don't raise here, allow the app to potentially continue

def format_timestamp(dt):
    return dt.strftime('%B %d %H:%M:%S.%f')[:-3]  # Only keep 3 decimal places for milliseconds


def log_metric(function_name, start_time, end_time, status, error=None):
    conn = get_connection()
    if conn is None:
        print(f"Skipping metric log for {function_name}: No DB connection.")
        return # Can't log if connection failed

    try:
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        formatted_start = format_timestamp(start_time)
        
        # Get next serial number
        next_sno_result = conn.execute("SELECT COALESCE(MAX(sno), 0) + 1 FROM function_metrics").fetchone()
        if next_sno_result is None:
             print(f"Could not determine next sno for function_metrics.")
             return
        next_sno = next_sno_result[0]

        print(next_sno, function_name, formatted_start, duration_ms, status, error)
        conn.execute("""
        INSERT INTO function_metrics 
        (sno, function_name, start_time, duration_ms, status, error)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (next_sno, function_name, formatted_start, duration_ms, status, error))
    except duckdb.IOException as e:
        print(f"Database locked, cannot log metric for {function_name}: {e}")
    except Exception as e:
        print(f"Error logging metric for {function_name}: {e}")

# to execute the function with metrics
# this is the decorator use it like this: @execute_with_metrics
def execute_with_metrics(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        try:
            result = func(*args, **kwargs)
            end_time = datetime.now()
            log_metric(func.__name__, start_time, end_time, "success")
            return result
        except Exception as e:
            end_time = datetime.now()
            log_metric(func.__name__, start_time, end_time, "error", str(e))
            raise

    return wrapper


# ------------------------------------this is for view -----------------------------------------


# to view the metrics
def get_metrics():
    conn = get_connection()
    if conn is None:
        return "Unable to fetch metrics - database connection failed."

    try:
        results = conn.execute("""
        SELECT 
            sno,
            function_name,
            start_time,
            duration_ms,
            status,
            COALESCE(error, '-') as error
        FROM function_metrics
        ORDER BY sno DESC
        LIMIT 10
        """).fetchall()
        headers = ['S.No', 'Function', 'Start Time', 'Duration (ms)', 'Status', 'Error']
        return tabulate(results, headers=headers, tablefmt='grid')
    except duckdb.IOException as e:
        print(f"Database locked, cannot fetch metrics: {e}")
        return "Unable to fetch metrics - database is locked."
    except Exception as e:
        print(f"Error getting metrics: {e}")
        # Return error string instead of raising, maybe more resilient for display
        return f"Error fetching metrics: {e}"

# ------------------------------------sample usage from here-----------------------------------------

# # Example usage
# @execute_with_metrics
# def sample_function():
#     # Simulate some work
#     time.sleep(1)
#     return "Success"

# @execute_with_metrics
# def failing_function():
#     # Simulate a failure
#     raise Exception("Sample error")

# if __name__ == '__main__':
#     setup_database()
    
#     # Run multiple times to show different timestamps
#     for _ in range(3):
#         try:
#             print(f"\nRunning sample function...")
#             sample_function()
#             time.sleep(0.5)  # Add small delay between runs
#         except Exception as e:
#             print(f"Error in sample function: {e}")
    
#     try:
#         print("\nRunning failing function...")
#         failing_function()
#     except Exception as e:
#         print(f"Error in failing function: {e}")
    
#     # Display metrics
#     print("\n📊 Function Execution Metrics (Last 10 calls):")
#     print(get_metrics())
