import duckdb
import time
from datetime import datetime
from tabulate import tabulate

# 🌐 Global connection
con = duckdb.connect(database='function_metrics.db', read_only=False)

def setup_database():
    con.execute("""
    CREATE TABLE IF NOT EXISTS function_metrics (
        sno INTEGER PRIMARY KEY,  -- Auto-incrementing serial number
        function_name VARCHAR,
        start_time VARCHAR,  -- Store as formatted string
        duration_ms INTEGER,
        status VARCHAR,
        error VARCHAR
    );
    """)
    print("✅ Metrics database setup complete.")

def format_timestamp(dt):
    return dt.strftime('%B %d %H:%M:%S.%f')[:-3]  # Only keep 3 decimal places for milliseconds

def log_metric(function_name, start_time, end_time, status, error=None):
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    formatted_start = format_timestamp(start_time)
    
    # Get next serial number
    next_sno = con.execute("SELECT COALESCE(MAX(sno), 0) + 1 FROM function_metrics").fetchone()[0]
    
    con.execute("""
    INSERT INTO function_metrics 
    (sno, function_name, start_time, duration_ms, status, error)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (next_sno, function_name, formatted_start, duration_ms, status, error))

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
    results = con.execute("""
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
