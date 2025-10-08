import os
import psycopg2
from fastapi import FastAPI, HTTPException

# Initialize the FastAPI app
app = FastAPI()

# Load the database connection string from an environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

# Define an API endpoint that accepts a time_range parameter
# We'll set a default value of '7d' (7 days) if no parameter is provided.
@app.get("/sensor_data")
def read_sensor_data(time_range: str = "7d"):
    """
    Fetches records from the sensor_data table based on a time range.
    Valid time_range values: 'today', '7d', '30d'.
    """
    
    # Base SQL query
    # It's good practice to list columns explicitly instead of using '*'
    sql_query = """
        SELECT id, timestamp, temperature, humidity, latitude, longitude, fire_score 
        FROM sensor_data
    """
    
    # Add the correct WHERE clause based on the time_range parameter.
    # PostgreSQL's INTERVAL syntax is perfect for this.
    if time_range == "today":
        # Get all records from the beginning of today
        sql_query += " WHERE timestamp >= NOW()::date"
    elif time_range == "7d":
        # Get all records from the last 7 days
        sql_query += " WHERE timestamp >= NOW() - INTERVAL '7 days'"
    elif time_range == "30d":
        # Get all records from the last 30 days
        sql_query += " WHERE timestamp >= NOW() - INTERVAL '30 days'"
    else:
        # If an invalid time_range is provided, return an error
        raise HTTPException(
            status_code=400, 
            detail="Invalid time_range. Use 'today', '7d', or '30d'."
        )
        
    # Add ordering to get the most recent data first
    sql_query += " ORDER BY timestamp DESC;"

    conn = None
    try:
        if not DATABASE_URL:
            raise HTTPException(status_code=500, detail="Database URL is not configured.")

        # Connect to your Neon database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Execute the final, constructed query
        cur.execute(sql_query)
        
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        
        # Format the data into a list of dictionaries (JSON-friendly)
        data = [dict(zip(column_names, row)) for row in rows]

        cur.close()
        return data

    except Exception as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch data from the database.")

    finally:
        if conn is not None:
            conn.close()