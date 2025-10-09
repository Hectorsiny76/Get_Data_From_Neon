import os
import psycopg2
from fastapi import FastAPI, Header, HTTPException
from dotenv import load_dotenv

# >>>>> 1. IMPORT THE CORS MIDDLEWARE <<<<<
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()
app = FastAPI()

# >>>>> 2. ADD THE MIDDLEWARE TO YOUR APP <<<<<
# This tells your API to allow requests from any origin (*).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Get your secrets from the environment
DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_API_KEY = os.getenv("UPLOADER_API_KEY")

# ===================================================================
# Your existing PUBLIC endpoint to READ data with time filters.
# ===================================================================
@app.get("/sensor_data")
def read_sensor_data(time_range: str = "7d"):
    """
    Fetches records from the sensor_data table based on a time range.
    Valid time_range values: 'today', '7d', '30d'.
    """
    sql_query = """
        SELECT id, timestamp, temperature, humidity, latitude, longitude, fire_score 
        FROM sensor_data
    """
    
    if time_range == "today":
        sql_query += " WHERE timestamp >= NOW()::date"
    elif time_range == "7d":
        sql_query += " WHERE timestamp >= NOW() - INTERVAL '7 days'"
    elif time_range == "30d":
        sql_query += " WHERE timestamp >= NOW() - INTERVAL '30 days'"
    else:
        raise HTTPException(
            status_code=400, 
            detail="Invalid time_range. Use 'today', '7d', or '30d'."
        )
        
    sql_query += " ORDER BY timestamp DESC;"

    conn = None
    try:
        if not DATABASE_URL:
            raise HTTPException(status_code=500, detail="Database URL is not configured.")
            
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(sql_query)
        
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        data = [dict(zip(column_names, row)) for row in rows]

        cur.close()
        return data

    except Exception as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch data from the database.")

    finally:
        if conn is not None:
            conn.close()

# ===================================================================
# The new PRIVATE endpoint for the uploader script to WRITE data.
# ===================================================================
@app.post("/data")
async def create_upload(data: dict, x_api_key: str = Header(None)):
    # 1. Security Check: Validate the API key
    if x_api_key != SECRET_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    conn = None
    try:
        # 2. Extract data from the request
        ts_id = data.get('thingspeak_id')
        ts = data.get('created_at')
        temp = data.get('temperature')
        hum = data.get('humidity')
        lat = data.get('latitude')
        lon = data.get('longitude')
        score = data.get('fire_score')
        
        # 3. Insert the new data into the database
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            sql = """
                INSERT INTO sensor_data (timestamp, temperature, humidity, latitude, longitude, fire_score, thingspeak_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (thingspeak_id) DO NOTHING;
            """
            cur.execute(sql, (ts, temp, hum, lat, lon, score, ts_id))
        conn.commit()
        
        print(f"Successfully inserted/updated data for ThingSpeak ID: {ts_id}")
        return {"status": "success", "data_received": data}
        
    except Exception as e:
        print(f"Database write error: {e}")
        raise HTTPException(status_code=500, detail="Failed to write data.")
    finally:
        if conn:
            conn.close()