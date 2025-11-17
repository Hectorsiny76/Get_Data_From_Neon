import os
import asyncio
from starlette.websockets import WebSocketState
import psycopg2
import json  # >>>>> 1. IMPORT JSON <<<<<
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks # >>>>> 2. IMPORT WEBSOCKETS <<<<<
from dotenv import load_dotenv
from typing import List, Optional  # >>>>> 3. IMPORT LIST FOR TYPE HINTING <<<<<
from datetime import datetime, timezone

# >>>>> We still need this middleware <<<<<
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()
app = FastAPI()

# >>>>> Your existing middleware setup (unchanged) <<<<<
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Get your secrets from the environment (unchanged)
DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_API_KEY = os.getenv("UPLOADER_API_KEY")

# ===================================================================
# >>>>> 4. ADD THE WEBSOCKET CONNECTION MANAGER <<<<<
# This class will manage all active client connections
# ===================================================================
class ConnectionManager:
    def __init__(self):
        # A list to hold all active WebSocket connections
        self.active_connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        # Accept the new connection
        await websocket.accept()
        # Add it to our list
        async with self._lock:
            self.active_connections.add(websocket)
        print("New client connected.")

    async def disconnect(self, websocket: WebSocket):
        # Remove the connection from the list
        async with self._lock:
            self.active_connections.discard(websocket)
        print("Client disconnected.")

    async def broadcast(self, message: str):
        # snapshot to avoid mutating while iterating
        async with self._lock:
            conns = list(self.active_connections)
            
        to_drop = []
        for ws in conns:
            try:
                if (ws.application_state == WebSocketState.CONNECTED
                    and ws.client_state == WebSocketState.CONNECTED):
                    await ws.send_text(message)
                else:
                    to_drop.append(ws)
            except Exception:
                to_drop.append(ws)
                
        if to_drop:
            async with self._lock:
                for ws in to_drop:
                    self.active_connections.discard(ws)

# Create a single, shared instance of the manager for our app
manager = ConnectionManager()


def coerce_ts(v):
    if not v:
        return datetime.now(timezone.utc)
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        # hande ISO8601 with z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


# ===================================================================
# >>>>> 5. ADD THE NEW WEBSOCKET ENDPOINT <<<<<
# This is where your websites/apps will connect to listen
# ===================================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Connect the client
    await manager.connect(websocket)
    try:
        # This loop just keeps the connection alive.
        # You could also use it to receive messages *from* the client if needed.
        while True:
            # Just keep the connection open
            await websocket.receive_text()  # acts like a heartbeat

    except WebSocketDisconnect:
        # When the client disconnects, remove them from the list
        await manager.disconnect(websocket)


# ===================================================================
# Your existing PUBLIC endpoint to READ data (UNCHANGED)
# This is still useful for loading historical data when a client first loads.
# ===================================================================
@app.get("/sensor_data")
def read_sensor_data(time_range: str = "30d", limit: int = 500, offset: int = 0):
    """
    Fetches records from the sensor_data table based on a time range.
    Valid time_range values: 'today', '7d', '30d'.
    """
    
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)
    
    
    base_sql = """
        SELECT id, timestamp, temperature, humidity, latitude, longitude, fire_score 
        FROM sensor_data
    """
    
    if time_range == "today":
        base_sql += " WHERE timestamp >= NOW()::date"
    elif time_range == "7d":
        base_sql += " WHERE timestamp >= NOW() - INTERVAL '7 days'"
    elif time_range == "30d":
        base_sql += " WHERE timestamp >= NOW() - INTERVAL '30 days'"
    else:
        raise HTTPException(
            status_code=400, 
            detail="Invalid time_range. Use 'today', '7d', or '30d'."
        )
        
    sql = base_sql + " ORDER BY timestamp DESC NULLS LAST, id DESC LIMIT %s OFFSET %s;"

    conn = None
    try:
        if not DATABASE_URL:
            raise HTTPException(status_code=500, detail="Database URL is not configured.")
            
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        cur = conn.cursor()
        cur.execute(sql, (limit, offset))
        
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
# >>>>> 6. YOUR MODIFIED ENDPOINT TO WRITE DATA <<<<<
# This is the endpoint your IoT uploader script calls.
# ===================================================================
@app.post("/data")
async def create_upload(data: dict, x_api_key: str = Header(None), background_tasks: BackgroundTasks = ...):
    # 1. Security Check: Validate the API key (unchanged)
    if x_api_key != SECRET_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    conn = None
    try:
        # 2. Extract data from the request (unchanged)
        ts_id = data.get('thingspeak_id')
        ts_raw = data.get('created_at')
        ts = coerce_ts(ts_raw)
        temp = data.get('temperature')
        hum = data.get('humidity')
        lat = data.get('latitude')
        lon = data.get('longitude')
        score = data.get('fire_score')
        
        # 3. Insert the new data into the database (unchanged)
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

        # >>>>> 4. NEW STEP: BROADCAST THE NEW DATA TO ALL LISTENERS <<<<<
        # We convert the 'data' dictionary into a JSON string to send it.
        # Broadcast afer responding
        payload = json.dumps(data)
        background_tasks.add_task(manager.broadcast, payload)
        
        return {"status": "success", "data_received": data}
        
    except Exception as e:
        print(f"Database write error: {e}")
        raise HTTPException(status_code=500, detail="Failed to write data.")
    finally:
        if conn:
            conn.close()
            
@app.get("/sensor_data/latest")
def read_latest(limit: int = 1):
    limit = max(1, min(limit, 1000))

    conn = None
    try:
        if not DATABASE_URL:
            raise HTTPException(status_code=500, detail="Database URL is not configured.")

        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, timestamp, temperature, humidity, latitude, longitude, fire_score, thingspeak_id
            FROM sensor_data
            ORDER BY timestamp DESC NULLS LAST, id DESC
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        print(f"/sensor_data/latest error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch latest data.")
    finally:
        if conn is not None:
            conn.close()
            
# Test the API
@app.get("/test_db")
def test_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sensor_data;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"status": "connected", "rows_in_table": count}
    except Exception as e:
        return {"status": "error", "detail": str(e)}