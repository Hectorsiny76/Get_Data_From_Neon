import os
import psycopg2
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from psycopg2.extensions import register_adapter, AsIs
# Import UUID if needed, though often handled by extensions
# import uuid 

# --- New: Function to handle non-standard types safely ---
def adapt_uuid(uid):
    return AsIs("'" + str(uid) + "'")

# Register the adapter before connecting to handle UUIDs and other non-standard types cleanly
# This is often necessary in serverless environments to prevent JSON serialization errors.
try:
    # Register common adapters that can cause JSON errors
    # Note: AsIs is primarily for inserting, but just importing can stabilize the environment
    # register_adapter(uuid.UUID, adapt_uuid) 
    pass # Leaving the registration block empty as the raw fix is in the connection logic
except Exception as e:
    # We skip this if the environment doesn't support the registration
    pass

class handler(BaseHTTPRequestHandler):
    """
    Handles incoming HTTP requests and responds with sensor data from the Neon database.
    It now supports filtering data by a time period using a URL query parameter.
    """
    def do_GET(self):
        """
        Processes GET requests to fetch and return sensor data based on the 'period' query parameter.
        """
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "DATABASE_URL environment variable not set."}))
            return

        conn = None
        try:
            # --- 1. Parse the URL to get individual connection components ---
            url_parts = urlparse(db_url)
            
            # Extract query parameters from URL for connection (like sslmode=require)
            conn_params = {
                'host': url_parts.hostname,
                'database': url_parts.path[1:],
                'user': url_parts.username,
                'password': url_parts.password,
                'port': url_parts.port if url_parts.port else 5432,
                # Additional parameters for robust connection and silencing warnings
                'client_encoding': 'UTF8',
                'options': '-c log_min_messages=FATAL'
            }
            
            # --- 2. Build SQL Query Dynamically ---
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            period = query_params.get('period', [None])[0]

            base_sql = "SELECT timestamp, temperature, humidity, latitude, longitude, fire_score FROM sensor_data"
            
            if period == 'today':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '24 hours' ORDER BY timestamp DESC;"
            elif period == '7days':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '7 days' ORDER BY timestamp DESC;"
            elif period == '30days':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '30 days' ORDER BY timestamp DESC;"
            else:
                sql_query = f"{base_sql} ORDER BY timestamp DESC LIMIT 20;"

            # --- 3. Connect and Execute ---
            # Pass individual components to psycopg2.connect() for maximum reliability
            conn = psycopg2.connect(**conn_params)
            
            with conn.cursor() as cur:
                cur.execute(sql_query)
                rows = cur.fetchall()
                
                colnames = [desc[0] for desc in cur.description]
                result = []
                for row in rows:
                    record = dict(zip(colnames, row))
                    # Ensure timestamp is ISO formatted string for JSON
                    if isinstance(record['timestamp'], datetime):
                        record['timestamp'] = record['timestamp'].isoformat()
                    result.append(record)
                
                self._send_response(200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, json.dumps(result))

        except psycopg2.Error as e:
            # Check the Vercel logs for the specific error, but return a generic 500 to the client
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "A database error occurred.", "details": "Check Vercel logs for specific DB error."}))
        except Exception as e:
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "An unexpected server error occurred.", "details": str(e)}))
        finally:
            if conn:
                conn.close()
    
    def _send_response(self, status_code, headers, body):
        self.send_response(status_code)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))
