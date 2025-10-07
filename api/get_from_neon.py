# This script is designed to be deployed as a serverless function on a platform like Vercel.
# It acts as a secure intermediary between your public website and your private database.

import os
import psycopg2
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

class handler(BaseHTTPRequestHandler):
    """
    Handles incoming HTTP requests and responds with sensor data from the Neon database.
    It now supports filtering data by a time period using a URL query parameter.
    Example: /api/get_sensor_data?period=today
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
            # --- 1. Parse the URL to get query parameters ---
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            period = query_params.get('period', [None])[0] # Get the 'period' value, default to None

            # --- 2. Build the SQL Query Dynamically ---
            # IMPORTANT: We use parameterized queries (%s) to prevent SQL injection attacks.
            base_sql = "SELECT timestamp, temperature, humidity, latitude, longitude, fire_score FROM sensor_data"
            
            if period == 'today':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '1 day' ORDER BY timestamp DESC;"
            elif period == '7days':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '7 days' ORDER BY timestamp DESC;"
            elif period == '30days':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '30 days' ORDER BY timestamp DESC;"
            else:
                # Default behavior: get the 20 most recent entries
                sql_query = f"{base_sql} ORDER BY timestamp DESC LIMIT 20;"

            # --- 3. Connect and Execute ---
            print(f"Connecting to database to execute query for period: {period or 'default'}")
            conn = psycopg2.connect(db_url)
            with conn.cursor() as cur:
                print(f"Executing SQL: {cur.mogrify(sql_query, []).decode('utf-8')}") # Log the query for debugging
                cur.execute(sql_query)
                rows = cur.fetchall()
                print(f"Fetched {len(rows)} records.")
                
                colnames = [desc[0] for desc in cur.description]
                result = [dict(zip(colnames, row)) for row in rows]
                
                for record in result:
                    record['timestamp'] = record['timestamp'].isoformat()
                
                self._send_response(200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, json.dumps(result))

        except psycopg2.Error as e:
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "A database error occurred.", "details": str(e)}))
        except Exception as e:
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "An unexpected server error occurred.", "details": str(e)}))
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")
    
    def _send_response(self, status_code, headers, body):
        self.send_response(status_code)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

