import os
import psycopg2
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

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
            # --- 1. Parse the URL to get query parameters ---
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            period = query_params.get('period', [None])[0]

            # --- 2. Build the SQL Query Dynamically ---
            base_sql = "SELECT timestamp, temperature, humidity, latitude, longitude, fire_score FROM sensor_data"
            
            if period == 'today':
                # Changed to 24 hours for better reliability
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '24 hours' ORDER BY timestamp DESC;"
            elif period == '7days':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '7 days' ORDER BY timestamp DESC;"
            elif period == '30days':
                sql_query = f"{base_sql} WHERE timestamp >= NOW() - interval '30 days' ORDER BY timestamp DESC;"
            else:
                # Default behavior: get the 20 most recent entries
                sql_query = f"{base_sql} ORDER BY timestamp DESC LIMIT 20;"

            # --- 3. Connect and Execute ---
            # Added client_encoding='UTF8' to stabilize the connection and prevent stray warnings.
            conn = psycopg2.connect(db_url, client_encoding='UTF8')
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
