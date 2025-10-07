import os
import psycopg2
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import AsIs

# Global variable to hold the connection pool instance
# The pool is initialized once when the serverless function starts up
db_pool = None

def get_connection_pool():
    """Initializes and returns the global connection pool."""
    global db_pool
    if db_pool is None:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            raise EnvironmentError("DATABASE_URL environment variable not set.")
        
        # --- 1. Parse the URL to get individual connection components ---
        url_parts = urlparse(db_url)
        
        conn_params = {
            'host': url_parts.hostname,
            'database': url_parts.path[1:],
            'user': url_parts.username,
            'password': url_parts.password,
            'port': url_parts.port if url_parts.port else 5432,
            # Additional parameters for stability in serverless environment
            'client_encoding': 'UTF8',
            'sslmode': 'require',  # Reverting to the secure, reliable mode
            # Final fix: Use options to suppress all but FATAL messages and set a reliable application name
            'options': '-c application_name=vercel_api -c log_min_messages=FATAL'
        }
        
        # --- 2. Initialize the Connection Pool ---
        # minconn=1, maxconn=3 is sufficient for a low-traffic API
        # The connection pool is necessary to handle Vercel's rapid function invocations
        db_pool = SimpleConnectionPool(1, 3, **conn_params)

    return db_pool

class handler(BaseHTTPRequestHandler):
    """
    Handles incoming HTTP requests using the connection pool.
    """
    def do_GET(self):
        pool = None
        conn = None
        try:
            # Get connection from the pool
            pool = get_connection_pool()
            conn = pool.getconn()
            
            # --- Build SQL Query Dynamically ---
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

            # --- Execute Query ---
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

        except EnvironmentError as e:
            # We are not going to raise this one, it's safe
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "Configuration Error", "details": str(e)}))
        except psycopg2.Error as e:
            # Reverted: Now catches the database error and returns the generic error message
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "A database error occurred.", "details": "Check Vercel logs for specific DB error."}))
        except Exception as e:
            # Reverted: Now catches general errors and returns a generic error message
            self._send_response(500, {'Content-Type': 'application/json'}, json.dumps({"error": "An unexpected server error occurred.", "details": str(e)}))
        finally:
            if pool and conn:
                # Return the connection to the pool, don't close it
                pool.putconn(conn)
    
    def _send_response(self, status_code, headers, body):
        self.send_response(status_code)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))
