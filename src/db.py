import sqlite3
from pathlib import Path
from src.config import DB_PATH

def get_connection():
    Path(DB_PATH).parent.mkdir(parents=True,exist_ok=True)
    conn=sqlite3.connect(DB_PATH)
    conn.row_factory=sqlite3.Row #to access by column name instead of indexes
    return conn

def init_db():
    conn=get_connection()
    cursor=conn.cursor() #executes sql queries
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                vehicle_id TEXT NOT NULL,
                soc REAL,
                soh REAL,
                battery_voltage REAL,
                current REAL,
                battery_temp REAL,
                charge_cycle REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS distance_travelled(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                vehicle_id TEXT NOT NULL,
                distance_km REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                vehicle_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                alert_value TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
    """)
    conn.commit()
    conn.close()
    