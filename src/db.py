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

def insert_telemetry(record):
    conn=get_connection()
    cursor=conn.cursor()

    cursor.execute("""
        INSERT INTO telemetry
        VALUES(?,?,?,?,?,?,?,?)
    """,(
        record["timestamp_ms"],
        record["vehicle_id"],
        record.get("soc"),
        record.get("soh"),
        record.get("battery_voltage"),
        record.get("current"),
        record.get("battery_temp"),
        record.get("charge_cycle")
    ))    
    conn.commit()
    conn.close()

def fetch_latest_telemetry(limit=5):
    conn=get_connection()
    cursor=conn.cursor()
    cursor.execute("""
        SELECT * FROM telemetry 
        ORDER BY id DESC
        LIMIT ?
    """,(limit,))
    rows=cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def insert_telemetry_batch(records):
    conn=get_connection()
    cursor=conn.cursor()

    cursor.executemany("""
        INSERT INTO telemetry(
            timestamp_ms,
            vehicle_id,
            soc,
            soh,
            battery_voltage,
            current,
            battery_temp,
            charge_cycle
        )
        VALUES (?,?,?,?,?,?,?,?)
    """,[
        (
            record["timestamp_ms"],
            record["vehicle_id"],
            record.get("soc"),
            record.get("soh"),
            record.get("battery_voltage"),
            record.get("current"),
            record.get("battery_temp"),
            record.get("charge_cycle")
        )
        for record in records
    ])
    conn.commit()
    conn.close()

def insert_distance_batch(records):
    conn=get_connection()
    cursor=conn.cursor()
    cursor.executemany("""
        INSERT INTO distance_travelled(
            timestamp_ms,
            vehicle_id,
            distance_km
        )
        VALUES(?,?,?)
    """,[
        (
            record["timestamp_ms"],
            record["vehicle_id"],
            record.get("distance_km")
        )
        for record in records
    ])
    conn.commit()
    conn.close()