import sqlite3
from pathlib import Path
import pandas as pd
from src.config import DB_PATH

OUTPUT_DIR=Path("output")
ALERTS_CSV_PATH=OUTPUT_DIR/"alerts_log.csv"
ALERTS_SUMMARY_CSV_PATH=OUTPUT_DIR/"alerts_summary.csv"

def load_telemetry_data():
    query="""
        SELECT timestamp_ms,vehicle_id,soc,soh,battery_voltage,current,battery_temp,charge_cycle
        FROM telemetry
        ORDER BY vehicle_id,timestamp_ms
    """
    conn=sqlite3.connect(DB_PATH)
    df=pd.read_sql_query(query,conn)
    conn.close()
    return df

def detect_basic(df):
    alerts=[]
    for _, row in df.iterrows():
        timestamp_ms=row["timestamp_ms"]
        vehicle_id=row["vehicle_id"]
        if pd.notna(row["soc"]) and row["soc"]<10:
            alerts.append({
                "timestamp_ms":timestamp_ms,
                "vehicle_id":vehicle_id,
                "alert_type":"LOW_BATTERY",
                "alert_value":row["soc"]
            })

        if(
            pd.notna(row["soc"])
            and pd.notna(row["current"])
            and row["soc"]>98
            and row["current"]<0
        ):
            alerts.append({
                "timestamp_ms":timestamp_ms,
                "vehicle_id":vehicle_id,
                "alert_type":"OVERCHARGING",
                "alert_value":row["current"]
            })
        
        if pd.notna(row["battery_temp"]) and row["battery_temp"]>50:
            alerts.append({
                "timestamp_ms":timestamp_ms,
                "vehicle_id":vehicle_id,
                "alert_type":"OVERHEATING",
                "alert_value":row["battery_temp"]
            })

        if(
            pd.notna(row["battery_voltage"])
            and (row["battery_voltage"]<44 or row["battery_voltage"]>58)
        ):
            alerts.append({
                "timestamp_ms":timestamp_ms,
                "vehicle_id":vehicle_id,
                "alert_type":"VOLTAGE_OUT_OF_RANGE",
                "alert_value":row["battery_voltage"]
            })
        
    return pd.DataFrame(alerts)

def detect_sensor(df):
    sensor_alerts=[]
    for vehicle_id,group in df.groupby("vehicle_id"):
        group=group.sort_values("timestamp_ms").copy()
        group["temp_missing"]=group["battery_temp"].isna()
        count=0
        for _,row in group.iterrows():
            if row["temp_missing"]:
                count+=1
            else:
                count=0
            if count>=10:
                sensor_alerts.append({
                    "timestamp_ms":row["timestamp_ms"],
                    "vehicle_id":vehicle_id,
                    "alert_type":"SENSOR_OFFLINE",
                    "alert_value":"NA"
                })
    return pd.DataFrame(sensor_alerts)

def save_alerts(alerts_df):
    if alerts_df.empty:
        return 
    conn=sqlite3.connect(DB_PATH)
    cursor=conn.cursor()
    cursor.executemany("""
        INSERT INTO alerts(
            timestamp_ms,
            vehicle_id,
            alert_type,
            alert_value
        )
        VALUES (?,?,?,?)
    """,[
        (
            int(row["timestamp_ms"]),
            row["vehicle_id"],
            row["alert_type"],
            str(row["alert_value"])
        )
        for _,row in alerts_df.iterrows()
    ])
    conn.commit()
    conn.close()

def build_summary(alerts_df):
    if alerts_df.empty:
        return pd.DataFrame()
    summary=(
        alerts_df.groupby(["alert_type","vehicle_id"])
        .size()
        .reset_index(name="alert_count")
        .sort_values(["alert_type","alert_count"],ascending=[True,False])
    )
    return summary

def main():
    OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
    df=load_telemetry_data()
    print(f"loaded {len(df)} telemetry rows")
    basic=detect_basic(df)
    sensor=detect_sensor(df)
    alerts_df=pd.concat([basic,sensor],ignore_index=True)
    if alerts_df.empty:
        print("no anomalies detected")
        return
    alerts_df["timestamp"]=pd.to_datetime(alerts_df["timestamp_ms"],unit="ms",utc=True)
    print(f"detected {len(alerts_df)} alerts")
    print(alerts_df.head(10))

    alerts_df.to_csv(ALERTS_CSV_PATH,index=False)
    print(f"saved alerts log to {ALERTS_CSV_PATH}")
    save_alerts(alerts_df)
    print("saved alerts to db")

    summary=build_summary(alerts_df)
    summary.to_csv(ALERTS_SUMMARY_CSV_PATH,index=False)
    print(f"saved alerts summary to {ALERTS_SUMMARY_CSV_PATH}")
    print("\nalerts by type:")
    print(alerts_df["alert_type"].value_counts())
    print("\nvehicles with most alerts:")
    print(alerts_df["vehicle_id"].value_counts().head(10))

if __name__=="__main__":
    main()