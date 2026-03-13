import sqlite3
import pandas as pd
from src.config import DB_PATH
from pathlib import Path

OUTPUT_DIR=Path("output")
SUMMARY_PATH=OUTPUT_DIR/"vehicle_energy_summary.csv"

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

def load_distance_data():
    query="""
        SELECT timestamp_ms,vehicle_id,distance_km
        FROM distance_travelled
        ORDER BY vehicle_id,timestamp_ms
    """
    conn=sqlite3.connect(DB_PATH)
    df=pd.read_sql_query(query,conn)
    conn.close()
    return df

def classify_state(current):
    if pd.isna(current):
        return "IDLE"
    if current<-0.5:
        return "CHARGING"
    if current>0.5:
        return "DISCHARGING"
    return "IDLE"

def prepare_telemetry(df):
    if df.empty:
        return df
    
    df=df.copy()
    df["timestamp"]=pd.to_datetime(df["timestamp_ms"],unit="ms",utc=True)
    df["state"]=df["current"].apply(classify_state)
    df["power_w"]=df["battery_voltage"]*(df["current"].abs())
    return df

def calc_energy(df):
    if df.empty:
        return df
    df=df.copy()
    df=df.sort_values(["vehicle_id","timestamp_ms"])

    df["prev_ts_ms"]=df.groupby("vehicle_id")["timestamp_ms"].shift(1)  #WHAT IS THE SHIFT 1 FOR?
    df["prev_power_w"]=df.groupby("vehicle_id")["power_w"].shift(1)
    
    df["time_diff_s"]=(df["timestamp_ms"]-df["prev_ts_ms"])/1000
    df["avg_power_w"]=(df["power_w"]+df["prev_power_w"])/2

    df["energy_wh"]=(df["avg_power_w"]*df["time_diff_s"])/3600
    df["energy_wh"]=df["energy_wh"].fillna(0)

    return df

def summarize(df,distance_df):
    if df.empty:
        return df
    summary=df.groupby("vehicle_id").apply(build_summary).reset_index()

    if not distance_df.empty:
        latest_distance=(
            distance_df.sort_values(["vehicle_id","timestamp_ms"])
            .groupby("vehicle_id",as_index=False)
            .tail(1)[["vehicle_id","distance_km"]] #WHAT IS TAIL, AS_INDEX
        )
        summary=summary.merge(latest_distance,on="vehicle_id",how="left")
    else:
        summary["distance_km"]=None
    
    summary["energy_effic_kwhpkm"]=(
        summary["total_energy_discharge_kwh"]/summary["distance_km"]
    )
    return summary

def build_summary(group):
    charge=group["state"]=="CHARGING"
    discharge=group["state"]=="DISCHARGING"
    idle=group["state"]=="IDLE"

    return pd.Series({
        "total_energy_charged_kwh":group.loc[charge,"energy_wh"].sum()/1000,
        "total_energy_discharge_kwh":group.loc[discharge,"energy_wh"].sum()/1000,
        "charge_time_h":group.loc[charge,"time_diff_s"].fillna(0).sum()/3600,
        "discharge_time_h":group.loc[discharge,"time_diff_s"].fillna(0).sum()/3600,
        "idle_time_h":group.loc[idle,"time_diff_s"].fillna(0).sum()/3600,
        "avg_charge_pow_w":group.loc[charge,"power_w"].mean(),
        "avg_discharge_pow_w":group.loc[discharge,"power_w"].mean()
    })

def save_summary(summary_df):
    OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
    summary_df.to_csv(SUMMARY_PATH,index=False)
    print(f"Saved summary CSV to {SUMMARY_PATH}")

def main():
    t_df=load_telemetry_data()
    d_df=load_distance_data()
    print(f"loaaded {len(t_df)} rows from telemetry data")
    print(f"loaaded {len(d_df)} rows from distance data")
    prepared_df=prepare_telemetry(t_df)
    if prepared_df.empty:
        print("no telemetry data found")
        return
    energy_df=calc_energy(prepared_df)
    summary_df=summarize(energy_df,d_df)
    print(summary_df.head(10))
    save_summary(summary_df)

if __name__=="__main__":
    main()