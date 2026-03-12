from src.db import insert_telemetry_batch, insert_distance_batch
from src.parsers import parse_latest_can, parse_distance_data
from src.utils import utc_now_ms,hours_ago_ms

def collect_latest_telemetry(client):
    vehicles=client.list_vehicles()
    print(f"fetched {len(vehicles)} vehicles")
    parsed_recs=[]
    failed_vehicles=[]

    for idx,vehicle in enumerate(vehicles,start=1):
        vehicle_no=vehicle.get("vehicleno")
        try:
            can_data=client.get_latest_can(vehicle_no)
            parsed_rec=parse_latest_can(vehicle_no,can_data)
            parsed_recs.append(parsed_rec)
            print(f"[{idx}/{len(vehicles)}] Success:{vehicle_no}")
        except Exception as exc:
            failed_vehicles.append(vehicle_no)
            print(f"[{idx}/{len(vehicles)}] Error for {vehicle_no}:{exc}")
        
    if parsed_recs:
        insert_telemetry_batch(parsed_recs)
        print(f"inserted {len(parsed_recs)} rows in telemetry db")
    
    return{
        "total_vehicles": len(vehicles),
        "success_count": len(parsed_recs),
        "failed_count": len(failed_vehicles),
        "failed_vehicles": failed_vehicles
    }

def collect_distance_data(client,hours=24):
    vehicles=client.list_vehicles()
    print(f"fetched {len(vehicles)} vehicles for distance collection")
    start_time=hours_ago_ms(hours)
    end_time=utc_now_ms()
    parsed_recs=[]
    failed_vehicles=[]
    for idx,vehicle in enumerate(vehicles,start=1):
        vehicle_no=vehicle.get("vehicleno")
        try:
            distance_data = client.get_distance_travelled(vehicle_no, start_time, end_time)
            parsed_rec = parse_distance_data(vehicle_no, distance_data, end_time)
            parsed_recs.append(parsed_rec)
            print(f"[{idx}/{len(vehicles)}] Distance success: {vehicle_no}")
        except Exception as exc:
            failed_vehicles.append(vehicle_no)
            print(f"[{idx}/{len(vehicles)}] Distance error for {vehicle_no}: {exc}")

    if parsed_recs:
        insert_distance_batch(parsed_recs)
        print(f"inserted {len(parsed_recs)} distance rows into DB")

    return {
        "total_vehicles": len(vehicles),
        "success_count": len(parsed_recs),
        "failed_count": len(failed_vehicles),
        "failed_vehicles": failed_vehicles,
        "start_time": start_time,
        "end_time": end_time,
    }