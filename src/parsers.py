from src.utils import utc_now_ms

def extract_metric(can_data,key):
    metric=can_data.get(key,{})
    if isinstance(metric,dict):
        return metric.get("value"),metric.get("timestamp")
    return None,None


#flatten api response
def parse_latest_can(vehicle_id,can_data):
    soc,soc_ts=extract_metric(can_data,"soc")
    soh,_=extract_metric(can_data,"soh")
    battery_voltage,voltage_ts=extract_metric(can_data,"battery_voltage")
    current,_=extract_metric(can_data,"current")
    battery_temp,_=extract_metric(can_data,"battery_temp")
    charge_cycle,_=extract_metric(can_data,"charge_cycle")
    timestamp_ms=soc_ts or voltage_ts or utc_now_ms() #if api timestamp is missing fallback to utc_now_ms()
    return{
        "timestamp_ms": timestamp_ms,
        "vehicle_id": vehicle_id,
        "soc": soc,
        "soh": soh,
        "battery_voltage": battery_voltage,
        "current": current,
        "battery_temp": battery_temp,
        "charge_cycle": charge_cycle
    }

def parse_distance_data(vehicle_id,distance_data,end_time):
    distance_km=None
    if isinstance(distance_data,dict):
        if "distance" in distance_data:
            distance_km=distance_data.get("distance")
        elif "distancetravelled" in distance_data:
            distance_km=distance_data.get("distancetravelled")
        elif "value" in distance_data:
            distance_km=distance_data.get("value")
    return{
        "timestamp_ms":end_time,
        "vehicle_id":vehicle_id,
        "distance_km":distance_km
    }