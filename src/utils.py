from datetime import datetime,timezone

def utc_now_ms():
    return int(datetime.now(timezone.utc).timestamp()*1000)

def ms_to_datetime(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms/1000, tz=timezone.utc)

def hours_ago_ms(hours):
    return utc_now_ms()-int(hours*3600*1000)