import time
from pprint import pprint
from src.api_client import IntellicarAPIClient
from src.db import init_db
from src.collector import collect_latest_telemetry,collect_distance_data
from src.config import FETCH_INTERVAL_SECONDS,DISTANCE_FETCH_INTERVAL_SECONDS


def main():
    init_db()
    print("db initialized")

    client=IntellicarAPIClient()
    token=client.login()
    print("login successful")
    print(f"token:{token[:10]}..")

    last_distance_run=0
    while True:
        cycle_start=time.time()
        print("\nstarting telemetry collection cycle...")
        try:
            t_summary=collect_latest_telemetry(client)
            print("\ntelemetry summary")
            pprint(t_summary)
        except Exception as exc:
            print(f"telemetry cycle failed:{exc}")
        
        now=time.time()
        run_distance=(now-last_distance_run)>=DISTANCE_FETCH_INTERVAL_SECONDS

        if run_distance:
            print("\nstarting distance collection cycle...")
            try:
                d_summary=collect_distance_data(client,hours=24)
                print("\ndistance summary")
                pprint(d_summary)
                last_distance_run=time.time()
            except Exception as exc:
                print(f"distance cycle failed:{exc}")
        
        elapsed=time.time()-cycle_start
        sleep_time=max(0,FETCH_INTERVAL_SECONDS-elapsed)
        print(f"cycle complete, sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)

if __name__=="__main__":
    main()