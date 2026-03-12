from pprint import pprint
from src.api_client import IntellicarAPIClient
from src.db import init_db


def main():
    init_db()
    print("db initialized")

    client=IntellicarAPIClient()
    token=client.login()
    print("login successful")
    print(f"token:{token[:10]}..")
    vehicles=client.list_vehicles()
    print(f"fetched {len(vehicles)} vehicles")
    if not vehicles:
        print("no vehicles found")
        return
    
    first=vehicles[0]
    print("first object")
    pprint(first)
    vehicle_no=first.get("vehicleno")
    print(f"fetching latest can for : {vehicle_no}")
    can_data=client.get_latest_can(vehicle_no)
    pprint(can_data)

if __name__=="__main__":
    main()