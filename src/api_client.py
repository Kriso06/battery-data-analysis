import time
import requests

from src.config import(
    BASE_URL,
    API_USERNAME,
    API_PASSWORD,
    LOGIN_PATH,
    LIST_VEHICLES_PATH,
    LATEST_CAN_PATH,
    DISTANCE_PATH,
    REQUEST_TIMEOUT_SECONDS
)

class IntellicarAPIClient:
    def __init__(self):
        self.session=requests.Session() #persistant HTTP session
        self.token=None
    
    def _build_url(self,path):  #"_" prefix==> internal use only
        return f"{BASE_URL}{path}"
    
    def login(self):
        if not BASE_URL:
            raise ValueError("BASE_URL missing in .env")
        if not API_USERNAME or not API_PASSWORD:
            raise ValueError("USERNAME or PASSWORD missing in .env")
        
        url=self._build_url(LOGIN_PATH)
        payload={
            "username":API_USERNAME.strip(),
            "password":API_PASSWORD.strip()
        }
        response=self.session.post(
            url,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS
        )

        data=response.json()
        
        if response.status_code>=400 or data.get("status")=="FAILURE":
            raise ValueError(f"login failed:{data.get('msg')}|details={data.get('err')}")

        token=data["data"]["token"]
        self.token=token #so that other methods can use the token
        return token
    
    def _post_with_token(self,path,payload):
        if not self.token:
            raise ValueError("Token missing. Call login() first")
        
        time.sleep(0.4)
        body={"token":self.token,**payload}
        response=self.session.post(
            self._build_url(path),
            json=body,
            timeout=REQUEST_TIMEOUT_SECONDS
        )   
        response.raise_for_status()
        data=response.json()
        if data.get("status")=="FAILURE":
            raise ValueError(f"API failure:{data.get('msg')} | err={data.get('err')}")
        return data
    
    def list_vehicles(self):
        data=self._post_with_token(LIST_VEHICLES_PATH,{})
        return data.get("data",[])
    
    def get_latest_can(self,vehicle_no):
        data=self._post_with_token(
            LATEST_CAN_PATH,
            {"vehicleno":vehicle_no}
        )
        return data.get("data",{})
    
    def get_distance_travelled(self,vehicle_no,start_time,end_time):
        data=self._post_with_token(
            DISTANCE_PATH,
            {
                "vehicleno":vehicle_no,
                "starttime":start_time,
                "endtime":end_time
            }
        )
        return data.get("data",{})