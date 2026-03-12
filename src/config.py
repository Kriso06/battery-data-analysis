import os 
from dotenv import load_dotenv

load_dotenv()
BASE_URL=os.getenv("INTELLICAR_BASE_URL","").rstrip("/")
API_USERNAME=os.getenv("INTELLICAR_USERNAME","")
API_PASSWORD=os.getenv("INTELLICAR_PASSWORD","")

LOGIN_PATH=os.getenv("LOGIN_PATH","/api/login")
LIST_VEHICLES_PATH=os.getenv("LIST_VEHICLES_PATH","/api/standard/listvehicles")
LATEST_CAN_PATH=os.getenv("LATEST_CAN_PATH","/api/standard/getlatestcan")
DISTANCE_PATH=os.getenv("DISTANCE_PATH","/api/standard/getdistancetravelled")

DB_PATH=os.getenv("DB_PATH","data/battery_data.db")

FETCH_INTERVAL_SECONDS=int(os.getenv("FETCH_INTERVAL_SECONDS","30"))
DISTANCE_FETCH_INTERVAL_SECONDS=int(os.getenv("DISTANCE_FETCH_INTERVAL_SECONDS","300")) #how often distance data is fetched
REQUEST_TIMEOUT_SECONDS=int(os.getenv("REQUEST_TIMEOUT_SECONDS","20"))
MAX_WORKERS=int(os.getenv("MAX_WORKERS","2")) #threadpool