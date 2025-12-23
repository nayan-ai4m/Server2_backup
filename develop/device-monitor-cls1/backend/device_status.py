import json
import platform
import subprocess
import time
import re
import logging
from prometheus_client import start_http_server, Gauge
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import threading
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logging.basicConfig(
    filename="/home/ai4m/develop/device_status.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

with open('devices.json') as f:
    devices = json.load(f)

def sanitize_metric_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()

device_gauges = {
    device['name']: Gauge(
        f"device_status_{sanitize_metric_name(device['name'])}",
        f"Status of {device['name']} (1=active, 0=inactive)"
    )
    for device in devices
}

device_status_cache = {}

def ping_device(ip):
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    try:
        subprocess.check_output(['ping', param, '1', ip], stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def monitor_devices():
    global device_status_cache
    while True:
        for device in devices:
            status = ping_device(device['ip'])
            v = 1 if status else 0
            device_gauges[device['name']].set(v)
            device_status_cache[device['name']] = v
            msg = f"{device['name']} ({device['ip']}): {'Active' if status else 'Inactive'}"
            print(msg)
            if not status:
                logging.warning(f"Inactive: {device['name']} ({device['ip']})")
        time.sleep(900)

@app.get("/status")
def status():
    # Returns a status list of all devices
    return JSONResponse([
        {
            "name": device['name'],
            "ip": device['ip'],
            "status": device_status_cache.get(device['name'], 0)
        }
        for device in devices
    ])

if __name__ == "__main__":
    start_http_server(8003)
    logging.info("Device monitor started")
    thread = threading.Thread(target=monitor_devices, daemon=True)
    thread.start()
    uvicorn.run(app, host="0.0.0.0", port=5000)
