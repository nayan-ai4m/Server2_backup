import json
import platform
import subprocess
import time
import re
import logging
from prometheus_client import start_http_server, Gauge

# Setup logging
logging.basicConfig(
    filename="/home/ai4m/develop/device_status.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load device configuration
with open('devices.json') as f:
    devices = json.load(f)

# Sanitize metric name for Prometheus compatibility
def sanitize_metric_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()

# Create a gauge for each device
device_gauges = {
    device['name']: Gauge(
        f"device_status_{sanitize_metric_name(device['name'])}",
        f"Status of {device['name']} (1=active, 0=inactive)"
    )
    for device in devices
}

# Ping function
def ping_device(ip):
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    try:
        subprocess.check_output(['ping', param, '1', ip], stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

# Main monitoring loop
def monitor_devices():
    while True:
        for device in devices:
            status = ping_device(device['ip'])
            device_gauges[device['name']].set(1 if status else 0)
            msg = f"{device['name']} ({device['ip']}): {'Active' if status else 'Inactive'}"
            print(msg)

            if not status:
                # Log inactive devices
                logging.warning(f"Inactive: {device['name']} ({device['ip']})")

        time.sleep(900)  # Wait 15 minutes

if __name__ == "__main__":
    start_http_server(8002)
    logging.info("Device monitor started")
    monitor_devices()

