from vidgear.gears import CamGear
import cv2
import datetime
import time

import logging
import sys
import traceback
import os
import signal
from mjpeg_streamer import MjpegServer, Stream

log_dir = "/var/log/camera_service"\

try:
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
except PermissionError:
    log_dir = "camera_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)


log_file = os.path.join(log_dir, f"camera_stream_loop4_{datetime.datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class Reconnecting_CamGear:
    def __init__(self, cam_address, reset_attempts=50, reset_delay=5):
        self.cam_address = cam_address
        self.reset_attempts = reset_attempts
        self.reset_delay = reset_delay
        self.frame = None
        self.source = None
        self.running = True
        self.connect()
        
    def connect(self):
        try:
            print(f"Connecting to camera at {self.cam_address}")
            self.source = CamGear(source=self.cam_address).start()
            print("Camera connection established")
        except Exception as e:
            logger.error(f"Error connecting to camera: {str(e)}")
            self.source = None
            
    def read(self):
        if not self.running:
            return None
            
        if self.source is None:
            logger.warning("Source is None, attempting to reconnect")
            self.connect()
            return self.frame
            
        if self.reset_attempts > 0:
            try:
                frame = self.source.read()
                if frame is None:
                    logger.warning("Received None frame, attempting to reconnect")
                    self.reset_attempts -= 1
                    self.source.stop()
                    print(
                        f"Re-connection Attempt-{self.reset_attempts} at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    time.sleep(self.reset_delay)
                    self.connect()
                    return self.frame
                else:
                    self.frame = frame
                    return frame
            except Exception as e:
                logger.error(f"Error reading frame: {str(e)}")
                self.reset_attempts -= 1
                if self.source:
                    try:
                        self.source.stop()
                    except Exception:
                        pass
                print(
                    f"Re-connection Attempt-{self.reset_attempts} at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                time.sleep(self.reset_delay)
                self.connect()
                return self.frame
        else:
            logger.error("Maximum reconnection attempts reached, exiting to allow service restart")
            sys.exit(1)
            return None
            
    def stop(self):
        print("Stopping camera stream")
        self.running = False
        self.reset_attempts = 0
        self.frame = None
        if self.source is not None:
            try:
                self.source.stop()
            except Exception as e:
                logger.error(f"Error stopping source: {str(e)}")

def signal_handler(sig, frame):
    print("Received shutdown signal, cleaning up...")
    if 'cam_stream' in globals():
        cam_stream.stop()
    if 'server' in globals():
        server.stop()
    print("Cleanup complete, exiting")
    sys.exit(0)

def main():
    global cam_stream, server
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("Starting camera stream application")
        
        cam_stream = Reconnecting_CamGear(
            cam_address="rtsp://admin:unilever2024@192.168.1.29:554/Streaming/Channels/102",
            reset_attempts=2000,  
            reset_delay=5,
        )
        
        try:
            stream = Stream("loop3", size=(1280, 720), quality=50, fps=20)
            server = MjpegServer("0.0.0.0", 3006)
            server.add_stream(stream)
            server.start()
            logger.info("MJPEG server started successfully")
        except Exception as e:
            logger.error(f"Failed to start MJPEG server: {str(e)}")
            cam_stream.stop()
            sys.exit(1)  
        
        last_successful_frame_time = time.time()
        frame_count = 0
        
        while True:
            try:
                frame = cam_stream.read()
                
                if frame is None:
                    logger.warning("Received None frame in main loop")
                    if time.time() - last_successful_frame_time > 60:  
                        logger.warning("No valid frames for 60 seconds, forcing reconnect")
                        cam_stream.stop()
                        time.sleep(2)
                        cam_stream = Reconnecting_CamGear(
                            cam_address="rtsp://admin:unilever2024@192.168.1.29:554/Streaming/Channels/102",
                            reset_attempts=10000,
                            reset_delay=5,
                        )
                        last_successful_frame_time = time.time()  
                    time.sleep(1) 
                    continue
                
                last_successful_frame_time = time.time()
                frame_count += 1
                
                if frame_count % 1000 == 0 or (time.time() - last_successful_frame_time > 300 and frame_count > 0):
                    logger.info(f"Camera stream running OK - {frame_count} frames processed")
                
                if not hasattr(server, 'is_running') or not server.is_running:
                    logger.error("MJPEG server not running, exiting for service restart")
                    sys.exit(1)
                
                try:
                    stream.set_frame(frame)
                except Exception as e:
                    logger.error(f"Error setting frame: {str(e)}")
                
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {str(e)}")
                logger.error(traceback.format_exc())
                
                time.sleep(2) 
                
                if time.time() - last_successful_frame_time > 180:  # 3 minutes of errors
                    logger.critical("Persistent errors for 3 minutes, exiting for service restart")
                    sys.exit(1)
    
    except Exception as e:
        logger.critical(f"Critical error in main function: {str(e)}")
        logger.critical(traceback.format_exc())
    
    finally:
        logger.info("Shutting down application")
        if 'cam_stream' in locals():
            cam_stream.stop()
        if 'server' in locals():
            server.stop()
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {str(e)}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

