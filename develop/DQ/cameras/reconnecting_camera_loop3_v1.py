from vidgear.gears import CamGear
import cv2
import datetime
import time
from mjpeg_streamer import MjpegServer, Stream

class Reconnecting_CamGear:
    def __init__(self, cam_address, reset_attempts=50, reset_delay=5):
        self.cam_address = cam_address
        self.reset_attempts = reset_attempts
        self.reset_delay = reset_delay
        self.source = CamGear(source=self.cam_address, rtsp_transport="tcp").start()  # ✅ Removed `stream_mode=True`
        self.running = True
        self.frame = None  # Initialize frame

    def read(self):
        if self.source is None:
            return None
        if self.running and self.reset_attempts > 0:
            frame = self.source.read()
            if frame is None:
                print(
                    f"Reconnection Attempt-{self.reset_attempts} at {datetime.datetime.now().strftime('%m-%d-%Y %I:%M:%S%p')}"
                )
                self.source.stop()
                time.sleep(self.reset_delay)
                self.reset_attempts -= 1
                self.source = CamGear(source=self.cam_address, rtsp_transport="tcp").start()  # ✅ Ensure proper restart
                return self.frame  # Return last known good frame
            else:
                self.frame = frame
                return frame
        return None

    def stop(self):
        self.running = False
        self.reset_attempts = 0
        self.frame = None
        if self.source:
            self.source.stop()
            self.source = None  # Explicitly release

if __name__ == "__main__":
    cam_stream = Reconnecting_CamGear(
        cam_address="rtsp://admin:unilever2024@192.168.1.29:554/Streaming/Channels/102",
        reset_attempts=2000,
        reset_delay=5,
    )

    stream = Stream("loop3", size=(1280, 720), quality=50, fps=20)
    server = MjpegServer("192.168.1.149", 3002)
    server.add_stream(stream)
    server.start()

    while True:
        frame = cam_stream.read()
        if frame is None:
            break
        stream.set_frame(frame)

    cam_stream.stop()
    server.stop()

