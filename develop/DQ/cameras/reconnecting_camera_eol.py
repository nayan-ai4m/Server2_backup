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
        self.source = CamGear(source=self.cam_address).start()
        self.running = True

    def read(self):
        if self.source is None:
            return None
        if self.running and self.reset_attempts > 0:
            frame = self.source.read()
            if frame is None:
                self.source.stop()
                self.reset_attempts -= 1
                print(
                    "Re-connection Attempt-{} occured at time:{}".format(
                        str(self.reset_attempts),
                        datetime.datetime.now().strftime("%m-%d-%Y %I:%M:%S%p"),
                    )
                )
                time.sleep(self.reset_delay)
                self.source = CamGear(source=self.cam_address).start()
                # return previous frame
                return self.frame
            else:
                self.frame = frame
                return frame
        else:
            return None

    def stop(self):
        self.running = False
        self.reset_attempts = 0
        self.frame = None
        if not self.source is None:
            self.source.stop()


if __name__ == "__main__":
    # open any valid video stream
    cam_stream = Reconnecting_CamGear(
        cam_address="rtsp://admin:unilever2024@192.168.1.29:554/Streaming/Channels/102",
        reset_attempts=2000,
        reset_delay=5,
    )


    stream = Stream("loop3", size=(1280, 720), quality=50, fps=20)

    server = MjpegServer("192.168.0.158", 3002)
    server.add_stream(stream)
    server.start()


    # loop over
    while True:

        # read frames from stream
        frame = cam_stream.read()

        # check for frame if None-type
        if frame is None:
            break
        stream.set_frame(frame) 
        # {do something with the frame here}

        # Show output window

        # check for 'q' key if pressed

    # close output window

    # safely close video stream
    cam_stream.stop()
    server.stop ()

