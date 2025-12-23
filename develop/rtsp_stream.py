from vidgear.gears import CamGear
import cv2

rtsp_url = "rtsp://192.168.1.168:8554/ds-test"
options = {"rtsp_transport": "tcp"}  # Enforce TCP for reliable streaming

stream = CamGear(source=rtsp_url, logging=True, **options).start()

while True:
    frame = stream.read()
    if frame is None:
        print("Stream disconnected, reconnecting...")
        stream.stop()
        stream = CamGear(source=rtsp_url, logging=True, **options).start()
        continue

    cv2.imshow("RTSP Stream", frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

stream.stop()
cv2.destroyAllWindows()

