import zmq
import json
import subprocess

def capture_image(rtsp_url, output_file):
    """
    Captures an image from an RTSP stream using FFmpeg.

    Parameters:
    - rtsp_url (str): The RTSP URL of the camera.
    - output_file (str): The file path where the image will be saved.

    Returns:
    - tuple: (success (bool), message (str))
    """
    ffmpeg_command = [
        'ffmpeg',
        '-rtsp_transport', 'tcp',
        '-i', rtsp_url,
        '-vframes', '1',
        '-q:v', '2',
        output_file
    ]
    try:
        # Run the FFmpeg command
        subprocess.run(
            ffmpeg_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        return True, "Image captured successfully."
    except subprocess.CalledProcessError as e:
        return False, f"FFmpeg failed: {e}"
    except Exception as e:
        return False, f"Error: {e}"


def main():
    # Create a ZeroMQ context
    context = zmq.Context()

    # Create a subscriber socket
    socket = context.socket(zmq.SUB)

    # Connect to the publisher (replace with your publisher's address)
    socket.connect("tcp://192.168.4.11:5556")

    # Subscribe to all topics
    socket.setsockopt_string(zmq.SUBSCRIBE, '')

    print("Listening for JSON messages...")
    while True:
        try:
            # Receive a message as a string
            message = socket.recv_json()

            # Parse the JSON message
            print("Received JSON message:", message)
        except json.JSONDecodeError as e:
            print("JSON decoding failed:", e)
        except Exception as e:
            print("An error occurred:", e)

if __name__ == "__main__":
    main()

