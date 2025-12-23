import os
import cv2
import numpy as np
import zmq
import datetime
import csv
import tritonclient.grpc as grpcclient
from tritonclient.utils import np_to_triton_dtype

# ========= CONFIG ==========
config = {
    'triton_url': "0.0.0.0:8006",
    'model_name': "slitting_model",
    'input_size': 1280,
    'focus_class': 2,
    'conf_thresh': 0.5,
    'batch_size': 30, # Change as needed
    'output_dir': "slitting_mc18"
}

os.makedirs(config['output_dir'], exist_ok=True)
csv_file = os.path.join(config['output_dir'], "slit_counts.csv")

# Initialize CSV file
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Frame_Name', 'Detection_Count'])

# ========= FUNCTIONS ==========
def preprocess(img):
    """Resize, normalize and prepare for Triton."""
    img_resized = cv2.resize(cv2.cvtColor(img, cv2.COLOR_BGR2RGB),
                             (config['input_size'], config['input_size']))
    img_norm = img_resized.astype(np.float32) / 255.0
    return img_norm.transpose(2, 0, 1)[np.newaxis, :]

def run_inference(client, img):
    """Run inference on Triton server."""
    inp = grpcclient.InferInput("images", img.shape, np_to_triton_dtype(img.dtype))
    inp.set_data_from_numpy(img)
    outputs = [
        grpcclient.InferRequestedOutput("det_boxes"),
        grpcclient.InferRequestedOutput("det_scores"),
        grpcclient.InferRequestedOutput("det_classes")
    ]
    result = client.infer(config['model_name'], inputs=[inp], outputs=outputs)
    return (result.as_numpy("det_boxes"),
            result.as_numpy("det_scores"),
            result.as_numpy("det_classes"))

def count_detections(boxes, scores, classes):
    """Count detections of focus_class above threshold."""
    if boxes is None or len(boxes) == 0:
        return 0

    scores = scores.squeeze()
    classes = classes.squeeze()

    mask = (scores > config['conf_thresh']) & (classes == config['focus_class'])
    return int(np.sum(mask))

# ========= MAIN LOOP ==========
if __name__ == "__main__":
    client = grpcclient.InferenceServerClient(url=config['triton_url'])

    # ZMQ subscriber
    context = zmq.Context()
    socket_mc18 = context.socket(zmq.SUB)
    socket_mc18.connect("tcp://localhost:5563")  # Change if server different
    socket_mc18.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc18, zmq.POLLIN)

    counts = []
    frame_idx = 0

    try:
        while True:
            socks = dict(poller.poll())
            if socket_mc18 in socks and socks[socket_mc18] == zmq.POLLIN:
                frame_bytes = socket_mc18.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                frame = cv2.flip(frame, -1)  # Rotate 180 degrees
                frame_idx += 1

                timestamp = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p")
                frame_name = f"frame_{frame_idx}_{timestamp}.jpg"

                # Run inference
                inp = preprocess(frame)
                boxes, scores, classes = run_inference(client, inp)
                det_count = count_detections(boxes, scores, classes)
                counts.append(det_count)

                # Draw detections
                if boxes is not None and len(boxes) > 0:
                    boxes = boxes.squeeze()
                    scores = scores.squeeze()
                    classes = classes.squeeze()
                    for i in range(len(boxes)):
                        box = boxes[i]
                        cls = int(classes[i])
                        score = float(scores[i])

                        if score > config['conf_thresh'] and cls == config['focus_class']:
                            x1, y1, x2, y2 = box.astype(int)
                            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

                # Save frame
                out_path = os.path.join(config['output_dir'], frame_name)
                cv2.imwrite(out_path, frame)

                # Save detection count in CSV
                with open(csv_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([frame_name, det_count])

                print(f"✅ Frame: {frame_name} | Detections: {det_count}")

                # Average every batch_size frames
                if len(counts) == config['batch_size']:
                    avg = np.mean(counts)
                    with open(csv_file, mode='a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([f"Average of last {config['batch_size']} frames", f"{avg:.2f}"])
                    print(f"📊 Average detections (last {config['batch_size']} frames): {avg:.2f}")
                    counts.clear()

    except KeyboardInterrupt:
        print("\n🔴 Stopped by user.")
    finally:
        socket_mc18.close()
        context.term()
