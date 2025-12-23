import os
import cv2
import numpy as np
import zmq
import datetime
import csv
import tritonclient.grpc as grpcclient

# ========= CONFIG ==========
config = {
    'models': {
        'white_sachet': {
            'url': '0.0.0.0:8006',
            'model_name': 'white_sachet'
        }
    },
    'zone': np.array([
        (21, 228),
        (1506, 233),
        (1517, 376),
        (19, 387)
        
    ], np.int32).reshape((-1, 1, 2)),
    'input_size': 1280,
    'conf_thresh': 0.5,
    'target_class': 0,
    'batch_size': 30   # Compute average over 16 frames
}

output_dir = "perforation_mc18"
os.makedirs(output_dir, exist_ok=True)
csv_file = os.path.join(output_dir, "perforation_counts.csv")

# Initialize CSV file
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Frame_Name', 'Perforation_Count'])

# ========= INFERENCE ==========
def run_inference(client, model_name, img):
    h, w = img.shape[:2]
    img_processed = cv2.resize(cv2.cvtColor(img, cv2.COLOR_BGR2RGB),
                               (config['input_size'], config['input_size'])) / 255.0
    input_tensor = img_processed.transpose(2, 0, 1)[np.newaxis].astype(np.float32)

    inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
    inputs[0].set_data_from_numpy(input_tensor)
    outputs = [grpcclient.InferRequestedOutput(n) for n in ['det_boxes', 'det_scores', 'det_classes']]

    results = client.infer(model_name=model_name, inputs=inputs, outputs=outputs)

    boxes = results.as_numpy('det_boxes')
    scores = results.as_numpy('det_scores')
    classes = results.as_numpy('det_classes')

    if boxes.shape[0] == 0:
        return np.array([]), np.array([]), np.array([])

    mask = scores > config['conf_thresh']
    boxes, scores, classes = boxes[mask], scores[mask], classes[mask]

    scale = np.array([w / config['input_size'], h / config['input_size'],
                      w / config['input_size'], h / config['input_size']])
    boxes = (boxes * scale).astype(int)

    return boxes, classes, scores


# ========= COUNT PERFORATIONS ==========
def count_perforations(boxes, classes, zone_polygon, target_class=0):
    count = 0
    for box, cls in zip(boxes, classes):
        if cls != target_class:
            continue
        x1, y1, x2, y2 = box
        cx, cy = int((x1 + x2) / 2), int((y1 + y2) / 2)
        inside = cv2.pointPolygonTest(zone_polygon, (cx, cy), False)
        if inside >= 0:
            count += 1
    return count


# ========= MAIN PROCESSOR LOOP ==========
if __name__ == "__main__":
    client = grpcclient.InferenceServerClient(url=config['models']['white_sachet']['url'])

    context = zmq.Context()
    socket_mc18 = context.socket(zmq.SUB)
    socket_mc18.connect("tcp://localhost:5563")
    socket_mc18.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc18, zmq.POLLIN)

    perforation_counts = []

    try:
        frame_idx = 0

        while True:
            socks = dict(poller.poll())
            if socket_mc18 in socks and socks[socket_mc18] == zmq.POLLIN:
                frame_bytes = socket_mc18.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                frame = cv2.flip(frame, -1)
                frame_idx += 1
                timestamp = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p")
                frame_name = f"frame_{frame_idx}_{timestamp}.jpg"

                boxes, classes, scores = run_inference(client, config['models']['white_sachet']['model_name'], frame)
                perforation_count = count_perforations(boxes, classes, config['zone'], config['target_class'])
                perforation_counts.append(perforation_count)

                # Annotate and save frame
                cv2.polylines(frame, [config['zone']], isClosed=True, color=(0, 255, 255), thickness=2)
                for box, cls in zip(boxes, classes):
                    x1, y1, x2, y2 = box
                    if cls == config['target_class']:
                        cx, cy = int((x1 + x2) / 2), int((y1 + y2) / 2)
                        inside = cv2.pointPolygonTest(config['zone'], (cx, cy), False)
                        color = (0, 255, 0) if inside >= 0 else (0, 0, 255)
                        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

                out_path = os.path.join(output_dir, frame_name)
                cv2.imwrite(out_path, frame)

                # Save individual frame perforation count in CSV
                with open(csv_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([frame_name, perforation_count])

                print(f"✅ Frame: {frame_name} | Perforations: {perforation_count}")

                # Print & save average every batch
                if len(perforation_counts) == config['batch_size']:
                    avg_count = np.mean(perforation_counts)
                    with open(csv_file, mode='a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([f"Average of last {config['batch_size']} frames", f"{avg_count:.2f}"])

                    print(f"📊 Average perforations (last {config['batch_size']} frames): {avg_count:.2f}")
                    perforation_counts.clear()

    except KeyboardInterrupt:
        print("\n🔴 Subscriber stopped by user.")
    finally:
        socket_mc18.close()
        context.term()


