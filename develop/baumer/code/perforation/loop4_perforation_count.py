"""
Loop4 Perforation Monitoring (MC25–MC30)

Real-time monitoring of Perforation across MC25–MC30:
- Captures frames from ZMQ camera feed
- Runs Triton inference to detect Perforation defects in target zone
- Computes batch-wise average Perforation defects
- Saves low-perforation images locally and generates public URL
- Updates TP status DB and logs events in Event DB
- Runs parallel workers for all configured machines
"""


import os
import cv2
import numpy as np
import datetime
import uuid
import json
import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor
import tritonclient.grpc as grpcclient
from tritonclient.utils import np_to_triton_dtype
import zmq

# ========= LOAD CONFIG ==========
with open("Loop4_perforation_config.json", "r") as f:
    config = json.load(f)

machines = config["machines"]
model_cfg = config["model"]
db_cfg = config["db"]
BASE_URL = db_cfg.get("base_url", "http://192.168.0.185:8015") 

LOW_PERF_DIR = "/home/ai4m/develop/data/baumer"

INFER_SAVE_DIR = "/home/ai4m/develop/data/inference_images_perforation"
os.makedirs(LOW_PERF_DIR, exist_ok=True)

# ========= ZMQ FRAME GENERATOR ==========
def zmq_frame_generator(port):
    """Subscribe to ZMQ stream and yield the latest frame only."""
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://localhost:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    socket.setsockopt(zmq.CONFLATE, 1)  # keep only the latest frame

    while True:
        try:
            frame_bytes = socket.recv()
            npimg = np.frombuffer(frame_bytes, dtype=np.uint8)
            frame = cv2.imdecode(npimg, 1)
            if frame is not None:
                yield frame
        except Exception as e:
            print(f"ZMQ Error on port {port}: {e}")
            time.sleep(1)

# ========= INFERENCE ==========
def run_inference(client, model_cfg, frame):
    h, w = frame.shape[:2]
    img = cv2.resize(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB),
                     (model_cfg['input_size'], model_cfg['input_size'])) / 255.0
    input_tensor = img.transpose(2, 0, 1)[np.newaxis].astype(np.float32)

    inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
    inputs[0].set_data_from_numpy(input_tensor)
    outputs = [grpcclient.InferRequestedOutput(n) for n in
               ['det_boxes', 'det_scores', 'det_classes']]

    results = client.infer(model_name=model_cfg['model_name'],
                           inputs=inputs,
                           outputs=outputs)

    boxes = results.as_numpy('det_boxes')
    scores = results.as_numpy('det_scores')
    classes = results.as_numpy('det_classes')

    if boxes.shape[0] == 0:
        return np.array([]), np.array([]), np.array([])

    mask = scores > model_cfg['conf_thresh']
    boxes, scores, classes = boxes[mask], scores[mask], classes[mask]

    scale = np.array([w / model_cfg['input_size'], h / model_cfg['input_size'],
                      w / model_cfg['input_size'], h / model_cfg['input_size']])
    boxes = (boxes * scale).astype(int)

    return boxes, classes, scores

# ========= COUNT PERFORATIONS ==========
def count_perforations(boxes, classes, zone_polygon, target_class):
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


# ========= SAVE INFERENCE IMAGE FOR TESTING ONLY ==========
def save_inference_image(frame, boxes, classes, scores, camera_id, zone_polygon):
    """Draw detection boxes + zone polygon and save annotated inference image."""
    annotated = frame.copy()

    # --- Draw Zone Polygon ---d
    cv2.polylines(
        annotated,
        [zone_polygon],
        isClosed=True,
        color=(255, 255, 0),  # Cyan-yellow zone outline
        thickness=2,
    )
    cv2.putText(
        annotated, "Zone", 
        tuple(zone_polygon[0][0]), 
        cv2.FONT_HERSHEY_SIMPLEX, 
        0.7, 
        (255, 255, 0), 
        2,
        cv2.LINE_AA
    )

    # --- Draw Detected Boxes ---
    for (box, cls, score) in zip(boxes, classes, scores):
        x1, y1, x2, y2 = box
        color = (0, 255, 0) if int(cls) == 0 else (0, 0, 255)
        cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 2)
        cv2.putText(
            annotated, f"cls:{int(cls)} {score:.2f}", 
            (x1, max(15, y1 - 5)), 
            cv2.FONT_HERSHEY_SIMPLEX, 
            0.5, 
            color, 
            1, 
            cv2.LINE_AA
        )

    # --- Save Annotated Image ---
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{camera_id}_inference_{timestamp}.jpg"
    save_path = os.path.join(INFER_SAVE_DIR, filename)
    cv2.imwrite(save_path, annotated)
    print(f"[{camera_id}] 💾 Saved inference image with zone: {save_path}")



# ========= INSERT STATUS & EVENT ==========
def insert_status(machine_cfg, avg_count, tp_db, event_db, base_url, frame=None):
    camera_id = machine_cfg["camera_id"]
    tp_table = machine_cfg["tp_table"]
    tp_column = machine_cfg["tp_column"]

    active = 1 if avg_count <= 4 else 0
    file_url = ""

    if active == 1 and frame is not None:
        timestamp_str = datetime.datetime.now().strftime("%m_%d_%Y_%I_%M_%S_%f_%p")
        filename = f"low_perforation_{camera_id}_{timestamp_str}.jpeg"

        # Save locally
        output_path = os.path.join(LOW_PERF_DIR, filename)
        cv2.imwrite(output_path, frame)

        # Generate public URL
        file_url = f"{base_url}/{filename}"
        print(f"[{camera_id}] 🔗 File URL: {file_url}")

    # TP STATUS DATA
    tp_data = {
        "uuid": str(uuid.uuid4()),
        "active": active,
        "filepath": file_url,
        "timestamp": datetime.datetime.now().isoformat(),
        "color_code": 3
    }

    try:
        conn_tp = psycopg2.connect(**tp_db)
        cur_tp = conn_tp.cursor()
        cur_tp.execute(f"UPDATE {tp_table} SET {tp_column} = %s", (json.dumps(tp_data),))
        if cur_tp.rowcount == 0:
            cur_tp.execute(f"INSERT INTO {tp_table} ({tp_column}) VALUES (%s)", (json.dumps(tp_data),))
        conn_tp.commit()
        cur_tp.close()
        conn_tp.close()
        print(f"[{camera_id}] ✅ TP status updated")
    except Exception as e:
        print(f"[{camera_id}] ❌ TP status update failed: {e}")

    # EVENT DATA
    if active == 1:
        event_data = (
            datetime.datetime.now(),
            tp_data["uuid"],
            "Baumer Camera",
            camera_id,
            None, None, None, None,
            file_url,
            "Low perforation",
            "Quality",
            None,
            None
        )
        try:
            conn_event = psycopg2.connect(**event_db)
            cur_event = conn_event.cursor()
            cur_event.execute("""
                INSERT INTO event_table (
                    timestamp, event_id, zone, camera_id, assigned_to,
                    action, remark, resolution_time, filename,
                    event_type, alert_type, assigned_time, acknowledge
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, event_data)
            conn_event.commit()
            cur_event.close()
            conn_event.close()
            print(f"[{camera_id}] ✅ Event logged")
        except Exception as e:
            print(f"[{camera_id}] ❌ Event log failed: {e}")

# ========= MACHINE WORKER ==========
def run_machine(machine_cfg, model_cfg, db_cfg):
    camera_id = machine_cfg["camera_id"]
    output_dir = machine_cfg["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    client = grpcclient.InferenceServerClient(url=model_cfg['url'])
    perforation_counts = []
    frame_idx = 0
    zone = np.array(machine_cfg["zone"], np.int32).reshape((-1, 1, 2))

    print(f"[{camera_id}] 🚀 Started worker on port {machine_cfg['zmq_port']}")

    frame_gen = zmq_frame_generator(machine_cfg["zmq_port"])
    flip_required = machine_cfg.get("flip_frame", False)

    for frame in frame_gen:
        if flip_required:
            frame = cv2.flip(frame, -1)

        frame_idx += 1


        boxes, classes, scores = run_inference(client, model_cfg, frame)
        #save_inference_image(frame, boxes, classes, scores, camera_id, zone)
        perforation_count = count_perforations(boxes, classes, zone, machine_cfg["target_class"])
        perforation_counts.append(perforation_count)

        print(f"[{camera_id}] Frame {frame_idx} | Count: {perforation_count}")

        # Compute average every batch
        if len(perforation_counts) == machine_cfg["batch_size"]:
            avg_count = np.mean(perforation_counts)
            print(f"[{camera_id}] 📊 Avg perforations ({machine_cfg['batch_size']} frames): {avg_count:.2f}")
            insert_status(machine_cfg, avg_count, db_cfg["tp_db"], db_cfg["event_db"], BASE_URL, frame)
            perforation_counts.clear()

# ========= MAIN ==========
if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=len(machines)) as executor:
        futures = [executor.submit(run_machine, m, model_cfg, db_cfg) for m in machines]
        for f in futures:
            f.result()


