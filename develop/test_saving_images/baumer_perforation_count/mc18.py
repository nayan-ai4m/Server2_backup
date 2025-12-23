import os
import cv2
import numpy as np
import datetime
import uuid
import json
import psycopg2
import zmq
import tritonclient.grpc as grpcclient

# ========= CONFIG (keep as-is) ==========
config = {
    'models': {
        'pink_sachet': {
            'url': '100.103.195.124:8006',
            'model_name': 'pink_sachet'
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
    'batch_size': 30  # Compute average over batch_size frames
}
output_dir = "perforation_mc18"
os.makedirs(output_dir, exist_ok=True)

# ========= LOAD DATABASE CONFIGS ==========
with open("db_config.json", "r") as f:
    db_config = json.load(f)

TP_DB = db_config["tp_db"]
EVENT_DB = db_config["event_db"]
BASE_URL = db_config.get("base_url", "http://192.168.0.185:8015")

# ========= INFERENCE FUNCTION ==========
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

# ========= INSERT STATUS & EVENT ==========
def insert_status(machine_cfg, avg_count, tp_db, event_db, base_url, frame=None):
    tp_table = machine_cfg["tp_table"]
    tp_column = machine_cfg["tp_column"]
    camera_id = machine_cfg["camera_id"]

    active = 1 if avg_count <= 4 else 0
    file_url = ""

    if active == 1 and frame is not None:
        # ✅ Folder where image will actually be saved
        low_perf_dir = "/home/ai4m/develop/data/baumer"
        os.makedirs(low_perf_dir, exist_ok=True)

        # ✅ Generate timestamp and filename
        timestamp_str = datetime.datetime.now().strftime("%m_%d_%Y_%I_%M_%S_%f_%p")
        filename = f"low_perforation_{camera_id}_{timestamp_str}.jpeg"

        # ✅ Save file locally
        output_path = os.path.join(low_perf_dir, filename)
        cv2.imwrite(output_path, frame)
        print(f"[{camera_id}] ✅ Image saved at: {output_path}")

        # ✅ Generate correct public URL (for UI)
        # base_url should correspond to where /home/ai4m/develop/data/baumer is served from
        file_url = f"{base_url}/{filename}"
        print(f"[{camera_id}] 🔗 File URL: {file_url}")

    # ========== TP STATUS TABLE ==========
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

    # ========== EVENT TABLE ==========
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


# ========= MAIN PROCESSOR LOOP ==========
if __name__ == "__main__":
    client = grpcclient.InferenceServerClient(url=config['models']['pink_sachet']['url'])
    perforation_counts = []

    # ZMQ/TCP connection for live frames
    context = zmq.Context()
    socket_mc18 = context.socket(zmq.SUB)
    socket_mc18.connect("tcp://localhost:5563")
    socket_mc18.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc18, zmq.POLLIN)

    frame_idx = 0
    while True:
        socks = dict(poller.poll(1000))
        if socket_mc18 in socks and socks[socket_mc18] == zmq.POLLIN:
            msg = socket_mc18.recv()
            nparr = np.frombuffer(msg, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if frame is None:
                continue
            frame = cv2.flip(frame, -1)
            frame_idx += 1
            frame_name = f"frame_{frame_idx}.jpg"
            print(f"➡️ Received frame {frame_idx} from ZMQ")

            boxes, classes, scores = run_inference(client, config['models']['pink_sachet']['model_name'], frame)
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

            print(f"✅ Frame: {frame_idx} | Perforations: {perforation_count}")

            # Compute average every batch
            if len(perforation_counts) == config['batch_size']:
                avg_count = np.mean(perforation_counts)
                print(f"📊 Average perforations (last {config['batch_size']} frames): {avg_count:.2f}")

                insert_status(avg_count, TP_DB, EVENT_DB, frame)
                perforation_counts.clear()

