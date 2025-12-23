import os
import cv2
import json
import uuid
import threading
import queue
import psycopg2
import datetime
import numpy as np
import zmq
import tritonclient.grpc as grpcclient
from tritonclient.utils import InferenceServerException

class DBManager:
    def __init__(self, conn_str):
        self.conn = psycopg2.connect(conn_str)

    def execute(self, query, params=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                self.conn.commit()
            print(f"DB Success: {query.split()[0]} operation")
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"DB Error: {e}")
            return False

    def check_table_empty(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0] == 0

class TritonModel:
    def __init__(self, url, model_name, size, threshold, classes):
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.size = size
        self.thresh = threshold
        self.classes = {int(k): (v[0], tuple(v[1])) for k, v in classes.items()}

    def infer(self, img):
        h, w = img.shape[:2]
        img_copy = img.copy()
        img_processed = cv2.resize(cv2.cvtColor(img_copy, cv2.COLOR_BGR2RGB), (self.size, self.size)) / 255.0
        input_tensor = img_processed.transpose(2,0,1)[np.newaxis].astype(np.float32)

        inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
        inputs[0].set_data_from_numpy(input_tensor)
        outputs = [grpcclient.InferRequestedOutput(n) for n in ['det_boxes','det_scores','det_classes']]

        results = self.client.infer(model_name=self.model_name, inputs=inputs, outputs=outputs)
        boxes = results.as_numpy('det_boxes')
        scores = results.as_numpy('det_scores')
        classes = results.as_numpy('det_classes')

        mask = scores > self.thresh
        boxes = (boxes[mask] * [w/self.size, h/self.size, w/self.size, h/self.size]).astype(int)
        return boxes, classes[mask], img_copy

class DetectionProcessor:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)['cameras']['mc25']
        
        self.input_dir = config['input_dir']
        self.output_dir = config['output_dir']
        os.makedirs(self.output_dir, exist_ok=True)
        
        db_config = config['database']
        self.db = DBManager(
            f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} "
            f"user={db_config['user']} password={db_config['password']} sslmode={db_config['sslmode']}"
        )
        
        self.models = {
            name: TritonModel(
                url=model_config['url'],
                model_name=model_config['model_name'],
                size=model_config['size'],
                threshold=model_config['threshold'],
                classes=model_config['classes']
            ) 
            for name, model_config in config['models'].items()
        }
        self.zone = config['zone']
        self.eyemark_threshold = config['eyemark_threshold']
        self.table_name = db_config['table']
        self.event_mapping = config['event_mapping']
        self.filepath_url_prefix = config['filepath_url_prefix']
        self.q = queue.Queue()
        self.table_empty = self.db.check_table_empty(self.table_name)

    def process(self, img):
        if img is None: return

        filename = str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".jpeg"
        for name, model in self.models.items():
            threading.Thread(target=self._run_model, args=(name, model, img.copy(), filename)).start()

        while threading.active_count() > 1 or not self.q.empty():
            if not self.q.empty():
                name, boxes, classes, img_copy = self.q.get()
                
                image = self._draw_boxes(boxes, classes, img_copy.copy())
                
                if name == 'white_sachet':
                    if 0 in classes:
                        self._handle_detection('redtape', boxes, classes, filename, 'tp17', img_copy)
                        print('redtap')
                    elif 7 in classes:
                        self._handle_detection('wrinkle', boxes, classes, filename, 'tp22', img_copy)
                        print('wrinkle')
                    else:
                        self.check_eyemark_shift(boxes, classes, filename, img_copy)

    def _run_model(self, name, model, img, filename):
        try:
            boxes, classes, img_copy = model.infer(img)
            self.q.put((name, boxes, classes, img_copy))
        except Exception as e:
            print(f"{name} failed: {e}")

    def _handle_detection(self, prefix, boxes, classes, filename, tp_column, img):
        output_path = f"{self.output_dir}/{prefix}_mc25_{filename}"
        img_with_boxes = self._draw_boxes(boxes, classes, img.copy())
        cv2.imwrite(output_path, img_with_boxes)

        now = datetime.datetime.now()
        event_type = self.event_mapping.get(prefix, prefix)
        event_data = (now, str(uuid.uuid4()), "Baumer Camera", "MC25", filename, event_type, "Quality")
        tp_data = json.dumps({
            "timestamp": now.isoformat(),
            "uuid": str(uuid.uuid4()),
            "active": 1,
            "filepath": f"{self.filepath_url_prefix}/{prefix}_mc25_{filename}",
            "color_code": 3
        })

        self.db.execute(
            "INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            event_data
        )

        if self.table_empty:
            self.db.execute(
                f"INSERT INTO {self.table_name} ({tp_column}) VALUES (%s)",
                (tp_data,)
            )
            self.table_empty = False
        else:
            self.db.execute(
                f"UPDATE {self.table_name} SET {tp_column} = %s",
                (tp_data,)
            )

        print(f"{prefix.upper()} DETECTED | Saved: {output_path} | DB Updated")

    def check_eyemark_shift(self, boxes, classes, filename, img):
        eyemarks = [box for box, cls in zip(boxes, classes) if cls == 1 and self._in_zone(box)]
        perforations = [box for box, cls in zip(boxes, classes) if cls == 3 and self._in_zone(box)]
        if len(eyemarks) > 0 and len(perforations) > 0:
            eyemark = eyemarks[0]
            eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2

            nearest_p = min(perforations, key=lambda p: abs(eyemark_centroid_y - (p[1] + p[3])/2))
            lower_bound = nearest_p[1] - self.eyemark_threshold
            upper_bound = nearest_p[3] + self.eyemark_threshold
            print(lower_bound, eyemark_centroid_y, upper_bound)
            if not (lower_bound <= eyemark_centroid_y <= upper_bound):
                self._handle_detection('eyemark_shift', boxes, classes, filename, 'tp20', img)
                return True
            return False
        else:
            return False

    def check_solo_eyemark(self, boxes, classes, filename, img):
        eyemarks = [box for box, cls in zip(boxes, classes) if cls == 1 and self._in_zone(box)]
        if len(eyemarks) != 1:
            return False
        eyemark = eyemarks[0]
        eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2
        print(eyemark_centroid_y)    
        
    def _in_zone(self, box):
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        zone_x1, zone_y1, zone_x2, zone_y2 = self.zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)

    def _draw_boxes(self, boxes, classes, img):
        for (x1, y1, x2, y2), cls in zip(boxes, classes):
            name, color = self.models['white_sachet'].classes.get(int(cls), ("unknown", (255,255,255)))
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            cv2.putText(img, name, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        return img

if __name__ == '__main__':
    processor = DetectionProcessor('config.json')
    context = zmq.Context()
    socket_mc25 = context.socket(zmq.SUB)
    socket_mc25.connect(processor.zmq_address)
    socket_mc25.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc25, zmq.POLLIN)
    try:
        while True:
            socks = dict(poller.poll())
            if socket_mc25 in socks and socks[socket_mc25] == zmq.POLLIN:
                frame_bytes = socket_mc25.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                processor.process(frame)
                print("received")
    except KeyboardInterrupt:
        print("Subscriber stopped by user.")
    finally:
        socket_mc25.close()
        context.term()