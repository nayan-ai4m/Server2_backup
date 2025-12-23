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
import logging
import time


class DBManager:
    def __init__(self, conn_str, table_name):
        self.conn = psycopg2.connect(conn_str)
        self.table_name = table_name

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

    def check_table_empty(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            return cur.fetchone()[0] == 0


class TritonModel:
    def __init__(self, url, model_name, size, threshold, classes=None):
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.size = size
        self.thresh = threshold
        self.classes = {
            int(k): (v[0], tuple(v[1])) for k, v in classes.items()
        } if classes else {
            0: ("redtape", (0,0,255)),
            1: ("eyemark", (0,255,0)),
            2: ("slitting_cut",(255,0,255)),
            3: ("perforation", (0,255,255)),
            4: ("matte-on-belt", (0,255,255)),
            6: ("slitting", (255,0,255)),
            7: ("wrinkle", (255,0,255)),
            9: ("perforation_bad",(0,255,255))
        }

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
        return boxes, classes[mask], img_copy, scores[mask]


class DetectionProcessor:
    def __init__(self, config, machine_id):
        self.input_dir = config['input_dir']
        self.output_dir = config['output_dir']
        self.machine_id = machine_id
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.db = DBManager(config['db_str'], config['database']['table'])
        
        self.models = {
            name: TritonModel(**{**cfg, 'classes': cfg.get('classes')}) 
            for name, cfg in config['models'].items()
        }
        self.zone = config['zone']
        self.eyemark_threshold = config['eyemark_threshold']
        self.event_mapping = config['event_mapping']
        self.class_names = config['class_names']
        self.event_columns = config['event_columns']
        self.filepath_prefix = config['filepath_prefix']
        self.confidence_threshold = config['confidence_threshold']
        self.horizontal_zone = tuple(config['horizontal_zone'])
        self.q = queue.Queue()
        self.table_empty = self.db.check_table_empty()
        
        # Add Triton client for perforation classifier
        self.triton_client = grpcclient.InferenceServerClient(url=config['triton_classifier_url'])

    def process(self, img):
        if img is None: return

        filename = str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".jpeg"
        for name, model in self.models.items():
            threading.Thread(target=self._run_model, args=(name, model, img.copy(), filename)).start()

        while threading.active_count() > 1 or not self.q.empty():
            if not self.q.empty():
                name, boxes, classes, img_copy, conf = self.q.get()
                
                image = self._draw_boxes(boxes, classes, img_copy.copy())
                
                if name == 'white_sachet':
                    if 0 in classes:
                        self._handle_detection('redtape', boxes, classes, filename, self.event_columns['redtape'], img_copy)
                        print('redtape')
                    elif 7 in classes:
                        self._handle_detection('wrinkle', boxes, classes, filename, self.event_columns['wrinkle'], img_copy)
                        print('wrinkle')
                    else:
                        self.check_eyemark_shift(boxes, classes, filename, img_copy, conf)

    def _run_model(self, name, model, img, filename):
        try:
            boxes, classes, img_copy, conf = model.infer(img)
            self.q.put((name, boxes, classes, img_copy, conf))
        except Exception as e:
            print(f"{name} failed: {e}")

    def _handle_detection(self, prefix, boxes, classes, filename, tp_column, img):
        output_path = f"{self.output_dir}/{prefix}_{self.machine_id}_{filename}"
        img_with_boxes = self._draw_boxes(boxes, classes, img.copy())
        cv2.imwrite(output_path, img_with_boxes)

        now = datetime.datetime.now()
        event_type = self.event_mapping.get(prefix, prefix)
        event_data = (now, str(uuid.uuid4()), "Baumer Camera", self.machine_id.upper(), filename, event_type, "Quality")
        tp_data = json.dumps({
            "timestamp": now.isoformat(),
            "uuid": str(uuid.uuid4()),
            "active": 1,
            "filepath": f"{self.filepath_prefix}{prefix}_{self.machine_id}_{filename}",
            "color_code": 3
        })

        self.db.execute(
            "INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            event_data
        )

        if self.table_empty:
            self.db.execute(
                f"INSERT INTO {self.db.table_name} ({tp_column}) VALUES (%s)",
                (tp_data,)
            )
            self.table_empty = False
        else:
            self.db.execute(
                f"UPDATE {self.db.table_name} SET {tp_column} = %s",
                (tp_data,)
            )

        print(f"{prefix.upper()} DETECTED | Saved: {output_path} | DB Updated")

    def check_eyemark_shift(self, boxes, classes, filename, img, conf):
        eyemarks = [box for box, cls, cf in zip(boxes, classes, conf) if cls == 1 and self._in_zone(box)]
        perforations = [box for box, cls, cf in zip(boxes, classes, conf) if cls == 3 and self._in_zone(box)]
        
        eyemarks_in_horizontal_zone = [
            box for box in eyemarks
            if self._in_horizontal_zone(box, self.horizontal_zone)
        ]
        
        if len(eyemarks) > 0:
            eyemark = eyemarks[0]
            eyemark_centroid_x = (eyemark[0] + eyemark[2]) / 2
            eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2
            
            perf_on_y = [
                box for box in perforations
                if box[1] <= eyemark_centroid_y <= box[3]
            ]
            perf_on_x = [
                box for box in perforations
                if box[0] <= eyemark_centroid_x <= box[2]
            ]
            
            if len(perforations) != 0:
                eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2
                nearest_p = min(perforations, key=lambda p: abs(eyemark_centroid_y - (p[1] + p[3]) / 2))
                lower_bound = nearest_p[1] - self.eyemark_threshold
                upper_bound = nearest_p[3] + self.eyemark_threshold
                if not (lower_bound <= eyemark_centroid_y <= upper_bound):
                    self._handle_detection('eyemark_shift', boxes, classes, filename, self.event_columns['eyemark_shift'], img)
                    return True
                return False
        else:
            return False

    def process_image(self, image, hsv_lower, hsv_upper, min_contour_area=1500):
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        mask = cv2.inRange(hsv, hsv_lower, hsv_upper)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        if len(contours) > 5:
            return []

        boxes = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area >= min_contour_area:
                x, y, w, h = cv2.boundingRect(cnt)
                boxes.append([x, y, x + w, y + h])

        return boxes

    def transform_image(self, image):
        try:
            image = cv2.resize(image, (224, 224))
            image = image.astype(np.float32) / 255.0
            mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
            std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
            image = (image - mean) / std
            image = np.transpose(image, (2, 0, 1))
            input_data = np.expand_dims(image, axis=0)
            return input_data
        except Exception as e:
            print(e)

    def check_bad_perforation(self, boxes, classes, filename, img):
        try:
            x1, y1, x2, y2 = self.zone
            cropped_img = img[y1:y2, x1:x2]
            cropped_img = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2RGB)
            input_data = self.transform_image(cropped_img)
            triton_input = grpcclient.InferInput("input", [1, 3, 224, 224], "FP32")
            triton_input.set_data_from_numpy(input_data)
            result = self.triton_client.infer(
                model_name="perforation_classify",
                inputs=[triton_input],
                outputs=[]
            )
            predictions = np.squeeze(result.as_numpy("output"))
            predicted_class = predictions.argmax()
            confidence = predictions[predicted_class]
            if confidence < self.confidence_threshold:
                print(f"Prediction confidence too low: {confidence:.3f}")
                return False
            label = self.class_names[predicted_class]
            print(f"Prediction for perforation zone: {label} with confidence {confidence:.3f}")
            if label == "bad":
                print("bad_perforation")
                self._handle_detection('perforation_missing', boxes, classes, filename, self.event_columns['perforation_missing'], img)
                return True
        except Exception as e:
            print(f"Error in check_bad_perforation: {e}")
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
        centroid_x = (x1+x2)/2
        centroid_y = (y1+y2) /2
        zone_x1, zone_y1, zone_x2, zone_y2 = self.zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2 )

    def _in_horizontal_zone(self, box, horizontal_zone):
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        zone_x1, zone_y1, zone_x2, zone_y2 = horizontal_zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)

    def _draw_boxes(self, boxes, classes, img):
        if (len(boxes) == 0 or len(classes) == 0 or len(boxes) != len(classes)):
            return img
        
        for box, cls in zip(boxes, classes):
            if len(box) != 4:
                continue
            x1, y1, x2, y2 = box
            name, color = self.models['white_sachet'].classes.get(int(cls), ("custom", (0, 0, 255)))
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            cv2.putText(img, name, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        return img


if __name__ == '__main__':
    # Define the machine ID
    MACHINE_ID = 'mc17'

    # Load configuration from config.json
    with open('config.json', 'r') as f:
        config_data = json.load(f)
    
    # Select configuration for mc17
    if MACHINE_ID not in config_data['cameras']:
        raise ValueError(f"Configuration for {MACHINE_ID} not found in config.json")
    
    config = config_data['cameras'][MACHINE_ID]
    
    for model_name, model_config in config['models'].items():
        required_keys = ['url', 'model_name', 'size', 'threshold']
        missing_keys = [key for key in required_keys if key not in model_config]
        if missing_keys:
            raise ValueError(f"Missing required keys {missing_keys} in model configuration for {model_name}")
    
    # Construct database connection string
    db = config['database']
    db_str = f"postgres://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}?sslmode={db['sslmode']}"
    
    # Prepare configuration dictionary
    config_dict = {
        'input_dir': config['input_dir'],
        'output_dir': config['output_dir'],
        'models': config['models'],
        'db_str': db_str,
        'zone': tuple(config['zone']),
        'eyemark_threshold': config['eyemark_threshold'],
        'event_mapping': config['event_mapping'],
        'class_names': config['class_names'],
        'event_columns': config['event_columns'],
        'filepath_prefix': config['filepath_prefix'],
        'confidence_threshold': config['confidence_threshold'],
        'horizontal_zone': config['horizontal_zone'],
        'triton_classifier_url': config['triton_classifier_url']
    }

    # Initialize processor with machine_id
    processor = DetectionProcessor(config_dict, MACHINE_ID)
    
    # Set up ZMQ
    context = zmq.Context()
    socket_mc17 = context.socket(zmq.SUB)
    socket_mc17.connect(config['zmq']['address'])
    socket_mc17.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc17, zmq.POLLIN)
    
    try:
        while True:
            socks = dict(poller.poll())
            if socket_mc17 in socks and socks[socket_mc17] == zmq.POLLIN:
                frame_bytes = socket_mc17.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                print(frame.shape)
                processor.process(frame)
    except KeyboardInterrupt:
        print("Subscriber stopped by user.")
    finally:
        socket_mc17.close()
        context.term()