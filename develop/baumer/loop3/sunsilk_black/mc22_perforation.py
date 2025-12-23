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
        input_tensor = img_processed.transpose(2, 0, 1)[np.newaxis].astype(np.float32)

        inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
        inputs[0].set_data_from_numpy(input_tensor)
        outputs = [grpcclient.InferRequestedOutput(n) for n in ['det_boxes', 'det_scores', 'det_classes']]

        results = self.client.infer(model_name=self.model_name, inputs=inputs, outputs=outputs)
        boxes = results.as_numpy('det_boxes')
        scores = results.as_numpy('det_scores')
        classes = results.as_numpy('det_classes')

        mask = scores > self.thresh
        boxes = (boxes[mask] * [w / self.size, h / self.size, w / self.size, h / self.size]).astype(int)
        return boxes, classes[mask], img_copy, scores[mask]


class DetectionProcessor:
    def __init__(self, config):
        required_keys = [
            'input_dir', 'output_dir', 'inference_output_dir', 'models', 'db_str', 'table_name',
            'zone', 'eyemark_threshold', 'camera_id', 'event_mapping', 'class_names',
            'event_columns', 'filepath_prefix', 'confidence_threshold', 'horizontal_zone',
            'triton_classifier_url', 'perforation_classifier_model', 'event_table',
            'zone_name', 'alert_type', 'color_code', 'preprocessing', 'hsv_lower',
            'hsv_upper', 'max_contours', 'min_contour_area', 'primary_model',
            'class_ids', 'file_extension', 'timestamp_format', 'socket_port'
        ]
        for key in required_keys:
            if key not in config:
                raise KeyError(f"Missing required configuration key: {key}")

        self.input_dir = config['input_dir']
        self.output_dir = config['output_dir']
        self.inference_output_dir = config['inference_output_dir']
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.inference_output_dir, exist_ok=True)
        self.db = DBManager(config['db_str'], config['table_name'])
        self.models = {
            name: TritonModel(**{**cfg, 'classes': cfg.get('classes')})
            for name, cfg in config['models'].items()
        }
        self.zone = tuple(config['zone'])
        self.eyemark_threshold = config['eyemark_threshold']
        self.camera_id = config['camera_id']
        self.event_mapping = config['event_mapping']
        self.class_names = config['class_names']
        self.event_columns = config['event_columns']
        self.filepath_prefix = config['filepath_prefix']
        self.confidence_threshold = config['confidence_threshold']
        self.horizontal_zone = tuple(config['horizontal_zone'])
        self.triton_classifier_url = config['triton_classifier_url']
        self.perforation_classifier_model = config['perforation_classifier_model']
        self.event_table = config['event_table']
        self.zone_name = config['zone_name']
        self.alert_type = config['alert_type']
        self.color_code = config['color_code']
        self.preprocessing = config['preprocessing']
        self.hsv_lower = np.array(config['hsv_lower'])
        self.hsv_upper = np.array(config['hsv_upper'])
        self.max_contours = config['max_contours']
        self.min_contour_area = config['min_contour_area']
        self.primary_model = config['primary_model']
        self.class_ids = config['class_ids']
        self.file_extension = config['file_extension']
        self.timestamp_format = config['timestamp_format']
        self.q = queue.Queue()
        self.table_empty = self.db.check_table_empty()
        self.triton_client = grpcclient.InferenceServerClient(url=self.triton_classifier_url)

    def process(self, img):
        if img is None:
            return

        filename = str(datetime.datetime.now().strftime(self.timestamp_format)) + self.file_extension
        for name, model in self.models.items():
            threading.Thread(target=self._run_model, args=(name, model, img.copy(), filename)).start()

        while threading.active_count() > 1 or not self.q.empty():
            if not self.q.empty():
                name, boxes, classes, img_copy, conf = self.q.get()

                image = self._draw_boxes(boxes, classes, img_copy.copy())
                cv2.imwrite(os.path.join(self.inference_output_dir,
                                         f"{datetime.datetime.now().strftime(self.timestamp_format)}{self.file_extension}"),
                            image)

                redtape_boxes = self.process_image(img, self.hsv_lower, self.hsv_upper, self.min_contour_area)

                if redtape_boxes:
                    redtape_classes = np.array([self.class_ids['redtape']] * len(redtape_boxes))
                    self._handle_detection('redtape', redtape_boxes, redtape_classes, filename,
                                          self.event_columns['redtape'], img.copy())
                    print('Redtape (HSV-based) detected')

                elif name == self.primary_model:
                    self.check_eyemark_shift(boxes, classes, filename, img_copy, conf)
                    self.check_bad_perforation(boxes, classes, filename, img_copy)

    def _run_model(self, name, model, img, filename):
        try:
            boxes, classes, img_copy, conf = model.infer(img)
            self.q.put((name, boxes, classes, img_copy, conf))
        except Exception as e:
            print(f"{name} failed: {e}")

    def _handle_detection(self, prefix, boxes, classes, filename, tp_column, img):
        output_path = f"{self.output_dir}/{prefix}_{self.camera_id.lower()}_{filename}"
        img_with_boxes = self._draw_boxes(boxes, classes, img.copy())
        cv2.imwrite(output_path, img_with_boxes)

        now = datetime.datetime.now()
        event_type = self.event_mapping.get(prefix, prefix)
        event_data = (now, str(uuid.uuid4()), self.zone_name, self.camera_id, filename, event_type, self.alert_type)
        tp_data = json.dumps({
            "timestamp": now.isoformat(),
            "uuid": str(uuid.uuid4()),
            "active": 1,
            "filepath": f"{self.filepath_prefix}{prefix}_{self.camera_id.lower()}_{filename}",
            "color_code": self.color_code
        })

        self.db.execute(
            f"INSERT INTO {self.event_table} (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
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

    def check_eyemark_shift(self, boxes, classes, filename, img, conf):
        eyemarks = [box for box, cls, cf in zip(boxes, classes, conf) if cls == self.class_ids['eyemark'] and self._in_zone(box)]
        perforations = [box for box, cls, cf in zip(boxes, classes, conf) if cls == self.class_ids['perforation'] and self._in_zone(box)]

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
                    self._handle_detection('eyemark_shift', boxes, classes, filename,
                                          self.event_columns['eyemark_shift'], img)
                    return True
                return False
        return False

    def process_image(self, image, hsv_lower, hsv_upper, min_contour_area):
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        mask = cv2.inRange(hsv, hsv_lower, hsv_upper)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        if len(contours) > self.max_contours:
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
            image = cv2.resize(image, tuple(self.preprocessing['resize_dims']))
            image = image.astype(np.float32) / 255.0
            mean = np.array(self.preprocessing['mean'], dtype=np.float32)
            std = np.array(self.preprocessing['std'], dtype=np.float32)
            image = (image - mean) / std
            image = np.transpose(image, (2, 0, 1))
            input_data = np.expand_dims(image, axis=0)
            return input_data
        except Exception as e:
            print(f"Error in transform_image: {e}")
            return None

    def check_bad_perforation(self, boxes, classes, filename, img):
        try:
            x1, y1, x2, y2 = self.zone
            cropped_img = img[y1:y2, x1:x2]
            cropped_img = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2RGB)
            input_data = self.transform_image(cropped_img)
            if input_data is None:
                return False
            triton_input = grpcclient.InferInput("input", [1, 3, *self.preprocessing['resize_dims']], "FP32")
            triton_input.set_data_from_numpy(input_data)
            result = self.triton_client.infer(
                model_name=self.perforation_classifier_model,
                inputs=[triton_input],
                outputs=[grpcclient.InferRequestedOutput("output")]
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
                self._handle_detection('perforation_missing', boxes, classes, filename,
                                      self.event_columns['perforation_missing'], img)
                return True
        except Exception as e:
            print(f"Error in check_bad_perforation: {e}")
        return False

    def check_solo_eyemark(self, boxes, classes, filename, img):
        eyemarks = [box for box, cls in zip(boxes, classes) if cls == self.class_ids['eyemark'] and self._in_zone(box)]
        if len(eyemarks) != 1:
            return False
        eyemark = eyemarks[0]
        eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2
        print(eyemark_centroid_y)
        return False

    def _in_zone(self, box):
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        zone_x1, zone_y1, zone_x2, zone_y2 = self.zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)

    def _in_horizontal_zone(self, box, horizontal_zone):
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        zone_x1, zone_y1, zone_x2, zone_y2 = horizontal_zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)

    def _draw_boxes(self, boxes, classes, img):
        if len(boxes) == 0 or len(classes) == 0 or len(boxes) != len(classes):
            return img
        for box, cls in zip(boxes, classes):
            if len(box) != 4:
                continue
            x1, y1, x2, y2 = box
            name, color = self.models[self.primary_model].classes.get(int(cls), ("custom", (0, 0, 255)))
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            cv2.putText(img, name, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        return img


if __name__ == '__main__':
    # Derive MACHINE_ID from script name
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    MACHINE_ID = script_name.upper()  # e.g., 'mc22' -> 'MC22'

    # Load configuration from config.json
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r') as f:
            all_configs = json.load(f)
        config = all_configs.get(MACHINE_ID)
        if not config:
            print(f"Error: No configuration found for camera {MACHINE_ID} in config.json")
            exit(1)
    except FileNotFoundError:
        print(f"Error: Configuration file config.json not found at {config_path}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config.json: {e}")
        exit(1)

    # Initialize processor with JSON config
    processor = DetectionProcessor(config)

    # Set up ZMQ socket
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://localhost:{config['socket_port']}")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    try:
        while True:
            socks = dict(poller.poll())
            if socket in socks and socks[socket] == zmq.POLLIN:
                frame_bytes = socket.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                print(frame.shape)
                processor.process(frame)
    except KeyboardInterrupt:
        print("Subscriber stopped by user.")
    finally:
        socket.close()
        context.term()