import os
import cv2
import json
import uuid
import sys
import threading
import queue
import psycopg2
import datetime
import numpy as np
import zmq
import tritonclient.grpc as grpcclient
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Tuple, Optional, Any


class DBManager:
    """Database manager with logging support"""
    def __init__(self, conn_str: str, logger: logging.Logger, table_name: str):
        self.conn = psycopg2.connect(conn_str)
        self.logger = logger
        self.table_name = table_name

    def execute(self, query: str, params: Optional[tuple] = None) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                self.conn.commit()
                self.logger.info(f"DB Success: {query.split()[0]} operation")
                return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"DB Error: {e}", exc_info=True)
            return False

    def check_table_empty(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                return cur.fetchone()[0] == 0
        except Exception as e:
            self.logger.error(f"Error checking table {self.table_name}: {e}")
            return True


class TritonModel:
    """Triton inference model wrapper"""
    def __init__(self, url: str, model_name: str, classes: Dict, logger: logging.Logger,
                 size: int = 1280, threshold: float = 0.25):
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.size = size
        self.thresh = threshold
        self.classes = classes
        self.logger = logger

    def infer(self, img: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        try:
            h, w = img.shape[:2]
            img_copy = img.copy()
            img_processed = cv2.resize(cv2.cvtColor(img_copy, cv2.COLOR_BGR2RGB),
                                      (self.size, self.size)) / 255.0
            input_tensor = img_processed.transpose(2, 0, 1)[np.newaxis].astype(np.float32)

            inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
            inputs[0].set_data_from_numpy(input_tensor)
            outputs = [grpcclient.InferRequestedOutput(n)
                      for n in ['det_boxes', 'det_scores', 'det_classes']]

            results = self.client.infer(model_name=self.model_name, inputs=inputs, outputs=outputs)
            boxes = results.as_numpy('det_boxes')
            scores = results.as_numpy('det_scores')
            classes = results.as_numpy('det_classes')

            mask = scores > self.thresh
            boxes = (boxes[mask] * [w/self.size, h/self.size, w/self.size, h/self.size]).astype(int)
            return boxes, classes[mask], img_copy, scores[mask]
        except Exception as e:
            self.logger.error(f"Inference error in {self.model_name}: {e}", exc_info=True)
            return np.array([]), np.array([]), img, np.array([])


class DetectionProcessor:
    """Unified detection processor for all machines and sachet types"""
    def __init__(self, machine_id: str, sachet_type: str, config_path: str = "config.json"):
        self.machine_id = machine_id.lower()
        self.sachet_type = sachet_type.lower()
        self.config = self._load_config(config_path)

        if self.machine_id not in self.config["machines"]:
            raise ValueError(f"Machine '{machine_id}' not found in configuration")

        if self.sachet_type not in self.config["sachet_types"]:
            raise ValueError(f"Sachet type '{sachet_type}' not found in configuration")

        self.machine_config = self.config["machines"][self.machine_id]
        self.sachet_config = self.config["sachet_types"][self.sachet_type]
        self.logger = self._setup_logging()

        # Initialize components
        self.db = DBManager(
            self.config["database"]["connection_string"],
            self.logger,
            self.machine_config["db_table"]
        )
        self.model = self._initialize_model()
        self.triton_client = grpcclient.InferenceServerClient(
            url=self.config["triton"]["classification_server"]
        )

        # Configuration
        self.zone = tuple(self.machine_config["zone"])
        self.eyemark_threshold = self.config["detection"]["eyemark_threshold"]
        self.horizontal_zone = tuple(self.config["detection"]["horizontal_zone"])
        self.output_dir = self.config["detection"]["output_dir"]
        os.makedirs(self.output_dir, exist_ok=True)

        self.q = queue.Queue()
        self.table_empty = self.db.check_table_empty()
        self.event_mapping = self.config["event_mapping"]
        self.class_names = ['bad', 'good']
        self.class_mappings = self.sachet_config["detection_class_mappings"]

        self.logger.info(f"DetectionProcessor initialized for {self.machine_id.upper()} - {self.sachet_type.upper()}")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, "r") as fp:
                return json.load(fp)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")

    def _setup_logging(self) -> logging.Logger:
        """Setup logging with rotation"""
        log_config = self.config.get("logging", {})
        logger = logging.getLogger(f"VisionDetector_{self.machine_id.upper()}_{self.sachet_type.upper()}")
        logger.setLevel(log_config.get("level", "INFO"))
        logger.handlers.clear()

        # File handler
        log_file = log_config.get("file", "vision_detector_{machine_id}_{sachet_type}.log").format(
            machine_id=self.machine_id,
            sachet_type=self.sachet_type
        )
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_config.get("max_bytes", 10485760),
            backupCount=log_config.get("backup_count", 5)
        )
        file_handler.setLevel(log_config.get("level", "INFO"))

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_config.get("level", "INFO"))

        # Formatter
        formatter = logging.Formatter(
            log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def _initialize_model(self) -> TritonModel:
        """Initialize Triton detection model"""
        class_defs = self.config["class_definitions"]
        classes = {int(k): (v["name"], tuple(v["color"])) for k, v in class_defs.items()}

        return TritonModel(
            url=self.config["triton"]["detection_server"],
            model_name=self.sachet_config["model_name"],
            classes=classes,
            logger=self.logger,
            size=self.config["triton"]["detection_model_size"],
            threshold=self.config["triton"]["detection_threshold"]
        )

    def process(self, img: np.ndarray):
        """Process incoming frame"""
        if img is None:
            return

        filename = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p") + ".jpeg"

        # Run inference
        boxes, classes, img_copy, conf = self.model.infer(img)

        # Draw boxes for visualization
        image = self._draw_boxes(boxes, classes, img_copy.copy())

        # Check for wrinkle first
        if self.class_mappings["wrinkle"] in classes:
            self._handle_detection('wrinkle', boxes, classes, filename, 'tp22', img_copy)
            self.logger.info('Wrinkle detected')
        # Check for redtape (only for red sachet based on old code)
        elif self.sachet_type == 'redtape' and self.class_mappings["redtape"] in classes:
            self._handle_detection('redtape', boxes, classes, filename, 'tp17', img_copy)
            self.logger.info('Redtape detected')
        else:
            # Check eyemark shift
            self.check_eyemark_shift(boxes, classes, filename, img_copy, conf)

    def _handle_detection(self, prefix: str, boxes: np.ndarray, classes: np.ndarray,
                         filename: str, tp_column: str, img: np.ndarray):
        """Handle detection event - save image and update database"""
        output_path = f"{self.output_dir}/{prefix}_{self.machine_id}_{filename}"
        img_with_boxes = self._draw_boxes(boxes, classes, img.copy())
        cv2.imwrite(output_path, img_with_boxes)

        now = datetime.datetime.now()
        camera_id = self.machine_config["camera_id"]
        event_type = self.event_mapping.get(prefix, prefix)
        file_url = f"{self.config['detection']['file_server_url']}/{prefix}_{self.machine_id}_{filename}"

        event_data = (now, str(uuid.uuid4()), "Baumer Camera", camera_id, filename, event_type, "Quality")
        tp_data = json.dumps({
            "timestamp": now.isoformat(),
            "uuid": str(uuid.uuid4()),
            "active": 1,
            "filepath": file_url,
            "color_code": 3
        })

        # Insert event
        self.db.execute(
            "INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            event_data
        )

        # Update status table
        table_name = self.machine_config["db_table"]
        if self.table_empty:
            self.db.execute(
                f"INSERT INTO {table_name}({tp_column}) VALUES (%s)",
                (tp_data,)
            )
            self.table_empty = False
        else:
            self.db.execute(
                f"UPDATE {table_name} SET {tp_column} = %s",
                (tp_data,)
            )

        self.logger.info(f"{prefix.upper()} DETECTED | Saved: {output_path} | DB Updated")

    def check_eyemark_shift(self, boxes: np.ndarray, classes: np.ndarray,
                           filename: str, img: np.ndarray, conf: np.ndarray) -> bool:
        """Check for eyemark shift detection"""
        eyemark_class = self.class_mappings["eyemark"]
        perforation_class = self.class_mappings["perforation"]

        eyemarks = [box for box, cls, cf in zip(boxes, classes, conf)
                   if cls == eyemark_class and self._in_zone(box)]
        perforations = [box for box, cls, cf in zip(boxes, classes, conf)
                       if cls == perforation_class and self._in_zone(box)]

        if len(eyemarks) > 0 and len(perforations) != 0:
            eyemark = eyemarks[0]
            eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2

            nearest_p = min(perforations,
                          key=lambda p: abs(eyemark_centroid_y - (p[1] + p[3]) / 2))
            lower_bound = nearest_p[1] - self.eyemark_threshold
            upper_bound = nearest_p[3] + self.eyemark_threshold

            if not (lower_bound <= eyemark_centroid_y <= upper_bound):
                self._handle_detection('eyemark_shift', boxes, classes, filename, 'tp20', img)
                return True

        return False

    def transform_image(self, image: np.ndarray) -> np.ndarray:
        """Transform image for classification model"""
        try:
            # Resize
            image = cv2.resize(image, (224, 224))

            # Normalize
            image = image.astype(np.float32) / 255.0
            mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
            std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
            image = (image - mean) / std

            # Transpose and add batch dimension
            image = np.transpose(image, (2, 0, 1))
            input_data = np.expand_dims(image, axis=0)

            return input_data
        except Exception as e:
            self.logger.error(f"Image transformation error: {e}")
            return None

    def check_bad_perforation(self, boxes: np.ndarray, classes: np.ndarray,
                             filename: str, img: np.ndarray) -> bool:
        """Check for bad perforation using classifier"""
        try:
            x1, y1, x2, y2 = self.zone
            cropped_img = img[y1:y2, x1:x2]
            cropped_img = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2RGB)

            input_data = self.transform_image(cropped_img)
            if input_data is None:
                return False

            triton_input = grpcclient.InferInput("input", [1, 3, 224, 224], "FP32")
            triton_input.set_data_from_numpy(input_data)

            result = self.triton_client.infer(
                model_name=self.config["triton"]["perforation_classifier"]["model_name"],
                inputs=[triton_input],
                outputs=[]
            )

            predictions = np.squeeze(result.as_numpy("output"))
            predicted_class = predictions.argmax()
            confidence = predictions[predicted_class]

            confidence_threshold = self.config["triton"]["perforation_classifier"]["confidence_threshold"]
            if confidence < confidence_threshold:
                self.logger.debug(f"Perforation confidence too low: {confidence:.3f}")
                return False

            label = self.class_names[predicted_class]
            self.logger.info(f"Perforation prediction: {label} (confidence: {confidence:.3f})")

            if label == "bad":
                self._handle_detection('perforation_missing', boxes, classes, filename, 'tp18', img)
                return True

        except Exception as e:
            self.logger.error(f"Error in check_bad_perforation: {e}", exc_info=True)

        return False

    def _in_zone(self, box: np.ndarray) -> bool:
        """Check if box centroid is within defined zone"""
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        zone_x1, zone_y1, zone_x2, zone_y2 = self.zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)

    def _in_horizontal_zone(self, box: np.ndarray, horizontal_zone: Tuple) -> bool:
        """Check if box centroid is within horizontal zone"""
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        zone_x1, zone_y1, zone_x2, zone_y2 = horizontal_zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)

    def _draw_boxes(self, boxes: np.ndarray, classes: np.ndarray, img: np.ndarray) -> np.ndarray:
        """Draw bounding boxes on image"""
        if len(boxes) == 0 or len(classes) == 0 or len(boxes) != len(classes):
            return img

        for box, cls in zip(boxes, classes):
            if len(box) != 4:
                continue

            x1, y1, x2, y2 = box
            name, color = self.model.classes.get(
                int(cls), ("custom", (0, 0, 255))
            )
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            cv2.putText(img, name, (x1, y1 - 10),
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

        return img

    def run(self):
        """Main loop - subscribe to ZMQ and process frames"""
        self.logger.info(f"Starting vision detector for {self.machine_id.upper()} - {self.sachet_type.upper()}")

        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        zmq_host = self.config["zmq"]["host"]
        zmq_port = self.machine_config["zmq_port"]
        socket.connect(f"tcp://{zmq_host}:{zmq_port}")
        socket.setsockopt_string(zmq.SUBSCRIBE, self.config["zmq"]["subscribe_filter"])

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        self.logger.info(f"Connected to ZMQ at tcp://{zmq_host}:{zmq_port}")

        try:
            while True:
                socks = dict(poller.poll())
                if socket in socks and socks[socket] == zmq.POLLIN:
                    frame_bytes = socket.recv()
                    frame = cv2.imdecode(
                        np.frombuffer(frame_bytes, dtype=np.uint8),
                        cv2.IMREAD_COLOR
                    )

                    if self.machine_config.get("flip_frame", False):
                        frame = cv2.flip(frame, -1)

                    self.logger.debug(f"Frame received: {frame.shape}")
                    self.process(frame)

        except KeyboardInterrupt:
            self.logger.info(f"Stopping vision detector for {self.machine_id.upper()} - {self.sachet_type.upper()}")
        finally:
            socket.close()
            context.term()


def main():
    """Main entry point"""
    if len(sys.argv) < 3:
        print("Usage: python vision_detector.py <machine_id> <sachet_type> [config_path]")
        print("\nExample: python vision_detector.py mc17 black")
        print("\nAvailable machines: mc17, mc18, mc19, mc20, mc21, mc22")
        print("Available sachet types: black, red, white, pink")
        sys.exit(1)

    machine_id = sys.argv[1]
    sachet_type = sys.argv[2]
    config_path = sys.argv[3] if len(sys.argv) > 3 else "config.json"

    try:
        processor = DetectionProcessor(
            machine_id=machine_id,
            sachet_type=sachet_type,
            config_path=config_path
        )
        processor.run()
    except Exception as e:
        print(f"Error initializing detector: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

