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

    def check_table_empty(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mc25_tp_status")
            return cur.fetchone()[0] == 0

class TritonModel:
    def __init__(self, url, model_name, size=1280, threshold=0.25):
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.size = size
        self.thresh = threshold
        # self.classes = {
        #     0: ("redtape", (0,0,255)),
        #     1: ("eyemark", (0,255,0)),
        #     2: ("slitting_cut",(255,0,255)),
        #     3: ("perforation", (0,255,255)),
        #     4: ("matte-on-belt", (0,255,255)),
        #     6: ("slitting", (255,0,255)),
        #     7: ("wrinkle", (255,0,255)),
        #     9: ("perforation_bad",(0,255,255))
        # }

        self.classes = {
            0: ("perforation", (0,255,255)),
            1: ("eyemark", (0,255,0)),
            2: ("redtape", (0,0,255)),
            3: ("wrinkle", (255,0,255)),
            4: ("matte-on-belt", (0,255,255)),
            5: ("slitting", (255,0,255))
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
        return boxes, classes[mask], img_copy

class DetectionProcessor:
    def __init__(self, input_dir, output_dir, models, db_str, zone, eyemark_threshold):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.db = DBManager(db_str)
        self.models = {name: TritonModel(**cfg) for name, cfg in models.items()}
        self.zone = zone  # (x1,y1,x2,y2)
        self.eyemark_threshold = eyemark_threshold
        self.q = queue.Queue()
        self.table_empty = self.db.check_table_empty()
        self.last_wrinkle_time = None
        self.cooldown_minutes = 5
        self.event_mapping = {
            'eyemark_shift': 'Vertical Eyemark Shift Detected',
            'redtape': 'Redtape Detected',
            'perforation_missing': 'Bad Perforation Detected',
            'wrinkle': 'Wrinkle Detected'
        }

    def process(self, img):
        #img = cv2.imread(img_path)
        if img is None: return

        filename = str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".jpeg"
        for name, model in self.models.items():
            threading.Thread(target=self._run_model, args=(name, model, img.copy(), filename)).start()

        while threading.active_count() > 1 or not self.q.empty():
            if not self.q.empty():
                name, boxes, classes, img_copy = self.q.get()
                
                image = self._draw_boxes(boxes, classes, img_copy.copy())
                #cv2.imwrite("/home/ai4m/develop/baumer/inference/mc25/mc25"+str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".png",image)
                if name == 'redtape' and 2 in classes:
                    self._handle_detection('redtape', boxes, classes, filename, 'tp17', img_copy)
                    print('redtap')

                if name == 'white_sachet':


                    # if 3 in classes:
                    #     self._handle_detection('wrinkle', boxes, classes, filename, 'tp22', img_copy)
                    #     print('wrinkle')

                    # ---- WRINKLE COOLDOWN HANDLING ----
                        if 3 in classes:
                            now = datetime.datetime.now()

                            if self.last_wrinkle_time is None:
                                allow_trigger = True
                            else:
                                diff = (now - self.last_wrinkle_time).total_seconds() / 60.0
                                allow_trigger = diff >= self.cooldown_minutes

                            if allow_trigger:
                                self._handle_detection('wrinkle', boxes, classes, filename, 'tp22', img_copy)
                                self.last_wrinkle_time = now
                                print(f"wrinkle triggered at {now}")
                            else:
                                print(f"wrinkle suppressed — waiting {self.cooldown_minutes} mins cooldown")
                            
                            # Do NOT check eye-mark when wrinkle is present
                            return
                        # ---- END WRINKLE LOGIC ----

                        # If no wrinkle → continue existing logic
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
        event_type = self.event_mapping.get(prefix, prefix)  # fallback to raw prefix if not mapped
        event_data = (now, str(uuid.uuid4()), "Baumer Camera", "MC25", filename, event_type, "Quality")
       # event_data = (now, str(uuid.uuid4()), "Baumer Camera", "MC17", filename, prefix, "Quality")
        tp_data = json.dumps({
            "timestamp": now.isoformat(),
            "uuid": str(uuid.uuid4()),
            "active": 1,
            "filepath": 'http://192.168.0.185:8015/'+prefix+'_mc25_'+filename,
            "color_code": 3
        })

        self.db.execute(
           "INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
           event_data
        )

        if self.table_empty:
           self.db.execute(
               f"INSERT INTO mc25_tp_status({tp_column}) VALUES (%s)",
               (tp_data,)
           )
           self.table_empty = False
        else:
           self.db.execute(
               f"UPDATE mc25_tp_status SET {tp_column} = %s",
               (tp_data,)
           )

        print(f"{prefix.upper()} DETECTED | Saved: {output_path} | DB Updated")

    def check_eyemark_shift(self, boxes, classes, filename, img):
        eyemarks = [box for box, cls in zip(boxes, classes) if cls == 1 and self._in_zone(box)]
        perforations = [box for box, cls in zip(boxes, classes) if cls == 0 and self._in_zone(box)]
        if len(eyemarks) > 0 and len(perforations) > 0 :
            eyemark = eyemarks[0]
            eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2

            nearest_p = min(perforations, key=lambda p: abs(eyemark_centroid_y - (p[1] + p[3])/2))
            lower_bound = nearest_p[1] - self.eyemark_threshold
            upper_bound = nearest_p[3] + self.eyemark_threshold
            print(lower_bound,eyemark_centroid_y,upper_bound)
            if not (lower_bound <= eyemark_centroid_y <= upper_bound):
                self._handle_detection('eyemark_shift', boxes, classes, filename, 'tp20', img)
                return True
            return False
        else:
            return False
    def check_solo_eyemark(self,boxes,classes,filename,img):
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

    def _draw_boxes(self, boxes, classes, img):
        for (x1,y1,x2,y2), cls in zip(boxes, classes):
            name, color = self.models['white_sachet'].classes.get(int(cls), ("unknown", (255,255,255)))
            cv2.rectangle(img, (x1,y1), (x2,y2), color, 2)
            cv2.putText(img, name, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        return img

if __name__ == '__main__':
    config = {
        'input_dir': 'baumer_images_180',
        'output_dir': '/home/ai4m/develop/data/baumer',
        'models': {
            'white_sachet': {'url': '0.0.0.0:8006', 'model_name': 'white_sachet'},
            'redtape': {'url': '0.0.0.0:8006', 'model_name': 'redtap'}
        },
        'db_str': "postgres://postgres:ai4m2024@192.168.1.168:5432/hul?sslmode=disable",
        'zone': (208,68,1048,222), #(1108,602, 1162,657),
        'eyemark_threshold': 10
    }

    processor = DetectionProcessor(**config)
    context = zmq.Context()
    socket_mc25 = context.socket(zmq.SUB)
    socket_mc25.connect("tcp://localhost:5564")
    socket_mc25.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc25, zmq.POLLIN)
    try:
        while True:
            socks = dict(poller.poll())
            if socket_mc25 in socks and socks[socket_mc25] == zmq.POLLIN:
                frame_bytes = socket_mc25.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                frame = cv2.flip(frame, -1)
                processor.process(frame)
                print("receved")
                #cv2.imwrite("/home/ai4m/develop/baumer/data/mc25/mc25"+str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".png",frame)

    except KeyboardInterrupt:
        print("Subscriber stopped by user.")
    finally:
        socket_mc25.close()
        context.term()
    """
    for img_file in [f for f in os.listdir(processor.input_dir) if f.lower().endswith(('.png','.jpg','.jpeg'))]:
        processor.process(os.path.join(processor.input_dir, img_file))
    """




