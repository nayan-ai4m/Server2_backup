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
            cur.execute("SELECT COUNT(*) FROM mc18_tp_status")
            return cur.fetchone()[0] == 0

class TritonModel:
    def __init__(self, url, model_name, size=1280, threshold=0.25):
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.size = size
        self.thresh = threshold
        self.classes = {
            0: ("redtape", (0,0,255)),
            1: ("eyemark", (0,255,0)),
            2: ("slitting_cut",(255,0,255)),
            3: ("perforation", (0,255,255)),
            4: ("matte-on-belt", (0,255,255)),
            6: ("slitting", (255,0,255)),
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
        return boxes, classes[mask], img_copy,scores[mask]

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
        self.event_mapping = {
            'eyemark_shift': 'Vertical Eyemark Shift Detected',
            'redtape': 'Redtape Detected',
            'perforation_missing': 'Bad Perforation Detected'

        }
        # Add Triton client for perforation classifier
        self.triton_client = grpcclient.InferenceServerClient(url="0.0.0.0:8006")
        
        # Define class names for perforation classifier model
        self.class_names = ['bad', 'good']

    def process(self, img):
        #img = cv2.imread(img_path)
        if img is None: return

        filename = str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".jpeg"
        for name, model in self.models.items():
            threading.Thread(target=self._run_model, args=(name, model, img.copy(), filename)).start()

        while threading.active_count() > 1 or not self.q.empty():
            if not self.q.empty():
                name, boxes, classes, img_copy,conf = self.q.get()
                
                image = self._draw_boxes(boxes, classes, img_copy.copy())
                cv2.imwrite("/home/ai4m/develop/DQ/cameras/baumer/inference/june4/mc18/"+str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".jpeg",image)
                
                hsv_lower = np.array([0, 100, 100])    # adjust based on test
                hsv_upper = np.array([10, 255, 255])   # adjust based on test
                redtape_boxes = self.process_image(img, hsv_lower, hsv_upper)

                if redtape_boxes:
                    redtape_classes = np.array([0] * len(redtape_boxes))  # mock class ID 0 for redtape
                    self._handle_detection('redtape', redtape_boxes, redtape_classes, filename, 'tp17', img.copy())
                    print('Redtape (HSV-based) detected')

                # if name == 'red_tape' and 0 in classes:
                #     self._handle_detection('redtape', boxes, classes, filename, 'tp17', img_copy)
                #     print('redtap')

                elif name == 'sunsilk_black':
                    self.check_eyemark_shift(boxes, classes, filename, img_copy,conf)

                

                
    def _run_model(self, name, model, img, filename):
        try:
            boxes, classes, img_copy,conf = model.infer(img)
            self.q.put((name, boxes, classes, img_copy,conf))
        except Exception as e:
            print(f"{name} failed: {e}")

    def _handle_detection(self, prefix, boxes, classes, filename, tp_column, img):
        output_path = f"{self.output_dir}/{prefix}_mc18_{filename}"
        img_with_boxes = self._draw_boxes(boxes, classes, img.copy())
        cv2.imwrite(output_path, img_with_boxes)

        now = datetime.datetime.now()
        event_type = self.event_mapping.get(prefix, prefix)  # fallback to raw prefix if not mapped
        event_data = (now, str(uuid.uuid4()), "Baumer Camera", "MC18", filename, event_type, "Quality")
       # event_data = (now, str(uuid.uuid4()), "Baumer Camera", "mc18", filename, prefix, "Quality")
        tp_data = json.dumps({
            "timestamp": now.isoformat(),
            "uuid": str(uuid.uuid4()),
            "active": 1,
            "filepath": 'http://192.168.0.158:8015/'+prefix+'_mc18_'+filename,
            "color_code": 3
        })

        self.db.execute(
            "INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            event_data
        )

        if self.table_empty:
            self.db.execute(
                f"INSERT INTO mc18_tp_status({tp_column}) VALUES (%s)",
                (tp_data,)
            )
            self.table_empty = False
        else:
            self.db.execute(
                f"UPDATE mc18_tp_status SET {tp_column} = %s",
                (tp_data,)
            )

        print(f"{prefix.upper()} DETECTED | Saved: {output_path} | DB Updated")

    # def check_eyemark_shift(self, boxes, classes, filename, img):
    #     eyemarks = [box for box, cls in zip(boxes, classes) if cls == 1 and self._in_zone(box)]
    #     perforations = [box for box, cls in zip(boxes, classes) if cls == 3 and self._in_zone(box)]

    #     if len(eyemarks) > 0 and len(perforations) > 0:
    #         eyemark = eyemarks[0]
    #         eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2

    #         nearest_p = min(perforations, key=lambda p: abs(eyemark_centroid_y - (p[1] + p[3]) / 2))
    #         lower_bound = nearest_p[1] - self.eyemark_threshold
    #         upper_bound = nearest_p[3] + self.eyemark_threshold
    #         print(lower_bound, eyemark_centroid_y, upper_bound)
    #         if not (lower_bound <= eyemark_centroid_y <= upper_bound):
    #             self._handle_detection('eyemark_shift', boxes, classes, filename, 'tp20', img)
    #             return True
    #         return False

    #     elif len(eyemarks) > 0 and len(perforations) == 0:
    #         return self.check_bad_perforation(boxes, classes, filename, img)

    #     else:
    #         return False
        
    def check_eyemark_shift(self, boxes, classes, filename, img,conf):
        # Get eyemarks and perforations within the defined zone
        eyemarks = [box for box, cls,cf in zip(boxes, classes,conf) if cls == 1 and self._in_zone(box)]
        perforations = [box for box, cls,cf in zip(boxes, classes,conf) if cls == 3 and self._in_zone(box)]
        
        # Define horizontal zone coordinates
        horizontal_zone = (1160, 524, 1434, 1458)  # (x1, y1, x2, y2)
        
        # Check if there are 3 eyemarks in the horizontal zone
        eyemarks_in_horizontal_zone = [
            box for box in eyemarks
            if self._in_horizontal_zone(box, horizontal_zone)
        ]
        
        if len(eyemarks) > 0:
            # Use the first eyemark (or adapt for multiple if needed)
            eyemark = eyemarks[0]
            # Calculate centroid of eyemark
            eyemark_centroid_x = (eyemark[0] + eyemark[2]) / 2
            eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2
            
            # Count perforations that align with eyemark centroid on x-axis
            perf_on_y = [
                box for box in perforations
                if box[1] <= eyemark_centroid_y <= box[3]
            ]
            perf_on_x = [
                box for box in perforations
                if box[0] <= eyemark_centroid_x <= box[2]
            ]
            
            # Only call check_bad_perforation if no perforation is found AND 
            # there are NOT 3 eyemarks in the horizontal zone



            #if len(perforations) < 2:
            #    if len(eyemarks_in_horizontal_zone) > 2:
            #        print("Skipping check_bad_perforation: 3 eyemarks detected in horizontal zone")
            #        return False
            #    else:
            #        return self.check_bad_perforation(boxes, classes, filename, img)
            

            if len(perforations) != 0:
                # Handle the normal eyemark shift logic here
                eyemark_centroid_y = (eyemark[1] + eyemark[3]) / 2
                nearest_p = min(perforations, key=lambda p: abs(eyemark_centroid_y - (p[1] + p[3]) / 2))
                lower_bound = nearest_p[1] - self.eyemark_threshold
                upper_bound = nearest_p[3] + self.eyemark_threshold
                if not (lower_bound <= eyemark_centroid_y <= upper_bound):
                    self._handle_detection('eyemark_shift', boxes, classes, filename, 'tp20', img)
                    return True
                return False
        else:
            return False


        

    
    def process_image(self, image, hsv_lower, hsv_upper, min_contour_area=1500):
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        mask = cv2.inRange(hsv, hsv_lower, hsv_upper)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        if len(contours) > 5:
            return [] # No boxes, return original image

        boxes = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area >= min_contour_area:
                x, y, w, h = cv2.boundingRect(cnt)
                boxes.append([x, y, x + w, y + h])

        return boxes



    def transform_image(self,image):
                try:
                    # Load image using PIL and convert to RGB

                    # Resize image using OpenCV
                    image = cv2.resize(image, (224, 224))

                    # Convert to float32 and normalize to [0, 1]
                    image = image.astype(np.float32) / 255.0

                    # Define mean and std for normalization (ImageNet values)
                    mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
                    std = np.array([0.229, 0.224, 0.225], dtype=np.float32)

                    # Normalize image: (image - mean) / std
                    image = (image - mean) / std

                    # Transpose to (channels, height, width)
                    image = np.transpose(image, (2, 0, 1))

                    # Add batch dimension
                    input_data = np.expand_dims(image, axis=0)

                    return input_data
                except Exception as e:
                    print(e)

    def check_bad_perforation(self, boxes, classes, filename, img):
        try:
            # Extract and crop zone from image
            x1, y1, x2, y2 = self.zone  # assuming self.zone is defined as (x1, y1, x2, y2)
            cropped_img = img[y1:y2, x1:x2]
            cropped_img = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2RGB)

            # Preprocess
            input_data = self.transform_image(cropped_img)

            # Prepare Triton input
            triton_input = grpcclient.InferInput("input", [1, 3, 224, 224], "FP32")
            triton_input.set_data_from_numpy(input_data)

            # Perform inference
            result = self.triton_client.infer(
                model_name="perforation_classify",
                inputs=[triton_input],
                outputs=[]
            )

            predictions = np.squeeze(result.as_numpy("output"))
            predicted_class = predictions.argmax()
            confidence = predictions[predicted_class]

            # Only proceed if confidence > 0.85
            if confidence < 0.50:
                print(f"Prediction confidence too low: {confidence:.3f}")
                return False

            label = self.class_names[predicted_class]  # ['bad', 'good']
            print(f"Prediction for perforation zone: {label} with confidence {confidence:.3f}")

            # If predicted bad, log it
            if label == "bad":
                print("bad_perforation")
                self._handle_detection('perforation_missing', boxes, classes, filename, 'tp18', img)
                return True

        except Exception as e:
            print(f"Error in check_bad_perforation: {e}")

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
    
    def _in_horizontal_zone(self, box, horizontal_zone):
        """Check if a bounding box centroid is within the specified horizontal zone"""
        x1, y1, x2, y2 = box
        centroid_x = (x1 + x2) / 2
        centroid_y = (y1 + y2) / 2
        
        zone_x1, zone_y1, zone_x2, zone_y2 = horizontal_zone
        return (zone_x1 <= centroid_x <= zone_x2 and
                zone_y1 <= centroid_y <= zone_y2)


    # def _draw_boxes(self, boxes, classes, img):
        
    #     for (x1, y1, x2, y2), cls in zip(boxes, classes):
    #         name, color = self.models['sunsilk_black_testing'].classes.get(int(cls), ("custom", (0, 0, 255)))  # Red box
    #         cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
    #         cv2.putText(img, name, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
    #     return img
    

    def _draw_boxes(self, boxes, classes, img):
    # Fix the validation to handle NumPy arrays properly
        if (len(boxes) == 0 or len(classes) == 0 or len(boxes) != len(classes)):
            return img
        
        for box, cls in zip(boxes, classes):
            # Ensure box has exactly 4 coordinates
            if len(box) != 4:
                continue
                
            x1, y1, x2, y2 = box
            name, color = self.models['sunsilk_black'].classes.get(int(cls), ("custom", (0, 0, 255)))
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            cv2.putText(img, name, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        return img


if __name__ == '__main__':
    config = {
        'input_dir': 'baumer_images_180',
        'output_dir': '/home/ai4m/develop/data/baumer',
        'models': {
            'sunsilk_black': {'url': '0.0.0.0:8006', 'model_name': 'sunsilk_black'}
        },
        'db_str': "postgres://postgres:ai4m2024@localhost:5432/hul?sslmode=disable",
        'zone': (615,276, 984,307), #(732,568,1656,660) ,#(986,620, 1298,664)
        'eyemark_threshold': 5
    }

    processor = DetectionProcessor(**config)
    context = zmq.Context()
    socket_mc18 = context.socket(zmq.SUB)
    socket_mc18.connect("tcp://localhost:5563")
    socket_mc18.setsockopt_string(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_mc18, zmq.POLLIN)
    try:
        while True:
            socks = dict(poller.poll())
            if socket_mc18 in socks and socks[socket_mc18] == zmq.POLLIN:
                frame_bytes = socket_mc18.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                print(frame.shape)
                processor.process(frame)
                #cv2.imwrite("/home/ai4m/develop/backup/develop/DQ/cameras/baumer/data_may27/june4/mc18/mc18"+str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f_%p"))+".png",frame)
                #cv2.imwrite("/home/ai4m/develop/backup/develop/DQ/cameras/baumer/mc18/may25/data/"+str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%p"))+".jpeg",frame)

    except KeyboardInterrupt:
        print("Subscriber stopped by user.")
    finally:
        socket_mc18.close()
        context.term()
    """
    for img_file in [f for f in os.listdir(processor.input_dir) if f.lower().endswith(('.png','.jpg','.jpeg'))]:
        processor.process(os.path.join(processor.input_dir, img_file))
    """


