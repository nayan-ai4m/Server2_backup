import argparse
import numpy as np
import os
import cv2
import zmq
import datetime
import uuid
import psycopg2
import sys
import random
import time
import uuid
import tritonclient.grpc as grpcclient
from tritonclient.utils import InferenceServerException
import json as js


# PostgreSQL configuration
check_query = """SELECT tp17 FROM mc17_tp_status;"""
insert_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
update_query = """
    UPDATE public.event_table
    SET timestamp = %s,
        zone = %s,
        camera_id = %s,
        filename = %s,
        event_type = %s,
        alert_type = %s
    WHERE event_id = %s;
"""
db_connection = """postgres://postgres:ai4m2024@localhost:5432/hul?sslmode=disable"""
connection = psycopg2.connect(db_connection)

def insert_row(data):
    try:
        cur = connection.cursor()
        cur.execute(insert_query, data)
        connection.commit()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"{exc_type} in {fname} at line {exc_tb.tb_lineno}")
        connection.rollback()
        print(e)

# def insert_tp_status(mc, col, json_data):
#     try:
#         if isinstance(json_data, dict):
#             json_str = js.dumps(json_data)
#         else:
#             json_str = json_data  # assume it's already a valid JSON string

#         cur = connection.cursor()
#         query = f'INSERT INTO {mc} ({col}) VALUES (%s);'
#         cur.execute(query, (json_str,))
#         connection.commit()
#         print(f"Inserted into {mc}.{col} successfully.")
#     except Exception as e:
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         print(f"{exc_type} in {fname} at line {exc_tb.tb_lineno}")
#         connection.rollback()
#         print(e) 

def upsert_tp_status(mc, col, json_data, check_query):
    try:
        if isinstance(json_data, dict):
            json_str = js.dumps(json_data)
        else:
            json_str = json_data  # assume it's already a valid JSON string

        cur = connection.cursor()
        cur.execute(check_query)
        result = cur.fetchone()

        if result is None:
            # No existing row — insert
            query = f'INSERT INTO {mc} ({col}) VALUES (%s);'
            cur.execute(query, (json_str,))
            print(f"Inserted into {mc}.{col}")
        else:
            # Existing row — update
            query = f'UPDATE {mc} SET {col} = %s;'
            cur.execute(query, (json_str,))
            print(f"Updated {mc}.{col}")

        connection.commit()

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"{exc_type} in {fname} at line {exc_tb.tb_lineno}")
        connection.rollback()
        print(e)

class TritonDetectionClient:
    def __init__(self, url: str, model_name: str, model_version: str = '1') -> None:
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.model_version = model_version
        self.input_height = 960
        self.input_width = 960
        self.conf_threshold = 0.60
        self.verify_model_metadata()

    def verify_model_metadata(self):
        try:
            metadata = self.client.get_model_metadata(self.model_name)
        except InferenceServerException as e:
            print(f"Failed to get model metadata: {e}")
            sys.exit(1)

    def prepare_input(self, image):
        self.img_height, self.img_width = image.shape[:2]
        img_resized = cv2.resize(image, (self.input_width, self.input_height))
        img_rgb = cv2.cvtColor(img_resized, cv2.COLOR_BGR2RGB)
        img_normalized = img_rgb / 255.0
        input_tensor = img_normalized.transpose(2, 0, 1)[np.newaxis, ...].astype(np.float32)
        return input_tensor

    def infer(self, image):
        input_tensor = self.prepare_input(image)
        inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]  # Use 'images' as input name
        inputs[0].set_data_from_numpy(input_tensor)
        outputs = [
            grpcclient.InferRequestedOutput('num_dets'),
            grpcclient.InferRequestedOutput('det_boxes'),
            grpcclient.InferRequestedOutput('det_scores'),
            grpcclient.InferRequestedOutput('det_classes')
        ]
        results = self.client.infer(
            model_name=self.model_name,
            model_version=self.model_version,
            inputs=inputs,
            outputs=outputs
        )
        num_dets = results.as_numpy('num_dets')
        det_boxes = results.as_numpy('det_boxes')
        det_scores = results.as_numpy('det_scores')
        det_classes = results.as_numpy('det_classes')

        return self.process_output(num_dets, det_boxes, det_scores, det_classes, image)

    def process_output(self, num_dets, det_boxes, det_scores, det_classes, image):
        valid_detections = det_scores > self.conf_threshold
        boxes = det_boxes[valid_detections]
        scores = det_scores[valid_detections]
        class_ids = det_classes[valid_detections]

        if len(boxes) == 0:
            return [], [], [], image
        boxes[:, [0, 2]] *= (self.img_width / self.input_width)
        boxes[:, [1, 3]] *= (self.img_height / self.input_height)
        boxes = boxes.astype(int)
        drawn_image = image.copy()
        for box, score, cls_id in zip(boxes, scores, class_ids):
            x1, y1, x2, y2 = box
            color = [random.randint(0, 255) for _ in range(3)]
            label = f"red_tape {score:.2f}"

            cv2.rectangle(drawn_image, (x1, y1), (x2, y2), color, 2)
            cv2.putText(drawn_image, label, (x1, y1 - 10),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

        return boxes, scores, class_ids, drawn_image
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url',type=str,required=False,default='localhost:8006',help='Inference server URL, default localhost:8001')
    parser.add_argument('--model',type=str,required=False,default='red_tape',help='Inference model name, default red_tape')
    parser.add_argument('--infer-path',type=str,required=False,default='./red_tape_inferred_frames/',help='Path to save inferred images, default ./inferred_frames/')
    args = parser.parse_args()
    os.makedirs(args.infer_path, exist_ok=True)
    client = TritonDetectionClient(args.url, args.model)

    context = zmq.Context()

    socket_mc18 = context.socket(zmq.SUB)
    socket_mc18.connect("tcp://localhost:5555")
    socket_mc18.setsockopt_string(zmq.SUBSCRIBE, "")

    socket_mc17 = context.socket(zmq.SUB)
    socket_mc17.connect("tcp://localhost:5555")
    socket_mc17.setsockopt_string(zmq.SUBSCRIBE, "")

    socket_mc26 = context.socket(zmq.SUB)
    socket_mc26.connect("tcp://localhost:5554")
    socket_mc26.setsockopt_string(zmq.SUBSCRIBE, "")

    poller = zmq.Poller()
    poller.register(socket_mc18, zmq.POLLIN)
    poller.register(socket_mc17, zmq.POLLIN)
    poller.register(socket_mc26, zmq.POLLIN)

    try:
        while True:
            socks = dict(poller.poll())

            # if socket_mc18 in socks and socks[socket_mc18] == zmq.POLLIN:
            #     frame_bytes = socket_mc18.recv()
            #     frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            #     print("New frame received from MC18:", frame.shape)

            #     if frame is not None:
            #         boxes, scores, class_ids, annotated_img = client.infer(frame)
            #         filename = f"mc18_detection_{int(time.time())}.jpeg"
            #         if len(boxes) > 0:
            #             print(f"Detections found in MC18: {len(boxes)}")
            #             cv2.imwrite(os.path.join(args.infer_path, filename), annotated_img)
            #             data = (
            #                 datetime.datetime.now(), str(uuid.uuid4()), "Baumer Camera", "MC18",
            #                 str(filename), "defect", "quality"
            #             )
            #             insert_row(data)
            #             try:
            #                 insert_tp_status("mc18_tp_status", "tp17", {"timestamp": datetime.datetime.now(), "uuid": str(uuid.uuid4()), "active": 1, "filepath": f"http://192.168.0.158:8015/{filename}", "color_code": 3})
            #             except Exception as e:
            #                 print(e)

            if socket_mc17 in socks and socks[socket_mc17] == zmq.POLLIN:
                frame_bytes = socket_mc17.recv()
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                print("New frame received from MC17:", frame.shape)

                if frame is not None:
                    boxes, scores, class_ids, annotated_img = client.infer(frame)
                    filename = f"mc17_detection_{int(time.time())}.png"
                    if len(boxes) > 0:
                        print(f"Detections found in MC17: {len(boxes)}")
                        cv2.imwrite(os.path.join(args.infer_path, filename), annotated_img)
                        data = (
                            datetime.datetime.now(), str(uuid.uuid4()), "Baumer Camera", "MC17",
                            str(filename), "red tape detected", "quality"
                        )
                        insert_row(data)
                        try:
                            # insert_tp_status("mc17_tp_status", "tp17", {"timestamp": str(datetime.datetime.now()), "uuid": str(uuid.uuid4()), "active": 1, "filepath": f"http://192.168.0.158:8015/{filename}", "color_code": 3})
                            upsert_tp_status(mc="mc17_tp_status", col="tp17",
                                             json_data={
                                                 "timestamp": str(datetime.datetime.now()), "uuid": str(uuid.uuid4()),
                                                 "active": 1, "filepath": f"http://192.168.0.158:8015/{filename}",
                                                 "color_code": 3
                                                 }, check_query=check_query
                                            )
                        except Exception as e:
                            print(e)

            # if socket_mc26 in socks and socks[socket_mc26] == zmq.POLLIN:
            #     frame_bytes = socket_mc26.recv()
            #     frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            #     print("New frame received from MC26:", frame.shape)

            #     if frame is not None:
            #         boxes, scores, class_ids, annotated_img = client.infer(frame)
            #         filename = f"mc26_detection_{int(time.time())}.png"
            #         if len(boxes) > 0:
            #             print(f"Detections found in MC26: {len(boxes)}")
            #             cv2.imwrite(os.path.join(args.infer_path, filename), annotated_img)
            #             data = (
            #                 datetime.datetime.now(), str(uuid.uuid4()), "Baumer Camera", "MC26",
            #                 str(filename), "defect", "quality"
            #             )
            #             insert_row(data)
            #             insert_tp_status("mc26_tp_status", "tp17", {"timestamp": datetime.datetime.now(), "uuid": str(uuid.uuid4()), "active": 1, "filepath": f"http://192.168.0.158:8015/{filename}", "color_code": 3})
    except KeyboardInterrupt:
        print("Subscriber stopped by user.")
    finally:
        socket_mc18.close()
        socket_mc17.close()
        socket_mc26.close()
        context.term()
        connection.close()
