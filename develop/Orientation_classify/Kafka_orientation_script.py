import os
import re
import json
import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import requests
import cv2
import numpy as np
import psycopg2
from uuid import uuid4
from kafka import KafkaConsumer
import threading
import queue
import time

# Database configuration
DB_CONFIG = {
    'host': '192.168.1.168',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

@dataclass
class ImageData:
    filename: str
    mc_id: str
    class_id: str
    x1: float
    y1: float
    x2: float
    y2: float
    timestamp: datetime
    kafka_data: Optional[Dict] = None  # Store original Kafka message data

@dataclass
class KafkaObjectData:
    object_id: str
    confidence: float
    x1: float
    y1: float
    x2: float
    y2: float
    class_name: str
    filepath: str
    roi_data: str
    additional_data: str

class KafkaImageClassifier:
    def __init__(self, 
                 input_folder: str, 
                 triton_url: str = "http://192.168.1.168:8007",
                 kafka_bootstrap_servers: str = "192.168.1.168:9092",
                 kafka_topics: List[str] = None):
        self.input_folder = input_folder
        self.triton_url = triton_url
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topics = kafka_topics or ["loop3", "loop4"]  # Default to 2 topics
        
        # Processing queues and state
        self.processed_images = set()
        self.results = []
        self.kafka_queue = queue.Queue()
        self.kafka_consumer = None
        self.consumer_thread = None
        self.running = False
        
    def start_kafka_consumer(self):
        """Start Kafka consumer in a separate thread"""
        def consume_messages():
            try:
                self.kafka_consumer = KafkaConsumer(
                    *self.kafka_topics,  # Subscribe to multiple topics
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='image_classifier_group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
                )
                
                print(f"🎧 Kafka consumer started, listening to topics: {self.kafka_topics}")
                
                for message in self.kafka_consumer:
                    if not self.running:
                        break
                        
                    try:
                        kafka_data = message.value
                        if kafka_data:
                          #  print(f"📨 Received Kafka message from topic '{message.topic}': {kafka_data.get('id', 'unknown_id')}")
                            # Add topic info to kafka_data for processing
                            kafka_data['_kafka_topic'] = message.topic
                            self.kafka_queue.put(kafka_data)
                    except Exception as e:
                        print(f"❌ Error processing Kafka message: {e}")
                        
            except Exception as e:
                print(f"❌ Kafka consumer error: {e}")
        
        self.running = True
        self.consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        self.consumer_thread.start()
    
    def stop_kafka_consumer(self):
        """Stop Kafka consumer"""
        self.running = False
        if self.kafka_consumer:
            self.kafka_consumer.close()
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        print("🛑 Kafka consumer stopped")
    
    def parse_kafka_objects(self, kafka_data: Dict) -> List[KafkaObjectData]:
        """Parse objects from Kafka message"""
        objects = []
        
        if 'objects' not in kafka_data:
            return objects
        
        for obj_str in kafka_data['objects']:
            try:
                # Parse object string format:
                # '3600|-0.1|1739.21|872.124|1923.42|1045.57|cld|/home/ai4m/develop/data/cam/mc18_raw_0_1292_474_1547_715_20250925_114404_433.jpeg||#|right_bad_orientation_standing|open_bad|left_bad_orientation_standing'
                parts = obj_str.split('|')
                
                if len(parts) >= 7:
                    obj_data = KafkaObjectData(
                        object_id=parts[0],
                        confidence=float(parts[1]),
                        x1=float(parts[2]),
                        y1=float(parts[3]),
                        x2=float(parts[4]),
                        y2=float(parts[5]),
                        class_name=parts[6],
                        filepath=parts[7] if len(parts) > 7 else "",
                        roi_data=parts[8] if len(parts) > 8 else "",
                        additional_data='|'.join(parts[9:]) if len(parts) > 9 else ""
                    )
                    objects.append(obj_data)
                    
            except (ValueError, IndexError) as e:
                print(f"⚠️ Error parsing object string '{obj_str}': {e}")
                
        return objects
    
    def parse_image_filename(self, filename: str) -> Optional[ImageData]:
        """Parse image filename to extract metadata including coordinates"""
        # Pattern for: mc{prefix}raw_{class_id}_{x}_{y}_{x+w}_{y+h}_{timestamp}.jpeg
        pattern = r'mc(\d+)_raw_(\d+)_(\d+\.?\d*)_(\d+\.?\d*)_(\d+\.?\d*)_(\d+\.?\d*)_(\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2})(?:_(\d+))?\.jpeg'
        match = re.match(pattern, filename)
        
        if not match:
            return None
        
        groups = match.groups()
        mc_id = groups[0]
        class_id = groups[1]
        x1 = float(groups[2])
        y1 = float(groups[3])
        x2 = float(groups[4])
        y2 = float(groups[5])
        timestamp_str = groups[6]
        
        # Parse timestamp
        timestamp = datetime.strptime(timestamp_str, '%Y_%m_%dT%H_%M_%S')
        
        return ImageData(
            filename=filename,
            mc_id=mc_id,
            class_id=class_id,
            x1=x1,
            y1=y1,
            x2=x2,
            y2=y2,
            timestamp=timestamp
        )
    
    def extract_image_data_from_kafka(self, kafka_data: Dict) -> List[ImageData]:
        """Extract ImageData from Kafka message, but use filename coordinates instead of Kafka coordinates"""
        images = []
        objects = self.parse_kafka_objects(kafka_data)
        
        for obj in objects:
            if not obj.filepath or obj.class_name != 'cld':
                continue
                
            # Extract filename from filepath
            filename = os.path.basename(obj.filepath)
            
            # Check if file exists in our input folder
            local_filepath = os.path.join(self.input_folder, filename)
            if not os.path.exists(local_filepath):
                #print(f"⚠️ File not found locally: {filename}")
                continue
            
            # Skip if already processed
            if filename in self.processed_images:
                continue
                
            # Parse filename to get coordinates and metadata - THIS IS THE KEY CHANGE
            img_data = self.parse_image_filename(filename)
            if not img_data:
                print(f"⚠️ Could not parse filename: {filename}")
                continue
            
            # Filter for specific MC IDs: 17-22 and 25-30
            if img_data.mc_id not in ['17', '18', '19', '20', '21', '22', '25', '26', '27', '28', '29', '30']:
                continue
            
            # Add Kafka data to the image data
            img_data.kafka_data = kafka_data
            
            images.append(img_data)
            self.processed_images.add(filename)
            
        return images
        
    def get_db_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except psycopg2.Error as e:
            print(f"Database connection error: {e}")
            return None
    
    def insert_event(self, mc_id: str, event_type_text: str, original_filename: str, saved_image_path: str = ""):
        """Insert event into event_table - use saved_image_path as filename if available"""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return
                
            cursor = conn.cursor()
            timestamp = datetime.now()
            camera_id = f"MC{mc_id}"
            zone = "Smart Camera"
            alert_type = "Productivity"
            event_id = str(uuid4())
            
            # Use saved image path as filename if available, otherwise use original filename
            filename_to_store = saved_image_path if saved_image_path else original_filename
            
            # Insert new record with filename (no filepath column)
            insert_query = """
                INSERT INTO event_table (
                    timestamp,
                    event_id,
                    zone,
                    camera_id,
                    event_type,
                    alert_type,
                    filename
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                timestamp,
                event_id,
                zone,
                camera_id,
                event_type_text,
                alert_type,
                filename_to_store
            ))
            print(f"✅ Inserted event record for {camera_id} with ID {event_id}")
            conn.commit()
            
        except psycopg2.Error as e:
            print(f"❌ Database error for MC{mc_id}: {str(e)}")
            if conn:
                conn.rollback()
            # Minimal fallback insert
            try:
                if conn is None:
                    conn = self.get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    event_id = str(uuid4())
                    filename_to_store = saved_image_path if saved_image_path else original_filename
                    cursor.execute("""
                        INSERT INTO event_table (
                            timestamp,
                            event_id,
                            camera_id,
                            event_type,
                            filename
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        datetime.now(),
                        event_id,
                        f"MC{mc_id}",
                        event_type_text,
                        filename_to_store
                    ))
                    conn.commit()
                    print(f"✅ Inserted minimal event record for MC{mc_id}")
            except Exception as e2:
                print(f"❌ Fallback insert failed for MC{mc_id}: {str(e2)}")
                if conn:
                    conn.rollback()
        finally:
            if conn:
                conn.close()

    def update_tp_table(self, mc_id: str, orientation_status: str, filepath: str = ""):
        """Update MC-specific tp_status table only - no main tp_table operations"""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return
                
            cursor = conn.cursor()
            timestamp = datetime.now()
            
            # Convert local filepath to HTTP URL if filepath is provided
            http_filepath = ""
            if filepath:
                # Extract just the filename from the full path
                filename = os.path.basename(filepath)
                # Create HTTP URL in the same format as tp17
                http_filepath = f"http://192.168.0.185:8015/infeed_orientation/{filename}"
            
            # Create the JSON data structure for the MC table
            tp_data = {
                "uuid": str(uuid4()),
                "active": 1,  # ALWAYS set to 1 for bad orientation detection
                "status": orientation_status,
                "filepath": http_filepath,
                "timestamp": timestamp.isoformat(),
                "color_code": 3,  # Always use 3 for bad_orientation (red alert)
                "orientation": orientation_status,
                "machine_part": "infeed"
            }
            
            # Use the MC-specific table name
            mc_table_name = f"mc{mc_id}_tp_status"
            
            # Check if the MC table exists
            check_table_query = """
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = %s AND table_schema = 'public'
            """
            cursor.execute(check_table_query, (mc_table_name,))
            if not cursor.fetchone():
                print(f"⚠️ Warning: Table {mc_table_name} not found")
                return
            
            # FOR BAD ORIENTATION: Always update/insert regardless of existing tp15 status
            # This ensures we always activate the alert for bad orientation
            
            # Check if MC table has any rows
            check_mc_rows_query = f"SELECT COUNT(*) FROM {mc_table_name}"
            cursor.execute(check_mc_rows_query)
            mc_row_count = cursor.fetchone()[0]
            
            if mc_row_count == 0:
                # MC table is empty, insert new row with tp15
                insert_query = f"""
                    INSERT INTO {mc_table_name} (tp15) 
                    VALUES (%s)
                """
                cursor.execute(insert_query, (json.dumps(tp_data),))
                print(f"insert TP15 : {tp_data}")
                print(f"✅ INSERTED new row in {mc_table_name} with tp15 = {orientation_status}")
                if http_filepath:
                    print(f"  📡 HTTP URL: {http_filepath}")
            else:
                # MC table has rows, update tp15 column (always update for bad orientation)
                update_query = f"""
                    UPDATE {mc_table_name} 
                    SET tp15 = %s
                """
                cursor.execute(update_query, (json.dumps(tp_data),))
                print(f"UPDATE TP15 : {tp_data}")
               
                print(f"✅ UPDATED {mc_table_name}.tp15 with {orientation_status} (ACTIVATED)")
                if http_filepath:
                    print(f"  📡 HTTP URL: {http_filepath}")
                    
            conn.commit()
            
            # Verify the update worked
            verify_query = f"SELECT tp15 FROM {mc_table_name} LIMIT 1"
            cursor.execute(verify_query)
            result = cursor.fetchone()
            if result and result[0]:
                verify_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
                if verify_data.get("active") == 1:
                    print(f"✅ VERIFIED: tp15 is now active in {mc_table_name}")
                else:
                    print(f"❌ VERIFICATION FAILED: tp15 active={verify_data.get('active')} in {mc_table_name}")
            
        except psycopg2.Error as e:
            print(f"❌ TP table update error for MC{mc_id}: {str(e)}")
            if conn:
                conn.rollback()
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error for MC{mc_id}: {str(e)}")
            if conn:
                conn.rollback()
        except Exception as e:
            print(f"❌ Unexpected error for MC{mc_id}: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
    


    # def update_tp_table(self, mc_id: str, orientation_status: str, filepath: str = ""):
    #     """Update MC-specific tp_status table only - no main tp_table operations"""
    #     conn = None
    #     try:
    #         conn = self.get_db_connection()
    #         if not conn:
    #             return
                
    #         cursor = conn.cursor()
    #         timestamp = datetime.now()
            
    #         # Convert local filepath to HTTP URL if filepath is provided
    #         http_filepath = ""
    #         if filepath:
    #             # Extract just the filename from the full path
    #             filename = os.path.basename(filepath)
    #             # Create HTTP URL in the same format as tp17
    #             http_filepath = f"http://192.168.0.185:8015/infeed_orientation/{filename}"
            
    #         # Create the JSON data structure for the MC table
    #         tp_data = {
    #             "uuid": str(uuid4()),
    #             "active": 1,
    #             "status": orientation_status,
    #             "filepath": http_filepath,
    #             "timestamp": timestamp.isoformat(),
    #             "color_code": 3 if orientation_status == "bad_orientation" else 1,
    #             "orientation": orientation_status,
    #             "machine_part": "infeed"
    #         }
            
    #         # Use the MC-specific table name
    #         mc_table_name = f"mc{mc_id}_tp_status"
            
    #         # Check if the MC table exists
    #         check_table_query = """
    #             SELECT table_name FROM information_schema.tables 
    #             WHERE table_name = %s AND table_schema = 'public'
    #         """
    #         cursor.execute(check_table_query, (mc_table_name,))
    #         if not cursor.fetchone():
    #             print(f"⚠️ Warning: Table {mc_table_name} not found")
    #             return
            
    #         # Check if MC table has tp15 with "active": 1
    #         check_mc_tp15_query = f"""
    #             SELECT tp15 FROM {mc_table_name} LIMIT 1
    #         """
    #         cursor.execute(check_mc_tp15_query)
    #         result = cursor.fetchone()
            
    #         tp15_exists_and_active = False
    #         if result and result[0]:
    #             try:
    #                 tp15_data_existing = json.loads(result[0]) if isinstance(result[0], str) else result[0]
    #                 if isinstance(tp15_data_existing, dict) and tp15_data_existing.get("active") == 1:
    #                     tp15_exists_and_active = True
    #                     print(f"✅ tp15 exists and is active in {mc_table_name}")
    #                 else:
    #                     print(f"⚠️ tp15 exists but is not active in {mc_table_name} (active={tp15_data_existing.get('active') if isinstance(tp15_data_existing, dict) else 'N/A'})")
    #             except (json.JSONDecodeError, TypeError) as e:
    #                 print(f"⚠️ Error parsing tp15 data in {mc_table_name}: {e}")
    #         else:
    #             print(f"⚠️ tp15 is empty or null in {mc_table_name}")
            
    #         if tp15_exists_and_active:
    #             # UPDATE: tp15 exists and is active in MC table, so update it
    #             update_query = f"""
    #                 UPDATE {mc_table_name} 
    #                 SET tp15 = %s
    #             """
    #             cursor.execute(update_query, (json.dumps(tp_data),))
                
    #             if cursor.rowcount > 0:
    #                 print(f"✅ UPDATED {mc_table_name}.tp15 with {orientation_status} (tp15 was active)")
    #                 if http_filepath:
    #                     print(f"  📡 HTTP URL: {http_filepath}")
    #             else:
    #                 print(f"⚠️ No rows updated in {mc_table_name}")
                    
    #         else:
    #             # INSERT/UPDATE: tp15 doesn't exist or is not active in MC table
    #             # Check if MC table has any rows
    #             check_mc_rows_query = f"SELECT COUNT(*) FROM {mc_table_name}"
    #             cursor.execute(check_mc_rows_query)
    #             mc_row_count = cursor.fetchone()[0]
                
    #             if mc_row_count == 0:
    #                 # MC table is empty, insert new row with tp15
    #                 insert_query = f"""
    #                     INSERT INTO {mc_table_name} (tp15) 
    #                     VALUES (%s)
    #                 """
    #                 cursor.execute(insert_query, (json.dumps(tp_data),))
    #                 print(f"✅ INSERTED new row in {mc_table_name} with tp15 = {orientation_status}")
    #                 if http_filepath:
    #                     print(f"  📡 HTTP URL: {http_filepath}")
    #             else:
    #                 # MC table has rows, update tp15 column
    #                 update_query = f"""
    #                     UPDATE {mc_table_name} 
    #                     SET tp15 = %s
    #                 """
    #                 cursor.execute(update_query, (json.dumps(tp_data),))
    #                 print(f"✅ UPDATED {mc_table_name}.tp15 with {orientation_status} (activated tp15)")
    #                 if http_filepath:
    #                     print(f"  📡 HTTP URL: {http_filepath}")
                    
    #         conn.commit()
            
    #     except psycopg2.Error as e:
    #         print(f"❌ TP table update error for MC{mc_id}: {str(e)}")
    #         if conn:
    #             conn.rollback()
    #     except json.JSONDecodeError as e:
    #         print(f"❌ JSON parsing error for MC{mc_id}: {str(e)}")
    #         if conn:
    #             conn.rollback()
    #     except Exception as e:
    #         print(f"❌ Unexpected error for MC{mc_id}: {str(e)}")
    #         if conn:
    #             conn.rollback()
    #     finally:
    #         if conn:
    #             conn.close()
        
    def crop_and_preprocess_image(self, image_path: str, image_data: ImageData) -> np.ndarray:
        """Crop the specific region from image and preprocess for Triton inference"""
        # Load the full image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
            
        # Extract crop coordinates from filename parsing (not Kafka data)
        x1, y1, x2, y2 = int(image_data.x1), int(image_data.y1), int(image_data.x2), int(image_data.y2)
        
        # Validate coordinates
        h, w = img.shape[:2]
        x1 = max(0, min(x1, w-1))
        y1 = max(0, min(y1, h-1))
        x2 = max(x1+1, min(x2, w))
        y2 = max(y1+1, min(y2, h))
        
        # Crop the region of interest
        cropped_img = img[y1:y2, x1:x2]
        
        # Check if crop is valid
        if cropped_img.size == 0:
            raise ValueError(f"Invalid crop region: ({x1},{y1}) to ({x2},{y2}) for image shape {img.shape}")
        
        # Convert to RGB
        cropped_img = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2RGB)
        
        # Resize to model input size
        target_size = (224, 224)
        cropped_img = cv2.resize(cropped_img, target_size)
        
        # Normalize to [0, 1]
        cropped_img = cropped_img.astype(np.float32) / 255.0
        
        # Convert to CHW format (Channel, Height, Width)
        cropped_img = np.transpose(cropped_img, (2, 0, 1))
        
        # Return without batch dimension - will be added in infer_with_triton
        return cropped_img
    
    def get_model_name(self, mc_id: str) -> str:
        """Determine which classification model to use based on MC ID"""
        mc_num = int(mc_id)
        
        if mc_num in [17, 19, 21, 26, 28, 30]:
            return "right_ori_classification_model"
        elif mc_num in [18, 20, 22, 25, 27, 29]:
            return "left_ori_classification_model"
        else:   
            print(f"Warning: MC{mc_num} not in expected range, using left_ori_classification_model as default")
            return "left_ori_classification_model"
    
    def save_cropped_image(self, image_path: str, image_data: ImageData, output_dir: str = "/home/ai4m/develop/Orientation_classify/crooping_images/") -> str:
        """Save the cropped region as a separate image for bad orientation"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Load the original image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
        
        # Get crop coordinates from filename parsing (not Kafka data)
        x1, y1, x2, y2 = int(image_data.x1), int(image_data.y1), int(image_data.x2), int(image_data.y2)
        
        # Validate and clip coordinates
        h, w = img.shape[:2]
        x1 = max(0, min(x1, w-1))
        y1 = max(0, min(y1, h-1))
        x2 = max(x1+1, min(x2, w))
        y2 = max(y1+1, min(y2, h))
        
        # Crop the region of interest
        cropped_img = img[y1:y2, x1:x2]
        
        # Check if crop is valid
        if cropped_img.size == 0:
            raise ValueError(f"Invalid crop region: ({x1},{y1}) to ({x2},{y2}) for image shape {img.shape}")
        
        # Create output filename with timestamp
        timestamp_str = image_data.timestamp.strftime('%Y%m%d_%H%M%S')
        output_filename = f"mc{image_data.mc_id}_bad_orientation_cropped_{timestamp_str}_{image_data.class_id}.jpg"
        output_path = os.path.join(output_dir, output_filename)
        
        # Save the cropped image
        success = cv2.imwrite(output_path, cropped_img)
        if not success:
            raise ValueError(f"Failed to save cropped image to: {output_path}")
        
        print(f"  🖼️ Saved cropped image: {output_filename}")
        return output_path
    
    def save_bad_orientation_image(self, image_path: str, image_data: ImageData, output_dir: str = "/home/ai4m/develop/data/baumer/infeed_orientation/") -> str:
        """Save image with bounding box for bad orientation detection"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Load the original image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
        
        # Get bounding box coordinates from filename parsing (not Kafka data)
        x1, y1, x2, y2 = int(image_data.x1), int(image_data.y1), int(image_data.x2), int(image_data.y2)
        
        # Validate and clip coordinates
        h, w = img.shape[:2]
        x1 = max(0, min(x1, w-1))
        y1 = max(0, min(y1, h-1))
        x2 = max(x1+1, min(x2, w))
        y2 = max(y1+1, min(y2, h))
        
        # Draw bounding box (red color for bad orientation)
        color = (0, 0, 255)  # BGR format: Red
        thickness = 3
        cv2.rectangle(img, (x1, y1), (x2, y2), color, thickness)
        
        # Add text label
        label = f"MC{image_data.mc_id}: BAD ORIENTATION"
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.7
        text_thickness = 2
        text_color = (0, 0, 255)  # Red text
        
        # Calculate text size to position it properly
        (text_width, text_height), baseline = cv2.getTextSize(label, font, font_scale, text_thickness)
        
        # Position text above the bounding box
        text_x = x1
        text_y = y1 - 10 if y1 > 30 else y2 + text_height + 10
        
        # Draw text background for better visibility
        cv2.rectangle(img, (text_x - 5, text_y - text_height - 5), 
                     (text_x + text_width + 5, text_y + baseline + 5), 
                     (255, 255, 255), -1)  # White background
        
        # Draw the text
        cv2.putText(img, label, (text_x, text_y), font, font_scale, text_color, text_thickness)
        
        # Create output filename with timestamp
        timestamp_str = image_data.timestamp.strftime('%Y%m%d_%H%M%S')
        output_filename = f"mc{image_data.mc_id}_bad_orientation_{timestamp_str}_{image_data.class_id}.jpg"
        output_path = os.path.join(output_dir, output_filename)
        
        # Save the image with bounding box
        success = cv2.imwrite(output_path, img)
        if not success:
            raise ValueError(f"Failed to save image to: {output_path}")
        
        print(f"  📷 Saved bad orientation image: {output_filename}")
        return output_path
    
    async def check_triton_status(self):
        """Check Triton server status and available models"""
        try:
            # Check server status
            status_url = f"{self.triton_url}/v2/health/ready"
            response = requests.get(status_url, timeout=10)
            print(f"🖥️ Triton server status: {response.status_code}")
            
            # List available models
            models_url = f"{self.triton_url}/v2/models"
            response = requests.get(models_url, timeout=10)
            if response.status_code == 200:
                models = response.json()
                model_names = [m['name'] for m in models]
                print(f"📋 Available models: {model_names}")
                
                # Check if required models are available
                if "right_ori_classification_model" not in model_names:
                    print("⚠️ WARNING: right_ori_classification_model not found - will use left model for all")
                if "left_ori_classification_model" not in model_names:
                    print("❌ ERROR: left_ori_classification_model not found!")
                    
                return models
            else:
                print(f"❌ Could not list models: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error checking Triton status: {e}")
            return None
    
    async def get_model_metadata(self, model_name: str):
        """Get model metadata to understand input/output format"""
        try:
            url = f"{self.triton_url}/v2/models/{model_name}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                metadata = response.json()
                print(f"📊 Model '{model_name}' metadata:")
                print(f"   Inputs: {metadata.get('inputs', [])}")
                print(f"   Outputs: {metadata.get('outputs', [])}")
                return metadata
            else:
                print(f"❌ Could not get metadata for model '{model_name}': {response.status_code}")
                return None
        except Exception as e:
            print(f"❌ Error getting model metadata: {e}")
            return None

    async def infer_with_triton(self, image_data: np.ndarray, model_name: str) -> tuple[str, float]:
        """Send image to Triton server for classification"""
        url = f"{self.triton_url}/v2/models/{model_name}/infer"
        
        # Define input/output names based on model config
        if model_name == "left_ori_classification_model":
            input_name = "input"
            output_name = "output"
        elif model_name == "right_ori_classification_model":
            input_name = "input" 
            output_name = "output"
        else:
            return "unknown_model", 0.0
        
        # Ensure proper shape - the model expects batch dimension = 1
        if len(image_data.shape) == 3:
            # Add batch dimension: [3, 224, 224] -> [1, 3, 224, 224]
            image_data = np.expand_dims(image_data, axis=0)
        elif len(image_data.shape) == 4 and image_data.shape[0] != 1:
            # Ensure batch size is 1
            image_data = image_data[:1]  # Take only first batch item
        
        # Expected shape: [1, 3, 224, 224]
        image_shape = list(image_data.shape)
        image_data_flat = image_data.flatten().tolist()
        
        payload = {
            "inputs": [
                {
                    "name": input_name,
                    "shape": image_shape,  # [3, 224, 224]
                    "datatype": "FP32",
                    "data": image_data_flat
                }
            ],
            "outputs": [
                {
                    "name": output_name
                }
            ]
        }
        
        try:
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            # Add timeout and retry logic
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    response = requests.post(url, json=payload, headers=headers, timeout=30)
                    
                    if response.status_code == 400:
                        # Print response for debugging
                        print(f"  🐛 Debug - Model: {model_name}")
                        print(f"  🐛 Debug - Input shape: {image_shape}")
                        print(f"  🐛 Debug - Input name: {input_name}, Output name: {output_name}")
                        print(f"  🐛 Debug - Response: {response.text[:300]}...")
                        return "bad_request", 0.0
                    elif response.status_code == 404:
                        print(f"  🐛 Model '{model_name}' not found on server")
                        return "model_not_found", 0.0
                    
                    response.raise_for_status()
                    result = response.json()
                    break
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    if attempt < max_retries - 1:
                        print(f"  ⚠️ Connection attempt {attempt + 1} failed, retrying...")
                        time.sleep(1)
                        continue
                    else:
                        raise e
            
            # Parse classification result - expecting 4-class output
            output_data = result["outputs"][0]["data"]
            
            if len(output_data) == 4:
                # 4-class classification: bad_sleeping, bad_standing, good_sleeping, good_standing
                predictions = np.array(output_data)
                predicted_class = np.argmax(predictions)
                confidence = float(predictions[predicted_class])
                
                # Define class mappings based on your model training order
                # You may need to adjust this order based on how your model was trained
                class_labels = ["bad_sleeping", "bad_standing", "good_sleeping", "good_standing"]
                # Get the specific classification
                classification = class_labels[predicted_class]
                
                return classification, confidence
            elif len(output_data) == 1:
                # Binary classification (single probability)
                confidence = float(output_data[0])
                classification = "good" if confidence > 0.2 else "bad"
                return classification, confidence
            else:
                # Multi-class classification (get argmax)
                predictions = np.array(output_data)
                predicted_class = np.argmax(predictions)
                confidence = float(predictions[predicted_class])
                
                # Fallback - assume first half are "good" classes
                classification = "good" if predicted_class < len(predictions)//2 else "bad"
                return classification, confidence
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Triton server error: {e}")
            return "connection_error", 0.0
        except KeyError as e:
            print(f"  ❌ Response format error: {e}")
            print(f"  🐛 Debug - Full response: {result}")
            return "format_error", 0.0
        except Exception as e:
            print(f"  ❌ Inference error: {e}")
            return "inference_error", 0.0

    async def process_image(self, image_data: ImageData) -> dict:
        """Process a single image through the classification pipeline"""
        print(f"🔍 Processing {image_data.filename} (from Kafka)")
        
        # Determine which model to use
        model_name = self.get_model_name(image_data.mc_id)
        side = "right" if int(image_data.mc_id) in [17, 19, 21, 26, 28, 30] else "left"
        
        # Full path to image
        image_path = os.path.join(self.input_folder, image_data.filename)
        
        try:
            # Crop and preprocess the specific region using filename coordinates
            processed_img = self.crop_and_preprocess_image(image_path, image_data)
            
            print(f"  📐 Cropped (filename coords): ({image_data.x1:.1f},{image_data.y1:.1f}) → ({image_data.x2:.1f},{image_data.y2:.1f})")
            
            # Classify with Triton model
            classification, confidence = await self.infer_with_triton(processed_img, model_name)
            
            # Initialize variables
            saved_image_path = ""
            saved_cropped_path = ""
            
            # Map classifications to orientations as requested
            if classification in ["bad_sleeping", "bad_standing"]:
                orientation_status = "bad_orientation"
                result_emoji = "❌"
                result_color = "BAD"
                
                # *** SAVE BOTH TYPES OF IMAGES FOR BAD ORIENTATION ***
                try:
                    # Save full image with bounding box
                    saved_image_path = self.save_bad_orientation_image(image_path, image_data)
                    
                    # Save cropped region 
                    saved_cropped_path = self.save_cropped_image(image_path, image_data)
                    
                except Exception as e:
                    print(f"  ⚠️ Failed to save images: {e}")
                    saved_image_path = ""
                    saved_cropped_path = ""
                
                # *** DB OPERATIONS ONLY FOR BAD ORIENTATION ***
                # Insert event for bad orientation with saved image path as filename
                event_type_text = f"Bad CLD orientation detected in {image_data.mc_id} infeed"
                self.insert_event(image_data.mc_id, event_type_text, image_path, saved_image_path)
                
                # Update TP table with bad orientation status and filepath
                self.update_tp_table(image_data.mc_id, orientation_status, saved_image_path)
                
            elif classification in ["good_sleeping", "good_standing"]:
                orientation_status = "good_orientation"
                result_emoji = "✅"
                result_color = "GOOD"
                # NO DB operations for good orientation, NO image saving
                
            else:
                orientation_status = "unknown_orientation"
                result_emoji = "⚠️"
                result_color = "UNKNOWN"
                # NO DB operations for unknown orientation, NO image saving
                
            print(f"  {result_emoji} Result: {classification.upper()} -> {orientation_status} (confidence: {confidence:.3f}) - MC{image_data.mc_id} ({side} side)")
            
            result = {
                'filename': image_data.filename,
                'mc_id': image_data.mc_id,
                'side': side,
                'class_id': image_data.class_id,
                'timestamp': image_data.timestamp.isoformat(),
                'model_used': model_name,
                'classification': classification,
                'orientation_status': orientation_status,  # Added mapped orientation
                'confidence': confidence,
                'orientation': 'sleeping' if 'sleeping' in classification else 'standing',
                'quality': 'good' if 'good' in classification else 'bad',
                'crop_region': {
                    'x1': image_data.x1,
                    'y1': image_data.y1, 
                    'x2': image_data.x2,
                    'y2': image_data.y2,
                    'width': image_data.x2 - image_data.x1,
                    'height': image_data.y2 - image_data.y1
                },
                'saved_image_path': saved_image_path,  # Path to saved image with bounding box
                'saved_cropped_path': saved_cropped_path,  # Path to saved cropped image
                'kafka_data': image_data.kafka_data,  # Store original Kafka message
                'processed_at': datetime.now().isoformat()
            }
            
            return result
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return {
                'filename': image_data.filename,
                'mc_id': image_data.mc_id,
                'side': side,
                'error': str(e),
                'classification': 'error',
                'orientation_status': 'error',
                'confidence': 0.0,
                'saved_image_path': '',
                'saved_cropped_path': '',
                'kafka_data': image_data.kafka_data,
                'processed_at': datetime.now().isoformat()
            }
    async def continuous_kafka_processing(self):
        """Continuously process Kafka messages and classify images"""
        print("🚀 Starting Kafka-based image processing...")
        
        # First check Triton server status and available models
        print("🔍 Checking Triton server...")
        await self.check_triton_status()
        
        # Get model metadata for available models
        await self.get_model_metadata("left_ori_classification_model")
        
        # Start Kafka consumer
        self.start_kafka_consumer()
        
        print("👀 Waiting for Kafka messages...")
        
        consecutive_empty_checks = 0
        
        try:
            while True:
                try:
                    # Check for new Kafka messages
                    kafka_messages = []
                    
                    # Drain the queue of Kafka messages
                    while not self.kafka_queue.empty():
                        try:
                            kafka_data = self.kafka_queue.get_nowait()
                            kafka_messages.append(kafka_data)
                        except queue.Empty:
                            break
                    
                    if kafka_messages:
                        consecutive_empty_checks = 0
                        
                        # Process each Kafka message
                        for kafka_data in kafka_messages:
                            # Extract image data from Kafka message
                            images = self.extract_image_data_from_kafka(kafka_data)
                            
                            if images:
                                # Show found images count
                                print(f"\n📸 Found {len(images)} new image(s)!")
                                
                                # Process each image
                                for image_data in images:
                                    result = await self.process_image(image_data)
                                    self.results.append(result)
                                    
                                    # Save results after each image (like the file-based script)
                                    self.save_results()
                                    print(f"✅ Total processed: {len(self.results)} images")
                                    print("👀 Waiting for next image...")
                                    
                                    # Small delay between images
                                    await asyncio.sleep(0.1)
                            # Remove the "no valid images" message to match your format
                        
                    else:
                        consecutive_empty_checks += 1
                        # Show periodic "waiting" message
                        if consecutive_empty_checks % 30 == 0:  # Every 60 seconds (30 * 2s)
                            print(f"⏳ Still waiting for Kafka messages... (checked {consecutive_empty_checks} times)")
                    
                    # Adaptive sleep - longer when no activity
                    if consecutive_empty_checks > 20:
                        await asyncio.sleep(3)  # Slower polling when inactive
                    else:
                        await asyncio.sleep(2)  # Normal polling
                    
                except KeyboardInterrupt:
                    print("\n🛑 Stopping Kafka processing...")
                    break
                except Exception as e:
                    print(f"❌ Error in Kafka processing: {e}")
                    await asyncio.sleep(5)
                    
        finally:
            # Stop Kafka consumer
            self.stop_kafka_consumer()
            
            # Final save and summary
            self.save_results()
            self.print_summary()
            print("🏁 Kafka processing stopped.")

    def save_results(self, output_file: str = None):
        """Save processing results to JSON file"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"classification_results_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"📄 Results saved to {output_file}")

    # async def continuous_kafka_processing(self):
    #     """Continuously process Kafka messages and classify images"""
    #     print("🚀 Starting Kafka-based image processing...")
        
    #     # First check Triton server status and available models
    #     print("🔍 Checking Triton server...")
    #     await self.check_triton_status()
        
    #     # Get model metadata for available models
    #     await self.get_model_metadata("left_ori_classification_model")
        
    #     # Start Kafka consumer
    #     self.start_kafka_consumer()
        
    #     print("👀 Waiting for Kafka messages...")
        
    #     consecutive_empty_checks = 0
        
    #     try:
    #         while True:
    #             try:
    #                 # Check for new Kafka messages
    #                 kafka_messages = []
                    
    #                 # Drain the queue of Kafka messages
    #                 while not self.kafka_queue.empty():
    #                     try:
    #                         kafka_data = self.kafka_queue.get_nowait()
    #                         kafka_messages.append(kafka_data)
    #                     except queue.Empty:
    #                         break
                    
    #                 if kafka_messages:
    #                     consecutive_empty_checks = 0
    #                     print(f"\n📨 Processing {len(kafka_messages)} Kafka message(s)!")
                        
    #                     # Process each Kafka message
    #                     for kafka_data in kafka_messages:
    #                         # Extract image data from Kafka message
    #                         images = self.extract_image_data_from_kafka(kafka_data)
                            
    #                         if images:
    #                             print(f"  🎯 Found {len(images)} image(s) to process from Kafka message {kafka_data.get('id', 'unknown')}")
                                
    #                             # Process each image
    #                             for image_data in images:
    #                                 result = await self.process_image(image_data)
    #                                 self.results.append(result)
                                    
    #                                 # Small delay between images
    #                                 await asyncio.sleep(0.1)
    #                         else:
    #                             print("  ")
    #                             #print(f"  ⚠️ No valid images found in Kafka message {kafka_data.get('id', 'unknown')}")
                        
    #                     # Save results after processing batch
    #                     # self.save_results()
    #                     print(f"✅ Total processed: {len(self.results)} images")
    #                     print("👀 Waiting for next Kafka message...")
    #                 else:
    #                     consecutive_empty_checks += 1
    #                     # Show periodic "waiting" message
    #                     if consecutive_empty_checks % 30 == 0:  # Every 60 seconds (30 * 2s)
    #                         print(f"⏳ Still waiting for Kafka messages... (checked {consecutive_empty_checks} times)")
                    
    #                 # Adaptive sleep - longer when no activity
    #                 if consecutive_empty_checks > 20:
    #                     await asyncio.sleep(3)  # Slower polling when inactive
    #                 else:
    #                     await asyncio.sleep(2)  # Normal polling
                    
    #             except KeyboardInterrupt:
    #                 print("\n🛑 Stopping Kafka processing...")
    #                 break
    #             except Exception as e:
    #                 print(f"❌ Error in Kafka processing: {e}")
    #                 await asyncio.sleep(5)
                    
    #     finally:
    #         # Stop Kafka consumer
    #         self.stop_kafka_consumer()
            
    #         # Final save and summary
    #         # self.save_results()
    #         self.print_summary()
    #         print("🏁 Kafka processing stopped.")
    
    def print_summary(self):
        """Print processing summary"""
        if not self.results:
            print("No results to summarize")
            return
            
        total = len(self.results)
        good_orientation_count = sum(1 for r in self.results if r.get('orientation_status') == 'good_orientation')
        bad_orientation_count = sum(1 for r in self.results if r.get('orientation_status') == 'bad_orientation')
        error_count = sum(1 for r in self.results if r.get('orientation_status') not in ['good_orientation', 'bad_orientation'])
        
        # Count by side
        left_count = sum(1 for r in self.results if r.get('side') == 'left')
        right_count = sum(1 for r in self.results if r.get('side') == 'right')
        
        # Count by MC ID
        mc_counts = {}
        for result in self.results:
            mc_id = result.get('mc_id', 'unknown')
            mc_counts[mc_id] = mc_counts.get(mc_id, 0) + 1
        
        print(f"\n=== Kafka Processing Summary ===")
        print(f"Total images processed: {total}")
        print(f"\n📊 By Orientation Status:")
        print(f"  ✅ Good Orientation: {good_orientation_count} ({good_orientation_count/total*100:.1f}%)")
        print(f"  ❌ Bad Orientation: {bad_orientation_count} ({bad_orientation_count/total*100:.1f}%)")
        print(f"  ⚠️ Errors: {error_count}")
        
        print(f"\n📍 By Side:")
        print(f"  Left side: {left_count}")
        print(f"  Right side: {right_count}")
        print(f"\n🏭 By MC ID:")
        for mc_id in sorted(mc_counts.keys()):
            side = "right" if int(mc_id) in [17, 19, 21, 26, 28, 30] else "left"
            print(f"  MC{mc_id} ({side}): {mc_counts[mc_id]}")
        
        # Show recent results
        if self.results:
            recent = self.results[-1]
            print(f"\n🕒 Most recent: {recent['filename']} -> {recent.get('orientation_status', 'unknown')} (confidence: {recent.get('confidence', 0):.3f})")

# Usage example
async def process_with_kafka():
    """Process images triggered by Kafka messages"""
    classifier = KafkaImageClassifier(
        input_folder="/home/ai4m/develop/data/cam",
        triton_url="http://192.168.1.168:8007",
        kafka_bootstrap_servers="192.168.1.168:9092",  # Update with your Kafka server
        kafka_topics=["loop3", "loop4"]  # Update with your 2 topic names
    )
    
    await classifier.continuous_kafka_processing()

if __name__ == "__main__":
    print("🔥 Kafka-Integrated Image Classifier - Starting...")
    print("🔍 Monitoring Kafka messages for image processing")
    print("🖥️ Triton Server: http://192.168.1.168:8007")
    print("📁 Image Folder: /home/ai4m/develop/data/cam")
    print("🎧 Kafka: 192.168.1.168:9092")  # Update with your Kafka configuration
    print("📡 Topics: loop3, loop4")  # Update with your actual topic names
    print("📐 Using coordinates from filename parsing (not Kafka)")
    print("🖼️ Saving cropped images to: /home/ai4m/develop/Orientation_classify/crooping_images/")
    print("📷 Saving full images with bbox to: /home/ai4m/develop/data/baumer/infeed_orientation/")
    print("ℹ️ Press Ctrl+C to stop")
    print("=" * 50)
    
    # Run Kafka-based processing
    asyncio.run(process_with_kafka())

