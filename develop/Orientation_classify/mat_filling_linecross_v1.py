import os
import re
import json
import asyncio
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass
import cv2
import numpy as np
import psycopg2
from uuid import uuid4
import tritonclient.grpc as grpcclient
from tritonclient.utils import InferenceServerException

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
    loop_id: str  # pre_taping_l3 or pre_taping_l4
    class_id: str
    x1: float
    y1: float
    x2: float
    y2: float
    timestamp: datetime

class MatClassifier:
    def __init__(self, input_folder: str, triton_url: str = "localhost:8006"):
        self.input_folder = input_folder
        self.triton_url = triton_url
        self.processed_images = set()
        self.results = []

        # Initialize Triton gRPC client
        try:
            self.triton_client = grpcclient.InferenceServerClient(url=triton_url, verbose=False)
            print(f"✅ Connected to Triton gRPC server at {triton_url}")
        except Exception as e:
            print(f"❌ Failed to connect to Triton server: {e}")
            self.triton_client = None

        # Create cropping images output directory for mat
        self.cropping_output_dir = "/home/ai4m/develop/Orientation_classify/croping_mat"
        os.makedirs(self.cropping_output_dir, exist_ok=True)
        print(f"📁 Cropped mat images will be saved to: {self.cropping_output_dir}")
        
    def get_db_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except psycopg2.Error as e:
            print(f"Database connection error: {e}")
            return None
    
    def insert_event(self, loop_id: str, event_type_text: str, original_filename: str, saved_image_path: str = ""):
        """Insert event into event_table"""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return
                
            cursor = conn.cursor()
            timestamp = datetime.now()
            camera_id = f"L{loop_id}_mat"  # e.g., "pre_taping_l3" or "pre_taping_l4"
            zone = "Smart Camera"
            alert_type = "Productivity"
            event_id = str(uuid4())
            
            # Use saved image path as filename if available, otherwise use original filename
            filename_to_store = saved_image_path if saved_image_path else original_filename
            
            # Insert new record with filename
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
            print(f"❌ Database error for Loop{loop_id}: {str(e)}")
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
                        f"L{loop_id}_mat",
                        event_type_text,
                        filename_to_store
                    ))
                    conn.commit()
                    print(f"✅ Inserted minimal event record for Loop{loop_id}")
            except Exception as e2:
                print(f"❌ Fallback insert failed for Loop{loop_id}: {str(e2)}")
                if conn:
                    conn.rollback()
        finally:
            if conn:
                conn.close()
    

    def update_tp_table(self, loop_id: str, mat_status: str, filepath: str = ""):
        """Update press_tp_status table and tp31 column"""
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
                # Create HTTP URL for mat images
                http_filepath = f"http://192.168.0.185:8015/mat/{filename}"
            
            # Determine which table and machine_part to use based on loop_id
            if loop_id == "pre_taping_l3":
                table_name = "tpmc_tp_status"
                machine_part = "loop3_mat"
            elif loop_id == "pre_taping_l4":
                table_name = "tpmc_tp_status_loop4"
                machine_part = "loop4_mat"
            else:
                print(f"⚠️ Unknown loop_id: {loop_id}")
                return
            
            # tp31 column is the same for both tables
            tp_column = "tp31"
            
            # Create the JSON data structure
            tp_data = {
                "uuid": str(uuid4()),
                "active": 1,
                "status": mat_status,
                "filepath": http_filepath,
                "timestamp": timestamp.isoformat(),
                "color_code": 3 if mat_status == "bad_mat" else 1,
                "mat": mat_status,
                "machine_part": machine_part
            }
            
            # Check if the table exists
            check_table_query = """
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = %s AND table_schema = 'public'
            """
            cursor.execute(check_table_query, (table_name,))
            if not cursor.fetchone():
                print(f"⚠️ Warning: Table {table_name} not found")
                return
            
            # Check if tp31 exists with "active": 1
            check_tp31_query = f"""
                SELECT {tp_column} FROM {table_name} LIMIT 1
            """
            cursor.execute(check_tp31_query)
            result = cursor.fetchone()
            
            tp31_exists_and_active = False
            if result and result[0]:
                try:
                    tp31_data_existing = json.loads(result[0]) if isinstance(result[0], str) else result[0]
                    if isinstance(tp31_data_existing, dict) and tp31_data_existing.get("active") == 1:
                        tp31_exists_and_active = True
                        print(f"✅ {tp_column} exists and is active in {table_name}")
                    else:
                        print(f"⚠️ {tp_column} exists but is not active in {table_name} (active={tp31_data_existing.get('active') if isinstance(tp31_data_existing, dict) else 'N/A'})")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"⚠️ Error parsing {tp_column} data in {table_name}: {e}")
            else:
                print(f"⚠️ {tp_column} is empty or null in {table_name}")
            
            if tp31_exists_and_active:
                # UPDATE: tp31 exists and is active
                update_query = f"""
                    UPDATE {table_name} 
                    SET {tp_column} = %s
                """
                cursor.execute(update_query, (json.dumps(tp_data),))
                
                if cursor.rowcount > 0:
                    print(f"✅ UPDATED {table_name}.{tp_column} with {mat_status} ({tp_column} was active)")
                    if http_filepath:
                        print(f"  📡 HTTP URL: {http_filepath}")
                else:
                    print(f"⚠️ No rows updated in {table_name}")
                    
            else:
                # INSERT/UPDATE: tp31 doesn't exist or is not active
                # Check if table has any rows
                check_rows_query = f"SELECT COUNT(*) FROM {table_name}"
                cursor.execute(check_rows_query)
                row_count = cursor.fetchone()[0]
                
                if row_count == 0:
                    # Table is empty, insert new row with tp31
                    insert_query = f"""
                        INSERT INTO {table_name} ({tp_column}) 
                        VALUES (%s)
                    """
                    cursor.execute(insert_query, (json.dumps(tp_data),))
                    print(f"✅ INSERTED new row in {table_name} with {tp_column} = {mat_status}")
                    if http_filepath:
                        print(f"  📡 HTTP URL: {http_filepath}")
                else:
                    # Table has rows, update tp31 column
                    update_query = f"""
                        UPDATE {table_name} 
                        SET {tp_column} = %s
                    """
                    cursor.execute(update_query, (json.dumps(tp_data),))
                    print(f"✅ UPDATED {table_name}.{tp_column} with {mat_status} (activated {tp_column})")
                    if http_filepath:
                        print(f"  📡 HTTP URL: {http_filepath}")
                    
            conn.commit()
            
        except psycopg2.Error as e:
            print(f"❌ TP table update error for {loop_id}: {str(e)}")
            if conn:
                conn.rollback()
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error for {loop_id}: {str(e)}")
            if conn:
                conn.rollback()
        except Exception as e:
            print(f"❌ Unexpected error for {loop_id}: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def parse_image_filename(self, filename: str) -> Optional[ImageData]:
        """Parse image filename to extract metadata
        Expected format: pre_taping_l3_raw_{class_id}_{x}_{y}_{x+w}_{y+h}_{timestamp}.png
                     or: pre_taping_l4_raw_{class_id}_{x}_{y}_{x+w}_{y+h}_{timestamp}.png
        """
        # Pattern for: l{3|4}_mat_raw_{class_id}_{x}_{y}_{x+w}_{y+h}_{timestamp}.png
        pattern = r'(pre_taping_l[34])_raw_(\d+)_(\d+\.?\d*)_(\d+\.?\d*)_(\d+\.?\d*)_(\d+\.?\d*)_(\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2})(?:_(\d+))?\.png'
        match = re.match(pattern, filename)
        
        if not match:
            return None
        
        groups = match.groups()
        loop_id = groups[0]  # "pre_taping_l3" or "pre_taping_l4"
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
            loop_id=loop_id,
            class_id=class_id,
            x1=x1,
            y1=y1,
            x2=x2,
            y2=y2,
            timestamp=timestamp
        )
    
    def scan_for_images(self) -> List[ImageData]:
        """Scan input folder for new mat images"""
        images = []
        
        if not os.path.exists(self.input_folder):
            print(f"Input folder {self.input_folder} does not exist")
            return images
            
        for filename in os.listdir(self.input_folder):
            if filename.endswith('.png') and filename not in self.processed_images:
                img_data = self.parse_image_filename(filename)
                # Keep only pre_taping_l3 and pre_taping_l4
                if img_data and img_data.loop_id in ['pre_taping_l3', 'pre_taping_l4']:
                    images.append(img_data)
                    self.processed_images.add(filename)
        
        # Sort by timestamp
        images.sort(key=lambda x: x.timestamp)
        return images
    
    
    
    def save_cropped_image_for_bad_mat(self, image_path: str, image_data: ImageData) -> str:
        """Save the cropped image for bad mat detection with expanded coordinates"""
        try:
            # Load the full image
            img = cv2.imread(image_path)
            if img is None:
                raise ValueError(f"Could not load image: {image_path}")
                
            # Extract crop coordinates and expand by 4 pixels on each side
            x1 = int(image_data.x1) - 40
            y1 = int(image_data.y1) - 10
            x2 = int(image_data.x2) + 4
            y2 = int(image_data.y2) + 10
            
            # Validate coordinates
            h, w = img.shape[:2]
            x1 = max(0, min(x1, w-1))
            y1 = max(0, min(y1, h-1))
            x2 = max(x1+1, min(x2, w))
            y2 = max(y1+1, min(y2, h))
            
            print(f"  📏 Expanded crop: ({x1},{y1}) → ({x2},{y2}) [+4px on each side]")
            
            # Crop the region of interest
            cropped_img = img[y1:y2, x1:x2]
            
            # Check if crop is valid
            if cropped_img.size == 0:
                raise ValueError(f"Invalid crop region: ({x1},{y1}) to ({x2},{y2}) for image shape {img.shape}")
            
            # Convert to RGB then back to BGR for consistency
            cropped_img_rgb = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2RGB)
            
            # Resize to model input size (224x224)
            target_size = (224, 224)
            cropped_img_resized = cv2.resize(cropped_img_rgb, target_size)
            
            # Convert back to BGR for OpenCV saving
            cropped_img_bgr = cv2.cvtColor(cropped_img_resized, cv2.COLOR_RGB2BGR)
            
            # Create filename for cropped image
            timestamp_str = image_data.timestamp.strftime('%Y%m%d_%H%M%S')
            cropped_filename = f"{image_data.loop_id}_bad_cropped_{timestamp_str}_{image_data.class_id}.png"
            cropped_path = os.path.join(self.cropping_output_dir, cropped_filename)
            
            # Save the cropped image
            success = cv2.imwrite(cropped_path, cropped_img_bgr)
            if not success:
                print(f"  ⚠️ Failed to save cropped image to: {cropped_path}")
                return ""
            
            print(f"  💾 Saved bad mat cropped image: {cropped_filename}")
            return cropped_path
            
        except Exception as e:
            print(f"  ⚠️ Error saving cropped image for bad mat: {e}")
            return ""
    
    def crop_and_preprocess_image(self, image_path: str, image_data: ImageData) -> np.ndarray:
        """Crop the specific region from image and preprocess for Triton inference with expanded coordinates"""
        # Load the full image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
            
        # Extract crop coordinates and expand by 4 pixels on each side
        x1 = int(image_data.x1) - 40
        y1 = int(image_data.y1) - 10
        x2 = int(image_data.x2) + 4
        y2 = int(image_data.y2) + 10
        
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

    def save_bad_mat_image(self, image_path: str, image_data: ImageData, output_dir: str = "/home/ai4m/develop/data/baumer/mat/") -> str:
        """Save image with bounding box for bad mat detection with expanded coordinates and coordinate labels"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Load the original image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
        
        # Get bounding box coordinates with 4 pixel expansion
        x1 = int(image_data.x1) - 40
        y1 = int(image_data.y1) - 10
        x2 = int(image_data.x2) + 4
        y2 = int(image_data.y2) + 10        
        # Validate and clip coordinates
        h, w = img.shape[:2]
        x1 = max(0, min(x1, w-1))
        y1 = max(0, min(y1, h-1))
        x2 = max(x1+1, min(x2, w))
        y2 = max(y1+1, min(y2, h))
        
        # Draw bounding box (red color for bad mat)
        color = (0, 0, 255)  # BGR format: Red
        thickness = 3
        cv2.rectangle(img, (x1, y1), (x2, y2), color, thickness)
        
        # Add main label
        label = f"{image_data.loop_id.upper()}: BAD MAT"
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
        
        # Draw the main label text
        cv2.putText(img, label, (text_x, text_y), font, font_scale, text_color, text_thickness)
        
        # Add coordinate labels on the bounding box
        coord_font_scale = 0.5
        coord_thickness = 1
        coord_color = (255, 255, 255)  # White text
        coord_bg_color = (0, 0, 255)  # Red background
        
        # Top-left corner coordinates - positioned on the left side
        tl_label = f"{x1},{y1}"
        (tl_w, tl_h), _ = cv2.getTextSize(tl_label, font, coord_font_scale, coord_thickness)
        # Position on the left side of the box, vertically centered
        tl_x = x1 - tl_w - 15
        tl_y = y1 + (y2 - y1) // 2 + tl_h // 2
        # Ensure it stays within image bounds
        if tl_x < 5:
            tl_x = 5
        cv2.rectangle(img, (tl_x - 3, tl_y - tl_h - 40), (tl_x + tl_w + 3, tl_y + 10), coord_bg_color, -1)
        cv2.putText(img, tl_label, (tl_x, tl_y), font, coord_font_scale, coord_color, coord_thickness)
        
        # Bottom-right corner coordinates - just pixel values
        br_label = f"{x2},{y2}"
        (br_w, br_h), _ = cv2.getTextSize(br_label, font, coord_font_scale, coord_thickness)
        cv2.rectangle(img, (x2 - br_w - 6, y2), (x2, y2 + br_h + 18), coord_bg_color, -1)
        cv2.putText(img, br_label, (x2 - br_w - 3, y2 + br_h + 4), font, coord_font_scale, coord_color, coord_thickness)
        
        # Add size information (width x height) at the bottom of the box
        box_width = x2 - x1
        box_height = y2 - y1
        size_label = f"{box_width}x{box_height}px"
        (size_w, size_h), _ = cv2.getTextSize(size_label, font, coord_font_scale, coord_thickness)
        size_x = x1 + (box_width - size_w) // 2  # Center the text
        size_y = y2 + size_h + 15
        cv2.rectangle(img, (size_x - 3, size_y - size_h - 40), (size_x + size_w + 3, size_y + 24), coord_bg_color, -1)
        cv2.putText(img, size_label, (size_x, size_y), font, coord_font_scale, coord_color, coord_thickness)
        
        # Create output filename with timestamp
        timestamp_str = image_data.timestamp.strftime('%Y%m%d_%H%M%S')
        output_filename = f"{image_data.loop_id}_bad_mat_{timestamp_str}_{image_data.class_id}.png"
        output_path = os.path.join(output_dir, output_filename)
        
        # Save the image with bounding box
        success = cv2.imwrite(output_path, img)
        if not success:
            raise ValueError(f"Failed to save image to: {output_path}")
        
        print(f"  📷 Saved bad mat image: {output_filename}")
        print(f"  📐 Box coordinates: ({x1},{y1}) → ({x2},{y2}) | Size: {box_width}x{box_height}px")
        return output_path

    def get_model_name(self, loop_id: str) -> str:
        """Determine which classification model to use based on loop ID"""
        if loop_id == "pre_taping_l3":
            return "l3l4_mat_classification_model"
        elif loop_id == "pre_taping_l4":
            return "l3l4_mat_classification_model"
        else:
            print(f"Warning: Unknown loop_id {loop_id}, using loop3 model as default")
            return "l3l4_mat_classification_model"
        
    async def check_triton_status(self):
        """Check Triton server status and available models"""
        try:
            if not self.triton_client:
                print("❌ Triton client not initialized")
                return None

            # Check if server is ready
            if self.triton_client.is_server_ready():
                print(f"🖥️ Triton server is ready")
            else:
                print(f"❌ Triton server is not ready")
                return None

            # Check if model is ready
            model_name = "l3l4_mat_classification_model"
            if self.triton_client.is_model_ready(model_name):
                print(f"✅ Model '{model_name}' is ready")
            else:
                print(f"❌ Model '{model_name}' is not ready")
                return None

            return True

        except Exception as e:
            print(f"❌ Error checking Triton status: {e}")
            return None

    async def get_model_metadata(self, model_name: str):
        """Get model metadata to understand input/output format"""
        try:
            if not self.triton_client:
                print("❌ Triton client not initialized")
                return None

            metadata = self.triton_client.get_model_metadata(model_name)
            print(f"📊 Model '{model_name}' metadata:")
            print(f"   Name: {metadata.name}")
            print(f"   Inputs: {metadata.inputs}")
            print(f"   Outputs: {metadata.outputs}")
            return metadata

        except Exception as e:
            print(f"❌ Error getting model metadata: {e}")
            return None

    def softmax(self, x):
        """Apply softmax to convert logits to probabilities"""
        exp_x = np.exp(x - np.max(x))  # Subtract max for numerical stability
        return exp_x / np.sum(exp_x)

    async def infer_with_triton(self, image_data: np.ndarray, model_name: str) -> tuple[str, float, float]:
        """Send image to Triton server for classification using gRPC
        Returns: (classification, probability, logit)
        """
        try:
            if not self.triton_client:
                print(f"  ❌ Triton client not initialized")
                return "client_error", 0.0, 0.0

            # Define input/output names
            input_name = "input"
            output_name = "output"

            # Ensure proper shape - the model expects batch dimension = 1
            if len(image_data.shape) == 3:
                # Add batch dimension: [3, 224, 224] -> [1, 3, 224, 224]
                image_data = np.expand_dims(image_data, axis=0)
            elif len(image_data.shape) == 4 and image_data.shape[0] != 1:
                # Ensure batch size is 1
                image_data = image_data[:1]

            # Expected shape: [1, 3, 224, 224]
            image_data = image_data.astype(np.float32)

            # Create input object
            inputs = [grpcclient.InferInput(input_name, image_data.shape, "FP32")]
            inputs[0].set_data_from_numpy(image_data)

            # Create output object
            outputs = [grpcclient.InferRequestedOutput(output_name)]

            # Perform inference
            response = self.triton_client.infer(
                model_name=model_name,
                inputs=inputs,
                outputs=outputs
            )

            # Get output as numpy array
            output_data = response.as_numpy(output_name)

            # Parse classification result - 2-class output [bad, good]
            logits = output_data[0]  # Get the logits [bad, good]

            # Apply softmax to get probabilities
            probabilities = self.softmax(logits)

            # Get predicted class (0=bad, 1=good)
            predicted_class = np.argmax(logits)
            logit_score = float(logits[predicted_class])
            probability = float(probabilities[predicted_class])

            # Class labels: [bad, good]
            class_labels = ["bad", "good"]
            classification = class_labels[predicted_class]

            return classification, probability, logit_score

        except InferenceServerException as e:
            print(f"  ❌ Triton inference error: {e}")
            return "inference_error", 0.0, 0.0
        except Exception as e:
            print(f"  ❌ Unexpected error: {e}")
            return "error", 0.0, 0.0

    async def process_image(self, image_data: ImageData) -> dict:
        """Process a single image through the classification pipeline"""
        print(f"🔍 Processing {image_data.filename}")
        
        # Determine which model to use
        model_name = self.get_model_name(image_data.loop_id)
        
        # Full path to image
        image_path = os.path.join(self.input_folder, image_data.filename)
        
        try:
            # Crop and preprocess the specific region
            processed_img = self.crop_and_preprocess_image(image_path, image_data)
            
            print(f"  📐 Cropped: ({image_data.x1:.1f},{image_data.y1:.1f}) → ({image_data.x2:.1f},{image_data.y2:.1f})")
            
            # Classify with Triton model
            classification, probability, logit = await self.infer_with_triton(processed_img, model_name)

            # Initialize variables
            saved_image_path = ""
            cropped_image_path = ""

            # Map classifications to mat status
            if classification == "bad":
                mat_status = "bad_mat"
                result_emoji = "❌"
                result_color = "BAD"

                # *** SAVE IMAGES ONLY FOR BAD MAT ***
                try:
                    # Save image with bounding box
                    saved_image_path = self.save_bad_mat_image(image_path, image_data)
                    # Save cropped image
                    cropped_image_path = self.save_cropped_image_for_bad_mat(image_path, image_data)
                except Exception as e:
                    print(f"  ⚠️ Failed to save bad mat images: {e}")
                    saved_image_path = ""
                    cropped_image_path = ""

                # *** DB OPERATIONS ONLY FOR BAD MAT ***
                # Insert event for bad mat with saved image path as filename
                event_type_text = f"Bad mat detected in {image_data.loop_id}"
                self.insert_event(image_data.loop_id, event_type_text, image_path, saved_image_path)

                # Update TP table with bad mat status and filepath
                self.update_tp_table(image_data.loop_id, mat_status, saved_image_path)

            elif classification == "good":
                mat_status = "good_mat"
                result_emoji = "✅"
                result_color = "GOOD"
                # NO DB operations for good mat, NO image saving

            else:
                mat_status = "unknown_mat"
                result_emoji = "⚠️"
                result_color = "UNKNOWN"
                # NO DB operations for unknown mat, NO image saving

            print(f"  {result_emoji} Result: {classification.upper()} -> {mat_status} (probability: {probability:.1%}, logit: {logit:.3f}) - {image_data.loop_id}")

            result = {
                'filename': image_data.filename,
                'loop_id': image_data.loop_id,
                'class_id': image_data.class_id,
                'timestamp': image_data.timestamp.isoformat(),
                'model_used': model_name,
                'classification': classification,
                'mat_status': mat_status,
                'probability': probability,
                'logit': logit,
                'crop_region': {
                    'x1': image_data.x1,
                    'y1': image_data.y1,
                    'x2': image_data.x2,
                    'y2': image_data.y2,
                    'width': image_data.x2 - image_data.x1,
                    'height': image_data.y2 - image_data.y1
                },
                'saved_image_path': saved_image_path,
                'cropped_image_path': cropped_image_path,
                'processed_at': datetime.now().isoformat()
            }
            
            return result
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return {
                'filename': image_data.filename,
                'loop_id': image_data.loop_id,
                'error': str(e),
                'classification': 'error',
                'mat_status': 'error',
                'probability': 0.0,
                'logit': 0.0,
                'saved_image_path': '',
                'cropped_image_path': '',
                'processed_at': datetime.now().isoformat()
            }
    
    async def process_all_images(self):
        """Process all images in the input folder"""
        print(f"Scanning for images in: {self.input_folder}")
        images = self.scan_for_images()
        
        if not images:
            print("No new images found to process")
            return
            
        print(f"Found {len(images)} images to process")
        
        # Process each image
        for image_data in images:
            result = await self.process_image(image_data)
            self.results.append(result)
            
            # Small delay between images
            await asyncio.sleep(0.1)
        
        self.print_summary()
    
    async def continuous_processing(self):
        """Continuously monitor folder and process new images"""
        print("🚀 Starting continuous mat image processing...")

        # First check Triton server status and available models
        print("🔍 Checking Triton server...")
        await self.check_triton_status()

        # Get model metadata
        await self.get_model_metadata("l3l4_mat_classification_model")

        print("👀 Watching for new mat images in folder...")

        consecutive_empty_scans = 0
        
        while True:
            try:
                # Scan for new images
                images = self.scan_for_images()
                
                if images:
                    consecutive_empty_scans = 0
                    print(f"\n📸 Found {len(images)} new mat image(s)!")
                    
                    # Process each new image
                    for image_data in images:
                        result = await self.process_image(image_data)
                        self.results.append(result)
                        
                        # Small delay between images
                        await asyncio.sleep(0.1)
                    
                    print(f"✅ Total processed: {len(self.results)} images")
                    print("👀 Waiting for next image...")
                else:
                    consecutive_empty_scans += 1
                    # Show periodic "waiting" message
                    if consecutive_empty_scans % 30 == 0:  # Every 60 seconds (30 * 2s)
                        print(f"⏳ Still waiting... (checked {consecutive_empty_scans} times)")
                
                # Adaptive sleep - longer when no activity
                if consecutive_empty_scans > 20:
                    await asyncio.sleep(3)  # Slower polling when inactive
                else:
                    await asyncio.sleep(2)  # Normal polling
                
            except KeyboardInterrupt:
                print("\n🛑 Stopping continuous processing...")
                break
            except Exception as e:
                print(f"❌ Error in continuous processing: {e}")
                await asyncio.sleep(5)
        
        self.print_summary()
        print("🏁 Processing stopped.")
    
    def print_summary(self):
        """Print processing summary"""
        if not self.results:
            print("No results to summarize")
            return
            
        total = len(self.results)
        good_mat_count = sum(1 for r in self.results if r.get('mat_status') == 'good_mat')
        bad_mat_count = sum(1 for r in self.results if r.get('mat_status') == 'bad_mat')
        error_count = sum(1 for r in self.results if r.get('mat_status') not in ['good_mat', 'bad_mat'])
        
        # Count by loop
        loop_counts = {}
        for result in self.results:
            loop_id = result.get('loop_id', 'unknown')
            loop_counts[loop_id] = loop_counts.get(loop_id, 0) + 1
        
        print(f"\n=== Mat Processing Summary ===")
        print(f"Total images processed: {total}")
        print(f"\n📊 By Mat Status:")
        print(f"  ✅ Good Mat: {good_mat_count} ({good_mat_count/total*100:.1f}%)")
        print(f"  ❌ Bad Mat: {bad_mat_count} ({bad_mat_count/total*100:.1f}%)")
        print(f"  ⚠️ Errors: {error_count}")
        
        print(f"\n🔄 By Loop ID:")
        for loop_id in sorted(loop_counts.keys()):
            print(f"  {loop_id}: {loop_counts[loop_id]}")
        
        # Show recent results
        if self.results:
            recent = self.results[-1]
            print(f"\n🕒 Most recent: {recent['filename']} -> {recent.get('mat_status', 'unknown')} (probability: {recent.get('probability', 0):.1%}, logit: {recent.get('logit', 0):.3f})")

# Usage examples
async def process_once():
    """Process all images in folder once"""
    classifier = MatClassifier(
        input_folder="/home/ai4m/develop/data/mat_cam/",
        triton_url="localhost:8006"
    )

    await classifier.process_all_images()

async def process_continuously():
    """Continuously monitor and process new images"""
    classifier = MatClassifier(
        input_folder="/home/ai4m/develop/data/mat_cam/",
        triton_url="localhost:8006"
    )

    await classifier.continuous_processing()

if __name__ == "__main__":
    print("🔥 Continuous Mat Classifier - Starting...")
    print("🔍 Monitoring folder: /home/ai4m/develop/data/mat_cam")
    print("🖥️ Triton Server gRPC: localhost:8006")
    print("📦 Model: l3l4_mat_classification_model")
    print("🏷️ Classes: bad, good")
    print("💾 Cropped images folder: /home/ai4m/develop/Orientation_classify/croping_mat")
    print("💾 Bad mat images folder: /home/ai4m/develop/data/baumer/mat/")
    print("ℹ️ Press Ctrl+C to stop")
    print("=" * 50)

    # Always run in continuous mode
    asyncio.run(process_continuously())




