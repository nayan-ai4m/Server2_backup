#!/usr/bin/env python3
"""
AI4M Camera Mapping System - Multi-Topic Version
================================================

This module provides real-time camera capture functionality for manufacturing line monitoring.
It maps machine events to appropriate camera streams and captures frames during events.
Now supports multiple Kafka topics for different production lines.

Author: Manufacturing Systems Team
Version: 1.1.0
License: Proprietary
"""

import cv2
import os
import json
import datetime
import logging
import time
import sys
from typing import Dict, Optional, Tuple, List
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import traceback


class CameraConfig:
    """Configuration class for camera streams and mappings."""
    
    # RTSP camera streams configuration
    CAMERA_STREAMS: Dict[str, str] = {
        # H265 streams - Production Line Cameras
        "l4_taping_view1": "rtsp://admin:unilever2024@192.168.1.26:554/Streaming/Channels/102",
        "l3_EOL": "rtsp://admin:unilever2024@192.168.1.29:554/Streaming/Channels/102",
        "l4_EOL": "rtsp://admin:unilever2024@192.168.1.28:554/Streaming/Channels/102",
        "mc17": "rtsp://admin:unilever2024@192.168.1.21:554/Streaming/Channels/102",
        "mc19": "rtsp://admin:unilever2024@192.168.1.20:554/Streaming/Channels/102",
        "mc21": "rtsp://admin:unilever2024@192.168.1.22:554/Streaming/Channels/102",
        "mc25": "rtsp://admin:unilever2024@192.168.1.27:554/Streaming/Channels/102",
        "mc27": "rtsp://admin:unilever2024@192.168.1.25:554/Streaming/Channels/102",
        "mc29": "rtsp://admin:unilever2024@192.168.1.23:554/Streaming/Channels/102",
        
        # H264 streams - Quality Control Cameras
        "l3_taping_view2": "rtsp://admin:unilever2024@192.168.1.31:554/Streaming/Channels/102",
        "l4_taping_view2": "rtsp://admin:unilever2024@192.168.1.32:554/Streaming/Channels/102",
        "l3_highbay": "rtsp://admin:unilever2024@192.168.1.30:554/Streaming/Channels/102",
        "l4_highbay": "rtsp://admin:unilever2024@192.168.1.33:554/Streaming/Channels/102",
        "l3_taping_view1": "rtsp://admin:unilever2024@192.168.1.24:554/Streaming/Channels/102",
    }
    
    # Multi-topic Kafka configuration
    KAFKA_CONFIG = {
        'topics': ['cctv_l3_3', 'cctv_l4_3'],  # Multiple topics
        'bootstrap_servers': '192.168.1.168:9092',
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'group_id': 'camera_capture_group_multi',  # Updated group ID
        'consumer_timeout_ms': 1000,
    }

    CAPTURE_TRIGGER_EVENTS = {
        "jamming",
        "bad_orientation",
        "possible_starvation",
    }

    
    # File system configuration
    SAVE_BASE_DIR = "/home/ai4m/develop/test_saving_images/images_saved/"
    IMAGE_FORMAT = "jpg"
    IMAGE_QUALITY = 95
    
    # Camera capture settings
    CAPTURE_TIMEOUT_MS = 5000
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 1


class Logger:
    """Centralized logging configuration for AI4M compliance."""
    
    @staticmethod
    def setup_logger(name: str = __name__) -> logging.Logger:
        """
        Setup standardized logger for manufacturing systems.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        
        if not logger.handlers:
            # Configure handler
            handler = logging.StreamHandler(sys.stdout)
            
            # AI4M standard format with timestamp, level, component, and message
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            
        return logger


class CameraMappingService:
    """
    Main service class for camera mapping and frame capture.
    
    This class handles the mapping of machine events to camera streams
    and manages the capture process with proper error handling and logging.
    Now supports multiple Kafka topics for different production lines.
    """
    
    def __init__(self, config: CameraConfig = None):
        """
        Initialize the camera mapping service.
        
        Args:
            config: Configuration object (defaults to CameraConfig)
        """
        self.config = config or CameraConfig()
        self.logger = Logger.setup_logger(self.__class__.__name__)
        
        # Initialize directories
        self._initialize_directories()
        
        self.logger.info("Multi-Topic Camera Mapping Service initialized successfully")
        self.logger.info(f"Configured topics: {self.config.KAFKA_CONFIG['topics']}")
    
    def _initialize_directories(self) -> None:
        """Create necessary directories for image storage."""
        try:
            for camera_name in self.config.CAMERA_STREAMS.keys():
                directory_path = os.path.join(self.config.SAVE_BASE_DIR, camera_name)
                os.makedirs(directory_path, exist_ok=True)
            
            self.logger.info(f"Initialized storage directories in {self.config.SAVE_BASE_DIR}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize directories: {str(e)}")
            raise
    
    def get_mapped_camera_name(self, machine_name: str) -> Optional[str]:
        """
        Map machine names to their corresponding camera names.
        
        Implements the AI4M standard mapping logic for production lines:
        - Line 3 (L3): mc17-mc22 machine controllers + EOL equipment
        - Line 4 (L4): mc25-mc30 machine controllers + EOL equipment
        
        Args:
            machine_name: Original machine identifier from event
            
        Returns:
            Mapped camera name or None if no mapping exists
            
        Raises:
            ValueError: If machine_name is empty or invalid
        """
        if not machine_name or not isinstance(machine_name, str):
            raise ValueError("Machine name must be a non-empty string")
        
        try:
            # Line 3 Machine Controller mappings
            if machine_name.startswith("mc17_") or machine_name.startswith("mc18_"):
                return "mc17"
            elif machine_name.startswith("mc19_") or machine_name.startswith("mc20_"):
                return "mc19"
            elif machine_name.startswith("mc21_") or machine_name.startswith("mc22_"):
                return "mc21"
            
            # Line 4 Machine Controller mappings
            elif machine_name.startswith("mc25_") or machine_name.startswith("mc26_"):
                return "mc25"
            elif machine_name.startswith("mc27_") or machine_name.startswith("mc28_"):
                return "mc27"
            elif machine_name.startswith("mc29_") or machine_name.startswith("mc30_"):
                return "mc29"
            
            # Line 3 End-of-Line mappings
            elif (machine_name.startswith("l3_outfeed") or 
                  machine_name.startswith("l3_tapping")):
                return "l3_taping_view1"
            elif machine_name in ["case_erector_l3", "check_weigher_rejection_l3"]:
                return "l3_EOL"
            elif machine_name in ["cw_outfeed_l3", "highbay_l3", "conveyor_to_highbay_l3"]:
                return "l3_highbay"
            elif machine_name == "pre_taping_l3":
                return "l3_taping_view2"
            
            # Line 4 End-of-Line mappings
            elif (machine_name.startswith("l4_outfeed") or 
                  machine_name.startswith("l4_tapping")):
                return "l4_taping_view1"
            elif machine_name in ["case_erector_l4", "check_weigher_rejection_l4"]:
                return "l4_EOL"
            elif machine_name in ["cw_outfeed_l4", "highbay_l4", "conveyor_to_highbay_l4"]:
                return "l4_highbay"
            elif machine_name == "pre_taping_l4":
                return "l4_taping_view2"
            
            # Fallback: direct mapping if camera exists
            base_name = machine_name.split("_")[0]
            if base_name in self.config.CAMERA_STREAMS:
                return base_name
            
            # No mapping found
            self.logger.warning(f"No camera mapping found for machine: {machine_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error in camera mapping for {machine_name}: {str(e)}")
            return None
    
    def capture_frame_with_retry(self, machine_name: str, status: str, topic: str = None) -> bool:
        """
        Capture frame from camera with retry logic and comprehensive error handling.
        
        Args:
            machine_name: Machine identifier from event
            status: Event status (fault, normal, etc.)
            topic: Source topic (for logging purposes)
            
        Returns:
            True if capture successful, False otherwise
        """
        mapped_camera = self.get_mapped_camera_name(machine_name)
        
        if not mapped_camera:
            self.logger.warning(f"Skipping capture - no camera mapping for: {machine_name}")
            return False
        
        rtsp_url = self.config.CAMERA_STREAMS.get(mapped_camera)
        if not rtsp_url:
            self.logger.error(f"No RTSP URL configured for camera: {mapped_camera}")
            return False
        
        topic_info = f" (from {topic})" if topic else ""
        self.logger.info(f"Capturing frame: {machine_name} -> {mapped_camera}{topic_info}")
        
        # Retry logic for robust capture
        for attempt in range(1, self.config.MAX_RETRY_ATTEMPTS + 1):
            try:
                success = self._capture_single_frame(
                    rtsp_url, mapped_camera, machine_name, status, attempt, topic
                )
                
                if success:
                    return True
                    
                if attempt < self.config.MAX_RETRY_ATTEMPTS:
                    self.logger.warning(f"Capture attempt {attempt} failed, retrying in {self.config.RETRY_DELAY_SECONDS}s")
                    time.sleep(self.config.RETRY_DELAY_SECONDS)
                
            except Exception as e:
                self.logger.error(f"Capture attempt {attempt} exception: {str(e)}")
                if attempt < self.config.MAX_RETRY_ATTEMPTS:
                    time.sleep(self.config.RETRY_DELAY_SECONDS)
        
        self.logger.error(f"All capture attempts failed for {machine_name}")
        return False
    
    def _capture_single_frame(self, rtsp_url: str, camera_name: str, 
                             machine_name: str, status: str, attempt: int, topic: str = None) -> bool:
        """
        Execute single frame capture attempt.
        
        Args:
            rtsp_url: RTSP stream URL
            camera_name: Mapped camera identifier
            machine_name: Original machine name
            status: Event status
            attempt: Current attempt number
            topic: Source topic (for filename)
            
        Returns:
            True if successful, False otherwise
        """
        cap = None
        try:
            # Initialize capture with timeout
            cap = cv2.VideoCapture(rtsp_url)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # Reduce buffer for real-time
            
            # Set timeout if supported
            if hasattr(cv2, 'CAP_PROP_OPEN_TIMEOUT_MSEC'):
                cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.config.CAPTURE_TIMEOUT_MS)
            
            ret, frame = cap.read()
            
            if not ret or frame is None:
                self.logger.warning(f"Failed to read frame from {camera_name} (attempt {attempt})")
                return False
            
            # Generate timestamped filename with topic info
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
            topic_suffix = f"_{topic}" if topic else ""
            filename = f"{status}_{machine_name}_{timestamp}{topic_suffix}.{self.config.IMAGE_FORMAT}"
            
            # Save frame
            save_dir = os.path.join(self.config.SAVE_BASE_DIR, camera_name)
            save_path = os.path.join(save_dir, filename)
            
            # Write with quality settings
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, self.config.IMAGE_QUALITY]
            success = cv2.imwrite(save_path, frame, encode_params)
            
            if success:
                file_size = os.path.getsize(save_path)
                self.logger.info(f"Frame saved successfully: {save_path} ({file_size} bytes)")
                return True
            else:
                self.logger.error(f"Failed to write image file: {save_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"Frame capture error: {str(e)}")
            self.logger.debug(traceback.format_exc())
            return False
            
        finally:
            if cap is not None:
                cap.release()
    
    def start_kafka_consumer(self) -> None:
        """
        Start Kafka consumer for multiple topics with proper error handling and reconnection logic.
        
        Implements AI4M standard for resilient message consumption across multiple topics.
        """
        consumer = None
        
        while True:
            try:
                self.logger.info("Initializing multi-topic Kafka consumer...")
                self.logger.info(f"Subscribing to topics: {self.config.KAFKA_CONFIG['topics']}")
                
                consumer = KafkaConsumer(
                    *self.config.KAFKA_CONFIG['topics'],  # Unpack topics list
                    bootstrap_servers=self.config.KAFKA_CONFIG['bootstrap_servers'],
                    auto_offset_reset=self.config.KAFKA_CONFIG['auto_offset_reset'],
                    enable_auto_commit=self.config.KAFKA_CONFIG['enable_auto_commit'],
                    group_id=self.config.KAFKA_CONFIG['group_id'],
                    consumer_timeout_ms=self.config.KAFKA_CONFIG['consumer_timeout_ms'],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                
                self.logger.info("Multi-topic Kafka consumer started successfully")
                
                # Main consumption loop
                for message in consumer:
                    try:
                        # Extract topic from message metadata
                        topic = message.topic
                        self._process_kafka_message(message, topic)
                    except Exception as e:
                        self.logger.error(f"Error processing message from {message.topic}: {str(e)}")
                        continue
                        
            except NoBrokersAvailable:
                self.logger.error("No Kafka brokers available. Retrying in 10 seconds...")
                time.sleep(10)
                
            except KafkaError as e:
                self.logger.error(f"Kafka error: {str(e)}. Retrying in 5 seconds...")
                time.sleep(5)
                
            except KeyboardInterrupt:
                self.logger.info("Shutdown requested by user")
                break
                
            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}")
                self.logger.debug(traceback.format_exc())
                time.sleep(5)
                
            finally:
                if consumer is not None:
                    consumer.close()
                    consumer = None

    def _should_capture_image(self, status: str) -> bool:
        """
        Check if the event status should trigger image capture.
        
        Args:
            status: Event status string
            
        Returns:
            True if status matches a fault condition, False otherwise
        """
        if not status:
            return False

        status_lower = status.lower()
        for trigger in self.config.CAPTURE_TRIGGER_EVENTS:
            if trigger in status_lower:  # partial + case-insensitive match
                return True
        return False
    
    def _process_kafka_message(self, message, topic: str) -> None:
        """
        Process individual Kafka message and trigger frame capture.
        
        Args:
            message: Kafka message object
            topic: Source topic name
        """
        try:
            event = message.value
            machine_name = event.get("machine_name")
            status = event.get("status", "unknown")
            timestamp = event.get("timestamp", datetime.datetime.now().isoformat())
            
            self.logger.info(f"Processing event from {topic}: machine={machine_name}, status={status}, ts={timestamp}")
            
            if not machine_name:
                self.logger.warning(f"Received event without machine_name from {topic}")
                return

            # Check if status should trigger capture
            if not self._should_capture_image(status):
                self.logger.info(f"Skipping capture (non-fault event) from {topic}: {status}")
                return
            
            # Trigger frame capture with topic info
            capture_success = self.capture_frame_with_retry(machine_name, status, topic)
            
            if capture_success:
                self.logger.info(f"Event processed successfully from {topic}: {machine_name}")
            else:
                self.logger.warning(f"Frame capture failed for event from {topic}: {machine_name}")
                
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"Invalid message format from {topic}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error processing message from {topic}: {str(e)}")


def main():
    """Main entry point following AI4M standards."""
    try:
        # Initialize logging
        logger = Logger.setup_logger("main")
        logger.info("Starting AI4M Multi-Topic Camera Mapping System")
        
        # Initialize service
        service = CameraMappingService()
        
        # Start consumer (blocking call)
        service.start_kafka_consumer()
        
    except KeyboardInterrupt:
        logger.info("Application shutdown requested")
    except Exception as e:
        logger.error(f"Application failed to start: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
