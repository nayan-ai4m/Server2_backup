#!/usr/bin/env python3
"""
Dynamic Vision Detector Manager
Automatically starts/stops detectors based on current batch SKU from database
"""

import psycopg2
import subprocess
import time
import logging
import json
import os
import signal
import sys
from typing import Dict, Optional, Tuple
from datetime import datetime
from logging.handlers import RotatingFileHandler

class DynamicDetectorManager:
    """Manages detectors based on real-time SKU information from database"""
    
    def __init__(self, base_config_path: str = "config.json", 
                 detector_config_path: str = "config_detector.json"):
        self.base_config = self._load_config(base_config_path)
        self.detector_config = self._load_config(detector_config_path)
        self.logger = self._setup_logging()
       # self.db_conn = self._connect_db()
        
        # Track currently running detectors {machine_id: (sachet_type, pid)}
        self.running_detectors: Dict[str, Tuple[str, int]] = {}
        
        # Track last known SKUs {machine_id: sku}
        self.last_skus: Dict[str, str] = {}
        
        # Track restart attempts {machine_id: count}
        self.restart_attempts: Dict[str, int] = {}
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.detector_script = os.path.join(self.script_dir, "vision_detector.py")
        
        # Get directories from config
        log_settings = self.detector_config["logging_settings"]
        self.pid_dir = os.path.join(self.script_dir, log_settings["pid_dir"])
        self.log_dir = os.path.join(self.script_dir, log_settings["detector_log_dir"])
        
        os.makedirs(self.pid_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Load loop configuration
        self.loop_config = self.detector_config["loop_configuration"]
        
        # Load SKU mapping
        self.sku_mapping = {
            sku: data["sachet_type"] 
            for sku, data in self.detector_config["sku_mapping"].items()
        }
        
        # Get settings
        self.manager_settings = self.detector_config["dynamic_manager"]
        self.fallback_settings = self.detector_config["fallback_settings"]
        self.db_settings = self.detector_config["database_settings"]
        self.db_conn = self._connect_db()

        self.logger.info("Dynamic Detector Manager initialized")
        self.logger.info(f"Loaded {len(self.sku_mapping)} SKU mappings")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {config_path}: {e}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging with rotation"""
        log_settings = self.detector_config["logging_settings"]
        
        logger = logging.getLogger("DynamicDetectorManager")
        logger.setLevel(log_settings["manager_log_level"])
        logger.handlers.clear()
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_settings["manager_log_file"],
            maxBytes=log_settings["manager_log_max_bytes"],
            backupCount=log_settings["manager_log_backup_count"]
        )
        file_handler.setLevel(log_settings["manager_log_level"])
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_settings["manager_log_level"])
        
        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _connect_db(self) -> psycopg2.extensions.connection:
        """Connect to PostgreSQL database with retry logic"""
        conn_str = self.detector_config["database"]["connection_string"]
        retry_attempts = self.db_settings["retry_attempts"]
        retry_delay = self.db_settings["retry_delay"]
        
        for attempt in range(retry_attempts):
            try:
                self.logger.info(f"Connecting to database (attempt {attempt + 1}/{retry_attempts})...")
                conn = psycopg2.connect(conn_str)
                self.logger.info("Database connection established")
                return conn
            except Exception as e:
                self.logger.error(f"Database connection failed: {e}")
                if attempt < retry_attempts - 1:
                    time.sleep(retry_delay)
                else:
                    raise
    
    def get_current_sku(self, loop_table: str) -> Optional[Dict]:
        """
        Get the most recent SKU from loop overview table
        Returns: {batch_sku: str, last_update: datetime} or None
        """
        try:
            with self.db_conn.cursor() as cur:
                query = f"""
                    SELECT batch_sku, last_update 
                    FROM {loop_table} 
                    ORDER BY last_update DESC 
                    LIMIT 1
                """
                cur.execute(query)
                result = cur.fetchone()
                
                if result:
                    return {
                        "batch_sku": result[0],
                        "last_update": result[1]
                    }
                return None
        except Exception as e:
            self.logger.error(f"Error querying {loop_table}: {e}")
            if self.fallback_settings["fallback_on_query_error"]:
                self.logger.warning("Using fallback settings due to query error")
            return None
    
    def map_sku_to_sachet(self, sku: str) -> Optional[str]:
        """Map SKU to sachet type"""
        # Try exact match first
        if sku in self.sku_mapping:
            sachet_type = self.sku_mapping[sku]
            # Get product details for logging
            product_info = self.detector_config["sku_mapping"][sku]
            self.logger.info(
                f"SKU mapped: {sku} -> {sachet_type} "
                f"({product_info['brand']} - {product_info['product_name']})"
            )
            return sachet_type
        
        # Try partial match (in case of variations)
        for sku_key, sachet_type in self.sku_mapping.items():
            if sku_key in sku or sku in sku_key:
                self.logger.warning(f"Partial SKU match: {sku} -> {sku_key} -> {sachet_type}")
                return sachet_type
        
        # Unknown SKU
        if self.fallback_settings["fallback_on_unknown_sku"]:
            fallback_type = self.fallback_settings["default_sachet_type"]
            self.logger.warning(f"Unknown SKU: {sku}, using fallback: {fallback_type}")
            return fallback_type
        
        self.logger.error(f"Unknown SKU: {sku}, no fallback enabled")
        return None
    
    def start_detector(self, machine_id: str, sachet_type: str) -> Optional[int]:
        """Start a detector for given machine and sachet type"""
        try:
            log_file = os.path.join(self.log_dir, f"{machine_id}_{sachet_type}.log")
            
            cmd = [
                "python3",
                self.detector_script,
                machine_id,
                sachet_type,
                os.path.join(self.script_dir, "config.json")
            ]
            
            self.logger.info(f"Starting detector: {machine_id}-{sachet_type}")
            
            with open(log_file, "a") as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setpgrp  # Create new process group
                )
            
            startup_delay = self.manager_settings["startup_delay"]
            time.sleep(startup_delay)
            
            # Check if process is still running
            if process.poll() is None:
                pid = process.pid
                
                # Save PID file
                pid_file = os.path.join(self.pid_dir, f"{machine_id}_{sachet_type}.pid")
                with open(pid_file, "w") as f:
                    f.write(str(pid))
                
                self.logger.info(f"✓ Started detector: {machine_id}-{sachet_type} (PID: {pid})")
                
                # Reset restart attempts on successful start
                if machine_id in self.restart_attempts:
                    del self.restart_attempts[machine_id]
                
                return pid
            else:
                self.logger.error(f"✗ Failed to start detector: {machine_id}-{sachet_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error starting detector {machine_id}-{sachet_type}: {e}", exc_info=True)
            return None
    
    def stop_detector(self, machine_id: str, sachet_type: str, pid: int):
        """Stop a running detector"""
        try:
            self.logger.info(f"Stopping detector: {machine_id}-{sachet_type} (PID: {pid})")
            
            timeout = self.manager_settings["shutdown_timeout"]
            
            # Try graceful shutdown first
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            
            # Check if still running
            try:
                os.kill(pid, 0)  # Check if process exists
                # Still running, wait a bit more
                time.sleep(timeout - 2)
                try:
                    os.kill(pid, 0)
                    # Still running, force kill
                    os.kill(pid, signal.SIGKILL)
                    self.logger.warning(f"Force killed detector: {machine_id}-{sachet_type}")
                except OSError:
                    pass
            except OSError:
                # Process already terminated
                pass
            
            # Remove PID file
            pid_file = os.path.join(self.pid_dir, f"{machine_id}_{sachet_type}.pid")
            if os.path.exists(pid_file):
                os.remove(pid_file)
            
            self.logger.info(f"✓ Stopped detector: {machine_id}-{sachet_type}")
            
        except Exception as e:
            self.logger.error(f"Error stopping detector {machine_id}-{sachet_type}: {e}")
    
    def update_machine_detector(self, machine_id: str, new_sku: str):
        """Update detector for a machine based on new SKU"""
        new_sachet = self.map_sku_to_sachet(new_sku)
        
        if new_sachet is None:
            self.logger.error(f"{machine_id}: Cannot determine sachet type for SKU: {new_sku}")
            return
        
        # Check if detector needs to change
        if machine_id in self.running_detectors:
            current_sachet, current_pid = self.running_detectors[machine_id]
            
            if current_sachet == new_sachet:
                self.logger.debug(f"{machine_id}: Already running correct detector ({new_sachet})")
                return
            
            # Stop current detector
            self.logger.info(
                f"{machine_id}: SKU changed from {self.last_skus.get(machine_id, 'unknown')} "
                f"to {new_sku}, switching from {current_sachet} to {new_sachet}"
            )
            self.stop_detector(machine_id, current_sachet, current_pid)
            del self.running_detectors[machine_id]
        
        # Check restart attempts
        max_attempts = self.manager_settings["max_restart_attempts"]
        attempts = self.restart_attempts.get(machine_id, 0)
        
        if attempts >= max_attempts:
            self.logger.error(
                f"{machine_id}: Max restart attempts ({max_attempts}) reached. "
                "Manual intervention required."
            )
            return
        
        # Start new detector
        pid = self.start_detector(machine_id, new_sachet)
        if pid:
            self.running_detectors[machine_id] = (new_sachet, pid)
            self.last_skus[machine_id] = new_sku
        else:
            # Track failed attempt
            self.restart_attempts[machine_id] = attempts + 1
            self.logger.warning(
                f"{machine_id}: Failed to start detector (attempt {attempts + 1}/{max_attempts})"
            )
    
    def scan_and_update(self):
        """Scan database and update all detectors"""
        self.logger.info("=" * 70)
        self.logger.info("Scanning database for SKU updates...")
        
        # Check each loop
        for loop_name, loop_data in self.loop_config.items():
            table_name = loop_data["table_name"]
            machines = loop_data["machines"]
            
            self.logger.info(f"Checking {loop_name.upper()} ({table_name})...")
            
            sku_data = self.get_current_sku(table_name)
            
            if sku_data:
                sku = sku_data["batch_sku"]
                last_update = sku_data["last_update"]
                self.logger.info(f"  SKU: {sku}")
                self.logger.info(f"  Last Update: {last_update}")
                
                # Update all machines in this loop
                for machine in machines:
                    # Only update if SKU changed or detector not running
                    if (self.last_skus.get(machine) != sku or 
                        machine not in self.running_detectors):
                        self.update_machine_detector(machine, sku)
                    else:
                        self.logger.debug(f"  {machine}: No change needed")
            else:
                self.logger.warning(f"  No data found in {table_name}")
                if self.fallback_settings["fallback_on_query_error"]:
                    fallback_type = self.fallback_settings["default_sachet_type"]
                    self.logger.info(f"  Using fallback sachet type: {fallback_type}")
                    for machine in machines:
                        if machine not in self.running_detectors:
                            self.update_machine_detector(machine, f"FALLBACK_{fallback_type.upper()}")
        
        self.logger.info(f"Active detectors: {len(self.running_detectors)}")
        for machine_id, (sachet_type, pid) in self.running_detectors.items():
            self.logger.info(f"  {machine_id}: {sachet_type} (PID: {pid})")
        self.logger.info("=" * 70)
    
    def health_check(self):
        """Check health of running detectors and restart if needed"""
        if not self.manager_settings["enable_auto_restart"]:
            return
        
        dead_detectors = []
        
        for machine_id, (sachet_type, pid) in self.running_detectors.items():
            try:
                # Check if process is still alive
                os.kill(pid, 0)
            except OSError:
                self.logger.warning(f"Detector {machine_id}-{sachet_type} (PID: {pid}) is dead")
                dead_detectors.append(machine_id)
        
        # Restart dead detectors
        for machine_id in dead_detectors:
            sachet_type, old_pid = self.running_detectors[machine_id]
            del self.running_detectors[machine_id]
            
            sku = self.last_skus.get(machine_id)
            if sku:
                self.logger.info(f"Attempting to restart detector: {machine_id}")
                self.update_machine_detector(machine_id, sku)
    
    def stop_all(self):
        """Stop all running detectors"""
        self.logger.info("Stopping all detectors...")
        for machine_id, (sachet_type, pid) in list(self.running_detectors.items()):
            self.stop_detector(machine_id, sachet_type, pid)
        self.running_detectors.clear()
        self.logger.info("All detectors stopped")
    
    def run(self, scan_interval: Optional[int] = None):
        """Main loop - continuously monitor and update detectors"""
        if scan_interval is None:
            scan_interval = self.manager_settings["scan_interval"]
        
        health_check_interval = self.detector_config["detector_defaults"]["health_check_interval"]
        
        self.logger.info(f"Starting dynamic detector manager")
        self.logger.info(f"  Scan interval: {scan_interval}s")
        self.logger.info(f"  Health check interval: {health_check_interval}s")
        
        last_health_check = time.time()
        
        try:
            while True:
                self.scan_and_update()
                
                # Check if it's time for health check
                current_time = time.time()
                if current_time - last_health_check >= health_check_interval:
                    self.health_check()
                    last_health_check = current_time
                
                time.sleep(scan_interval)
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        finally:
            self.stop_all()
            if self.db_conn:
                self.db_conn.close()
            self.logger.info("Dynamic Detector Manager stopped")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Dynamic Vision Detector Manager")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to base config file (default: config.json)"
    )
    parser.add_argument(
        "--detector-config",
        default="config_detector.json",
        help="Path to detector config file (default: config_detector.json)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        help="Scan interval in seconds (overrides config)"
    )
    parser.add_argument(
        "--scan-once",
        action="store_true",
        help="Scan once and exit (for testing)"
    )
    
    args = parser.parse_args()
    
    try:
        manager = DynamicDetectorManager(
            base_config_path=args.config,
            detector_config_path=args.detector_config
        )
        
        if args.scan_once:
            manager.scan_and_update()
        else:
            manager.run(scan_interval=args.interval)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
