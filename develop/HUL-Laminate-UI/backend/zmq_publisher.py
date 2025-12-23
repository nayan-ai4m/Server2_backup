# zmq_publisher.py
import zmq
import json
import threading
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime


class ZMQConfigPublisher:
    """ZMQ Publisher for broadcasting machine configuration updates to Server 1."""

    def __init__(self, bind_address: str = "tcp://192.168.1.168:8030"):
        self.bind_address = bind_address
        self.context: Optional[zmq.Context] = None
        self.socket: Optional[zmq.Socket] = None
        self._lock = threading.Lock()
        self._is_connected = False

    def connect(self):
        """Initialize ZMQ context and bind publisher socket."""
        try:
            with self._lock:
                if not self._is_connected:
                    self.context = zmq.Context()
                    self.socket = self.context.socket(zmq.PUB)
                    self.socket.bind(self.bind_address)
                    self._is_connected = True
                    print(f"✅ ZMQ Publisher bound to {self.bind_address}")
        except Exception as e:
            print(f"❌ Failed to bind ZMQ Publisher: {e}")
            raise

    def disconnect(self):
        """Close ZMQ socket and context."""
        try:
            with self._lock:
                if self._is_connected:
                    if self.socket:
                        self.socket.close()
                    if self.context:
                        self.context.term()
                    self._is_connected = False
                    print("🔌 ZMQ Publisher disconnected")
        except Exception as e:
            print(f"⚠️ Error disconnecting ZMQ Publisher: {e}")

    def publish_config_update(self, machine_id: str, config_data: Dict[str, Any]) -> bool:
        """
        Publish machine configuration update to ZMQ subscribers.

        Args:
            machine_id: Machine identifier (e.g., "17", "18", etc.)
            config_data: Configuration dictionary to publish

        Returns:
            bool: True if published successfully, False otherwise
        """
        try:
            if not self._is_connected:
                self.connect()

            # Prepare the message payload
            message = {
                "type": "config_updated",
                "machine_id": machine_id,
                "updated_config": config_data,
                "timestamp": datetime.utcnow().isoformat(),
                "server": "server2"
            }

            # Publish the message
            message_json = json.dumps(message)
            self.socket.send_string(message_json)

            print(f"📤 ZMQ: Published config update for machine {machine_id}")
            return True

        except Exception as e:
            print(f"❌ ZMQ publish error for machine {machine_id}: {e}")
            return False

    def publish_machine_config_update(self, machine_id: str, config_data: Dict[str, Any]) -> bool:
        """
        Publish machine configuration update (A, T_eff, t parameters).

        Args:
            machine_id: Machine identifier
            config_data: Configuration dictionary to publish

        Returns:
            bool: True if published successfully, False otherwise
        """
        try:
            if not self._is_connected:
                self.connect()

            # Prepare the message payload for machine config updates
            message = {
                "type": "machine_config_updated",
                "machine_id": machine_id,
                "updated_config": config_data,
                "timestamp": datetime.utcnow().isoformat(),
                "server": "server2"
            }

            # Publish the message
            message_json = json.dumps(message)
            self.socket.send_string(message_json)

            print(f"📤 ZMQ: Published machine config update for machine {machine_id}")
            return True

        except Exception as e:
            print(f"❌ ZMQ publish error for machine config {machine_id}: {e}")
            return False

    async def async_publish_config_update(self, machine_id: str, config_data: Dict[str, Any]) -> bool:
        """
        Async wrapper for publishing config updates.
        Runs the synchronous publish method in a thread executor.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.publish_config_update, machine_id, config_data)

    async def async_publish_machine_config_update(self, machine_id: str, config_data: Dict[str, Any]) -> bool:
        """
        Async wrapper for publishing machine config updates.
        Runs the synchronous publish method in a thread executor.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.publish_machine_config_update, machine_id, config_data)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Global publisher instance
publisher = ZMQConfigPublisher()


# Utility functions for easy import
async def publish_config_update(machine_id: str, config_data: Dict[str, Any]) -> bool:
    """Convenience function to publish config updates."""
    return await publisher.async_publish_config_update(machine_id, config_data)


async def publish_machine_config_update(machine_id: str, config_data: Dict[str, Any]) -> bool:
    """Convenience function to publish machine config updates."""
    return await publisher.async_publish_machine_config_update(machine_id, config_data)