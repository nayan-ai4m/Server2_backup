# websocket_handler.py - Enhanced version with config update support

from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime
from typing import List, Dict, Any
import json

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        # NEW: Add dedicated config update connections
        self.config_update_connections: List[WebSocket] = []

    # Existing methods remain the same
    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str):
        if user_id in self.active_connections:
            self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def send_personal_notification(self, user_id: str, message: Dict[str, Any]):
        if user_id in self.active_connections:
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"Failed to send message to {user_id}: {e}")

    # NEW: Config update connection management
    async def connect_config_updates(self, websocket: WebSocket):
        """Connect a client that wants to receive config updates"""
        await websocket.accept()
        self.config_update_connections.append(websocket)
        print(f"✅ Config update client connected. Total: {len(self.config_update_connections)}")

    def disconnect_config_updates(self, websocket: WebSocket):
        """Disconnect a config update client"""
        if websocket in self.config_update_connections:
            self.config_update_connections.remove(websocket)
            print(f"❌ Config update client disconnected. Total: {len(self.config_update_connections)}")

    # NEW: Broadcast config updates to all connected clients
    async def broadcast_config_update(self, machine_id: str, config: dict):
        """Send config updates to all connected config update clients"""
        if not self.config_update_connections:
            print("⚠️ No config update clients connected")
            return

        message = {
            "machine_id": machine_id,
            "updated_config": config,
            "timestamp": datetime.now().isoformat()
        }
        
        disconnected = []
        successful = 0
        
        for connection in self.config_update_connections:
            try:
                await connection.send_json(message)
                successful += 1
            except Exception as e:
                print(f"Failed to send config update: {e}")
                disconnected.append(connection)
        
        # Clean up disconnected connections
        for conn in disconnected:
            self.disconnect_config_updates(conn)
            
        print(f"📤 Config update sent to {successful} clients for machine {machine_id}")

    # Enhanced acknowledgment handling
    async def handle_acknowledgment(self, ack_message: dict):
        """Process acknowledgments received from Server 1"""
        machine_id = ack_message.get("machine_id")
        status = ack_message.get("status")
        message = ack_message.get("message")
        timestamp = ack_message.get("timestamp")

        if status == "success":
            print(f"✅ [ACK] Machine {machine_id}: {message}")
        else:
            print(f"❌ [ACK] Machine {machine_id}: {message}")
        
        # You can add database logging here if needed
        # log_acknowledgment_to_db(machine_id, status, message, timestamp)

manager = ConnectionManager()
