# websocket_handler.py
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        # Regular clients: user_id → [WebSocket]
        self.active_connections: Dict[str, List[WebSocket]] = {}

        # Special clients that listen for config updates
        self.config_update_connections: List[WebSocket] = []

        # Lock for concurrency safety
        self._lock = asyncio.Lock()

    # ---------------- Regular client methods ----------------
    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        async with self._lock:
            self.active_connections.setdefault(user_id, []).append(websocket)
        print(f"✅ Client {user_id} connected. Total: {len(self.active_connections[user_id])}")

    async def disconnect(self, websocket: WebSocket, user_id: str):
        async with self._lock:
            if user_id in self.active_connections and websocket in self.active_connections[user_id]:
                self.active_connections[user_id].remove(websocket)
                if not self.active_connections[user_id]:
                    del self.active_connections[user_id]
        print(f"🔌 Client {user_id} disconnected")

    async def send_personal_notification(self, user_id: str, message: Dict[str, Any]):
        async with self._lock:
            connections = list(self.active_connections.get(user_id, []))

        disconnected = []
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"⚠️ Failed to send to {user_id}: {e}")
                disconnected.append(connection)

        # cleanup
        for conn in disconnected:
            await self.disconnect(conn, user_id)

    # ---------------- Config update clients ----------------
    async def connect_config_updates(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.config_update_connections.append(websocket)
        print(f"✅ Config update client connected. Total: {len(self.config_update_connections)}")

    async def disconnect_config_updates(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.config_update_connections:
                self.config_update_connections.remove(websocket)
        print(f"🔌 Config update client disconnected. Total: {len(self.config_update_connections)}")

    async def broadcast_config_update(self, machine_id: str, config: dict):
        message = {
            "type": "machine_config_updated",
            "machine_id": machine_id,
            "updated_config": config,   # <--- keep raw posted config!
            "timestamp": datetime.utcnow().isoformat(),
        }
        for ws in list(self.config_update_connections):
            try:
                await ws.send_json(message)
            except Exception:
                self.config_update_connections.remove(ws)
        return len(self.config_update_connections)

    # ---------------- Acknowledgments ----------------
    async def handle_acknowledgment(self, ack_message: dict):
        machine_id = ack_message.get("machine_id")
        status = ack_message.get("status")
        message = ack_message.get("message")
        timestamp = ack_message.get("timestamp")

        if status == "success":
            print(f"✅ [ACK] Machine {machine_id}: {message}")
        else:
            print(f"❌ [ACK] Machine {machine_id}: {message}")

        # TODO: Add DB logging if required

    # ---------------- Heartbeat ----------------
    async def send_heartbeat(self):
        """Send heartbeat ping to config update clients."""
        async with self._lock:
            connections = list(self.config_update_connections)

        for conn in connections:
            try:
                await conn.send_json({"type": "ping", "timestamp": datetime.utcnow().isoformat()})
            except Exception as e:
                print(f"⚠️ Heartbeat failed: {e}")
                await self.disconnect_config_updates(conn)

    async def periodic_heartbeat(self, interval: float = 30.0):
        """Run heartbeat forever in background."""
        while True:
            try:
                await self.send_heartbeat()
            except Exception as e:
                print(f"⚠️ Heartbeat error: {e}")
            await asyncio.sleep(interval)


# Global instance
manager = ConnectionManager()

