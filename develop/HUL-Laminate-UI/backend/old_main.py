from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from websocket_handler import manager
import json
import traceback
import uvicorn
from datetime import datetime
import os
from dataclasses import asdict, dataclass
from pydantic import BaseModel
import nats
from typing import List, Dict, Optional, Any

from laminate_sealing import fetch_latest_suggestion
from stroke_calculator import (
    load_model,
    get_required_strokes,
    append_to_csv,
    get_stroke_calculator,
)
from config_handler import router as config_router


# ------------------ FASTAPI SETUP ------------------

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(config_router)


# ------------------ DATA CLASSES ------------------


@dataclass
class Payload:
    plc: str
    name: str
    value: float
    status: bool
    command: str


@dataclass
class Response:
    plc: str
    ack: bool
    message: str


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


# ------------------ Pydantic MODELS ------------------


class StrokeRequest(BaseModel):
    required_pressure: float
    current_pressure: float
    current_stroke1: float
    current_stroke2: float


class PlcItem(BaseModel):
    id: int
    key: str
    value: float
    plc: str


class AcknowledgedResponse(BaseModel):
    plc: str
    acknowledged: str
    acknowledged_timestamp: Optional[str]
    ignored_timestamp: Optional[str] = None
    timestamp: str


class SuggestionData(BaseModel):
    plcResponse: List[PlcItem]
    acknowledgedResponse: AcknowledgedResponse


# ------------------ GLOBALS ------------------

stroke_store: Dict[str, float] = {}
hopper_store: Dict[str, float] = {}
temperature_store: Dict[str, float] = {}
machine_store: Dict[str, float] = {}
laminate_store: Dict[str, float] = {}
leakage_suggestions: Dict[str, float] = {}

nats_client: Optional[nats.NATS] = None

PLC_TOPIC_MAP = {
    "17": "adv.217",
    "18": "adv.217",
    "19": "adv.160",
    "20": "adv.160",
    "21": "adv.150",
    "22": "adv.150",
    "25": "adv.154",
    "26": "adv.154",
    "27": "adv.153",
    "30": "adv.153",
}


# ------------------ HELPERS ------------------


async def get_nats_connection() -> nats.NATS:
    """Create or reuse NATS connection"""
    global nats_client
    if nats_client is None or not nats_client.is_connected:
        nats_client = await nats.connect(
            "nats://192.168.1.149:4222",
            connect_timeout=5,
            max_reconnect_attempts=3,
            reconnect_time_wait=1,
        )
    return nats_client


async def send_nats_request(
    plc: str, command: str, name: str, value: float, status: bool
) -> Dict[str, Any]:
    """Send request to NATS server"""
    try:
        nc = await get_nats_connection()
        topic = PLC_TOPIC_MAP.get(plc, f"plc.{plc}")

        payload = Payload(
            plc=plc, name=name, command=command, value=value, status=status
        )
        response = await nc.request(
            topic, json.dumps(asdict(payload)).encode(), timeout=5.0
        )

        payload_response = Response(**json.loads(response.data.decode()))
        return asdict(payload_response)

    except nats.errors.TimeoutError:
        raise HTTPException(status_code=504, detail="NATS request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NATS error: {str(e)}")


# ------------------ STARTUP/SHUTDOWN ------------------


@app.on_event("startup")
async def enhanced_startup():
    try:
        await get_nats_connection()
        if load_model():
            print("✅ Stroke model loaded successfully")
        else:
            print("⚠️ Warning: stroke model load failed")
        
        print("🚀 Server 2 started successfully")
        print(f"📡 WebSocket endpoints available:")
        print(f"   - /ws/{{user_id}} (for regular clients)")
        print(f"   - /ws/config_updates (for Server 1)")
        
    except Exception as e:
        print(f"❌ Startup error: {e}")


@app.on_event("shutdown")
async def on_shutdown():
    global nats_client
    if nats_client and nats_client.is_connected:
        await nats_client.close()


# ------------------ ENDPOINTS ------------------


@app.get("/test")
async def test_backend():
    return {"message": "Backend is connected. Test successful."}


@app.get("/latest/{machine_id}/{value_type}")
async def get_latest(machine_id: int, value_type: str):
    """
    Fetch latest strokes or temperature for a given machine.
    Example: /latest/17/strokes or /latest/20/temperature
    """
    try:
        if value_type not in ["strokes", "temperature"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid value_type. Use 'strokes' or 'temperature'.",
            )
        return fetch_latest_suggestion(f"mc{machine_id}", value_type)
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching {value_type} for mc{machine_id}: {e}",
        )


@app.post("/calculate-strokes")
async def calculate_strokes(data: StrokeRequest):
    try:
        new_s1, new_s2 = get_required_strokes(
            data.required_pressure, data.current_stroke1, data.current_stroke2
        )
        if new_s1 is None or new_s2 is None:
            calculator = get_stroke_calculator()
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Failed to calculate strokes",
                    "is_trained": calculator.is_trained,
                    "csv_exists": os.path.exists(calculator.csv_path),
                },
            )

        success = append_to_csv(new_s1, new_s2, data.required_pressure)
        if success:
            load_model()

        return {
            "stroke1": round(new_s1, 3),
            "stroke2": round(new_s2, 3),
            "success": True,
            "model_updated": success,
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/retrain-stroke-model")
async def retrain_stroke_model():
    if load_model():
        return {"message": "Model retrained successfully", "success": True}
    raise HTTPException(status_code=500, detail="Failed to retrain model")


@app.get("/stroke-model-status")
async def get_stroke_model_status():
    calculator = get_stroke_calculator()
    return {
        "is_trained": calculator.is_trained,
        "csv_exists": os.path.exists(calculator.csv_path),
        "csv_path": calculator.csv_path,
    }


# ------------------ WEBSOCKET ------------------


@app.websocket("/ws/config_updates")
async def config_updates_websocket(websocket: WebSocket):
    """WebSocket endpoint for config update clients (like Server 1)"""
    await manager.connect_config_updates(websocket)
    print("🌐 Config updates client connected")

    try:
        while True:
            # Listen for incoming messages from the client
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")

                if msg_type == "json_received_ack":
                    # Handle acknowledgments from Server 1
                    await manager.handle_acknowledgment(message)
                elif msg_type == "ping":
                    # Handle ping requests
                    await websocket.send_json(
                        {"type": "pong", "timestamp": datetime.now().isoformat()}
                    )
                else:
                    print(f"Unknown message type: {msg_type}")

            except json.JSONDecodeError as e:
                print(f"Invalid JSON received: {e}")
                await websocket.send_json(
                    {"type": "error", "message": "Invalid JSON format"}
                )

    except WebSocketDisconnect:
        print("🔌 Config updates client disconnected")
        manager.disconnect_config_updates(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect_config_updates(websocket)


# Add this HTTP endpoint to trigger config updates (ADD TO EXISTING main.py)
@app.post("/send-config-update/{machine_id}")
async def send_config_update(machine_id: str, config: dict):
    """
    Trigger a config update broadcast to all connected config update clients

    Usage:
    POST /send-config-update/17
    Body: {"setting1": "value1", "setting2": "value2"}
    """
    try:
        await manager.broadcast_config_update(machine_id, config)
        return {
            "status": "success",
            "message": f"Config update sent for machine {machine_id}",
            "machine_id": machine_id,
            "connected_clients": len(manager.config_update_connections),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to send config update: {str(e)}"
        )


# Add endpoint to check connected clients (ADD TO EXISTING main.py)
@app.get("/config-update-status")
async def config_update_status():
    """Check how many config update clients are connected"""
    return {
        "connected_clients": len(manager.config_update_connections),
        "total_connections": sum(
            len(conns) for conns in manager.active_connections.values()
        ),
    }


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await manager.connect(websocket, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")

                if msg_type == "ping":
                    await websocket.send_json(
                        {"type": "pong", "timestamp": datetime.now().isoformat()}
                    )
                elif msg_type == "json_received_ack":
                    await manager.handle_acknowledgment(message)
                elif msg_type == "subscribe_machine":
                    await websocket.send_json(
                        {
                            "type": "subscription_confirmed",
                            "machine_id": message.get("machine_id"),
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "Invalid JSON"})
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)


# ------------------ ENTRYPOINT ------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8010)
