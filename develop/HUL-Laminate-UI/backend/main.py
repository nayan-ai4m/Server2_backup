import asyncio
import json
import logging
import os
import traceback
from datetime import datetime
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Any

import uvicorn
import nats
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from websocket_handler import manager  # must expose `manager = ConnectionManager()`
from laminate_sealing import fetch_latest_suggestion
from stroke_calculator import (
    load_model,
    get_required_strokes,
    append_to_csv,
    get_stroke_calculator,
)
from config_handler import router as config_router
from gsm_history_handler import router as gsm_history_router
from zmq_publisher import publisher as zmq_publisher

# ------------------ LOGGING SETUP ------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------ FASTAPI SETUP ------------------
app = FastAPI(title="Server 2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(config_router)
app.include_router(gsm_history_router)

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

# class GSMPlcWriteRequest(BaseModel):
#     key: List[str]
#     value: List[float]
#     plc: int

class PlcItem(BaseModel):
    id: int
    key: str
    value: float
    plc: str

class GSMPlcWriteRequest(BaseModel):
    plcResponse: List[PlcItem]

# ------------------ GLOBALS ------------------
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
    global nats_client
    if nats_client is None or not nats_client.is_connected:
        nats_client = await nats.connect(
            "nats://192.168.1.149:4222",
            connect_timeout=5,
            max_reconnect_attempts=3,
            reconnect_time_wait=1,
        )
    return nats_client

async def send_nats_request(plc: str, command: str, name: str, value: float, status: bool) -> Dict[str, Any]:
    try:
        nc = await get_nats_connection()
        topic = PLC_TOPIC_MAP.get(plc, f"plc.{plc}")

        payload = Payload(plc=plc, name=name, command=command, value=value, status=status)
        response = await nc.request(topic, json.dumps(asdict(payload)).encode(), timeout=5.0)

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
            print("⚠️ Stroke model load failed")

        # Initialize ZMQ Publisher
        zmq_publisher.connect()

        # Start background heartbeat
        app.state.heartbeat_task = asyncio.create_task(manager.periodic_heartbeat(interval=30.0))

        print("🚀 Server 2 started successfully")
        print("📡 WebSocket endpoints:")
        print("   - /ws/{user_id}")
        print("   - /ws/config_updates")
        print("📡 ZMQ Publisher:")
        print("   - tcp://192.168.1.168:8014")
    except Exception as e:
        print(f"❌ Startup error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    hb = getattr(app.state, "heartbeat_task", None)
    if hb and not hb.done():
        hb.cancel()
        try:
            await hb
        except asyncio.CancelledError:
            pass

    # Disconnect ZMQ Publisher
    zmq_publisher.disconnect()

    global nats_client
    if nats_client and nats_client.is_connected:
        await nats_client.close()

# ------------------ HTTP ENDPOINTS ------------------
@app.get("/test")
async def test_backend():
    return {"message": "Server 2 is running successfully!"}

@app.get("/latest/{machine_id}/{value_type}")
async def get_latest(machine_id: int, value_type: str):
    try:
        if value_type not in ["strokes", "temperature"]:
            raise HTTPException(status_code=400, detail="Invalid value_type")
        return fetch_latest_suggestion(f"mc{machine_id}", value_type)
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching {value_type}: {e}")

@app.post("/calculate-strokes")
async def calculate_strokes(data: StrokeRequest):
    try:
        new_s1, new_s2 = get_required_strokes(
            data.required_pressure, data.current_stroke1, data.current_stroke2
        )
        if new_s1 is None or new_s2 is None:
            calculator = get_stroke_calculator()
            raise HTTPException(status_code=500, detail={"error": "Failed", "is_trained": calculator.is_trained})

        success = append_to_csv(new_s1, new_s2, data.required_pressure)
        if success:
            load_model()

        return {"stroke1": round(new_s1, 3), "stroke2": round(new_s2, 3), "success": True, "model_updated": success}
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
    return {"is_trained": calculator.is_trained, "csv_exists": os.path.exists(calculator.csv_path)}

@app.post("/send-config-update/{machine_id}")
async def send_config_update(machine_id: str, request: Request):
    try:
        data = await request.json()
        print(data)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    delivered = await manager.broadcast_config_update(machine_id, data)
    return {"status": "sent", "machine_id": machine_id, "delivered_to": delivered, "timestamp": datetime.utcnow().isoformat()}

@app.get("/config-update-status")
async def config_update_status():
    return {"connected_clients": len(manager.config_update_connections)}

# @app.post("/gsm_plc_write")
# async def gsm_plc_write(data: GSMPlcWriteRequest):
#     """
#     Endpoint to receive GSM PLC write requests and print the payload
#     """
#     try:
#         print("="*30)
#         print("GSM PLC WRITE REQUEST RECEIVED")
#         print("="*30)
#         print(f"PLC ID: {data.plc}")

#         # Map keys to values for readability
#         params = dict(zip(data.key, data.value))
#         print(f"HMI_Hor_Seal_Front_27: {params.get('HMI_Hor_Seal_Front_27', 'N/A')}°C")
#         print(f"HMI_Hor_Seal_Rear_28: {params.get('HMI_Hor_Seal_Rear_28', 'N/A')}°C")
#         print(f"HMI_Hor_Sealer_Strk_1: {params.get('HMI_Hor_Sealer_Strk_1', 'N/A')}")
#         print(f"HMI_Hor_Sealer_Strk_2: {params.get('HMI_Hor_Sealer_Strk_2', 'N/A')}")
#         print("="*30)

#         # Convert to dict for additional logging
#         payload_dict = {
#             "plc": data.plc,
#             "key": data.key,
#             "value": data.value,
#             "parameters": params
#         }
#         print(f"Full payload: {json.dumps(payload_dict, indent=2)}")
#         print("="*50)

#         return {
#             "status": "success",
#             "message": "GSM PLC write request received and printed",
#             "payload": payload_dict,
#             "timestamp": datetime.utcnow().isoformat()
#         }

#     except Exception as e:
#         print(f"Error processing GSM PLC write request: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.post("/gsm_plc_write", response_model=Dict[str, Any])
async def gsm_plc_write(data: GSMPlcWriteRequest):
    """Send PLC write commands via NATS for all parameters."""
    try:
        plc_response = data.plcResponse
        logger.info(f"Received PLC write request with {len(plc_response)} parameters")

        if not plc_response:
            raise HTTPException(status_code=400, detail="No PLC parameters provided")

        # Track all responses and failures
        successful_writes = []
        failed_writes = []

        for item in plc_response:
            try:
                nats_response = await send_nats_request(
                    plc=item.plc, command="update", name=item.key, value=item.value, status=True
                )
                logger.info(f"✅ NATS success for {item.key}: {nats_response}")
                successful_writes.append({
                    "key": item.key,
                    "value": item.value,
                    "response": nats_response
                })
            except HTTPException as http_err:
                # HTTPException from send_nats_request (timeout or NATS error)
                logger.error(f"❌ NATS failed for {item.key}: {http_err.detail}")
                failed_writes.append({
                    "key": item.key,
                    "value": item.value,
                    "error": http_err.detail
                })
            except Exception as e:
                # Unexpected error
                logger.error(f"❌ Unexpected error for {item.key}: {str(e)}", exc_info=True)
                failed_writes.append({
                    "key": item.key,
                    "value": item.value,
                    "error": str(e)
                })

        # If ANY write failed, return error
        if failed_writes:
            error_summary = f"{len(failed_writes)}/{len(plc_response)} parameter(s) failed to write"
            logger.error(f"PLC write completed with failures: {error_summary}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": error_summary,
                    "successful": successful_writes,
                    "failed": failed_writes
                }
            )

        # All writes successful
        logger.info(f"✅ All {len(successful_writes)} PLC parameters written successfully")
        return {
            "status": "success",
            "message": f"Successfully wrote {len(successful_writes)} parameter(s) to PLC",
            "successful_writes": successful_writes
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing PLC write request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing PLC write request: {str(e)}")

# ------------------ WEBSOCKET ENDPOINTS ------------------
# @app.websocket("/ws/{user_id}")
# async def websocket_endpoint(websocket: WebSocket, user_id: str):
#     await manager.connect(websocket, user_id)
#     try:
#         while True:
#             data = await websocket.receive_text()
#             try:
#                 message = json.loads(data)
#                 msg_type = message.get("type")
#                 if msg_type == "ping":
#                     await websocket.send_json({"type": "pong", "timestamp": datetime.utcnow().isoformat()})
#                 elif msg_type == "json_received_ack":
#                     await manager.handle_acknowledgment(message)
#                 elif msg_type == "subscribe_machine":
#                     await websocket.send_json({"type": "subscription_confirmed", "machine_id": message.get("machine_id"), "timestamp": datetime.utcnow().isoformat()})
#             except json.JSONDecodeError:
#                 await websocket.send_json({"type": "error", "message": "Invalid JSON"})
#     except WebSocketDisconnect:
#         await manager.disconnect(websocket, user_id)
#     except Exception as e:
#         await manager.disconnect(websocket, user_id)
#         print("WebSocket error:", e)

@app.websocket("/ws/config_update")
async def config_updates_websocket(websocket: WebSocket):
    await manager.connect_config_updates(websocket)
    print("🌐 Config updates client connected")

    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")

                if msg_type == "json_received_ack":
                    await manager.handle_acknowledgment(message)
                elif msg_type == "ping":
                    await websocket.send_json({"type": "pong", "timestamp": datetime.utcnow().isoformat()})
                else:
                    print(f"Unknown message type: {msg_type}")
            except json.JSONDecodeError as e:
                print(f"Invalid JSON received: {e}")
                await websocket.send_json({"type": "error", "message": "Invalid JSON format"})
    except WebSocketDisconnect:
        print("🔌 Config updates client disconnected")
        await manager.disconnect_config_updates(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await manager.disconnect_config_updates(websocket)

# ------------------ ENTRYPOINT ------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8014, loop="asyncio", ws="websockets", reload=False)

