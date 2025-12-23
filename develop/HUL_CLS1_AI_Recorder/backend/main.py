from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from websocket_handler import manager
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
import traceback
import uvicorn
from fastapi.responses import JSONResponse, StreamingResponse
import io
from datetime import datetime, timedelta
import os
import cv2
import asyncio
import zmq
import zmq.asyncio

from dataclasses import asdict, dataclass
from pydantic import BaseModel
from loop_data import fetch_loop3_data, fetch_loop4_data

# from machine_data import fetch_machine_data
from machine_data import fetch_machine_data, fetch_all_machines_with_previous
from graph_data import fetch_device_graph_data, fetch_last_5mins_data
from event_data import fetch_today_events, fetch_latest_event
from setpoints_data import (
    fetch_machine_setpoints_data,
    fetch_recent_machine_setpoints,
    fetch_machine_setpoints_loop4,
)
from temperature_data_loop4 import fetch_machine_temperatures_loop4
from graph_ver_hor import (
    fetch_horizontal_sealer_graph,
    fetch_hopper_data,
    fetch_vertical_sealer_full_graph,
)
from notification_handler import notification_queue, offline_notifications
from database import get_db, close_db, get_server2_db, close_server2_db
from database import get_device_db, close_db
from get_last_stopage import fetch_last_10_stoppages
from machine_up_time import fetch_latest_machine_data
import nats
from tpoints_events import (
    fetch_tp_status_data,
    mark_tp_inactive,
    fetch_historical_work_instructions,
)
from typing import List, Dict, Optional, Any
from psycopg2.extras import RealDictCursor
from get_history import fetch_stoppages_history, fetch_production_history
from fetch_prediction_overview_data import fetch_prediction_overview_data
import re
from fastapi.staticfiles import StaticFiles
from gsm_values_insertion import insert_gsm_values, get_latest_gsm
from cockpit_suggetions import fetch_cockpit_suggetions, update_laminate_settings
from fastapi import Query
from typing import List
from fastapi.concurrency import run_in_threadpool
import logging
from auth import (
    authenticate_user,
    get_all_users,
    get_all_users_for_admin,
    update_user_password,
    update_user_operator_access,
    create_user,
    delete_user,
)

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ZMQ & interval config
POLL_INTERVAL_SECONDS = 5
IMMEDIATE_FETCH_TIMEOUT = (
    3  # seconds - only used when cache is empty and client requests
)
ZMQ_BIND_ADDR = "tcp://*:8011"


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


class TemperatureData(BaseModel):
    key: str
    value: float
    plc: str


class StrokeData(BaseModel):
    key: str
    value: float
    plc: str


class MachineDegreeData(BaseModel):
    key: str
    value: float
    plc: str


class HopperData(BaseModel):
    key: str
    value: float
    plc: str


class MachinecontrolRequest(BaseModel):
    action: str
    machineName: str


class LaminateData(BaseModel):
    key: str
    value: float
    plc: str


class MachineStatus(BaseModel):
    id: str
    color: Optional[str] = None
    active: Optional[str] = None
    status: Optional[str] = None
    active_tp: Optional[Dict[str, Any]] = None


class MachineStatusLoop4(BaseModel):
    id: str
    color: Optional[str] = None
    active: Optional[str] = None
    status: Optional[str] = None


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
    # role: Optional[str]


class SuggestionData(BaseModel):
    plcResponse: List[PlcItem]
    acknowledgedResponse: AcknowledgedResponse


class LoginRequest(BaseModel):
    employee_id: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    message: str
    user: Optional[Dict[str, Any]] = None


stroke_store: Dict[str, float] = {}
hopper_store: Dict[str, float] = {}
temperature_store: Dict[str, float] = {}
machine_store: Dict[str, float] = {}
laminate_store: Dict[str, float] = {}
leakage_suggestions: Dict[str, float] = {}

leakage_key = {
    "suggestion_lekage_key": [
        "HMI_Hor_Seal_Front_27",
        "HMI_Hor_Seal_Rear_28",
        "HMI_Hor_Sealer_Strk_1",
        "HMI_Hor_Sealer_Strk_2",
    ]
}

nats_client: Optional[nats.NATS] = None
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_nats_connection():
    """Create or reuse NATS connection"""
    global nats_client
    try:
        if nats_client is None or not nats_client.is_connected:
            nats_client = await nats.connect(
                "nats://192.168.1.149:4222",
                connect_timeout=5,
                max_reconnect_attempts=3,
                reconnect_time_wait=1,
            )
        # print("COnnection Established")
        return nats_client
    except Exception as e:
        raise HTTPException(
            status_code=503, detail=f"Failed to connect to NATS server: {str(e)}"
        )


async def send_nats_request(
    plc: str, command: str, name: str, value: float, status: bool
) -> Dict[str, Any]:
    """Send request to NATS server with retry logic"""
    try:
        nc = await get_nats_connection()
        print("Established")
        # topic = "plc.217" if plc in ["17", "18"] else f"plc.{plc}"
        if plc in ["17", "18"]:
            topic = "adv.217"
        elif plc in ["19", "20"]:
            topic = "adv.160"
        elif plc in ["21", "22"]:
            topic = "adv.150"
        elif plc in ["25", "26"]:
            topic = "adv.154"
        elif plc in ["27", "30"]:
            topic = "adv.153"

        else:
            topic = f"plc.{plc}"
        print(f"Sending request to topic: {topic}")
        payload = Payload(
            plc=plc, name=name, command=command, value=value, status=status
        )

        response = await nc.request(
            topic, json.dumps(asdict(payload)).encode(), timeout=5.0
        )
        print("Response Natts", response)

        try:
            payload_response = Response(**json.loads(response.data.decode()))
            print("Payload Response", payload_response)
            return asdict(payload_response)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=500, detail=f"Invalid response format: {str(e)}"
            )

    except nats.errors.TimeoutError:
        raise HTTPException(status_code=504, detail="NATS request timed out")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"NATS communication error: {str(e)}"
        )


@app.on_event("startup")
async def startup_event():
    """Initialize NATS connection on startup"""
    try:
        await get_nats_connection()
    except Exception as e:
        print(f"Failed to establish initial NATS connection: {str(e)}")


@app.on_event("shutdown")
async def shutdown_event():
    """Close NATS connection on shutdown"""
    global nats_client
    if nats_client and nats_client.is_connected:
        await nats_client.close()


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    try:
        await manager.connect(websocket, user_id)
        if user_id in offline_notifications:
            for notification in offline_notifications[user_id]:
                await websocket.send_json(notification)
            del offline_notifications[user_id]

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket, user_id)


# ============ AUTHENTICATION ENDPOINTS ============


@app.post("/api/login", response_model=LoginResponse)
async def login(login_request: LoginRequest):
    """
    Authenticate user with employee_id and password
    """
    try:
        user = authenticate_user(login_request.employee_id, login_request.password)

        if user:
            return LoginResponse(success=True, message="Login successful", user=user)
        else:
            return LoginResponse(
                success=False, message="Invalid employee ID or password", user=None
            )
    except Exception as e:
        logging.error(f"Login error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")


@app.get("/api/users")
async def get_users():
    """
    Get all active users for dropdown population
    Returns list of users with employee_id, name, and operator_access
    """
    try:
        users = get_all_users()
        # Format for react-select dropdown with operator_access
        formatted_users = [
            {
                "label": f"{user['name']} - {user['employee_id']}",
                "value": user["employee_id"],
                "operator_access": user.get("operator_access", ""),
            }
            for user in users
        ]
        return {"success": True, "users": formatted_users}
    except Exception as e:
        logging.error(f"Error fetching users: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch users: {str(e)}")


@app.get("/api/admin/users")
async def get_admin_users():
    """
    Get all users with full details for admin management
    """
    try:
        users = get_all_users_for_admin()
        return {"success": True, "users": users}
    except Exception as e:
        logging.error(f"Error fetching users for admin: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch users: {str(e)}")


class UpdatePasswordRequest(BaseModel):
    employee_id: str
    new_password: str


@app.post("/api/admin/update-password")
async def api_update_password(request: UpdatePasswordRequest):
    """
    Update a user's password
    """
    try:
        success = update_user_password(request.employee_id, request.new_password)
        if success:
            return {"success": True, "message": "Password updated successfully"}
        else:
            return {
                "success": False,
                "message": "Failed to update password. User not found.",
            }
    except Exception as e:
        logging.error(f"Error updating password: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to update password: {str(e)}"
        )


class UpdateOperatorAccessRequest(BaseModel):
    employee_id: str
    operator_access: str


@app.post("/api/admin/update-operator-access")
async def api_update_operator_access(request: UpdateOperatorAccessRequest):
    """
    Update a user's operator access
    """
    try:
        success = update_user_operator_access(
            request.employee_id, request.operator_access
        )
        if success:
            return {"success": True, "message": "Operator access updated successfully"}
        else:
            return {
                "success": False,
                "message": "Failed to update operator access. User not found.",
            }
    except Exception as e:
        logging.error(f"Error updating operator access: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to update operator access: {str(e)}"
        )


class CreateUserRequest(BaseModel):
    employee_id: str
    name: str
    password: str
    operator_access: str = ""


@app.post("/api/admin/create-user")
async def api_create_user(request: CreateUserRequest):
    """
    Create a new user
    """
    try:
        result = create_user(
            request.employee_id, request.name, request.password, request.operator_access
        )
        return result
    except Exception as e:
        logging.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create user: {str(e)}")


class DeleteUserRequest(BaseModel):
    employee_id: str


@app.post("/api/admin/delete-user")
async def api_delete_user(request: DeleteUserRequest):
    """
    Delete a user (soft delete)
    """
    try:
        result = delete_user(request.employee_id)
        return result
    except Exception as e:
        logging.error(f"Error deleting user: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete user: {str(e)}")


# GET


@app.post("/assign-task/")
async def assign_task(request: Request):
    try:
        data = await request.json()
        assignee = data.get("assignee")
        task_description = data.get("task_description")
        event_id = data.get("event_id")
        alert_type = data.get("alert_type")
        action_remarks = data.get("action_remark", "")

        if not all([assignee, task_description, event_id]):
            return JSONResponse(
                content={"error": "Missing required fields"}, status_code=400
            )

        conn, cursor = get_db()
        if not conn or not cursor:
            return JSONResponse(
                content={"error": "Database connection failed"}, status_code=500
            )

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                query = """
                UPDATE event_table
                SET
                    assigned_to = %s,
                    action = 'Assigned',
                    remark = %s,
                    event_type = %s,
                    alert_type = %s,
                    timestamp = %s
                WHERE event_id = %s
                RETURNING *
                """

                unique_timestamp = datetime.now()
                cursor.execute(
                    query,
                    (
                        assignee,
                        action_remarks,
                        task_description,
                        alert_type,
                        unique_timestamp,
                        event_id,
                    ),
                )

                updated_record = cursor.fetchone()
                conn.commit()

                notification = {
                    "type": "assignment",
                    "event_id": event_id,
                    "task_description": task_description,
                    "assigned_at": unique_timestamp.isoformat(),
                    "alert_type": alert_type,
                    "assignee": assignee,
                    "action_remarks": action_remarks,
                }

                if assignee in manager.active_connections:
                    await manager.send_personal_notification(assignee, notification)
                else:
                    if assignee not in offline_notifications:
                        offline_notifications[assignee] = []
                    offline_notifications[assignee].append(notification)

                if updated_record:
                    serializable_record = {}
                    for key, value in updated_record.items():
                        if isinstance(value, datetime):
                            serializable_record[key] = value.isoformat()
                        else:
                            serializable_record[key] = value

                    return JSONResponse(
                        content={
                            "message": "Task assigned successfully",
                            "assigned_record": serializable_record,
                            "notification": notification,
                        },
                        status_code=200,
                    )
                else:
                    return JSONResponse(
                        content={"error": f"No event found with ID: {event_id}"},
                        status_code=404,
                    )

        except Exception as e:
            conn.rollback()
            print(f"Database error: {e}")
            return JSONResponse(
                content={"error": "Database update failed", "details": str(e)},
                status_code=500,
            )
        finally:
            conn.close()

    except Exception as e:
        print(f"Unexpected error: {e}")
        return JSONResponse(
            content={"error": "Unexpected server error", "details": str(e)},
            status_code=500,
        )


@app.post("/close-task/")
async def close_task(request: Request):
    try:
        data = await request.json()
        print("data", data)

        event_id = data.get("event_id")
        close_time = data.get("closeTime")
        action_remarks = data.get("actionRemarks", "")
        if close_time is None:
            return JSONResponse(
                content={"error": "Missing close time"}, status_code=400
            )
        if event_id is None:
            return JSONResponse(content={"error": "Missing event ID"}, status_code=400)
        print(
            f"Processing: event_id={event_id}, close_time={close_time}, action_remarks={action_remarks}"
        )
        conn, cursor = get_db()
        if not conn or not cursor:
            return JSONResponse(
                content={"error": "Database connection error"}, status_code=500
            )
        try:

            if "-" in close_time and ":" in close_time:

                date_part, time_part = close_time.split()

                day, month, year = date_part.split("-")

                formatted_date = f"{year}-{month}-{day} {time_part}"

                query = """
                UPDATE event_table
                SET action = 'Closed', resolution_time = %s::timestamp, remark = %s
                WHERE event_id = %s
                """
                cursor.execute(query, (formatted_date, action_remarks, event_id))

            elif ":" in close_time and "-" not in close_time:
                query = """
                UPDATE event_table
                SET action = 'Closed', resolution_time = CURRENT_DATE + %s::time, remark = %s
                WHERE event_id = %s
                """
                cursor.execute(query, (close_time, action_remarks, event_id))
            else:

                return JSONResponse(
                    content={
                        "error": "Invalid date/time format. Expected DD-MM-YYYY HH:MM or HH:MM"
                    },
                    status_code=400,
                )

            conn.commit()
            print("Database update successful")

            # Notify subscribers about task closure
            if hasattr(app, "subscribers") and app.subscribers:
                for subscriber in app.subscribers:
                    if subscriber in app.subscribers:  # Double check the key exists
                        try:
                            await app.subscribers[subscriber].put(
                                {
                                    "type": "closure",
                                    "eventId": event_id,
                                    "closeTime": close_time,
                                    "actionRemarks": action_remarks,
                                }
                            )
                        except Exception as notify_err:
                            print(
                                f"Error notifying subscriber {subscriber}: {str(notify_err)}"
                            )

            return JSONResponse(content={"message": "Task closed successfully"})

        except Exception as db_err:
            print(f"Database error: {str(db_err)}")
            conn.rollback()
            return JSONResponse(
                content={"error": f"Database error: {str(db_err)}"}, status_code=500
            )
        finally:
            close_db(conn, cursor)

    except Exception as e:
        print(f"Unhandled error in close_task: {str(e)}")
        return JSONResponse(
            content={"error": f"Server error: {str(e)}"}, status_code=500
        )


@app.get("/notifications/{user_id}")
async def get_pending_notifications(user_id: str):

    if user_id in offline_notifications:
        notifications = offline_notifications[user_id]
        del offline_notifications[user_id]
        return JSONResponse(content={"notifications": notifications}, status_code=200)
    return JSONResponse(content={"notifications": []}, status_code=200)


@app.get("/loopoverview")
async def get_loop_overview():
    try:
        loop3_data = fetch_loop3_data()
        loop4_data = fetch_loop4_data()

        response = {"loop3": loop3_data, "loop4": loop4_data}
        return response
    except Exception as e:
        print(f"Error in /loopoverview endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching loop overview data")


"""
async def publisher_task(publisher_socket):

    print("[Publisher] started, poll interval:", POLL_INTERVAL_SECONDS)
    while True:
        try:
            data = await update_cache_from_db()
            if data:
                for loop_name, loop_info in data.items():
                    machines = loop_info.get("machines", {})
                    last_update = loop_info.get("last_update")

                    for machine_id, machine_data in machines.items():
                        payload = {
                            "loop": loop_name,
                            "machine_id": machine_id,
                            "last_update": last_update,
                            "published_at": datetime.now(timezone.utc).isoformat(),
                            "data": machine_data
                        }

                        # Send JSON per machine
                        await publisher_socket.send_json(payload)
                        print(f"[Publisher] published {loop_name}/{machine_id} at {payload['published_at']}")
            else:
                print("[Publisher] no data returned from DB query.")
        except Exception as e:
            print("[Publisher] error in publisher_task:", e)
            print(traceback.format_exc())
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

@app.on_event("startup")
async def startup_event():
    # print("[FastAPI] startup: creating ZMQ publisher socket")
    # ctx = zmq.asyncio.Context()
    # publisher = ctx.socket(zmq.PUB)
    # publisher.bind(ZMQ_BIND_ADDR)
    # app.state.zmq_ctx = ctx
    # app.state.publisher = publisher

    # asyncio.create_task(publisher_task(publisher))
    # print("[FastAPI] publisher task created")
    ctx = zmq.asyncio.Context()
    publisher_socket = ctx.socket(zmq.PUB)
    publisher_socket.bind(ZMQ_BIND_ADDR)
    print(f"[Publisher] ZMQ PUB socket bound to {ZMQ_BIND_ADDR}")

    await publisher_task(publisher_socket)

@app.on_event("shutdown")
async def shutdown_event():
    try:
        if hasattr(app.state, "publisher"):
            app.state.publisher.close()
        if hasattr(app.state, "zmq_ctx"):
            app.state.zmq_ctx.term()
    except Exception:
        pass 

"""


@app.get("/machinetabledata")
async def get_machine_table_data():
    try:
        # Fetch all machines data with both current and previous shift
        machine_data = await fetch_all_machines_with_previous()
        return machine_data
    except Exception as e:
        logger.error(f"Error in /machinetabledata endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching machine table data")


"""
@app.get("/machinetabledata")
async def get_machine_table_data():

    cached = await get_cached_data()
    if cached:
        ts = await get_cache_timestamp()
        return {
            "loop3": cached.get("loop3_overview", {}),
            "loop4": cached.get("loop4_overview", {}),
            "cached_at_utc": ts
        }

    
    try:
        data = await asyncio.wait_for(update_cache_from_db(), timeout=IMMEDIATE_FETCH_TIMEOUT)
        if data:
            ts = await get_cache_timestamp()
            return {
                "loop3": data.get("loop3_overview", {}),
                "loop4": data.get("loop4_overview", {}),
                "cached_at_utc": ts
            }
        else:
            raise HTTPException(status_code=504, detail="No data available")
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout fetching machine data; no cached data available")
    except Exception as e:
        print("[API] error in /machinetabledata:", e)
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching machine table data")
"""
# @app.get("/machinetabledata")
# async def get_machine_table_data():
#     try:
#         machine_data = await fetch_machine_data()
#         print(machine_data)
#         return {
#             "loop3": machine_data.get("loop3_overview", {}),
#             "loop4": machine_data.get("loop4_overview", {})
#         }
#     except Exception as e:
#         print(f"Error in /machinetabledata endpoint: {e}")
#         print(traceback.format_exc())
#         raise HTTPException(status_code=500, detail="Error fetching machine table data")


@app.get("/predictiontabledata")
async def get_prediction_table_data():
    try:
        prediction_data = fetch_prediction_overview_data()
        response = {"prediction_overview": prediction_data}
        return response
    except Exception as e:
        print(f"Error in /predictiontabledata endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail="Error fetching prediction table data"
        )


# fetch-device-data (IOT api data)
@app.get("/fetch-device-data")
async def fetch_device_data(request: Request, recent: bool = True):
    """Fetch the most recent record for a given device."""
    try:

        if recent:
            data = fetch_device_graph_data(limit=24)
            if not data:
                data = fetch_device_graph_data(limit=24)
        else:
            data = fetch_device_graph_data(limit=24)

        if not data:
            raise HTTPException(
                status_code=404, detail="No data found for the given device."
            )

        final_response = [
            {
                "machine_id": item["label"],
                "x_rms_vel": item["x_rms_vel"],
                "y_rms_vel": item["y_rms_vel"],
                "z_rms_vel": item["z_rms_vel"],
                "spl_db": item["spl_db"],
                "temp_c": item["temp_c"],
            }
            for item in data
        ]

        return JSONResponse(content={"data": final_response})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fetch-last-5mins-data")
async def fetch_last_5mins_data_endpoint(
    request: Request, machine_name: str = None, title: str = None
):
    """
    Fetch telemetry data for the last 5 minutes, optionally filtered by machine numbers and title.
    """
    try:
        machine_name_list = machine_name.split(",") if machine_name else None

        title_condition = ""
        if title:
            title_condition = f" AND label ILIKE '%{title}%'"

        data = fetch_last_5mins_data(machine_name_list, title_condition)

        if not data:
            raise HTTPException(
                status_code=404, detail="No data found for the given parameters."
            )

        final_response = [
            {
                "machine_id": item["label"],
                "timestamp": item["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                "x_rms_vel": item["x_rms_vel"],
                "y_rms_vel": item["y_rms_vel"],
                "z_rms_vel": item["z_rms_vel"],
            }
            for item in data
        ]

        return JSONResponse(content={"data": final_response})

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/get_today_events")
async def get_today_events(
    machine_name: str = Query(None, alias="machine"),
    assigned_to: str = Query(None, alias="assigned"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    limit: int = Query(10, ge=1, le=100, description="Number of records per page"),
    alert_type: str = Query(
        None, alias="alert_type", description="Filter by alert type"
    ),
    source: str = Query(None, alias="source", description="Filter by source"),
):
    """
    Fetch today's events with pagination support.

    Parameters:
    - machine: Filter by machine name (e.g., "mc17")
    - assigned: Filter by assigned user
    - page: Page number (default: 1)
    - limit: Records per page (default: 10, max: 100)
    - alert_type: Filter by alert type (Productivity, Breakdown, Quality)
    - source: Filter by source (Smart Camera, PLC, IOT Sensor, etc.)

    Returns:
    - events: List of event records
    - total_records: Total number of records matching the filters
    - page: Current page number
    - page_size: Records per page
    """
    try:
        result = fetch_today_events(
            machine_name=machine_name,
            assigned_to=assigned_to,
            page_no=page,
            page_size=limit,
            alert_type=alert_type,
            source=source,
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail="Unexpected error fetching today's events"
        )


@app.get("/get_latest_event")
async def get_latest_event():
    try:
        event = fetch_latest_event()
        return {"event": event}
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail="Unexpected error fetching the latest event"
        )


@app.get("/machine_setpoints")
async def get_machine_setpoints(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22)$")
):
    data = fetch_machine_setpoints_data(machine_id)
    return data


@app.get("/machine_setpoints_loop4")
async def get_machine_setpoints_loop4(
    machine_id: str = Query(..., pattern="^mc(25|26|27|30)$")
):
    data = fetch_machine_setpoints_loop4(machine_id)
    return data


@app.get("/machine-temps-loop4")
async def get_machine_temps_loop4(
    machine_id: str = Query(..., pattern="^mc(25|26|27|28|29|30)$")
):
    """
    Get calculated temperature averages for Loop 4 machines.
    - Vertical sealing temps: Average of 13 zones (front & rear)
    - Horizontal sealing temps: Single values (front & rear)
    """
    data = fetch_machine_temperatures_loop4(machine_id)
    return data


@app.get("/sealar_front_temperature")
async def get_sealar_front_temperature(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")
):
    data = fetch_recent_machine_setpoints(machine_id)
    # print("data",data)
    return JSONResponse(content=data)


@app.post("/api/temperature")
async def save_temperature(data: TemperatureData):
    try:
        temperature_store[data.key] = data.value
        print(temperature_store[data.key])

        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc, command="update", name=data.key, value=data.value, status=True
        )

        return {
            "status": "success",
            "message": "Temperature saved successfully",
            "data": {"temperature": temperature_store, "nats_response": nats_response},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error saving temperature: {str(e)}"
        )


@app.post("/api/control/start")
async def start_machine(request: MachinecontrolRequest):
    # Extract PLC number from machineName (e.g., "mc17" -> "17")
    plc_number = request.machineName.replace("mc", "")

    nats_response = await send_nats_request(
        plc=plc_number, command="TOGGLE", name="HMI_I_Start", value=0.0, status=True
    )

    return {
        "status": "success",
        "message": f"Machine {request.machineName} started successfully",
        "data": {"nats_response": nats_response},
    }


@app.post("/api/control/stop")
async def stop_machine(request: MachinecontrolRequest):
    # Extract PLC number from machineName (e.g., "mc17" -> "17")
    plc_number = request.machineName.replace("mc", "")

    nats_response = await send_nats_request(
        plc=plc_number, command="TOGGLE", name="HMI_I_Stop", value=0.0, status=False
    )

    return {
        "status": "success",
        "message": f"Machine {request.machineName} stopped successfully",
        "data": {"nats_response": nats_response},
    }


@app.post("/api/control/reset")
async def reset_machine(request: MachinecontrolRequest):
    # Extract PLC number from machineName (e.g., "mc17" -> "17")
    plc_number = request.machineName.replace("mc", "")

    nats_response = await send_nats_request(
        plc=plc_number, command="TOGGLE", name="HMI_I_Reset", value=0.0, status=False
    )

    return {
        "status": "success",
        "message": f"Machine {request.machineName} reset successfully",
        "data": {"nats_response": nats_response},
    }


@app.post("/api/stroke")
async def save_strokeData(data: StrokeData):
    try:
        stroke_store[data.key] = data.value
        print(data)
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc, command="update", name=data.key, value=data.value, status=True
        )

        return {
            "status": "success",
            "message": "Temperature saved successfully",
            "data": {"temperature": stroke_store, "nats_response": nats_response},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error saving temperature: {str(e)}"
        )


@app.post("/api/hopper")
async def save_Hopper(data: HopperData):
    try:
        hopper_store[data.key] = data.value

        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc, command="update", name=data.key, value=data.value, status=True
        )

        return {
            "status": "success",
            "message": "Temperature saved successfully",
            "data": {"temperature": hopper_store, "nats_response": nats_response},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error saving temperature: {str(e)}"
        )


# Machine -degree
@app.post("/api/machine-degree")
async def save_machineDegreeData(data: MachineDegreeData):
    try:
        machine_store[data.key] = data.value
        print(data)
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc, command="update", name=data.key, value=data.value, status=True
        )

        return {
            "status": "success",
            "message": "Machine Degree Updated Sucessfully",
            "data": {"machine_store": machine_store, "nats_response": nats_response},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error saving machine degree: {str(e)}"
        )


## Laminate settings
@app.post("/api/laminate-settings")
async def save_laminate_settings(data: LaminateData):
    try:
        laminate_store[data.key] = data.value
        print(data)
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc, command="update", name=data.key, value=data.value, status=True
        )

        return {
            "status": "success",
            "message": "Machine Degree Updated Sucessfully",
            "data": {"laminate_store": laminate_store, "nats_response": nats_response},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error saving machine degree: {str(e)}"
        )


# SUGESSTIONS


@app.post("/suggestions/leakage")
async def save_leakage_suggestion(data: SuggestionData):
    try:
        plc_response = data.plcResponse
        acknowledged_response = data.acknowledgedResponse

        print("Received plc_response:", plc_response)
        print("Received acknowledged_response:", acknowledged_response)

        for item in plc_response:
            try:
                nats_response = await send_nats_request(
                    plc=item.plc,
                    command="update",
                    name=item.key,
                    value=item.value,
                    status=True,
                )
                print(f"NATS response for {item.key}:", nats_response)
            except Exception as nats_err:
                print(traceback.format_exc())
                print(f"Error in NATS for {item.key}:", nats_err)

        update_result = update_laminate_settings(
            machine_number=acknowledged_response.plc,
            status=acknowledged_response.acknowledged,
            acknowledge_time=acknowledged_response.acknowledged_timestamp,
            ignored_time=None,
            timestamp=acknowledged_response.timestamp,
            role=None,
        )
        print("Update result:", update_result)
        return {
            "status": "success",
            "message": "NATS + DB operations successful",
            "update_result": update_result,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("Error:", e)
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error processing leakage suggestion: {str(e)}"
        )


@app.post("/ignored-suggestions")
async def save_ignored_suggestion(request: Request):
    try:
        data = await request.json()
        print("Received data:", data)

        # Validate required fields
        required_fields = ["plc", "acknowledged", "timestamp"]
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            raise HTTPException(
                status_code=400, detail=f"Missing required fields: {missing_fields}"
            )

        # Use ignored_time instead of ignored_timestamp
        update_result = update_laminate_settings(
            machine_number=data.get("plc"),
            status=data.get("acknowledged"),
            acknowledge_time=None,  # Ignored suggestions don't set acknowledge_time
            ignored_time=data.get("ignored_time"),  # Correct key
            timestamp=data.get("timestamp"),
        )
        print("Update result:", update_result)
        return {
            "status": "success",
            "message": "Database operation successful",
            "update_result": update_result,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("Error:", e)
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error processing ignored suggestion: {str(e)}"
        )


# Lamninate GSM
@app.post("/laminate-gsm")
async def save_laminate_gsm_value(request: Request):
    try:
        data = await request.json()
        print("Received data:", data)

        insert_gsm_values(data)

        return {"status": "success", "message": "Data inserted successfully"}

    except Exception as e:
        print("Error:", e)
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error inserting data: {str(e)}")


@app.get("/machine_up_time")
async def get_(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")
):
    data = fetch_latest_machine_data(machine_id)
    print("data", data)
    return JSONResponse(content=data)


@app.get("/stoppages")
async def get_stoppage_data(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")
):
    data = fetch_last_10_stoppages(machine_id)
    # print("data",data)
    return JSONResponse(content=data)


@app.put("/update_setpoint")
async def update_setpoint(machine_id: str, key: str, value: str):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    machine_column = (
        machine_id.lower()
    )  # Ensure the machine ID is lowercase for consistency
    # print("machine_column:",machine_column)
    if machine_column not in [f"mc{i}" for i in range(17, 31) if i not in (23, 24)]:
        raise HTTPException(status_code=400, detail="Invalid machine ID")


# History API's


@app.get("/stoppages_history")
async def get_stoppage_history(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    start_date: str = Query(
        None, description="Filter by start date in YYYY-MM-DD format"
    ),
    end_date: str = Query(None, description="Filter by end date in YYYY-MM-DD format"),
    shift: str = Query(
        "all", description="Shift filter: shift_1, shift_2, shift_3, or all"
    ),
    page_no: int = Query(1, description="Page number for pagination"),
):
    data = fetch_stoppages_history(machine_id, shift, start_date, end_date, page_no)
    return JSONResponse(content=data)


@app.get("/production_history")
async def get_production_history(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    start_date: str = Query(
        None, description="Filter by start date in YYYY-MM-DD format"
    ),
    end_date: str = Query(None, description="Filter by end date in YYYY-MM-DD format"),
    shift: str = Query(
        "all", description="Shift filter: shift_1, shift_2, shift_3, or all"
    ),
    page_no: int = Query(1, description="Page number for pagination"),
):
    data = fetch_production_history(machine_id, shift, start_date, end_date, page_no)
    return JSONResponse(content=data)


MACHINE_COLUMNS = {
    (17, 18, 19, 20, 21, 25, 26): ["timestamp", "hopper_1_level", "hopper_2_level"],
    (27, 30): ["timestamp", "hopper_level_left", "hopper_level_right"],
    (28, 29): [],  # No explicit hopper-related columns
}


def get_hopper_columns(machine_numbar: int):
    """Retrieve the correct hopper column names based on machine number."""
    for key, columns in MACHINE_COLUMNS.items():
        if machine_numbar in key:
            return columns
    return None


def extract_machine_number(table_name: str) -> int:
    match = re.search(r"\d+", table_name)
    return int(match.group()) if match else None


@app.get("/hopper-levels")
def get_hopper_data(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    last_timestamp: Optional[str] = None,
):
    table_name = f"{machine_id}"
    machine_numbar = extract_machine_number(machine_id)
    columns = get_hopper_columns(machine_numbar)

    if columns is None:
        raise HTTPException(status_code=400, detail="Invalid machine number")

    if not columns:
        return {"message": f"No hopper data available for machine {machine_id}"}

    try:
        conn, cursor = get_db()

        if last_timestamp:
            # Check if any new data exists after last_timestamp
            check_query = f"""
                SELECT {', '.join(columns)} 
                FROM {table_name} 
                WHERE timestamp > %s 
                ORDER BY timestamp DESC 
                LIMIT 1
            """
            cursor.execute(check_query, (last_timestamp,))
            result = cursor.fetchone()
            if result:
                data = [dict(zip(columns, result))]
            else:
                # No new records, return last 100
                query = f"""
                    SELECT {', '.join(columns)} 
                    FROM {table_name} 
                    ORDER BY timestamp DESC 
                    LIMIT 100
                """
                cursor.execute(query)
                result = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in result]
        else:
            # No last_timestamp given, return last 100
            query = f"""
                SELECT {', '.join(columns)} 
                FROM {table_name} 
                ORDER BY timestamp DESC 
                LIMIT 100
            """
            cursor.execute(query)
            result = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in result]

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        close_db(conn, cursor)

    return {"machine_id": machine_id, "data": data}


@app.get("/tp-points")
async def get_tp_status(
    machine_id: str = Query(
        ...,
        pattern=r"^(mc(17|18|19|20|21|22|25|26|27|28|29|30)|tpmc|highbay|press|ce|ckwr|tpmc4|highbay4|press4|ce4|ckwr4|tpmc3|highbay3|press3|ce3|ckwr3|)$",
    ),
    role: Optional[str] = Query(default=None),
):
    data = fetch_tp_status_data(machine_id, role)
    return data


@app.get("/machine-status", response_model=List[MachineStatus])
def get_machine_status():
    try:
        conn, cursor = get_server2_db()
        cursor.execute(
            """
            SELECT 
                machine_status::json->>'id' AS id,
                machine_status::json->>'color' AS color,
                machine_status::json->>'active' AS active,
                machine_status::json->>'status' AS status,
                machine_status::json->>'active_tp' AS active_tp
            FROM overview_tp_status_l3
        """
        )
        rows = cursor.fetchall()

        result = [
            {
                "id": row[0],
                "color": row[1],
                "active": row[2],
                "status": row[3],
                "active_tp": json.loads(row[4]) if row[4] else None,
            }
            for row in rows
        ]
        return result
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_server2_db(conn, cursor)


@app.get("/machine-status-loop4", response_model=List[MachineStatus])
def get_machine_status():
    try:
        conn, cursor = get_server2_db()
        cursor.execute(
            """
            SELECT 
                machine_status::json->>'id' AS id,
                machine_status::json->>'color' AS color,
                machine_status::json->>'active' AS active,
                machine_status::json->>'status' AS status,
                machine_status::json->>'active_tp' AS active_tp
            FROM overview_tp_status_loop4
        """
        )
        rows = cursor.fetchall()

        result = [
            {
                "id": row[0],
                "color": row[1],
                "active": row[2],
                "status": row[3],
                "active_tp": json.loads(row[4]) if row[4] else None,
            }
            for row in rows
        ]
        return result
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_server2_db(conn, cursor)


# filed Dots


@app.get("/field-tp-color-status", response_model=List[MachineStatus])
def get_fieldtp_status(loop: str = Query("both", enum=["3", "4", "both"])):
    try:
        conn, cursor = get_server2_db()
        loop3_mc_ids = [f"MC {i}" for i in range(17, 23)]
        loop3_special_keys = ["Ck Wr 3", "CE 3", "Tp MC 3", "Press 3", "HighBay 3"]
        loop3_ids = loop3_mc_ids + loop3_special_keys

        loop4_mc_ids = [f"MC {i}" for i in range(25, 31)]
        loop4_special_keys = ["Ck Wr 4", "CE 4", "Tp MC 4", "Press 4", "HighBay 4"]
        loop4_ids = loop4_mc_ids + loop4_special_keys

        result = []

        if loop in ("3", "both"):
            cursor.execute(
                f"""
                SELECT 
                    machine_status::json->>'id' AS id,
                    machine_status::json->>'color' AS color,
                    machine_status::json->>'active' AS active,
                    machine_status::json->>'status' AS status,
                    machine_status::json->>'active_tp' AS active_tp
                FROM field_overview_tp_status_l3
                WHERE machine_status::json->>'id' IN ({','.join(['%s'] * len(loop3_ids))})
                """,
                loop3_ids,
            )
            loop3_data = cursor.fetchall()
            result += [
                {
                    "id": row[0],
                    "color": row[1],
                    "active": row[2],
                    "status": row[3],
                    "active_tp": json.loads(row[4]) if row[4] else None,
                }
                for row in loop3_data
            ]

        if loop in ("4", "both"):
            cursor.execute(
                f"""
                SELECT 
                    machine_status::json->>'id' AS id,
                    machine_status::json->>'color' AS color,
                    machine_status::json->>'active' AS active,
                    machine_status::json->>'status' AS status,
                    machine_status::json->>'active_tp' AS active_tp
                FROM field_overview_tp_status_l4
                WHERE machine_status::json->>'id' IN ({','.join(['%s'] * len(loop4_ids))})
                """,
                loop4_ids,
            )
            loop4_data = cursor.fetchall()
            result += [
                {
                    "id": row[0],
                    "color": row[1],
                    "active": row[2],
                    "status": row[3],
                    "active_tp": json.loads(row[4]) if row[4] else None,
                }
                for row in loop4_data
            ]
        # print(result)
        return result

    except Exception as e:
        return {"error": str(e)}
    finally:
        close_server2_db(conn, cursor)


@app.get("/cockpit-tp-color-status", response_model=List[MachineStatus])
def get_cockpit_overview(loop: str = Query("both", enum=["3", "4", "both"])):
    try:
        conn, cursor = get_server2_db()
        loop3_mc_ids = [f"MC {i}" for i in range(17, 23)]
        loop3_special_keys = ["Ck Wr 3", "CE 3", "Tp MC 3", "Press 3", "HighBay 3"]
        loop3_ids = loop3_mc_ids + loop3_special_keys

        loop4_mc_ids = [f"MC {i}" for i in range(25, 31)]
        loop4_special_keys = ["Ck Wr 4", "CE 4", "Tp MC 4", "Press 4", "HighBay 4"]
        loop4_ids = loop4_mc_ids + loop4_special_keys

        result = []

        if loop in ("3", "both"):
            cursor.execute(
                f"""
                SELECT 
                    machine_status::json->>'id' AS id,
                    machine_status::json->>'color' AS color,
                    machine_status::json->>'active' AS active,
                    machine_status::json->>'status' AS status,
                    machine_status::json->>'active_tp' AS active_tp
                FROM cockpit_overview_tp_status_l3
                WHERE machine_status::json->>'id' IN ({','.join(['%s'] * len(loop3_ids))})
                """,
                loop3_ids,
            )
            loop3_data = cursor.fetchall()
            result += [
                {
                    "id": row[0],
                    "color": row[1],
                    "active": row[2],
                    "status": row[3],
                    "active_tp": json.loads(row[4]) if row[4] else None,
                }
                for row in loop3_data
            ]

        if loop in ("4", "both"):
            cursor.execute(
                f"""
                SELECT 
                    machine_status::json->>'id' AS id,
                    machine_status::json->>'color' AS color,
                    machine_status::json->>'active' AS active,
                    machine_status::json->>'status' AS status,
                    machine_status::json->>'active_tp' AS active_tp
                FROM cockpit_overview_tp_status_l4
                WHERE machine_status::json->>'id' IN ({','.join(['%s'] * len(loop4_ids))})
                """,
                loop4_ids,
            )
            loop4_data = cursor.fetchall()
            result += [
                {
                    "id": row[0],
                    "color": row[1],
                    "active": row[2],
                    "status": row[3],
                    "active_tp": json.loads(row[4]) if row[4] else None,
                }
                for row in loop4_data
            ]
        # print(result)
        return result

    except Exception as e:
        return {"error": str(e)}
    finally:
        close_server2_db(conn, cursor)


@app.post("/clear-notification")
async def clear_notification(payload: dict):
    # print("payload",payload)
    machine = payload["machine"]
    uuid = payload["uuid"]
    updated_time = payload.get("updated_time")

    result = mark_tp_inactive(machine, uuid, updated_time)

    # print("Cleared Notification:", payload)
    return {"status": "success", "cleared_id": payload["uuid"], "updated": result}


@app.get("/sealer-temp")
def get_sealer_temp_data(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    last_timestamp: Optional[str] = None,
):
    table_name = machine_id

    # Column mapping based on machine_id
    if machine_id in ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22"]:
        db_columns = ["timestamp", "hor_sealer_rear_1_temp", "hor_sealer_front_1_temp"]
    elif machine_id in ["mc25", "mc26", "mc27", "mc30"]:
        db_columns = ["timestamp", "hor_sealer_rear", "hor_sealer_front"]
    elif machine_id in ["mc28", "mc29"]:
        db_columns = [
            "timestamp",
            "horizontal_sealer_rear_1_temp",
            "horizontal_sealer_front_1_temp",
        ]
    else:
        raise HTTPException(status_code=400, detail="Invalid machine_id")

    response_columns = [
        "timestamp",
        "hor_sealer_rear_1_temp",
        "hor_sealer_front_1_temp",
    ]

    try:
        conn, cursor = get_db()

        if last_timestamp:
            query = f"""
                SELECT {', '.join(db_columns)}
                FROM {table_name}
                WHERE timestamp > %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            cursor.execute(query, (last_timestamp,))
            result = cursor.fetchone()
            if result:
                row = dict(zip(response_columns, result))
                data = [row]
            else:
                query = f"""
                    SELECT {', '.join(db_columns)}
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    LIMIT 100
                """
                cursor.execute(query)
                result = cursor.fetchall()
                data = [dict(zip(response_columns, row)) for row in result]
        else:
            query = f"""
                SELECT {', '.join(db_columns)}
                FROM {table_name}
                ORDER BY timestamp DESC
                LIMIT 100
            """
            cursor.execute(query)
            result = cursor.fetchall()
            data = [dict(zip(response_columns, row)) for row in result]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return {"machine_id": machine_id, "data": data}


@app.get("/graph-data")
def get_graph_data(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    last_timestamp: Optional[str] = None,
):
    try:
        conn, cursor = get_db()

        # Query the latest record or based on last_timestamp

        cursor.execute(
            "SELECT timestamp, "
            + machine_id
            + " FROM graph_status ORDER BY timestamp DESC LIMIT 1"
        )
        results = cursor.fetchall()
        formatted_results = []
        for result in results:
            timestamp = result[0].strftime("%Y-%m-%dT%H:%M:%S%z")  # Format timestamp
            data = result[1]  # Data corresponding to the machine_id column
            formatted_results = data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return formatted_results


@app.get("/graph-status-loop4")
def get_graph_status_loop4(
    machine_id: str = Query(..., pattern="^mc(25|26|27|28|29|30)$"),
    last_timestamp: Optional[str] = None,
):
    """
    Fetch hopper level and horizontal sealer data for Loop 4 machines (MC25-MC30).
    Returns data in the same format as /graph-data endpoint for compatibility with charts.
    Data from _mid table (5-second intervals) is aggregated into 10-second buckets with averaged values.

    Horizontal Sealer:
    - Upper Limit = Set Point + 5
    - Lower Limit = Set Point - 5
    """
    try:
        conn, cursor = get_db()

        # Table names
        table_name_fast = f"{machine_id}_fast"
        table_name_mid = f"{machine_id}_mid"

        # Extract machine number for conditional logic
        machine_num = int(machine_id.replace("mc", "").replace("MC", ""))

        # Fetch hopper data with 10-second bucket aggregation
        # MC25, MC26 = Dual hopper (hopper_left_level, hopper_right_level)
        # MC27, MC28, MC29, MC30 = Single hopper (hopper_level only)

        if machine_num in [25, 26]:
            # Dual-hopper machines query
            hopper_query = f"""
                WITH bucketed_fast AS (
                    SELECT
                        time_bucket('10 seconds', timestamp) AS bucket_timestamp,
                        AVG(hopper_left_level) AS avg_hopper_left,
                        AVG(hopper_right_level) AS avg_hopper_right
                    FROM {table_name_fast}
                    WHERE hopper_left_level IS NOT NULL
                       OR hopper_right_level IS NOT NULL
                    GROUP BY bucket_timestamp
                ),
                mid_with_buckets AS (
                    SELECT
                        time_bucket('10 seconds', timestamp) AS bucket_timestamp,
                        hopper_left_extreme_low_level,
                        hopper_left_low_level,
                        hopper_left_high_level,
                        hopper_right_extreme_low_level,
                        hopper_right_low_level,
                        hopper_right_high_level,
                        ROW_NUMBER() OVER (PARTITION BY time_bucket('10 seconds', timestamp) ORDER BY timestamp DESC) as rn
                    FROM {table_name_mid}
                )
                SELECT
                    bf.bucket_timestamp,
                    bf.avg_hopper_left,
                    bf.avg_hopper_right,
                    COALESCE(mb.hopper_left_extreme_low_level, 10.0) as left_exlow,
                    COALESCE(mb.hopper_left_low_level, 20.0) as left_low,
                    COALESCE(mb.hopper_left_high_level, 80.0) as left_high,
                    COALESCE(mb.hopper_right_extreme_low_level, 10.0) as right_exlow,
                    COALESCE(mb.hopper_right_low_level, 20.0) as right_low,
                    COALESCE(mb.hopper_right_high_level, 80.0) as right_high
                FROM bucketed_fast bf
                LEFT JOIN mid_with_buckets mb ON bf.bucket_timestamp = mb.bucket_timestamp AND mb.rn = 1
                ORDER BY bf.bucket_timestamp DESC
                LIMIT 100
            """
        else:
            # Single-hopper machines query (MC27, MC28, MC29, MC30)
            hopper_query = f"""
                WITH bucketed_fast AS (
                    SELECT
                        time_bucket('10 seconds', timestamp) AS bucket_timestamp,
                        AVG(hopper_level) AS avg_hopper_left,
                        NULL::double precision AS avg_hopper_right
                    FROM {table_name_fast}
                    WHERE hopper_level IS NOT NULL
                    GROUP BY bucket_timestamp
                ),
                mid_with_buckets AS (
                    SELECT
                        time_bucket('10 seconds', timestamp) AS bucket_timestamp,
                        hopper_extreme_low_level,
                        hopper_low_level,
                        hopper_high_level,
                        ROW_NUMBER() OVER (PARTITION BY time_bucket('10 seconds', timestamp) ORDER BY timestamp DESC) as rn
                    FROM {table_name_mid}
                )
                SELECT
                    bf.bucket_timestamp,
                    bf.avg_hopper_left,
                    bf.avg_hopper_right,
                    COALESCE(mb.hopper_extreme_low_level, 10.0) as left_exlow,
                    COALESCE(mb.hopper_low_level, 20.0) as left_low,
                    COALESCE(mb.hopper_high_level, 80.0) as left_high,
                    10.0 as right_exlow,
                    20.0 as right_low,
                    80.0 as right_high
                FROM bucketed_fast bf
                LEFT JOIN mid_with_buckets mb ON bf.bucket_timestamp = mb.bucket_timestamp AND mb.rn = 1
                ORDER BY bf.bucket_timestamp DESC
                LIMIT 100
            """

        cursor.execute(hopper_query)
        hopper_results = cursor.fetchall()

        # Fetch horizontal sealer temperature data with 10-second bucket aggregation from _mid table
        # Database has 5-second intervals, we aggregate to 10 seconds for chart display
        sealer_query = f"""
            SELECT
                time_bucket('10 seconds', timestamp) AS bucket_timestamp,
                AVG(hor_temp_27_pv_front) AS avg_front_temp_pv,
                AVG(hor_temp_27_sv_front) AS avg_front_temp_sv,
                AVG(hor_temp_28_pv_rear) AS avg_rear_temp_pv,
                AVG(hor_temp_28_sv_rear) AS avg_rear_temp_sv
            FROM {table_name_mid}
            WHERE hor_temp_27_pv_front IS NOT NULL
               OR hor_temp_27_sv_front IS NOT NULL
               OR hor_temp_28_pv_rear IS NOT NULL
               OR hor_temp_28_sv_rear IS NOT NULL
            GROUP BY bucket_timestamp
            ORDER BY bucket_timestamp DESC
            LIMIT 100
        """

        cursor.execute(sealer_query)
        sealer_results = cursor.fetchall()

        # Transform hopper data
        hopper_level_1 = []
        hopper_level_2 = []

        for row in hopper_results:
            bucket_timestamp = row[0]
            avg_hopper_left = row[1]
            avg_hopper_right = row[2]
            left_exlow = float(row[3])
            left_low = float(row[4])
            left_high = float(row[5])
            right_exlow = float(row[6])
            right_low = float(row[7])
            right_high = float(row[8])

            # Format for hopper 1 (left)
            if avg_hopper_left is not None:
                hopper_level_1.append(
                    {
                        "timestamp": bucket_timestamp.isoformat(),
                        "actualValue": float(avg_hopper_left),
                        "lowerLimit": left_low,
                        "upperLimit": left_high,
                        "exlow": left_exlow,
                    }
                )

            # Format for hopper 2 (right)
            if avg_hopper_right is not None:
                hopper_level_2.append(
                    {
                        "timestamp": bucket_timestamp.isoformat(),
                        "actualValue": float(avg_hopper_right),
                        "lowerLimit": right_low,
                        "upperLimit": right_high,
                        "exlow": right_exlow,
                    }
                )

        # Transform horizontal sealer data
        hor_sealer_front_1 = []
        hor_sealer_rear_1 = []

        for row in sealer_results:
            bucket_timestamp = row[0]
            avg_front_temp_pv = row[1]  # Process Value (actual)
            avg_front_temp_sv = row[2]  # Set Value (setpoint)
            avg_rear_temp_pv = row[3]  # Process Value (actual)
            avg_rear_temp_sv = row[4]  # Set Value (setpoint)

            # Format for front sealer
            if avg_front_temp_pv is not None and avg_front_temp_sv is not None:
                hor_sealer_front_1.append(
                    {
                        "timestamp": bucket_timestamp.isoformat(),
                        "actualValue": float(avg_front_temp_pv),
                        "setPoint": float(avg_front_temp_sv),
                        "upperLimit": float(avg_front_temp_sv) + 5,  # Set Point + 5
                        "lowerLimit": float(avg_front_temp_sv) - 5,  # Set Point - 5
                    }
                )

            # Format for rear sealer
            if avg_rear_temp_pv is not None and avg_rear_temp_sv is not None:
                hor_sealer_rear_1.append(
                    {
                        "timestamp": bucket_timestamp.isoformat(),
                        "actualValue": float(avg_rear_temp_pv),
                        "setPoint": float(avg_rear_temp_sv),
                        "upperLimit": float(avg_rear_temp_sv) + 5,  # Set Point + 5
                        "lowerLimit": float(avg_rear_temp_sv) - 5,  # Set Point - 5
                    }
                )

        # Reverse to get chronological order (oldest to newest)
        hopper_level_1.reverse()
        hopper_level_2.reverse()
        hor_sealer_front_1.reverse()
        hor_sealer_rear_1.reverse()

        formatted_results = {
            "hopper_level": hopper_level_1,
            "hopper_level_2": hopper_level_2,
            "hor_sealer_front_1": hor_sealer_front_1,
            "hor_sealer_rear_1": hor_sealer_rear_1,
        }

        return formatted_results

    except Exception as e:
        print(f"Error fetching Loop 4 graph data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)


@app.get("/checkweigher-data")
def get_checkweigher_data(
    loop_no: int = Query(..., ge=3, le=4), last_timestamp: Optional[str] = None
):
    try:
        conn, cursor = get_db()

        machine_id = f"loop{loop_no}_checkweigher"

        cursor.execute(
            "SELECT timestamp, "
            + machine_id
            + " FROM graph_status ORDER BY timestamp DESC LIMIT 1"
        )
        results = cursor.fetchall()
        formatted_results = []
        for result in results:
            timestamp = result[0].strftime("%Y-%m-%dT%H:%M:%S%z")  # Format timestamp
            data = result[1]  # Data corresponding to the machine_id column
            formatted_results = data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return formatted_results


@app.get("/tank-data")
def get_tank_data(
    loop_no: int = Query(..., ge=3, le=4), last_timestamp: Optional[str] = None
):
    try:
        conn, cursor = get_db()

        machine_id = f"loop{loop_no}_sku"

        cursor.execute(
            "SELECT timestamp, "
            + machine_id
            + " FROM graph_status ORDER BY timestamp DESC LIMIT 1"
        )
        results = cursor.fetchall()
        formatted_results = []
        for result in results:
            timestamp = result[0].strftime("%Y-%m-%dT%H:%M:%S%z")  # Format timestamp
            data = result[1]  # Data corresponding to the machine_id column
            formatted_results = data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return formatted_results


###


@app.get("/cockpit_suggetions")
async def get_cockpit_suggetions(
    machine_number: str = Query(
        ..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"
    ),
    role: Optional[str] = Query(default=None),
):
    try:
        settings_data = fetch_cockpit_suggetions(machine_number, role)
        return settings_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/latest-gsm")
async def get_latest_gsm_endpoint(
    machine_number: str = Query(
        ..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"
    )
):
    try:
        data = get_latest_gsm(machine_number)
        return data
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error fetching latest GSM data: {str(e)}"
        )


###


@app.get("/historical-work-instructions")
async def get_historical_work_instructions(
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    shift: Optional[str] = Query(
        None,
        pattern="^[ABC]$",
        description="Shift: A (7AM-3PM), B (3PM-11PM), C (11PM-7AM)",
    ),
    loop: Optional[str] = Query(
        None,
        pattern="^(3|4|both)$",
        description="Loop: 3 (MC17-MC22), 4 (MC25-MC30), both or None (all machines)",
    ),
    # machine_number: Optional[str] = Query(None, pattern="^(mc(17|18|19|20|21|22|25|26|27|28|29|30|)|all)$", description="Machine number (e.g., mc17, mc25)")
    machine_number: str = Query(
        ...,
        pattern="^(mc(17|18|19|20|21|22|25|26|27|28|29|30)|all|mcckwr3|ckwr3|ckwr4|mcckwr4|mctp3|mctp4|tpmc3|tpmc4|mcce3|ce3|mcce4|ce4|mchighbay3|mchighbay4|hig    hbay3|highbay4|mcpress3|press3|mcpress4|press4)$",
    ),
):
    print(start_time, end_time, loop, machine_number, shift)
    try:
        result = await fetch_historical_work_instructions(
            start_time, end_time, loop, machine_number, shift
        )

        return result

    except Exception as e:
        print(f"Error in get_historical_work_instructions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============ TP HISTORY ANALYTICS ENDPOINTS ============


# Load touchpoints data for TP descriptions
def load_touchpoints_data():
    """Load touchpoints data from JSON file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(script_dir, "touchpoints.json")
        with open(json_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading touchpoints.json: {e}")
        return []


TOUCHPOINTS_DATA = load_touchpoints_data()


@app.get("/api/admin/tp-descriptions")
async def get_tp_descriptions():
    """
    Get all TP code descriptions from touchpoints.json.
    Returns a dictionary mapping TP codes to their descriptions/titles.
    """
    try:
        descriptions = {}
        for item in TOUCHPOINTS_DATA:
            key = list(item.keys())[0]
            value = item[key]
            descriptions[key.lower()] = {
                "title": value.get("title", "Description not available"),
                "instruction": value.get("instruction", ""),
                "prediction_category": value.get("prediction_category", ""),
                "rolesVisibleTo": value.get("rolesVisibleTo", []),
            }
        return {"success": True, "descriptions": descriptions}
    except Exception as e:
        logging.error(f"Error getting TP descriptions: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get TP descriptions: {str(e)}"
        )


class TPHistoryRequest(BaseModel):
    machine: str
    date: str
    shift: str  # A, B, or C
    time_window: int = 0  # grouping window in minutes


@app.get("/api/admin/tp-history")
async def get_tp_history(
    machine: str = Query(..., description="Machine name (e.g., 'MC 17', 'Tp MC 3')"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    shift: str = Query(
        ...,
        pattern="^[ABC]$",
        description="Shift: A (7AM-3PM), B (3PM-11PM), C (11PM-7AM)",
    ),
):
    """
    Fetch TP history data for a specific machine, date, and shift.
    Returns raw event data for client-side processing.
    """
    try:
        # Calculate shift times
        date_obj = datetime.strptime(date, "%Y-%m-%d")

        if shift == "A":
            start_time = date_obj.replace(hour=7, minute=0, second=0)
            end_time = date_obj.replace(hour=15, minute=0, second=0)
        elif shift == "B":
            start_time = date_obj.replace(hour=15, minute=0, second=0)
            end_time = date_obj.replace(hour=23, minute=0, second=0)
        else:  # Shift C
            start_time = date_obj.replace(hour=23, minute=0, second=0)
            end_time = (date_obj + timedelta(days=1)).replace(
                hour=7, minute=0, second=0
            )

        # Connect to the tp_status_history database
        conn = psycopg2.connect(
            host="100.103.195.124", database="hul", user="postgres", password="ai4m2024"
        )
        cursor = conn.cursor()

        query = """
            SELECT event_timestamp, tp_column
            FROM tp_status_history
            WHERE machine = %s
            AND event_timestamp >= %s
            AND event_timestamp < %s
            ORDER BY event_timestamp;
        """

        cursor.execute(query, (machine, start_time, end_time))
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        # Format data for response
        data = [
            {"timestamp": row[0].isoformat() if row[0] else None, "tp_code": row[1]}
            for row in rows
            if row[1]  # Filter out null tp_codes
        ]

        return {
            "success": True,
            "machine": machine,
            "date": date,
            "shift": shift,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_records": len(data),
            "data": data,
        }

    except Exception as e:
        logging.error(f"Error fetching TP history: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch TP history: {str(e)}"
        )


@app.get("/api/admin/tp-history-comparison")
async def get_tp_history_comparison(
    machines: str = Query(..., description="Comma-separated machine names"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    shift: str = Query(..., pattern="^[ABC]$", description="Shift: A, B, or C"),
):
    """
    Fetch TP history data for multiple machines for comparison.
    """
    try:
        machine_list = [m.strip() for m in machines.split(",")]

        # Calculate shift times
        date_obj = datetime.strptime(date, "%Y-%m-%d")

        if shift == "A":
            start_time = date_obj.replace(hour=7, minute=0, second=0)
            end_time = date_obj.replace(hour=15, minute=0, second=0)
        elif shift == "B":
            start_time = date_obj.replace(hour=15, minute=0, second=0)
            end_time = date_obj.replace(hour=23, minute=0, second=0)
        else:  # Shift C
            start_time = date_obj.replace(hour=23, minute=0, second=0)
            end_time = (date_obj + timedelta(days=1)).replace(
                hour=7, minute=0, second=0
            )

        # Connect to database
        conn = psycopg2.connect(
            host="100.103.195.124", database="hul", user="postgres", password="ai4m2024"
        )
        cursor = conn.cursor()

        results = {}

        for machine in machine_list:
            query = """
                SELECT event_timestamp, tp_column
                FROM tp_status_history
                WHERE machine = %s
                AND event_timestamp >= %s
                AND event_timestamp < %s
                ORDER BY event_timestamp;
            """

            cursor.execute(query, (machine, start_time, end_time))
            rows = cursor.fetchall()

            results[machine] = [
                {"timestamp": row[0].isoformat() if row[0] else None, "tp_code": row[1]}
                for row in rows
                if row[1]
            ]

        cursor.close()
        conn.close()

        return {
            "success": True,
            "date": date,
            "shift": shift,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "machines": results,
        }

    except Exception as e:
        logging.error(f"Error fetching TP history comparison: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch TP history: {str(e)}"
        )


# ============ END TP HISTORY ANALYTICS ENDPOINTS ============


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
