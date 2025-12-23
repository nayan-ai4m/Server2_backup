from fastapi import FastAPI, HTTPException,Request,Query,WebSocket,WebSocketDisconnect
from websocket_handler import manager
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
import traceback
import uvicorn
from fastapi.responses import JSONResponse,StreamingResponse
from datetime import datetime,timedelta
#from typing import List,Dict
import os
import cv2
import asyncio
from dataclasses import asdict, dataclass
from pydantic import BaseModel
from loop_data import fetch_loop3_data, fetch_loop4_data 
from machine_data import fetch_machine_data
from graph_data import fetch_device_graph_data, fetch_last_5mins_data
from event_data import fetch_today_events,fetch_latest_event
from setpoints_data import fetch_machine_setpoints_data,fetch_recent_machine_setpoints
from graph_ver_hor import fetch_horizontal_sealer_graph,fetch_hopper_data,fetch_vertical_sealer_full_graph
from notification_handler import notification_queue, offline_notifications
from database import get_db, close_db
from database import get_device_db, close_db
from get_last_stopage import fetch_last_10_stoppages
from machine_up_time import fetch_latest_machine_data
import nats
from tpoints_events import fetch_tp_status_data, mark_tp_inactive
from typing import List,Dict,Optional,Any
from psycopg2.extras import RealDictCursor 
from get_history import fetch_stoppages_history, fetch_production_history
import re   
from fastapi.staticfiles import StaticFiles

 
app = FastAPI()
# app.mount("/images", StaticFiles(directory="screenshots"), name="images")

origins = ["*"]  
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@dataclass
class Payload:
    plc:str
    name:str
    value:float
    status:bool
    command:str


@dataclass
class Response:
    plc: str
    ack:bool
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
class HopperData(BaseModel):
    key: str
    value: float
    plc: str
class MachinecontrolRequest(BaseModel):
    action: str
    machineName: str

stroke_store: Dict[str, float] = {}
hopper_store: Dict[str, float] = {}
temperature_store: Dict[str, float] = {}


machine_state = {
    "status": None,
    "machineName": None
}
nats_client: Optional[nats.NATS] = None


# RTSP_URL = "rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mov" #Streaming Response 
# #"rtsp://admin:unilever2024@192.168.1.29:554/Streaming/Channels/102"

# def gen_frames():
#     """Yield frames from RTSP stream as MJPEG."""
#     # cap = cv2.VideoCapture(RTSP_URL)
#     cap = cv2.VideoCapture("rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mov", cv2.CAP_FFMPEG)

#     if not cap.isOpened():
#         print("Failed to open RTSP stream.")
#         return

#     while True:
#         try:
#             success, frame = cap.read()
#             if not success:
#                 print("Frame read failed, retrying...")
#                 continue

#             ret, buffer = cv2.imencode(".jpg", frame)
#             if not ret:
#                 continue

#             frame_bytes = buffer.tobytes()
#             yield (
#                 b"--frame\r\n"
#                 b"Content-Type: image/jpeg\r\n\r\n" +
#                 frame_bytes +
#                 b"\r\n"
#             )
#         except Exception as e:
#             print("Error streaming frame:", str(e))
#             break

#     cap.release()

# @app.get("/video")
# def video_feed():
#     """Returns the RTSP stream as MJPEG."""
#     return StreamingResponse(
#         gen_frames(),
#         media_type="multipart/x-mixed-replace; boundary=frame"
#     )
    

async def get_nats_connection():
    """Create or reuse NATS connection"""
    global nats_client
    try:
        if nats_client is None or not nats_client.is_connected:
            nats_client = await nats.connect(
                "nats://192.168.1.149:4222",
                connect_timeout=5,
                max_reconnect_attempts=3,
                reconnect_time_wait=1
            )
        # print("COnnection Established")
        return nats_client
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to NATS server: {str(e)}")

async def send_nats_request(plc: str, command: str, name: str, value: float, status: bool) -> Dict[str, Any]:
    """Send request to NATS server with retry logic"""
    try:
        nc = await get_nats_connection()
        print("Established")
        #topic = "plc.217" if plc in ["17", "18"] else f"plc.{plc}"
        if plc in ["17", "18"]:
            topic = "adv.217"
        elif plc in ["19", "20"]:
            topic = "adv.160"
        elif plc in ["21", "22"]:
            topic = "adv.150"
        elif plc in ["25","26"]:
            topic = "adv.154"
        elif plc in ["27", "30"]:
            topic = "adv.153"

        else:
            topic = f"plc.{plc}"
        print(f"Sending request to topic: {topic}") 
        payload = Payload(
            plc=plc,
            name=name,
            command=command,
            value=value,
            status=status
        )
        
        response = await nc.request(
            topic,
            json.dumps(asdict(payload)).encode(),
            timeout=5.0
        )
        print("Response Natts", response)
        
        try:
            payload_response = Response(**json.loads(response.data.decode()))
            print("Payload Response", payload_response)
            return asdict(payload_response)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"Invalid response format: {str(e)}")
            
    except nats.errors.TimeoutError:
        raise HTTPException(status_code=504, detail="NATS request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NATS communication error: {str(e)}")

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

#GET 

@app.post("/assign-task/")
async def assign_task(request: Request):
    try:
        data = await request.json()
        assignee = data.get('assignee')
        task_description = data.get('task_description')
        event_id = data.get('event_id')
        alert_type = data.get('alert_type')
        action_remarks = data.get("action_remark", "")

        if not all([assignee, task_description, event_id]):
            return JSONResponse(
                content={"error": "Missing required fields"},
                status_code=400
            )

        conn, cursor = get_db()
        if not conn or not cursor:
            return JSONResponse(
                content={"error": "Database connection failed"},
                status_code=500
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
                        event_id
                    )
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
                            "notification": notification
                        },
                        status_code=200
                    )
                else:
                    return JSONResponse(
                        content={"error": f"No event found with ID: {event_id}"},
                        status_code=404
                    )

        except Exception as e:
            conn.rollback()
            print(f"Database error: {e}")
            return JSONResponse(
                content={"error": "Database update failed", "details": str(e)},
                status_code=500
            )
        finally:
            conn.close()

    except Exception as e:
        print(f"Unexpected error: {e}")
        return JSONResponse(
            content={"error": "Unexpected server error", "details": str(e)},
            status_code=500
        )

@app.post("/close-task/")
async def close_task(request: Request):
    try:
        data = await request.json()
        print("data", data)
        
        event_id = data.get("event_id")
        close_time = data.get("closeTime")
        action_remarks = data.get("actionRemarks", "")  # Default to empty string if not provided
        
        # Validate required fields
        if close_time is None:
            return JSONResponse(content={"error": "Missing close time"}, status_code=400)
        if event_id is None:
            return JSONResponse(content={"error": "Missing event ID"}, status_code=400)
        
        # Debug log
        print(f"Processing: event_id={event_id}, close_time={close_time}, action_remarks={action_remarks}")
        
        # Get database connection
        conn, cursor = get_db()
        if not conn or not cursor:
            return JSONResponse(content={"error": "Database connection error"}, status_code=500)
        
        # Fix the time format issue
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
                    content={"error": "Invalid date/time format. Expected DD-MM-YYYY HH:MM or HH:MM"}, 
                    status_code=400
                )
            
            conn.commit()
            print("Database update successful")
            
            # Notify subscribers about task closure
            if hasattr(app, 'subscribers') and app.subscribers:
                for subscriber in app.subscribers:
                    if subscriber in app.subscribers:  # Double check the key exists
                        try:
                            await app.subscribers[subscriber].put({
                                "type": "closure",
                                "eventId": event_id,
                                "closeTime": close_time,
                                "actionRemarks": action_remarks
                            })
                        except Exception as notify_err:
                            print(f"Error notifying subscriber {subscriber}: {str(notify_err)}")
            
            return JSONResponse(content={"message": "Task closed successfully"})
        
        except Exception as db_err:
            print(f"Database error: {str(db_err)}")
            conn.rollback()
            return JSONResponse(content={"error": f"Database error: {str(db_err)}"}, status_code=500)
        finally:
            close_db(conn, cursor)
    
    except Exception as e:
        print(f"Unhandled error in close_task: {str(e)}")
        return JSONResponse(content={"error": f"Server error: {str(e)}"}, status_code=500)

@app.get("/notifications/{user_id}")
async def get_pending_notifications(user_id: str):
    # print("user id --->>>", user_id)
    if user_id in offline_notifications:
        notifications = offline_notifications[user_id]
        # print("notification", notifications)
        del offline_notifications[user_id]
        return JSONResponse(content={"notifications": notifications}, status_code=200)
    return JSONResponse(content={"notifications": []}, status_code=200)

@app.get("/loopoverview")
async def get_loop_overview():
    try:
        loop3_data = fetch_loop3_data()
        loop4_data = fetch_loop4_data()

        response = {
            "loop3": loop3_data,
            "loop4": loop4_data
        }
        # print("Response Loop Overview", response)
        return response
    except Exception as e:
        print(f"Error in /loopoverview endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching loop overview data")
        
@app.get("/machinetabledata")
async def get_machine_table_data():
    try:
        machine_data = fetch_machine_data()
        response = {
            "loop3": machine_data.get("loop3_overview", {}),
            "loop4": machine_data.get("loop4_overview", {})
        }
        # print("Response Machine Table Data", response)
        return response
    except Exception as e:
        print(f"Error in /machinetabledata endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching machine table data")

#fetch-device-data (IOT api data)
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
            raise HTTPException(status_code=404, detail="No data found for the given device.")

        final_response = [
            {
                "machine_id": item["label"],
                "x_rms_vel": item["x_rms_vel"],
                "y_rms_vel": item["y_rms_vel"],
                "z_rms_vel": item["z_rms_vel"],
                "spl_db": item["spl_db"],
                "temp_c": item["temp_c"]
            } for item in data
        ]

        return JSONResponse(content={"data": final_response})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fetch-last-5mins-data")
async def fetch_last_5mins_data_endpoint(request: Request, machine_name: str = None, title: str = None):
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
            raise HTTPException(status_code=404, detail="No data found for the given parameters.")

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
async def get_today_events(machine_name: str = Query(None, alias="machine"), 
    assigned_to: str = Query(None, alias="assigned")):
    
    try:
       #print("Machine name:",machine_name)
        events = fetch_today_events(machine_name, assigned_to)
        return {"events": events}
    except HTTPException as e:
        raise e  
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Unexpected error fetching today's events")
        
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
        raise HTTPException(status_code=500, detail="Unexpected error fetching the latest event")
        
@app.get("/machine_setpoints")
async def get_machine_setpoints(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_machine_setpoints_data(machine_id)
    return data

@app.get("/sealar_front_temperature")
async def get_sealar_front_temperature(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
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
            plc=data.plc,
            command="update",
            name=data.key,
            value=data.value,
            status=True
            )
            
        return {
            "status": "success",
            "message": "Temperature saved successfully",
            "data": {
            "temperature": temperature_store,
            "nats_response": nats_response
            }
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving temperature: {str(e)}")


@app.post("/api/control/start")
async def start_machine(request: MachinecontrolRequest):
    if machine_state["status"] == "Start":
        raise HTTPException(status_code=400, detail=f"Machine is already running.")

    machine_state["status"] = "Start"

    
    name_data = {"status": machine_state["status"]}

    nats_response = await send_nats_request(
        plc=request.machineName,  
        command="toggle",
        name=machine_state["status"],  
        value=0.0,
        status=True
    )
    
    return {
        "status": "success",
        "message": f"Machine {request.machineName} started successfully",
        "data": {
            "machine_state": machine_state,
            "nats_response": nats_response
        }
    }

@app.post("/api/control/stop")
async def stop_machine(request: MachinecontrolRequest):
    if machine_state["status"] == "Stop":
        raise HTTPException(status_code=400, detail=f"Machine is already stopped.")

    machine_state["status"] = "Stop"

    
    name_data = {"status": machine_state["status"]}

    nats_response = await send_nats_request(
        plc=request.machineName,
        command="toggle",
        name=machine_state["status"],  
        value=0.0,
        status=False
    )

    return {
        "status": "success",
        "message": f"Machine {request.machineName} stopped successfully",
        "data": {
            "machine_state": machine_state,
            "nats_response": nats_response
        }
    }

@app.post("/api/control/reset")
async def reset_machine(request: MachinecontrolRequest):
    machine_state["status"] = "Reset"

    name_data = {"status": machine_state["status"]}

 
    nats_response = await send_nats_request(
        plc=request.machineName,
        command="toggle",
        name=machine_state["status"],  
        value=0.0,
        status=False
    )

    return {
        "status": "success",
        "message": f"Machine {request.machineName} reset successfully",
        "data": {
            "machine_state": machine_state,
            "nats_response": nats_response
        }
    }
@app.post("/api/stroke")
async def save_strokeData(data: StrokeData):
    try:
        stroke_store[data.key] = data.value
        print(data) 
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc,
            command="update",
            name=data.key,
            value=data.value,
            status=True
        )
        
        return {
            "status": "success",
            "message": "Temperature saved successfully",
            "data": {
                "temperature": stroke_store,
                "nats_response": nats_response
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving temperature: {str(e)}")


@app.post("/api/hopper")
async def save_Hopper(data: HopperData):
    try:
        hopper_store[data.key] = data.value
        
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc,
            command="update",
            name=data.key,
            value=data.value,
            status=True
        )
        
        return {
            "status": "success",
            "message": "Temperature saved successfully",
            "data": {
                "temperature": hopper_store,
                "nats_response": nats_response
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving temperature: {str(e)}")

@app.get("/machine_up_time")
async def get_(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_latest_machine_data(machine_id)
    print("data",data)
    return JSONResponse(content=data)

@app.get("/stoppages")
async def get_stoppage_data(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_last_10_stoppages(machine_id)
    # print("data",data)
    return JSONResponse(content=data)
    
@app.put("/update_setpoint")
async def update_setpoint(machine_id: str, key: str, value: str):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    machine_column = machine_id.lower()  # Ensure the machine ID is lowercase for consistency
    # print("machine_column:",machine_column)
    if machine_column not in [f"mc{i}" for i in range(17, 31) if i not in (23, 24)]:
        raise HTTPException(status_code=400, detail="Invalid machine ID")

#History API's 

@app.get("/stoppages_history")
async def get_stoppage_history(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    start_date: str = Query(None, description="Filter by start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="Filter by end date in YYYY-MM-DD format"),
    shift: str = Query("all", description="Shift filter: shift_1, shift_2, shift_3, or all"),
    page_no: int = Query(1, description="Page number for pagination")
):
    data = fetch_stoppages_history(machine_id, shift, start_date, end_date, page_no)
    return JSONResponse(content=data)

@app.get("/production_history")
async def get_production_history(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    start_date: str = Query(None, description="Filter by start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="Filter by end date in YYYY-MM-DD format"),
    shift: str = Query("all", description="Shift filter: shift_1, shift_2, shift_3, or all"),
    page_no: int = Query(1, description="Page number for pagination")
):
    data = fetch_production_history(machine_id, shift, start_date, end_date, page_no)
    return JSONResponse(content=data)

MACHINE_COLUMNS = {
    (17, 18, 19, 20, 21, 25, 26): ["timestamp","hopper_1_level", "hopper_2_level"],
    (27, 30): ["timestamp","hopper_level_left", "hopper_level_right"],
    (28, 29): []  # No explicit hopper-related columns
}

def get_hopper_columns(machine_numbar: int):
    """Retrieve the correct hopper column names based on machine number."""
    for key, columns in MACHINE_COLUMNS.items():
        if machine_numbar in key:
            return columns
    return None
def extract_machine_number(table_name: str) -> int:
    match = re.search(r'\d+', table_name)
    return int(match.group()) if match else None

@app.get("/hopper-levels")
def get_hopper_data(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    last_timestamp: Optional[str] = None
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
    pattern=r"^(mc(17|18|19|20|21|22|25|26|27|28|29|30)|tpmc|highbay|press|ce|ckwr)$"
)
):
    data = fetch_tp_status_data(machine_id)
    return data
 
class MachineStatus(BaseModel):
    id: str
    color: Optional[str] = None
    active: Optional[str] = None
    status: Optional[str] = None
@app.get("/machine-status", response_model=List[MachineStatus])
def get_machine_status():
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT 
                machine_status::json->>'id' AS id,
                machine_status::json->>'color' AS color,
                machine_status::json->>'active' AS active,
                machine_status::json->>'status' AS status
            FROM overview_tp_status
        """)
        rows = cursor.fetchall()

        result = [
            {"id": row[0], "color": row[1], "active": row[2], "status": row[3]}
            for row in rows
        ]
        return result
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(conn, cursor)

   
@app.post("/clear-notification")
async def clear_notification(payload: dict):
    machine = payload["machine"]  # e.g., "MC 17"
    uuid = payload["uuid"]
    updated_time = payload.get("updated_time")

    # Mark the specific TP as inactive
    result = mark_tp_inactive(machine, uuid, updated_time)

    print("Cleared Notification:", payload)
    return {"status": "success", "cleared_id": payload["id"], "updated": result}

@app.get("/sealer-temp")
def get_sealer_temp_data(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    last_timestamp: Optional[str] = None
):
    table_name = machine_id

    # Column mapping based on machine_id
    if machine_id in ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22"]:
        db_columns = ["timestamp", "hor_sealer_rear_1_temp", "hor_sealer_front_1_temp"]
    elif machine_id in ["mc25", "mc26", "mc27","mc30"]:
        db_columns = ["timestamp", "hor_sealer_rear", "hor_sealer_front"]
    elif machine_id in ["mc28", "mc29"]:
        db_columns = ["timestamp", "horizontal_sealer_rear_1_temp", "horizontal_sealer_front_1_temp"]
    else:
        raise HTTPException(status_code=400, detail="Invalid machine_id")

    response_columns = ["timestamp", "hor_sealer_rear_1_temp", "hor_sealer_front_1_temp"]

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
    last_timestamp: Optional[str] = None
):
    try:
        conn, cursor = get_db()

        # Query the latest record or based on last_timestamp
         
        cursor.execute(
            "SELECT timestamp, " + machine_id + " FROM graph_status ORDER BY timestamp DESC LIMIT 1"
        )
        results = cursor.fetchall() 
        formatted_results = []
        for result in results:
            timestamp = result[0].strftime('%Y-%m-%dT%H:%M:%S%z')  # Format timestamp
            data = result[1]  # Data corresponding to the machine_id column
            formatted_results = data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return formatted_results


@app.get("/checkweigher-data")
def get_checkweigher_data(loop_no: int = Query(..., ge=3, le=4), last_timestamp: Optional[str] = None):
    try:
        conn, cursor = get_db()

        machine_id = f"loop{loop_no}_checkweigher"
         
        cursor.execute(
            "SELECT timestamp, " + machine_id + " FROM graph_status ORDER BY timestamp DESC LIMIT 1"
        )
        results = cursor.fetchall() 
        formatted_results = []
        for result in results:
            timestamp = result[0].strftime('%Y-%m-%dT%H:%M:%S%z')  # Format timestamp
            data = result[1]  # Data corresponding to the machine_id column
            formatted_results = data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return formatted_results

@app.get("/tank-data")
def get_tank_data(loop_no: int = Query(..., ge=3, le=4), last_timestamp: Optional[str] = None):
    try:
        conn, cursor = get_db()

        machine_id = f"loop{loop_no}_sku"
         
        cursor.execute(
            "SELECT timestamp, " + machine_id + " FROM graph_status ORDER BY timestamp DESC LIMIT 1"
        )
        results = cursor.fetchall() 
        formatted_results = []
        for result in results:
            timestamp = result[0].strftime('%Y-%m-%dT%H:%M:%S%z')  # Format timestamp
            data = result[1]  # Data corresponding to the machine_id column
            formatted_results = data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        close_db(conn, cursor)

    return formatted_results

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
