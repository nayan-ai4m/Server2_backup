from fastapi import FastAPI, HTTPException,Request,Query,WebSocket,WebSocketDisconnect
from websocket_handler import manager
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
import traceback
import uvicorn
from fastapi.responses import JSONResponse,StreamingResponse
from datetime import datetime,timedelta
from dataclasses import asdict, dataclass
import nats
from typing import List,Dict,Optional,Any
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from loop_data import fetch_loop3_data, fetch_loop4_data 
from machine_data import fetch_machine_data
from graph_data import fetch_device_graph_data,fetch_last_5mins_data
from event_data import fetch_today_events,fetch_latest_event
from setpoints_data import fetch_machine_setpoints_data,fetch_recent_machine_setpoints
from graph_ver_hor import fetch_horizontal_sealer_graph,fetch_hopper_data,fetch_vertical_sealer_full_graph
from notification_handler import notification_queue, offline_notifications
from database import get_db, close_db
from tpoints_events import fetch_tp_status_data, mark_tp_inactive,fetch_historical_work_instructions

from contextlib import asynccontextmanager 
import asyncio 
app = FastAPI()

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
    plc: str
    tag: str
    command: str
    value:float
    status:bool


@dataclass
class Response:
    plc: str
    command:str
    ack:bool

@dataclass
class Request:
    plc:str
    tag:str
    value:float
    status:bool
    command:str


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
# machine_state: Dict[str, str] = {"status": "HMI_I_Stop"}
machine_state = {
    "status": None,
    "machineName": None
}
nats_client: Optional[nats.NATS] = None

###

async def get_nats_connection():
    """Create or reuse NATS connection"""
    global nats_client
    try:
        if nats_client is None or not nats_client.is_connected:
            nats_client = await nats.connect(
                "nats://192.168.0.170:4222",
                connect_timeout=5,
                max_reconnect_attempts=3,
                reconnect_time_wait=1
            )
        return nats_client
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to NATS server: {str(e)}")

async def send_nats_request(plc: str, command: str, tag: str, value: float, status: bool) -> Dict[str, Any]:
    """Send request to NATS server with retry logic"""
    try:
        nc = await get_nats_connection()
        print("Established")
        
        payload = Request(
            plc=plc,
            tag=tag,
            command=command,
            value=value,
            status=status
        )
        
        response = await nc.request(
            "plc",
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

@app.get("/historical-work-instructions")
async def get_historical_work_instructions(
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    shift: Optional[str] = Query(None, pattern="^[ABC]$", description="Shift: A (7AM-3PM), B (3PM-11PM), C (11PM-7AM)"),
    loop: Optional[str] = Query(None, pattern="^(3|4|both)$", description="Loop: 3 (MC17-MC22), 4 (MC25-MC30), both or None (all machines)"),
    # machine_number: Optional[str] = Query(None, pattern="^(mc(17|18|19|20|21|22|25|26|27|28|29|30|)|all)$", description="Machine number (e.g., mc17, mc25)")
    machine_number: str = Query(..., pattern="^(mc(17|18|19|20|21|22|25|26|27|28|29|30)|all|mcckwr3|ckwr3|ckwr4|mcckwr4|mctp3|mctp4|tpmc3|tpmc4|mcce3|ce3|mcce4|ce4|mchighbay3|mchighbay4|highbay3|highbay4|mcpress3|press3|mcpress4|press4)$")):   
    print(start_time,end_time,loop,machine_number,shift)
    try:
        result = await fetch_historical_work_instructions(start_time, end_time, loop, machine_number, shift)
        
        return result
    
    except Exception as e:
        print(f"Error in get_historical_work_instructions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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

@app.post("/assign-task/")
async def assign_task(request: Request):
    try:
        data = await request.json()
        assignee = data.get('assignee')
        task_description = data.get('task_description')
        event_id = data.get('event_id')
        alert_type = data.get('alert_type')

        if not all([assignee, task_description, event_id]):
            return JSONResponse(
                content={"error": "Missing required fields"},
                status_code=400
            )


        conn,cursor = get_db()
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
                        task_description,
                        task_description,
                        alert_type,
                        unique_timestamp,
                        event_id,
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
                    "assignee": assignee
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
        
@app.get("/fetch-device-data")
async def fetch_device_data(request: Request, recent: bool = True):
    """
    Fetch the most recent record for a given device. 
    If no recent data is found, fetch the last 100 records.
    """
    try:
        # Fetch the most recent or last 100 records
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
async def get_today_events(machine_name: str = Query(None, alias="machine")):
    try:
        events = fetch_today_events(machine_name)
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
    
"""
@app.get("/horizontal_sealer_graph")
async def get_horizontal_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_horizontal_sealer_graph(machine_id)
    # print("hor",data)
    return data
    
@app.get("/verticle_sealer_graph")
async def get_verticle_sealer_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_vertical_sealer_full_graph(machine_id)
    # print("ver",data)
    return data
    
@app.get("/hopper_level_graph")
async def get_machine_setpoints(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_hopper_data(machine_id)
    # print("verticle_sealer_graph",data)
    return data
"""
    
@app.put("/update_setpoint")
async def update_setpoint(machine_id: str, key: str, value: str):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    machine_column = machine_id.lower()  
    # print("machine_column:",machine_column)
    if machine_column not in [f"mc{i}" for i in range(17, 31) if i not in (23, 24)]:
        raise HTTPException(status_code=400, detail="Invalid machine ID")

@app.post("/api/temperature")
async def save_temperature(data: TemperatureData):
    try:
        temperature_store[data.key] = data.value
        
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc,
            command="UPDATE",
            tag=data.key,
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
    if machine_state["status"] == "HMI_I_Start":
        raise HTTPException(status_code=400, detail=f"Machine is already running.")

    machine_state["status"] = "HMI_I_Start"

    
    tag_data = {"status": machine_state["status"]}

    nats_response = await send_nats_request(
        plc=request.machineName,  
        command="TOGGLE",
        tag=tag_data,  
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
    if machine_state["status"] == "HMI_I_Stop":
        raise HTTPException(status_code=400, detail=f"Machine is already stopped.")

    machine_state["status"] = "HMI_I_Stop"

    
    tag_data = {"status": machine_state["status"]}

    nats_response = await send_nats_request(
        plc=request.machineName,
        command="TOGGLE",
        tag=tag_data,  
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
    machine_state["status"] = "HMI_I_Reset"

    tag_data = {"status": machine_state["status"]}

 
    nats_response = await send_nats_request(
        plc=request.machineName,
        command="TOGGLE",
        tag=tag_data,  
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
        
        # Send NATS request
        nats_response = await send_nats_request(
            plc=data.plc,
            command="UPDATE",
            tag=data.key,
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
            command="UPDATE",
            tag=data.key,
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


# @app.post("/api/control/start")
# async def start_machine():
#     if machine_state["status"] == "HMI_I_Start":
#         raise HTTPException(status_code=400, detail="Machine is already running.")
    
#     machine_state["status"] = "HMI_I_Start"
    
#     # Send NATS request
#     nats_response = await send_nats_request(
#         plc="22",
#         command="control",
#         tag=machine_state,
#         value=0.0,  
#         status=True
#     )
    
#     return {
#         "status": "success",
#         "message": "Machine started successfully",
#         "data": {
#             "machine_state": machine_state,
#             "nats_response": nats_response
#         }
#     }

# @app.post("/api/control/stop")
# async def stop_machine():
#     if machine_state["status"] == "HMI_I_Stop":
#         raise HTTPException(status_code=400, detail="Machine is already stopped.")
    
#     machine_state["status"] = "HMI_I_Stop"
    
#     # Send NATS request
#     nats_response = await send_nats_request(
#         plc="22",
#         command="control",
#         tag=machine_state,
#         value=0.0,  
#         status=False
#     )
    
#     return {
#         "status": "success",
#         "message": "Machine stopped successfully",
#         "data": {
#             "machine_state": machine_state,
#             "nats_response": nats_response
#         }
#     }

# @app.post("/api/control/reset")
# async def reset_machine():
#     machine_state["status"] = "HMI_I_Reset"
    
#     # Send NATS request
#     nats_response = await send_nats_request(
#         plc="22",
#         command="control",
#         tag=machine_state,
#         value=0.0,  
#         status=False
#     )
    
#     return {
#         "status": "success",
#         "message": "Machine reset successfully",
#         "data": {
#             "machine_state": machine_state,
#             "nats_response": nats_response
#         }
#     }



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
