from fastapi import FastAPI, HTTPException, Request, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime
import json
import logging
import asyncio
import nats
import uvicorn
import re
import os
from database import get_db, get_server2_db, close_db, close_server2_db
from loop_data import fetch_loop3_data, fetch_loop4_data
from machine_data import fetch_machine_data
from setpoints_data import fetch_machine_setpoints_data
from get_last_stopage import fetch_last_10_stoppages
from machine_up_time import fetch_latest_machine_data
from get_history import fetch_stoppages_history, fetch_production_history


from fastapi.responses import JSONResponse 

app = FastAPI(title="Machine Management API")

# Restrict CORS for security
origins = ["*"]#os.getenv("CORS_ORIGINS", "http://localhost,http://your-frontend-domain.com").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class Payload(BaseModel):
    plc: str
    name: str
    value: float
    status: bool
    command: str

class Response(BaseModel):
    plc: str
    ack: bool
    message: str

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

class MachineControlRequest(BaseModel):
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

# Global Stores
stroke_store: Dict[str, float] = {}
hopper_store: Dict[str, float] = {}
temperature_store: Dict[str, float] = {}
machine_store: Dict[str, float] = {}
laminate_store: Dict[str, float] = {}
machine_state = {"status": None, "machineName": None}
nats_client: Optional[nats.NATS] = None

# NATS Connection
async def get_nats_connection():
    """Create or reuse NATS connection."""
    global nats_client
    try:
        if nats_client is None or not nats_client.is_connected:
            nats_client = await nats.connect(
                os.getenv("NATS_URL", "nats://192.168.1.149:4222"),
                connect_timeout=5,
                max_reconnect_attempts=3,
                reconnect_time_wait=1
            )
        return nats_client
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to NATS server: {str(e)}")

async def send_nats_request(plc: str, command: str, name: str, value: float, status: bool) -> Dict[str, Any]:
    """Send request to NATS server."""
    try:
        nc = await get_nats_connection()
        topic = (
            "adv.217" if plc in ["17", "18"] else
            "adv.160" if plc in ["19", "20"] else
            "adv.150" if plc in ["21", "22"] else
            "adv.154" if plc in ["25", "26"] else
            "adv.153" if plc in ["27", "30"] else
            f"plc.{plc}"
        )
        payload = Payload(plc=plc, name=name, command=command, value=value, status=status)
        response = await nc.request(topic, json.dumps(payload.dict()).encode(), timeout=5.0)
        return Response(**json.loads(response.data.decode())).dict()
    except nats.errors.TimeoutError:
        raise HTTPException(status_code=504, detail="NATS request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NATS communication error: {str(e)}")

# Lifecycle Events
@app.on_event("startup")
async def startup_event():
    try:
        await get_nats_connection()
    except Exception as e:
        logger.error(f"Failed to establish initial NATS connection: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    global nats_client
    if nats_client and nats_client.is_connected:
        await nats_client.close()

# Dependencies
def get_db_connection():
    conn, cursor = get_db()
    try:
        yield conn, cursor
    finally:
        close_db(conn, cursor)

def get_server2_db_connection():
    conn, cursor = get_server2_db()
    try:
        yield conn, cursor
    finally:
        try:
            close_server2_db(conn, cursor)
        except Exception as e:
            logger.error(f"Error closing server2_db connection: {str(e)}", exc_info=True)

# Reusable Query Function
def fetch_tp_status_data_from_db(table: str, machine_ids: Optional[List[str]] = None, conn: Any = None, cursor: Any = None) -> List[MachineStatus]:
    """Fetch TP status data from the specified table."""
    try:
        query = """
            SELECT 
                machine_status::json->>'id' AS id,
                machine_status::json->>'color' AS color,
                machine_status::json->>'active' AS active,
                machine_status::json->>'status' AS status,
                machine_status::json->>'active_tp' AS active_tp
            FROM {}
        """.format(table)
        if machine_ids:
            query += f" WHERE machine_status::json->>'id' IN ({','.join(['%s'] * len(machine_ids))})"
        cursor.execute(query, machine_ids or ())
        return [
            MachineStatus(
                id=row[0],
                color=row[1],
                active=row[2],
                status=row[3],
                active_tp=json.loads(row[4]) if row[4] else None
            ) for row in cursor.fetchall()
        ]
    except Exception as e:
        logger.error(f"Error fetching TP status from {table}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching TP status: {str(e)}")

# Endpoints
@app.get("/loopoverview", response_model=Dict[str, Any])
async def get_loop_overview():
    """Fetch overview data for loops 3 and 4."""
    try:
        loop3_data = fetch_loop3_data()
        loop4_data = fetch_loop4_data()
        return {"loop3": loop3_data, "loop4": loop4_data}
    except Exception as e:
        logger.error(f"Error in /loopoverview: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching loop overview data")

@app.get("/machinetabledata", response_model=Dict[str, Any])
async def get_machine_table_data():
    """Fetch machine table data for loops 3 and 4."""
    try:
        logger.info("Starting /machinetabledata endpoint")
        machine_ids = [f"mc{i}" for i in list(range(17, 23)) + list(range(25, 31))]
        logger.info(f"Processing machines: {machine_ids}")

        final_data = {
            "loop3": {"last_update": "", "machines": {}},
            "loop4": {"last_update": "", "machines": {}}
        }

        tasks = [fetch_machine_data(mid) for mid in machine_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_machines = 0
        failed_machines = 0
        for i, res in enumerate(results):
            machine_id = machine_ids[i]
            if isinstance(res, Exception):
                failed_machines += 1
                logger.error(f"Error fetching data for {machine_id}: {str(res)}", exc_info=True)
                continue
            successful_machines += 1
            for loop_key in ["loop3", "loop4"]:
                loop_data = res.get(loop_key, {})
                if loop_data and loop_data.get("machines"):
                    if loop_data["last_update"] and (
                        not final_data[loop_key]["last_update"] or
                        loop_data["last_update"] > final_data[loop_key]["last_update"]
                    ):
                        final_data[loop_key]["last_update"] = loop_data["last_update"]
                    final_data[loop_key]["machines"].update(loop_data["machines"])

        logger.info(f"Processing complete: {successful_machines} successful, {failed_machines} failed")
        return final_data
    except Exception as e:
        logger.error(f"Critical error in /machinetabledata: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching machine table data")




@app.get("/machine_setpoints", response_model=Dict[str, Any])
async def get_machine_setpoints(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    result = await asyncio.to_thread(fetch_machine_setpoints_data, machine_id)
    return result

@app.get("/machine_up_time", response_model=Dict[str, Any])
async def get_machine_up_time(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    """Fetch latest machine uptime data."""
    return fetch_latest_machine_data(machine_id)

@app.get("/stoppages")
async def get_stoppage_data(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_last_10_stoppages(machine_id)
    # print("data",data)
    return JSONResponse(content=data)



@app.get("/stoppages_history", response_model=Dict[str, Any])
async def get_stoppage_history(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    start_date: str = Query(None, description="Filter by start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="Filter by end date in YYYY-MM-DD format"),
    shift: str = Query("all", description="Shift filter: shift_1, shift_2, shift_3, or all"),
    page_no: int = Query(1, description="Page number for pagination")
):
    """Fetch stoppage history for a machine."""
    return fetch_stoppages_history(machine_id, shift, start_date, end_date, page_no)

@app.get("/production_history", response_model=Dict[str, Any])
async def get_production_history(
    machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$"),
    start_date: str = Query(None, description="Filter by start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="Filter by end date in YYYY-MM-DD format"),
    shift: str = Query("all", description="Shift filter: shift_1, shift_2, shift_3, or all"),
    page_no: int = Query(1, description="Page number for pagination")
):
    """Fetch production history for a machine."""
    return fetch_production_history(machine_id, shift, start_date, end_date, page_no)



@app.get("/machine-status", response_model=List[MachineStatus])
def get_machine_status_loop3(db: tuple = Depends(get_server2_db_connection)):
    """Fetch machine status for loop 3."""
    conn, cursor = db
    return fetch_tp_status_data_from_db("overview_tp_status_l3", conn=conn, cursor=cursor)

@app.get("/machine-status-loop4", response_model=List[MachineStatus])
def get_machine_status_loop4(db: tuple = Depends(get_server2_db_connection)):
    """Fetch machine status for loop 4."""
    conn, cursor = db
    return fetch_tp_status_data_from_db("overview_tp_status_loop4", conn=conn, cursor=cursor)

@app.get("/field-tp-color-status", response_model=List[MachineStatus])
def get_fieldtp_status(loop: str = Query("both", enum=["3", "4", "both"]), db: tuple = Depends(get_server2_db_connection)):
    """Fetch field TP status for specified loops."""
    conn, cursor = db
    loop3_ids = [f"MC {i}" for i in range(17, 23)] + ["Ck Wr 3", "CE 3", "Tp MC 3", "Press 3", "HighBay 3"]
    loop4_ids = [f"MC {i}" for i in range(25, 31)] + ["Ck Wr 4", "CE 4", "Tp MC 4", "Press 4", "HighBay 4"]
    result = []

    if loop in ("3", "both"):
        result.extend(fetch_tp_status_data_from_db("field_overview_tp_status_l3", loop3_ids, conn, cursor))
    if loop in ("4", "both"):
        result.extend(fetch_tp_status_data_from_db("field_overview_tp_status_l4", loop4_ids, conn, cursor))
    return result

@app.get("/cockpit-tp-color-status", response_model=List[MachineStatus])
def get_cockpit_overview(loop: str = Query("both", enum=["3", "4", "both"]), db: tuple = Depends(get_server2_db_connection)):
    """Fetch cockpit TP status for specified loops."""
    conn, cursor = db
    loop3_ids = [f"MC {i}" for i in range(17, 23)] + ["Ck Wr 3", "CE 3", "Tp MC 3", "Press 3", "HighBay 3"]
    loop4_ids = [f"MC {i}" for i in range(25, 31)] + ["Ck Wr 4", "CE 4", "Tp MC 4", "Press 4", "HighBay 4"]
    result = []

    if loop in ("3", "both"):
        result.extend(fetch_tp_status_data_from_db("cockpit_overview_tp_status_l3", loop3_ids, conn, cursor))
    if loop in ("4", "both"):
        result.extend(fetch_tp_status_data_from_db("cockpit_overview_tp_status_l4", loop4_ids, conn, cursor))
    return result




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

