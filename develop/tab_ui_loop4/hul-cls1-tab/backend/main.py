from fastapi import FastAPI, HTTPException, Request, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import traceback
import psycopg2
import asyncio
import uvicorn
import os
from tpoints_events import fetch_tp_status_data, mark_tp_inactive
from database import get_db, close_db  
import json

app = FastAPI()


origins = ["*"]  
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------- MODELS ----------------------------
class MachineStatus(BaseModel):
    id: str
    color: Optional[str] = None
    active: Optional[str] = None
    status: Optional[str] = None
    active_tp: Optional[Dict[str, Any]] = None


# ---------------------------- ROUTES ----------------------------


@app.get("/machine-status-loop4", response_model=List[MachineStatus])
def get_machine_status():
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT 
                machine_status::json->>'id' AS id,
                machine_status::json->>'color' AS color,
                machine_status::json->>'active' AS active,
                machine_status::json->>'status' AS status,
                machine_status::json->>'active_tp' AS active_tp
            FROM overview_tp_status_loop4
        """)
        rows = cursor.fetchall()

        result = [
            {"id": row[0], "color": row[1], "active": row[2], "status": row[3],
             "active_tp": json.loads(row[4]) if row[4] else None}
            for row in rows
        ]
        return result
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(conn, cursor)

#filed Dots 



@app.get("/field-tp-color-status", response_model=List[MachineStatus])
def get_fieldtp_status(loop: str = Query("both", enum=["3", "4", "both"])):
    try:
        conn, cursor = get_db()
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
                loop3_ids
            )
            loop3_data = cursor.fetchall()
            result += [
                {"id": row[0], "color": row[1], "active": row[2], "status": row[3],
                 "active_tp": json.loads(row[4]) if row[4] else None
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
                loop4_ids
            )
            loop4_data = cursor.fetchall()
            result += [
                {"id": row[0], "color": row[1], "active": row[2], "status": row[3],
                  "active_tp": json.loads(row[4]) if row[4] else None}
                for row in loop4_data
            ]
        # print(result)
        return result

    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(conn, cursor)


@app.post("/clear-notification")
async def clear_notification(payload: dict):
    # print("payload",payload)
    machine = payload["machine"]  
    uuid = payload["uuid"]
    updated_time = payload.get("updated_time")

    result = mark_tp_inactive(machine, uuid, updated_time)

    # print("Cleared Notification:", payload)
    return {"status": "success", "cleared_id": payload["uuid"], "updated": result}




# ---------------------------- ENTRY POINT ----------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8008)
