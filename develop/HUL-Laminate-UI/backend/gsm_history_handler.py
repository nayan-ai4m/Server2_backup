# backend/gsm_history_handler.py
import json
import os
import re
import traceback
from datetime import datetime
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from database import get_db2, close_db2


router = APIRouter()


class GSMHistoryInsert(BaseModel):
    machine_id: str
    config_json: Dict[str, Any]
    front_jaw_temp: Optional[float]
    rear_jaw_temp: Optional[float]
    stroke1: Optional[float]
    stroke2: Optional[float]


class GSMHistoryStatusUpdate(BaseModel):
    status: str  # "applied" or "rejected"


def insert_gsm_history(
    machine_id: str,
    config_json: Dict[str, Any],
    front_jaw_temp: Optional[float],
    rear_jaw_temp: Optional[float],
    stroke1: Optional[float],
    stroke2: Optional[float],
) -> Optional[int]:
    """
    Insert a new GSM history record into the database.
    Returns the inserted record ID or None if failed.
    """
    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(
            status_code=500, detail="Database connection failed"
        )

    try:
        # Extract machine number from machine_id (e.g., "MC17" -> "17", or "17" -> "17")
        machine_number = "".join(re.findall(r"\d+", str(machine_id)))

        insert_query = """
            INSERT INTO gsm_history
            (machine_id, config_json, front_jaw_temp, rear_jaw_temp, stroke1, stroke2, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        cursor.execute(
            insert_query,
            (
                machine_number,
                json.dumps(config_json),
                front_jaw_temp,
                rear_jaw_temp,
                stroke1,
                stroke2,
                "pending",  # Initial status
            ),
        )

        record_id = cursor.fetchone()[0]
        conn.commit()

        print(f"✅ GSM history record inserted: ID={record_id}, Machine={machine_number}")
        return record_id

    except Exception as e:
        conn.rollback()
        print(f"❌ Error inserting GSM history: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Failed to insert GSM history: {str(e)}"
        )
    finally:
        close_db2(conn, cursor)


def update_gsm_history_status(record_id: int, status: str) -> bool:
    """
    Update the status of a GSM history record.
    Status should be "applied" or "rejected".
    Returns True if successful, False otherwise.
    """
    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(
            status_code=500, detail="Database connection failed"
        )

    try:
        if status not in ["applied", "rejected"]:
            raise ValueError(f"Invalid status: {status}. Must be 'applied' or 'rejected'")

        update_query = """
            UPDATE gsm_history
            SET status = %s
            WHERE id = %s
        """

        cursor.execute(update_query, (status, record_id))

        if cursor.rowcount == 0:
            raise HTTPException(
                status_code=404, detail=f"GSM history record {record_id} not found"
            )

        conn.commit()

        print(f"✅ GSM history record {record_id} updated to status: {status}")
        return True

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ Error updating GSM history status: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Failed to update GSM history: {str(e)}"
        )
    finally:
        close_db2(conn, cursor)


def get_machine_config_json(machine_id: str) -> Dict[str, Any]:
    """
    Load the entire machine config JSON file for the given machine ID.
    Returns the config dictionary.
    """
    machine_number = "".join(re.findall(r"\d+", str(machine_id)))
    config_path = f"config_mc/config_mc{machine_number}.json"

    if not os.path.exists(config_path):
        raise HTTPException(
            status_code=404,
            detail=f"Configuration file for machine {machine_id} not found at {config_path}",
        )

    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)
        return config_data
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error reading configuration file: {str(e)}",
        )


@router.post("/gsm_history/insert", response_model=Dict[str, Any])
async def create_gsm_history_record(data: GSMHistoryInsert):
    """
    Insert a new GSM history record when AI suggestions are displayed to the user.
    The config_json should be the entire machine configuration JSON.
    """
    try:
        record_id = insert_gsm_history(
            machine_id=data.machine_id,
            config_json=data.config_json,
            front_jaw_temp=data.front_jaw_temp,
            rear_jaw_temp=data.rear_jaw_temp,
            stroke1=data.stroke1,
            stroke2=data.stroke2,
        )

        return {
            "status": "success",
            "message": "GSM history record created",
            "record_id": record_id,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in create_gsm_history_record endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error creating GSM history record: {str(e)}"
        )


@router.put("/gsm_history/{record_id}/status", response_model=Dict[str, Any])
async def update_gsm_history_record_status(record_id: int, data: GSMHistoryStatusUpdate):
    """
    Update the status of a GSM history record.
    Status should be "applied" (when user clicks Apply) or "rejected" (when user clicks Reject).
    """
    try:
        update_gsm_history_status(record_id, data.status)

        return {
            "status": "success",
            "message": f"GSM history record {record_id} updated to status: {data.status}",
            "record_id": record_id,
            "new_status": data.status,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in update_gsm_history_record_status endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error updating GSM history status: {str(e)}"
        )


@router.get("/gsm_history/machine_config/{machine_id}")
async def get_machine_config_for_history(machine_id: str):
    """
    Helper endpoint to fetch the entire machine config JSON.
    Useful for the frontend to get the config when inserting GSM history.
    """
    try:
        config_data = get_machine_config_json(machine_id)
        return {
            "status": "success",
            "machine_id": machine_id,
            "config_json": config_data,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching machine config: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching machine config: {str(e)}"
        )
