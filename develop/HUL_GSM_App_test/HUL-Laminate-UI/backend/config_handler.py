# backend/config_handler.py
import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import os
import re
from pydantic import StrictFloat

from websocket_handler import manager
from datetime import datetime
from zmq_publisher import publish_config_update, publish_machine_config_update


class CustomJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, dict):
            # Handle dictionary serialization with proper formatting
            items = []
            for key, value in obj.items():
                if isinstance(value, float):
                    # Format scientific notation properly
                    if abs(value) < 1e-4 or abs(value) >= 1e4:
                        formatted_value = f"{value:.2e}"
                    else:
                        formatted_value = f"{value:.6f}".rstrip("0").rstrip(".")
                    items.append(f'"{key}": {formatted_value}')
                else:
                    items.append(f'"{key}": {json.dumps(value)}')
            return "{\n  " + ",\n  ".join(items) + "\n}"
        return super().encode(obj)


router = APIRouter()


class ConfigUpdate(BaseModel):
    d1: StrictFloat
    d2: StrictFloat
    d3: StrictFloat
    SIT: StrictFloat


@router.get("/get_config/{machine_name}")
async def get_config(machine_name: str):
    machine_number = "".join(re.findall(r"\d+", machine_name))
    config_path = f"config_mc/config_mc{machine_number}.json"
    if not os.path.exists(config_path):
        raise HTTPException(
            status_code=404,
            detail=f"Configuration file for machine {machine_name} not found.",
        )
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)
        return config_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error reading configuration file: {str(e)}"
        )


@router.post("/update_config/{machine_name}")
async def update_config(machine_name: str, config_update: ConfigUpdate):
    machine_number = "".join(re.findall(r"\d+", machine_name))
    config_path = f"config_mc/config_mc{machine_number}.json"
    if not os.path.exists(config_path):
        raise HTTPException(
            status_code=404,
            detail=f"Configuration file for machine {machine_name} not found.",
        )
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)

        # Values are already converted from microns to meters in frontend
        # Mapping: d1=Layer3(PP), d2=Layer2(MET), d3=Layer1(BOPP)
        config_data["d1"] = config_update.d1  # PP layer thickness (already in meters)
        config_data["d2"] = config_update.d2  # MET layer thickness (already in meters)
        config_data["d3"] = config_update.d3  # BOPP layer thickness (already in meters)
        config_data["SIT"] = config_update.SIT
        #config_data["d1"] = config_update.d1
        #config_data["d2"] = config_update.d2
        #config_data["d3"] = config_update.d3
        #config_data["SIT"] = config_update.SIT
        



        with open(config_path, "w") as f:
            json.dump(config_data, f, indent=2, cls=CustomJSONEncoder)

        # Publish config update via ZMQ (replaces WebSocket)
        await publish_config_update(machine_number, config_data)

        # Also broadcast to WebSocket clients (for backward compatibility)
        await manager.broadcast_config_update(machine_number, config_data)

        return {"message": f"Configuration for {machine_name} updated successfully."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error updating configuration: {str(e)}"
        )


class MachineConfigUpdate(BaseModel):
    A: float
    T_eff: float
    t: float


@router.post("/update_machine_config/{machine_name}")
async def update_machine_config(machine_name: str, config_update: MachineConfigUpdate):
    machine_number = "".join(re.findall(r"\d+", machine_name))
    config_path = f"config_mc/config_mc{machine_number}.json"
    if not os.path.exists(config_path):
        raise HTTPException(
            status_code=404,
            detail=f"Configuration file for machine {machine_name} not found.",
        )
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)

        # Update the specified config values (Alpha values are read-only)
        config_data["A"] = config_update.A
        config_data["T_eff"] = config_update.T_eff
        config_data["t"] = config_update.t

        with open(config_path, "w") as f:
            json.dump(config_data, f, indent=2, cls=CustomJSONEncoder)

        # Publish machine config update via ZMQ (replaces WebSocket)
        await publish_machine_config_update(machine_number, config_data)

        # Also broadcast to WebSocket clients (for backward compatibility)
        await manager.broadcast_config_update(machine_number, config_data)

        return {
            "message": f"Machine configuration for {machine_name} updated successfully."
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error updating machine configuration: {str(e)}"
        )
