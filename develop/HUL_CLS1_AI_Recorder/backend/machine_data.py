from fastapi import FastAPI, HTTPException
import json
import asyncio
import logging
from database import get_db, close_db  # Your synchronous DB helpers

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_previous_shift(current_shift: str) -> str:
    """Determine previous shift in the cycle A -> B -> C -> A"""
    shift_cycle = {
        "A": "C",
        "B": "A",
        "C": "B"
    }
    return shift_cycle.get(current_shift, "C")

async def fetch_all_machines_with_previous():
    """
    Fetch all machines data for both current and previous shift.
    Returns data structured as current_shift and previous_shift.
    """
    result_data = {
        "loop3": {"current_shift": {"last_update": "", "machines": {}}, "previous_shift": {"last_update": "", "machines": {}}},
        "loop4": {"current_shift": {"last_update": "", "machines": {}}, "previous_shift": {"last_update": "", "machines": {}}}
    }

    for table_name in ["loop3_overview_new", "loop4_overview_new"]:
        loop_key = table_name.replace("_overview_new", "")

        def run_query():
            conn, cursor = get_db()
            if not conn or not cursor:
                raise HTTPException(status_code=500, detail="Database connection error")

            try:
                # Query for current shift (latest record)
                current_query = f"""
                SELECT last_update, machines, shift
                FROM {table_name}
                ORDER BY last_update DESC
                LIMIT 1
                """

                logger.info(f"Fetching current shift data from {table_name}")
                cursor.execute(current_query)
                current_result = cursor.fetchone()

                current_data = {"last_update": "", "machines": {}}
                previous_data = {"last_update": "", "machines": {}}

                if current_result:
                    last_update, machines_json, current_shift = current_result

                    # Parse machines JSON
                    machines_data = (
                        json.loads(machines_json) if isinstance(machines_json, str)
                        else machines_json
                    ) if machines_json else {}

                    current_data = {
                        "last_update": str(last_update) if last_update else "",
                        "machines": machines_data
                    }

                    # Determine previous shift
                    previous_shift_letter = get_previous_shift(current_shift)

                    # Query for previous shift
                    previous_query = f"""
                    SELECT last_update, machines
                    FROM {table_name}
                    WHERE shift = %s
                    ORDER BY last_update DESC
                    LIMIT 1
                    """

                    logger.info(f"Fetching previous shift ({previous_shift_letter}) data from {table_name}")
                    cursor.execute(previous_query, (previous_shift_letter,))
                    previous_result = cursor.fetchone()

                    if previous_result:
                        prev_last_update, prev_machines_json = previous_result

                        # Parse previous machines JSON
                        prev_machines_data = (
                            json.loads(prev_machines_json) if isinstance(prev_machines_json, str)
                            else prev_machines_json
                        ) if prev_machines_json else {}

                        previous_data = {
                            "last_update": str(prev_last_update) if prev_last_update else "",
                            "machines": prev_machines_data
                        }

                return {
                    "current_shift": current_data,
                    "previous_shift": previous_data
                }

            except Exception as e:
                logger.error(f"Error fetching data from {table_name}: {e}", exc_info=True)
                raise HTTPException(
                    status_code=500,
                    detail=f"Error fetching data from {table_name}"
                )
            finally:
                close_db(conn, cursor)

        try:
            row = await asyncio.wait_for(
                asyncio.to_thread(run_query),
                timeout=10.0
            )
            result_data[loop_key] = row
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching data from {table_name}")
            raise HTTPException(status_code=504, detail=f"Timeout fetching data from {table_name}")
        except Exception as e:
            logger.error(f"Error in async task for {table_name}: {e}", exc_info=True)
            raise

    return result_data

async def fetch_machine_data(machine_id: str):
    query = """
    SELECT
        last_update,
        machines -> %s AS machine_data
    FROM {table_name}
    ORDER BY last_update DESC
    LIMIT 1
    """

    result_data = {
        "loop3": {"last_update": "", "machines": {}},
        "loop4": {"last_update": "", "machines": {}}
    }

    for table_name in ["loop3_overview_new", "loop4_overview_new"]:

        def run_query():
            conn, cursor = get_db()
            if not conn or not cursor:
                raise HTTPException(status_code=500, detail="Database connection error")

            try:
                logger.info(f"Executing query for {table_name} with machine_id {machine_id}")
                formatted_query = query.format(table_name=table_name)
                cursor.execute(formatted_query, (machine_id,))
                result = cursor.fetchone()

                if result:
                    last_update, machine_json = result
                    machine_data = (
                        json.loads(machine_json) if isinstance(machine_json, str)
                        else machine_json
                    ) if machine_json else {}

                    if machine_data:
                        machine_dict = {machine_id: machine_data}
                        return {
                            "last_update": str(last_update) if last_update else "",
                            "machines": machine_dict
                        }
                return {"last_update": "", "machines": {}}
            except Exception as e:
                logger.error(f"Error fetching data from {table_name} for machine {machine_id}: {e}", exc_info=True)
                raise HTTPException(
                    status_code=500,
                    detail=f"Error fetching data from {table_name} for machine {machine_id}"
                )
            finally:
                close_db(conn, cursor)

        try:
            row = await asyncio.wait_for(
                asyncio.to_thread(run_query),
                timeout=10.0
            )
            result_data[table_name.replace("_overview", "")] = row
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching data from {table_name} for machine {machine_id}")
            raise HTTPException(status_code=504, detail=f"Timeout fetching data from {table_name} for machine {machine_id}")
        except Exception as e:
            logger.error(f"Error in async task for {table_name} with machine {machine_id}: {e}", exc_info=True)
            raise

    return result_data