"""
Authentication module for user login
Handles user authentication against the users table in the database
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from database import get_db, close_db
from typing import Optional, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def authenticate_user(employee_id: str, password: str) -> Optional[Dict]:
    """
    Authenticate a user by employee_id and password

    Args:
        employee_id: The employee ID to authenticate
        password: The password to verify

    Returns:
        Dict with user info if authentication successful, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return None

        # Query to fetch user by employee_id
        query = """
            SELECT id, employee_id, name, password, is_active
            FROM users
            WHERE employee_id = %s AND is_active = TRUE
        """

        cursor.execute(query, (employee_id,))
        user = cursor.fetchone()

        if not user:
            logger.warning(f"User not found: {employee_id}")
            return None

        # Convert tuple to dict (columns: id, employee_id, name, password, is_active)
        user_dict = {
            "id": user[0],
            "employee_id": user[1],
            "name": user[2],
            "password": user[3],
            "is_active": user[4]
        }

        # Verify password (plain text comparison)
        if user_dict['password'] != password:
            logger.warning(f"Invalid password for user: {employee_id}")
            return None

        # Authentication successful - return user info without password
        logger.info(f"User authenticated successfully: {employee_id}")
        return {
            "id": user_dict['id'],
            "employee_id": user_dict['employee_id'],
            "name": user_dict['name'],
            "is_active": user_dict['is_active']
        }

    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        return None
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def get_user_by_employee_id(employee_id: str) -> Optional[Dict]:
    """
    Get user information by employee_id (without password)

    Args:
        employee_id: The employee ID to lookup

    Returns:
        Dict with user info if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return None

        query = """
            SELECT id, employee_id, name, is_active
            FROM users
            WHERE employee_id = %s AND is_active = TRUE
        """

        cursor.execute(query, (employee_id,))
        user = cursor.fetchone()

        if user:
            return {
                "id": user[0],
                "employee_id": user[1],
                "name": user[2],
                "is_active": user[3]
            }
        return None

    except Exception as e:
        logger.error(f"Error fetching user: {str(e)}")
        return None
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def get_all_users() -> list:
    """
    Get all active users (for dropdown population)

    Returns:
        List of dicts with employee_id, name, and operator_access
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return []

        query = """
            SELECT employee_id, name, operator_access
            FROM users
            WHERE is_active = TRUE
            ORDER BY name
        """

        cursor.execute(query)
        users = cursor.fetchall()

        # Convert tuples to dicts
        user_list = [
            {"employee_id": user[0], "name": user[1], "operator_access": user[2] or ""}
            for user in users
        ]

        return user_list

    except Exception as e:
        logger.error(f"Error fetching all users: {str(e)}")
        return []
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def get_all_users_for_admin() -> list:
    """
    Get all users with full details for admin management

    Returns:
        List of dicts with id, employee_id, name, operator_access, is_active
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return []

        query = """
            SELECT id, employee_id, name, operator_access, is_active
            FROM users
            ORDER BY name
        """

        cursor.execute(query)
        users = cursor.fetchall()

        user_list = [
            {
                "id": user[0],
                "employee_id": user[1],
                "name": user[2],
                "operator_access": user[3] or "",
                "is_active": user[4]
            }
            for user in users
        ]

        return user_list

    except Exception as e:
        logger.error(f"Error fetching all users for admin: {str(e)}")
        return []
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def update_user_password(employee_id: str, new_password: str) -> bool:
    """
    Update a user's password

    Args:
        employee_id: The employee ID of the user
        new_password: The new password to set

    Returns:
        True if update successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return False

        query = """
            UPDATE users
            SET password = %s
            WHERE employee_id = %s
        """

        cursor.execute(query, (new_password, employee_id))
        conn.commit()

        if cursor.rowcount > 0:
            logger.info(f"Password updated successfully for user: {employee_id}")
            return True
        else:
            logger.warning(f"No user found with employee_id: {employee_id}")
            return False

    except Exception as e:
        logger.error(f"Error updating password: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def update_user_operator_access(employee_id: str, operator_access: str) -> bool:
    """
    Update a user's operator access

    Args:
        employee_id: The employee ID of the user
        operator_access: The new operator access string (comma-separated)

    Returns:
        True if update successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return False

        query = """
            UPDATE users
            SET operator_access = %s
            WHERE employee_id = %s
        """

        cursor.execute(query, (operator_access, employee_id))
        conn.commit()

        if cursor.rowcount > 0:
            logger.info(f"Operator access updated successfully for user: {employee_id}")
            return True
        else:
            logger.warning(f"No user found with employee_id: {employee_id}")
            return False

    except Exception as e:
        logger.error(f"Error updating operator access: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def create_user(employee_id: str, name: str, password: str, operator_access: str = "") -> Dict:
    """
    Create a new user in the database

    Args:
        employee_id: The employee ID for the new user
        name: The name of the user
        password: The password for the user
        operator_access: Optional operator access string (comma-separated)

    Returns:
        Dict with success status and message
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return {"success": False, "message": "Database connection failed"}

        # Check if employee_id already exists
        check_query = "SELECT id FROM users WHERE employee_id = %s"
        cursor.execute(check_query, (employee_id,))
        existing = cursor.fetchone()

        if existing:
            logger.warning(f"User with employee_id {employee_id} already exists")
            return {"success": False, "message": "Employee ID already exists"}

        # Insert new user
        insert_query = """
            INSERT INTO users (employee_id, name, password, operator_access, is_active)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING id
        """

        cursor.execute(insert_query, (employee_id, name, password, operator_access))
        new_id = cursor.fetchone()[0]
        conn.commit()

        logger.info(f"User created successfully: {employee_id} (ID: {new_id})")
        return {"success": True, "message": "User created successfully", "user_id": new_id}

    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        if conn:
            conn.rollback()
        return {"success": False, "message": f"Error creating user: {str(e)}"}
    finally:
        if conn and cursor:
            close_db(conn, cursor)


def delete_user(employee_id: str) -> Dict:
    """
    Permanently delete a user from the database

    Args:
        employee_id: The employee ID of the user to delete

    Returns:
        Dict with success status and message
    """
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        if not conn or not cursor:
            logger.error("Failed to connect to database")
            return {"success": False, "message": "Database connection failed"}

        # Hard delete - permanently remove the user from database
        query = """
            DELETE FROM users
            WHERE employee_id = %s
        """

        cursor.execute(query, (employee_id,))
        conn.commit()

        if cursor.rowcount > 0:
            logger.info(f"User permanently deleted: {employee_id}")
            return {"success": True, "message": "User permanently deleted"}
        else:
            logger.warning(f"No user found with employee_id: {employee_id}")
            return {"success": False, "message": "User not found"}

    except Exception as e:
        logger.error(f"Error deleting user: {str(e)}")
        if conn:
            conn.rollback()
        return {"success": False, "message": f"Error deleting user: {str(e)}"}
    finally:
        if conn and cursor:
            close_db(conn, cursor)
