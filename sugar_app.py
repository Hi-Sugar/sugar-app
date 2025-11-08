import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date

# Connect to SQLite database (stored locally)
# The database file will be created in the current working directory
conn = sqlite3.connect('sugar_app.db', check_same_thread=False)
c = conn.cursor()

def create_tables():
    """Create necessary tables if they do not exist"""
    c.execute('''CREATE TABLE IF NOT EXISTS asset_types (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    category TEXT
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_type_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    unit TEXT,
                    FOREIGN KEY(asset_type_id) REFERENCES asset_types(id)
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS departments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS rooms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    department_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    FOREIGN KEY(department_id) REFERENCES departments(id)
                )''')
    # asset_holdings tracks each asset assigned to a room with baseline quantity and additional info
    c.execute('''CREATE TABLE IF NOT EXISTS asset_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    room_id INTEGER NOT NULL,
                    serial_number TEXT,
                    baseline_qty INTEGER DEFAULT 0,
                    date_received TEXT,
                    received_by TEXT,
                    manager_in_charge TEXT,
                    origin TEXT,
                    equipment_status TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(room_id) REFERENCES rooms(id),
                    UNIQUE(asset_id, room_id, serial_number)
                )''')
    # transactions log inventory movements between rooms
    c.execute('''CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    from_room_id INTEGER,
                    to_room_id INTEGER,
                    txn_type TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    serial_number TEXT,
                    txn_date TEXT NOT NULL,
                    delivered_by TEXT,
                    received_by TEXT,
                    created_by TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(from_room_id) REFERENCES rooms(id),
                    FOREIGN KEY(to_room_id) REFERENCES rooms(id)
                )''')
    # transfer_requests for employee-initiated transfers requiring manager approval
    c.execute('''CREATE TABLE IF NOT EXISTS transfer_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    from_room_id INTEGER NOT NULL,
                    to_room_id INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    reason TEXT,
                    requested_by TEXT,
                    requested_on TEXT,
                    status TEXT DEFAULT 'Pending',
                    approved_by TEXT,
                    approved_on TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(from_room_id) REFERENCES rooms(id),
                    FOREIGN KEY(to_room_id) REFERENCES rooms(id)
                )''')
    # daily_counts store daily inventory counting results
    c.execute('''CREATE TABLE IF NOT EXISTS daily_counts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    room_id INTEGER NOT NULL,
                    count_date TEXT NOT NULL,
                    counted_by TEXT,
                    qty_counted INTEGER NOT NULL,
                    variance INTEGER,
                    note TEXT,
                    reviewed_by_manager INTEGER DEFAULT 0,
                    manager_reviewed_by TEXT,
                    manager_reviewed_on TEXT,
                    qty_given INTEGER,
                    qty_received INTEGER,
                    used_qty INTEGER,
                    withdraw_qty INTEGER,
                    equipment_status TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(room_id) REFERENCES rooms(id)
                )''')
    # alerts triggered when daily counts deviate from baseline
    c.execute('''CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    daily_count_id INTEGER NOT NULL,
                    severity TEXT,
                    acknowledged INTEGER DEFAULT 0,
                    acknowledged_by TEXT,
                    acknowledged_on TEXT,
                    FOREIGN KEY(daily_count_id) REFERENCES daily_counts(id)
                )''')

    # Users table for authentication and role-based access
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    role TEXT NOT NULL
                )''')

    # Holding requests table for employee declarations of new baseline quantities
    c.execute('''CREATE TABLE IF NOT EXISTS holding_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    room_id INTEGER NOT NULL,
                    serial_number TEXT,
                    baseline_qty INTEGER NOT NULL,
                    date_requested TEXT,
                    requested_by TEXT,
                    status TEXT DEFAULT 'Pending',
                    approved_by TEXT,
                    approved_on TEXT,
                    origin TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(room_id) REFERENCES rooms(id)
                )''')
    # Withdrawal requests table for employees requesting supplies (Văn phòng phẩm)
    c.execute('''CREATE TABLE IF NOT EXISTS withdrawal_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    room_id INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    requested_by TEXT,
                    requested_on TEXT,
                    status TEXT DEFAULT 'Pending',
                    approved_by TEXT,
                    approved_on TEXT,
                    note TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(room_id) REFERENCES rooms(id)
                )''')
    conn.commit()

# Call create_tables on import
def ensure_db_initialized():
    create_tables()
    # Perform schema migrations for existing databases
    # Add category column to asset_types if not exists
    try:
        c.execute("ALTER TABLE asset_types ADD COLUMN category TEXT")
    except Exception:
        pass
    # Add origin and equipment_status columns to asset_holdings if not exists
    for col_def in [
        "origin TEXT",
        "equipment_status TEXT"
    ]:
        col_name = col_def.split()[0]
        try:
            c.execute(f"ALTER TABLE asset_holdings ADD COLUMN {col_def}")
        except Exception:
            pass
    # Add origin column to holding_requests if not exists
    try:
        c.execute("ALTER TABLE holding_requests ADD COLUMN origin TEXT")
    except Exception:
        pass
    # Add additional columns to daily_counts if not exists
    for col_def in [
        "qty_given INTEGER",
        "qty_received INTEGER",
        "used_qty INTEGER",
        "withdraw_qty INTEGER",
        "equipment_status TEXT"
    ]:
        try:
            c.execute(f"ALTER TABLE daily_counts ADD COLUMN {col_def}")
        except Exception:
            pass
    # Ensure withdrawal_requests table exists
    c.execute('''CREATE TABLE IF NOT EXISTS withdrawal_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id INTEGER NOT NULL,
                    room_id INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    requested_by TEXT,
                    requested_on TEXT,
                    status TEXT DEFAULT 'Pending',
                    approved_by TEXT,
                    approved_on TEXT,
                    note TEXT,
                    FOREIGN KEY(asset_id) REFERENCES assets(id),
                    FOREIGN KEY(room_id) REFERENCES rooms(id)
                )''')
    conn.commit()

ensure_db_initialized()

# Ensure there is at least one default user. The nursing role (Phòng điều dưỡng) acts as a super manager.
def create_default_users():
    # Check if users table exists and if there are any users
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if not c.fetchone():
        # Users table hasn't been created yet
        return
    count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        # Populate initial users: a nursing super user, a manager, and a standard employee
        c.executemany(
            "INSERT INTO users (username, role) VALUES (?, ?)",
            [
                ('pd_admin', 'Phòng điều dưỡng'),
                ('manager', 'Quản lý'),
                ('employee', 'Nhân viên'),
            ]
        )
        conn.commit()

create_default_users()

# Utility functions for database operations

def add_asset_type(name: str, category: str) -> bool:
    """Insert a new asset type with a category. Returns True if success, False if duplicate."""
    try:
        c.execute(
            "INSERT INTO asset_types (name, category) VALUES (?, ?)",
            (name.strip(), category.strip() if category else None),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_asset_types() -> pd.DataFrame:
    return pd.read_sql_query("SELECT id, name, category FROM asset_types ORDER BY name", conn)

def add_asset(asset_type_id: int, name: str, unit: str) -> bool:
    try:
        c.execute("INSERT INTO assets (asset_type_id, name, unit) VALUES (?,?,?)", (asset_type_id, name.strip(), unit.strip()))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_assets() -> pd.DataFrame:
    query = """
    SELECT a.id, a.name, a.unit, at.id as type_id, at.name as type_name
    FROM assets a
    JOIN asset_types at ON a.asset_type_id = at.id
    ORDER BY at.name, a.name
    """
    return pd.read_sql_query(query, conn)

def add_department(name: str) -> bool:
    try:
        c.execute("INSERT INTO departments (name) VALUES (?)", (name.strip(),))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_departments() -> pd.DataFrame:
    return pd.read_sql_query("SELECT id, name FROM departments ORDER BY name", conn)

def add_room(department_id: int, name: str) -> bool:
    try:
        c.execute("INSERT INTO rooms (department_id, name) VALUES (?,?)", (department_id, name.strip()))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_rooms() -> pd.DataFrame:
    query = """
    SELECT r.id, r.name, d.id as department_id, d.name as department_name
    FROM rooms r
    JOIN departments d ON r.department_id = d.id
    ORDER BY d.name, r.name
    """
    return pd.read_sql_query(query, conn)

# Functions related to asset holdings

def add_or_update_holding(asset_id: int, room_id: int, serial_number: str, baseline_qty: int,
                          date_received: str, received_by: str, manager_in_charge: str,
                          origin: str = None, equipment_status: str = None):
    """Insert or update an asset holding record.
    If a holding with the same asset, room and serial exists, update baseline and details.
    Additional optional fields include origin (place of manufacture) and equipment_status.
    """
    # normalize inputs
    serial = serial_number.strip() if serial_number else None
    origin_val = origin.strip() if origin else None
    status_val = equipment_status.strip() if equipment_status else None
    # Determine if record exists
    if serial:
        row = c.execute(
            "SELECT id FROM asset_holdings WHERE asset_id=? AND room_id=? AND serial_number=?",
            (asset_id, room_id, serial),
        ).fetchone()
    else:
        row = c.execute(
            "SELECT id FROM asset_holdings WHERE asset_id=? AND room_id=? AND serial_number IS NULL",
            (asset_id, room_id),
        ).fetchone()
    if row:
        holding_id = row[0]
        # update record
        c.execute(
            "UPDATE asset_holdings SET baseline_qty=?, date_received=?, received_by=?, manager_in_charge=?, serial_number=?, origin=?, equipment_status=? WHERE id=?",
            (
                baseline_qty,
                date_received,
                received_by,
                manager_in_charge,
                serial,
                origin_val,
                status_val,
                holding_id,
            ),
        )
    else:
        # insert new record
        c.execute(
            "INSERT INTO asset_holdings (asset_id, room_id, serial_number, baseline_qty, date_received, received_by, manager_in_charge, origin, equipment_status) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                asset_id,
                room_id,
                serial,
                baseline_qty,
                date_received,
                received_by,
                manager_in_charge,
                origin_val,
                status_val,
            ),
        )
    conn.commit()


def update_baseline(holding_id: int, new_qty: int):
    c.execute("UPDATE asset_holdings SET baseline_qty=? WHERE id=?", (new_qty, holding_id))
    conn.commit()


def compute_qty_on_hand(asset_id: int, room_id: int) -> int:
    """Compute current quantity on hand in a room for an asset based on baseline and transactions"""
    baseline_row = c.execute("SELECT baseline_qty FROM asset_holdings WHERE asset_id=? AND room_id=?", (asset_id, room_id)).fetchone()
    baseline = baseline_row[0] if baseline_row else 0
    in_qty_row = c.execute("SELECT IFNULL(SUM(qty),0) FROM transactions WHERE asset_id=? AND to_room_id=? AND txn_type IN ('IN','TRANSFER')", (asset_id, room_id)).fetchone()
    out_qty_row = c.execute("SELECT IFNULL(SUM(qty),0) FROM transactions WHERE asset_id=? AND from_room_id=? AND txn_type IN ('OUT','TRANSFER')", (asset_id, room_id)).fetchone()
    in_qty = in_qty_row[0] if in_qty_row and in_qty_row[0] is not None else 0
    out_qty = out_qty_row[0] if out_qty_row and out_qty_row[0] is not None else 0
    return baseline + in_qty - out_qty


def get_last_count(asset_id: int, room_id: int):
    row = c.execute("SELECT qty_counted, variance FROM daily_counts WHERE asset_id=? AND room_id=? ORDER BY count_date DESC LIMIT 1", (asset_id, room_id)).fetchone()
    if row:
        return row[0], row[1]
    return None, None


def get_holdings_dataframe(filter_dept_id=None, filter_room_id=None, filter_type_id=None, filter_asset_id=None) -> pd.DataFrame:
    query = """
    SELECT h.id as holding_id, d.id as dept_id, d.name as department, r.id as room_id, r.name as room,
           at.id as type_id, at.name as asset_type, a.id as asset_id, a.name as asset_name, a.unit as unit,
           h.serial_number as serial_number, h.date_received, h.received_by, h.manager_in_charge,
           h.origin as origin, h.equipment_status as equipment_status, h.baseline_qty
    FROM asset_holdings h
    JOIN rooms r ON h.room_id = r.id
    JOIN departments d ON r.department_id = d.id
    JOIN assets a ON h.asset_id = a.id
    JOIN asset_types at ON a.asset_type_id = at.id
    WHERE 1=1
    """
    params = []
    if filter_dept_id:
        query += " AND d.id = ?"
        params.append(filter_dept_id)
    if filter_room_id:
        query += " AND r.id = ?"
        params.append(filter_room_id)
    if filter_type_id:
        query += " AND at.id = ?"
        params.append(filter_type_id)
    if filter_asset_id:
        query += " AND a.id = ?"
        params.append(filter_asset_id)
    df = pd.read_sql_query(query, conn, params=params)
    # compute qty_on_hand and last_count
    if not df.empty:
        qty_on_hand_list = []
        last_count_list = []
        variance_list = []
        for _, row in df.iterrows():
            qty_hand = compute_qty_on_hand(row['asset_id'], row['room_id'])
            qty_on_hand_list.append(qty_hand)
            last_count, variance = get_last_count(row['asset_id'], row['room_id'])
            last_count_list.append(last_count)
            variance_list.append(variance)
        df['qty_on_hand'] = qty_on_hand_list
        df['last_count'] = last_count_list
        df['variance'] = variance_list
    return df

# Functions related to transactions

def add_transaction(asset_id: int, from_room_id: int, to_room_id: int, txn_type: str, qty: int, serial_number: str, txn_date: str, delivered_by: str, received_by: str, created_by: str):
    c.execute("""INSERT INTO transactions (asset_id, from_room_id, to_room_id, txn_type, qty, serial_number, txn_date, delivered_by, received_by, created_by) VALUES (?,?,?,?,?,?,?,?,?,?)""",
              (asset_id, from_room_id, to_room_id, txn_type, qty, serial_number, txn_date, delivered_by, received_by, created_by))
    conn.commit()


def get_transactions_dataframe() -> pd.DataFrame:
    query = """
    SELECT t.id, t.txn_date, a.name as asset_name, at.name as asset_type, t.txn_type,
           fr.name as from_room, tr.name as to_room, t.qty, t.serial_number,
           t.delivered_by, t.received_by, t.created_by
    FROM transactions t
    JOIN assets a ON t.asset_id = a.id
    JOIN asset_types at ON a.asset_type_id = at.id
    LEFT JOIN rooms fr ON t.from_room_id = fr.id
    LEFT JOIN rooms tr ON t.to_room_id = tr.id
    ORDER BY t.txn_date DESC
    """
    return pd.read_sql_query(query, conn)

# Functions related to transfer requests

def add_transfer_request(asset_id: int, from_room_id: int, to_room_id: int, qty: int, reason: str, requested_by: str):
    now = datetime.now().isoformat()
    c.execute("INSERT INTO transfer_requests (asset_id, from_room_id, to_room_id, qty, reason, requested_by, requested_on) VALUES (?,?,?,?,?,?,?)",
              (asset_id, from_room_id, to_room_id, qty, reason, requested_by, now))
    conn.commit()

def get_transfer_requests_dataframe(include_all: bool = True, username: str = None) -> pd.DataFrame:
    # If include_all is False, filter to requests by user
    query = """
    SELECT tr.id, tr.status, tr.requested_on, tr.qty, tr.reason,
           a.name as asset_name, at.name as asset_type,
           fr.name as from_room, trr.name as to_room,
           tr.requested_by, tr.approved_by, tr.approved_on
    FROM transfer_requests tr
    JOIN assets a ON tr.asset_id = a.id
    JOIN asset_types at ON a.asset_type_id = at.id
    JOIN rooms fr ON tr.from_room_id = fr.id
    JOIN rooms trr ON tr.to_room_id = trr.id
    """
    params = []
    if not include_all and username:
        query += " WHERE tr.requested_by = ?"
        params.append(username)
    query += " ORDER BY tr.requested_on DESC"
    return pd.read_sql_query(query, conn, params=params)


def approve_transfer_request(request_id: int, manager_name: str):
    # Fetch details
    row = c.execute("SELECT asset_id, from_room_id, to_room_id, qty FROM transfer_requests WHERE id=? AND status='Pending'", (request_id,)).fetchone()
    if row:
        asset_id, from_room_id, to_room_id, qty = row
        now = datetime.now().isoformat()
        # Update status
        c.execute("UPDATE transfer_requests SET status='Approved', approved_by=?, approved_on=? WHERE id=?", (manager_name, now, request_id))
        # Create transactions for transfer (negative and positive)
        # Out from from_room
        add_transaction(asset_id, from_room_id, None, 'TRANSFER', qty, None, now, None, None, manager_name)
        # In to to_room
        add_transaction(asset_id, None, to_room_id, 'TRANSFER', qty, None, now, None, None, manager_name)
        conn.commit()
        return True
    return False

def reject_transfer_request(request_id: int, manager_name: str):
    now = datetime.now().isoformat()
    c.execute("UPDATE transfer_requests SET status='Rejected', approved_by=?, approved_on=? WHERE id=?", (manager_name, now, request_id))
    conn.commit()

# Functions related to withdrawal requests (phiếu lãnh văn phòng phẩm)
def add_withdrawal_request(asset_id: int, room_id: int, qty: int, requested_by: str, note: str = None):
    """Create a withdrawal request. Employees use this to request supplies such as stationery."""
    now = datetime.now().isoformat()
    c.execute(
        "INSERT INTO withdrawal_requests (asset_id, room_id, qty, requested_by, requested_on, note) VALUES (?,?,?,?,?,?)",
        (asset_id, room_id, qty, requested_by, now, note),
    )
    conn.commit()

def get_withdrawal_requests_dataframe(include_all: bool = True, username: str = None) -> pd.DataFrame:
    """Retrieve withdrawal requests. Optionally filter by requester."""
    query = (
        "SELECT wr.id, wr.status, wr.requested_on, wr.qty, wr.note, "
        "a.name as asset_name, at.name as asset_type, "
        "r.name as room_name, d.name as department_name, "
        "wr.requested_by, wr.approved_by, wr.approved_on "
        "FROM withdrawal_requests wr "
        "JOIN assets a ON wr.asset_id = a.id "
        "JOIN asset_types at ON a.asset_type_id = at.id "
        "JOIN rooms r ON wr.room_id = r.id "
        "JOIN departments d ON r.department_id = d.id"
    )
    params = []
    if not include_all and username:
        query += " WHERE wr.requested_by = ?"
        params.append(username)
    query += " ORDER BY wr.requested_on DESC"
    return pd.read_sql_query(query, conn, params=params)

def approve_withdrawal_request(request_id: int, manager_name: str) -> bool:
    """Approve a withdrawal request and add inventory to the requested room."""
    row = c.execute(
        "SELECT asset_id, room_id, qty FROM withdrawal_requests WHERE id=? AND status='Pending'",
        (request_id,),
    ).fetchone()
    if row:
        asset_id, room_id, qty = row
        now = datetime.now().isoformat()
        # Update request status
        c.execute(
            "UPDATE withdrawal_requests SET status='Approved', approved_by=?, approved_on=? WHERE id=?",
            (manager_name, now, request_id),
        )
        # Create transaction representing supply in (as an IN transaction to the room)
        add_transaction(asset_id, None, room_id, 'IN', qty, None, now, None, None, manager_name)
        conn.commit()
        return True
    return False

def reject_withdrawal_request(request_id: int, manager_name: str):
    """Reject a withdrawal request."""
    now = datetime.now().isoformat()
    c.execute(
        "UPDATE withdrawal_requests SET status='Rejected', approved_by=?, approved_on=? WHERE id=?",
        (manager_name, now, request_id),
    )
    conn.commit()

# Functions related to daily counts

def add_daily_count(
    asset_id: int,
    room_id: int,
    qty_counted: int,
    counted_by: str,
    note: str,
    qty_given: int = None,
    qty_received: int = None,
    used_qty: int = None,
    withdraw_qty: int = None,
    equipment_status: str = None,
) -> int:
    """Record a daily inventory count for an asset in a room.

    Additional parameters allow tracking of category-specific metrics:
    - qty_given / qty_received for CSSD/Đồ vải,
    - used_qty / withdraw_qty for Văn phòng phẩm,
    - equipment_status for Thiết bị/Công cụ.
    """
    # Get baseline for variance
    baseline_row = c.execute(
        "SELECT baseline_qty FROM asset_holdings WHERE asset_id=? AND room_id=?",
        (asset_id, room_id),
    ).fetchone()
    baseline = baseline_row[0] if baseline_row else 0
    variance = qty_counted - baseline
    now = datetime.now().isoformat()
    c.execute(
        "INSERT INTO daily_counts (asset_id, room_id, count_date, counted_by, qty_counted, variance, note, qty_given, qty_received, used_qty, withdraw_qty, equipment_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            asset_id,
            room_id,
            now,
            counted_by,
            qty_counted,
            variance,
            note,
            qty_given,
            qty_received,
            used_qty,
            withdraw_qty,
            equipment_status,
        ),
    )
    daily_id = c.lastrowid
    conn.commit()
    # If variance, create alert
    if variance != 0:
        severity = 'Low'
        if baseline != 0:
            ratio = abs(variance) / baseline
            if ratio > 0.2:
                severity = 'High'
            elif ratio > 0.1:
                severity = 'Medium'
        c.execute(
            "INSERT INTO alerts (daily_count_id, severity) VALUES (?,?)",
            (daily_id, severity),
        )
        conn.commit()
    return variance

def get_daily_counts_dataframe(filter_room_id=None, filter_asset_id=None, include_all=True) -> pd.DataFrame:
    # join with rooms and assets and departments
    query = """
    SELECT dc.id, dc.count_date, dc.qty_counted, dc.variance, dc.note, dc.reviewed_by_manager,
           dc.counted_by, dc.manager_reviewed_by, dc.manager_reviewed_on,
           a.name as asset_name, at.name as asset_type,
           r.name as room_name, d.name as department_name
    FROM daily_counts dc
    JOIN assets a ON dc.asset_id = a.id
    JOIN asset_types at ON a.asset_type_id = at.id
    JOIN rooms r ON dc.room_id = r.id
    JOIN departments d ON r.department_id = d.id
    WHERE 1=1
    """
    params = []
    if filter_room_id:
        query += " AND r.id = ?"
        params.append(filter_room_id)
    if filter_asset_id:
        query += " AND a.id = ?"
        params.append(filter_asset_id)
    query += " ORDER BY dc.count_date DESC"
    return pd.read_sql_query(query, conn, params=params)

def review_daily_count(count_id: int, manager_name: str):
    now = datetime.now().isoformat()
    c.execute("UPDATE daily_counts SET reviewed_by_manager=1, manager_reviewed_by=?, manager_reviewed_on=? WHERE id=?", (manager_name, now, count_id))
    # acknowledge alert if exists
    c.execute("UPDATE alerts SET acknowledged=1, acknowledged_by=?, acknowledged_on=? WHERE daily_count_id=?", (manager_name, now, count_id))
    conn.commit()

# Functions related to alerts

def get_alerts_dataframe(only_unacknowledged: bool = True) -> pd.DataFrame:
    query = """
    SELECT al.id, al.severity, al.acknowledged, al.acknowledged_by, al.acknowledged_on,
           dc.id as daily_id, dc.count_date, dc.qty_counted, dc.variance, dc.reviewed_by_manager,
           a.name as asset_name, at.name as asset_type,
           r.name as room_name, d.name as department_name
    FROM alerts al
    JOIN daily_counts dc ON al.daily_count_id = dc.id
    JOIN assets a ON dc.asset_id = a.id
    JOIN asset_types at ON a.asset_type_id = at.id
    JOIN rooms r ON dc.room_id = r.id
    JOIN departments d ON r.department_id = d.id
    """
    if only_unacknowledged:
        query += " WHERE al.acknowledged = 0"
    query += " ORDER BY dc.count_date DESC"
    return pd.read_sql_query(query, conn)

def acknowledge_alert(alert_id: int, user_name: str):
    now = datetime.now().isoformat()
    c.execute("UPDATE alerts SET acknowledged=1, acknowledged_by=?, acknowledged_on=? WHERE id=?", (user_name, now, alert_id))
    conn.commit()

# User management utilities
def add_user(username: str, role: str) -> bool:
    """Create a new user with a given role. Returns False if username already exists."""
    try:
        c.execute("INSERT INTO users (username, role) VALUES (?, ?)", (username.strip(), role))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_users() -> pd.DataFrame:
    """Retrieve all users."""
    return pd.read_sql_query("SELECT id, username, role FROM users ORDER BY username", conn)

def update_user_role(user_id: int, new_role: str):
    """Update the role of an existing user."""
    c.execute("UPDATE users SET role=? WHERE id=?", (new_role, user_id))
    conn.commit()

def delete_user(user_id: int):
    """Delete a user by ID."""
    c.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()

def get_user_role(username: str) -> str:
    """Get the role of a user by username. Returns None if not found."""
    row = c.execute("SELECT role FROM users WHERE username=?", (username,)).fetchone()
    return row[0] if row else None

# Holding request utilities
def add_holding_request(
    asset_id: int,
    room_id: int,
    serial_number: str,
    baseline_qty: int,
    requested_by: str,
    origin: str = None,
):
    """Create a holding request. Employees use this to declare baseline quantities pending approval.

    Optional origin field to record place of manufacture or source."""
    now = datetime.now().isoformat()
    origin_val = origin.strip() if origin else None
    c.execute(
        "INSERT INTO holding_requests (asset_id, room_id, serial_number, baseline_qty, date_requested, requested_by, origin) VALUES (?,?,?,?,?,?,?)",
        (asset_id, room_id, serial_number, baseline_qty, now, requested_by, origin_val),
    )
    conn.commit()

def get_holding_requests_dataframe(status_filter: str = None) -> pd.DataFrame:
    """Retrieve holding requests. Optionally filter by status (Pending, Approved, Rejected)."""
    query = """
    SELECT hr.id, hr.status, hr.date_requested, hr.baseline_qty,
           a.name as asset_name, at.name as asset_type,
           r.name as room_name, d.name as department_name,
           hr.serial_number, hr.requested_by, hr.approved_by, hr.approved_on
    FROM holding_requests hr
    JOIN assets a ON hr.asset_id = a.id
    JOIN asset_types at ON a.asset_type_id = at.id
    JOIN rooms r ON hr.room_id = r.id
    JOIN departments d ON r.department_id = d.id
    """
    params = []
    if status_filter:
        query += " WHERE hr.status = ?"
        params.append(status_filter)
    query += " ORDER BY hr.date_requested DESC"
    return pd.read_sql_query(query, conn, params)

def approve_holding_request(request_id: int, approver_name: str) -> bool:
    """Approve a holding request and update the asset_holdings table accordingly."""
    row = c.execute("SELECT asset_id, room_id, serial_number, baseline_qty FROM holding_requests WHERE id=? AND status='Pending'", (request_id,)).fetchone()
    if row:
        asset_id, room_id, serial, baseline_qty = row
        now = datetime.now().isoformat()
        # Fetch origin from request
        origin_val = c.execute("SELECT origin FROM holding_requests WHERE id=?", (request_id,)).fetchone()[0]
        # Apply the requested baseline to asset_holdings (origin is optional)
        add_or_update_holding(
            asset_id,
            room_id,
            serial,
            baseline_qty,
            now,
            approver_name,
            approver_name,
            origin=origin_val,
            equipment_status=None,
        )
        c.execute(
            "UPDATE holding_requests SET status='Approved', approved_by=?, approved_on=? WHERE id=?",
            (approver_name, now, request_id),
        )
        conn.commit()
        return True
    return False

def reject_holding_request(request_id: int, approver_name: str):
    """Reject a holding request."""
    now = datetime.now().isoformat()
    c.execute(
        "UPDATE holding_requests SET status='Rejected', approved_by=?, approved_on=? WHERE id=?",
        (approver_name, now, request_id),
    )
    conn.commit()
# Export holdings to Excel

def export_holdings_to_excel(df: pd.DataFrame) -> bytes:
    # Use pandas to create an Excel file in memory
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='AssetHoldings')
    return output.getvalue()

# Main Streamlit app

def main():
    st.set_page_config(page_title="SUGAR APP - Asset Management", layout="wide")
    # Apply custom CSS for a more modern look
    st.markdown(
        """
        <style>
        .stApp {
            background-color: #f7f9fc;
        }
        .stMetric > div {
            font-size: 1.2rem;
        }
        /* Sidebar tweaks */
        [data-testid="stSidebar"] {
            background-color: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("SUGAR APP - Quản lý tài sản")

    # Authentication based on users table
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if not st.session_state.logged_in:
        st.subheader("Đăng nhập")
        username = st.text_input("Tên người dùng")
        if st.button("Đăng nhập"):
            if not username:
                st.warning("Vui lòng nhập tên người dùng")
            else:
                role = get_user_role(username)
                if role:
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.role = role
                    st.rerun()
                else:
                    st.error("Người dùng không tồn tại. Vui lòng liên hệ người quản trị.")
        return

    # Show user info and logout option
    st.sidebar.markdown(f"**Người dùng:** {st.session_state.username}")
    st.sidebar.markdown(f"**Vai trò:** {st.session_state.role}")
    if st.sidebar.button("Đăng xuất"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    # Load data for sidebar filters
    departments_df = get_departments()
    rooms_df = get_rooms()
    asset_types_df = get_asset_types()
    assets_df = get_assets()

    # Define navigation based on role
    role = st.session_state.role
    if role == 'Quản lý':
        pages = [
            'Dashboard', 'Loại tài sản', 'Tài sản', 'Khoa', 'Phòng', 'Tồn kho',
            'Giao/Nhận', 'Phiếu điều chuyển', 'Phiếu lãnh', 'Kiểm đếm', 'Cảnh báo', 'Xuất báo cáo'
        ]
    elif role == 'Phòng điều dưỡng':
        pages = [
            'Dashboard', 'Loại tài sản', 'Tài sản', 'Khoa', 'Phòng', 'Tồn kho',
            'Giao/Nhận', 'Phiếu điều chuyển', 'Phiếu lãnh', 'Kiểm đếm', 'Cảnh báo', 'Xuất báo cáo', 'Quản lý người dùng'
        ]
    else:
        # Nhân viên
        pages = [
            'Tồn kho', 'Giao/Nhận', 'Phiếu điều chuyển', 'Phiếu lãnh', 'Kiểm đếm', 'Khai báo tồn kho'
        ]
    # Use radio instead of selectbox to list all options visibly
    selection = st.sidebar.radio("Chọn chức năng", pages)

    if selection == 'Dashboard':
        show_dashboard()
    elif selection == 'Loại tài sản':
        page_asset_types()
    elif selection == 'Tài sản':
        page_assets()
    elif selection == 'Khoa':
        page_departments()
    elif selection == 'Phòng':
        page_rooms()
    elif selection == 'Tồn kho':
        page_holdings()
    elif selection == 'Giao/Nhận':
        page_transactions()
    elif selection == 'Phiếu điều chuyển':
        page_transfer_requests()
    elif selection == 'Phiếu lãnh':
        page_withdrawal_requests()
    elif selection == 'Kiểm đếm':
        page_daily_counts()
    elif selection == 'Cảnh báo':
        page_alerts()
    elif selection == 'Xuất báo cáo':
        page_export()
    elif selection == 'Quản lý người dùng':
        page_user_management()
    elif selection == 'Khai báo tồn kho':
        page_declare_holdings()

# Page implementations

def show_dashboard():
    st.header("Tổng quan")
    # Compute metrics
    total_asset_types = get_asset_types().shape[0]
    total_assets = get_assets().shape[0]
    total_departments = get_departments().shape[0]
    total_rooms = get_rooms().shape[0]
    pending_requests = c.execute("SELECT COUNT(*) FROM transfer_requests WHERE status='Pending'").fetchone()[0]
    unack_alerts = c.execute("SELECT COUNT(*) FROM alerts WHERE acknowledged=0").fetchone()[0]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Loại tài sản", total_asset_types)
        st.metric("Tài sản", total_assets)
    with col2:
        st.metric("Khoa", total_departments)
        st.metric("Phòng", total_rooms)
    with col3:
        st.metric("Yêu cầu điều chuyển chờ duyệt", pending_requests)
        st.metric("Cảnh báo tồn kho", unack_alerts)

    st.subheader("Số liệu tồn kho theo phòng")
    holdings_df = get_holdings_dataframe()
    if not holdings_df.empty:
        # Summarize totals by room
        summary = holdings_df.groupby(['department', 'room'])[['baseline_qty', 'qty_on_hand']].sum().reset_index()
        summary['chênh lệch'] = summary['qty_on_hand'] - summary['baseline_qty']
        st.dataframe(summary)
    else:
        st.info("Chưa có dữ liệu tồn kho.")


def page_asset_types():
    st.header("Quản lý loại tài sản")
    with st.form("add_asset_type_form"):
        new_name = st.text_input("Tên loại tài sản")
        category_options = ["Đồ vải", "CSSD", "Văn phòng phẩm", "Thiết bị/Công cụ"]
        category_option = st.selectbox("Danh mục", options=category_options, index=0)
        submitted = st.form_submit_button("Thêm")
        if submitted:
            if new_name.strip() == "":
                st.error("Tên loại tài sản không được để trống.")
            else:
                if add_asset_type(new_name, category_option):
                    st.success("Đã thêm loại tài sản mới.")
                    st.experimental_rerun()
                else:
                    st.error("Loại tài sản đã tồn tại.")
    df = get_asset_types()
    st.subheader("Danh sách loại tài sản")
    if df.empty:
        st.info("Chưa có loại tài sản.")
    else:
        for _, row in df.iterrows():
            cols = st.columns([0.6, 0.3, 0.1])
            cols[0].write(f"{row['name']}")
            cols[1].write(f"{row['category'] if row['category'] else ''}")
            if cols[2].button("Xoá", key=f"del_at_{row['id']}"):
                c.execute("DELETE FROM asset_types WHERE id=?", (row['id'],))
                conn.commit()
                st.rerun()


def page_assets():
    st.header("Quản lý tài sản")
    types_df = get_asset_types()
    if types_df.empty:
        st.info("Bạn cần thêm Loại tài sản trước.")
    else:
        with st.form("add_asset_form"):
            type_option = st.selectbox("Chọn loại tài sản", options=types_df['name'].tolist(), index=0)
            asset_name = st.text_input("Tên tài sản")
            unit = st.text_input("Đơn vị tính")
            submit = st.form_submit_button("Thêm tài sản")
            if submit:
                if not asset_name.strip():
                    st.error("Tên tài sản không được để trống.")
                else:
                    type_id = types_df[types_df['name'] == type_option]['id'].values[0]
                    if add_asset(type_id, asset_name, unit):
                        st.success("Đã thêm tài sản.")
                    else:
                        st.error("Tài sản đã tồn tại hoặc xảy ra lỗi.")
    assets_df = get_assets()
    st.subheader("Danh sách tài sản")
    if assets_df.empty:
        st.info("Chưa có tài sản.")
    else:
        st.dataframe(assets_df)


def page_departments():
    st.header("Quản lý Khoa")
    with st.form("add_department_form"):
        dept_name = st.text_input("Tên Khoa")
        submit = st.form_submit_button("Thêm Khoa")
        if submit:
            if not dept_name.strip():
                st.error("Tên Khoa không được để trống.")
            else:
                if add_department(dept_name):
                    # Show success and force a rerun so the department list refreshes
                    st.success("Đã thêm Khoa.")
                    # Reload the page to refresh department list
                    st.rerun()
                else:
                    st.error("Khoa đã tồn tại hoặc xảy ra lỗi.")
    df = get_departments()
    st.subheader("Danh sách Khoa")
    if df.empty:
        st.info("Chưa có Khoa.")
    else:
        for _, row in df.iterrows():
            cols = st.columns([0.8, 0.2])
            cols[0].write(f"{row['name']}")
            if cols[1].button("Xoá", key=f"del_dept_{row['id']}"):
                # Check if any rooms belong to this department
                room_count = c.execute("SELECT COUNT(*) FROM rooms WHERE department_id=?", (row['id'],)).fetchone()[0]
                if room_count > 0:
                    st.error("Không thể xoá Khoa vì đã có phòng thuộc Khoa này.")
                else:
                    c.execute("DELETE FROM departments WHERE id=?", (row['id'],))
                    conn.commit()
                    st.rerun()


def page_rooms():
    st.header("Quản lý Phòng")
    depts_df = get_departments()
    if depts_df.empty:
        st.info("Bạn cần thêm Khoa trước.")
    else:
        with st.form("add_room_form"):
            dept_option = st.selectbox("Chọn Khoa", options=depts_df['name'].tolist())
            room_name = st.text_input("Tên Phòng")
            submit = st.form_submit_button("Thêm Phòng")
            if submit:
                if not room_name.strip():
                    st.error("Tên Phòng không được để trống.")
                else:
                    dept_id = depts_df[depts_df['name'] == dept_option]['id'].values[0]
                    if add_room(dept_id, room_name):
                        # Show success and force a rerun so the room list refreshes
                        st.success("Đã thêm Phòng.")
                        # Reload the page to refresh room list
                        st.rerun()
                    else:
                        st.error("Phòng đã tồn tại hoặc xảy ra lỗi.")
    rooms_df = get_rooms()
    st.subheader("Danh sách Phòng")
    if rooms_df.empty:
        st.info("Chưa có Phòng.")
    else:
        st.dataframe(rooms_df)


def page_holdings():
    st.header("Quản lý Tồn kho")
    # Filter options
    depts = get_departments()
    rooms = get_rooms()
    types = get_asset_types()
    assets = get_assets()
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    dept_filter = col_f1.selectbox("Lọc Khoa", options=['Tất cả'] + depts['name'].tolist()) if not depts.empty else None
    room_filter = None
    if dept_filter and dept_filter != 'Tất cả':
        dept_id = depts[depts['name'] == dept_filter]['id'].values[0]
        room_options = rooms[rooms['department_id'] == dept_id]['name'].tolist()
        room_filter = col_f2.selectbox("Lọc Phòng", options=['Tất cả'] + room_options)
    else:
        room_filter = col_f2.selectbox("Lọc Phòng", options=['Tất cả'] + rooms['name'].tolist() if not rooms.empty else ['Tất cả'])
    type_filter = col_f3.selectbox("Lọc loại tài sản", options=['Tất cả'] + types['name'].tolist()) if not types.empty else None
    asset_filter = None
    if type_filter and type_filter != 'Tất cả':
        type_id_f = types[types['name'] == type_filter]['id'].values[0]
        asset_options = assets[assets['type_id'] == type_id_f]['name'].tolist()
        asset_filter = col_f4.selectbox("Tên tài sản", options=['Tất cả'] + asset_options)
    else:
        asset_filter = col_f4.selectbox("Tên tài sản", options=['Tất cả'] + assets['name'].tolist() if not assets.empty else ['Tất cả'])
    # Convert selections to IDs
    filter_dept_id = depts[depts['name'] == dept_filter]['id'].values[0] if dept_filter and dept_filter != 'Tất cả' else None
    filter_room_id = rooms[rooms['name'] == room_filter]['id'].values[0] if room_filter and room_filter != 'Tất cả' else None
    filter_type_id = types[types['name'] == type_filter]['id'].values[0] if type_filter and type_filter != 'Tất cả' else None
    filter_asset_id = assets[assets['name'] == asset_filter]['id'].values[0] if asset_filter and asset_filter != 'Tất cả' else None
    df_holdings = get_holdings_dataframe(filter_dept_id, filter_room_id, filter_type_id, filter_asset_id)
    st.subheader("Danh sách Tồn kho")
    if df_holdings.empty:
        st.info("Không có bản ghi tồn kho.")
    else:
        # Display with highlight for variance
        def highlight_variance(val):
            """Color-code the variance column.

            Negative values (missing) are red, positive values (surplus) are orange,
            and zero (exact match) is green.
            """
            if val is None:
                return ''
            try:
                v = float(val)
            except Exception:
                return ''
            if v < 0:
                return 'background-color: #FFCCCC'  # Missing (Thiếu)
            elif v > 0:
                return 'background-color: #FFF0B3'  # Surplus (Thừa)
            else:
                return 'background-color: #CCFFCC'  # Enough (Đủ)
        styled_df = df_holdings.rename(columns={
            'department': 'Khoa',
            'room': 'Phòng',
            'asset_type': 'Loại tài sản',
            'asset_name': 'Tên tài sản',
            'unit': 'ĐVT',
            'serial_number': 'Số serial',
            'date_received': 'Ngày nhận',
            'received_by': 'Người nhận',
            'manager_in_charge': 'Người quản lý',
            'origin': 'Nơi sản xuất',
            'equipment_status': 'Tình trạng',
            'baseline_qty': 'Cơ số ban đầu',
            'qty_on_hand': 'Tồn hệ thống',
            'last_count': 'Số kiểm đếm gần nhất',
            'variance': 'Chênh lệch'
        })
        # Style for equipment status
        def highlight_status(val):
            if val is None:
                return ''
            if val == 'Hư hỏng':
                return 'color: red; font-weight: bold'
            elif val == 'Hoạt động tốt':
                return 'color: green'
            elif val == 'Chưa sử dụng':
                return 'color: gray'
            else:
                return ''
        st.dataframe(
            styled_df.style
            .applymap(highlight_variance, subset=['Chênh lệch'])
            .applymap(highlight_status, subset=['Tình trạng'])
        )
    # Section to add or update holdings (for manager and nursing role)
    if st.session_state.role in ['Quản lý', 'Phòng điều dưỡng']:
        st.subheader("Thêm/ Cập nhật bản ghi Tồn kho")
        with st.form("add_holding_form"):
            asset_option = st.selectbox("Chọn tài sản", options=assets['name'].tolist()) if not assets.empty else None
            room_option = st.selectbox("Chọn Phòng", options=rooms['name'].tolist()) if not rooms.empty else None
            serial = st.text_input("Số serial (có thể để trống nếu không có)")
            baseline = st.number_input("Cơ số ban đầu", min_value=0, step=1, value=0)
            date_recv = st.date_input("Ngày nhận", value=date.today())
            received_by = st.text_input("Người nhận")
            manager_in_charge = st.text_input("Người quản lý")
            origin = st.text_input("Nơi sản xuất (tuỳ chọn)")
            status_option = st.selectbox(
                "Tình trạng (cho thiết bị/công cụ)",
                options=["", "Chưa sử dụng", "Hoạt động tốt", "Hư hỏng"],
                index=0,
            )
            submit_holding = st.form_submit_button("Lưu")
            if submit_holding:
                if not asset_option or not room_option:
                    st.error("Vui lòng chọn Tài sản và Phòng")
                else:
                    asset_id = assets[assets['name'] == asset_option]['id'].values[0]
                    room_id = rooms[rooms['name'] == room_option]['id'].values[0]
                    add_or_update_holding(
                        asset_id,
                        room_id,
                        serial,
                        int(baseline),
                        date_recv.isoformat(),
                        received_by,
                        manager_in_charge,
                        origin=origin if origin else None,
                        equipment_status=status_option if status_option else None,
                    )
                    st.success("Đã lưu bản ghi tồn kho.")
                    st.rerun()
        # Update baseline quantity for existing records
        st.subheader("Cập nhật cơ số")
        if not df_holdings.empty:
            holding_ids = df_holdings['holding_id'].tolist()
            hold_options = [f"{df_holdings.loc[i, 'asset_name']} - {df_holdings.loc[i, 'room']} (Serial: {df_holdings.loc[i, 'serial_number'] if df_holdings.loc[i, 'serial_number'] else 'N/A'})" for i in range(len(df_holdings))]
            selected_hold = st.selectbox("Chọn bản ghi", options=hold_options)
            new_baseline = st.number_input("Cập nhật cơ số", min_value=0, step=1)
            if st.button("Cập nhật"):
                idx = hold_options.index(selected_hold)
                hold_id = holding_ids[idx]
                update_baseline(hold_id, int(new_baseline))
                st.success("Đã cập nhật cơ số.")
                st.rerun()

        # Section to approve pending holding requests
        st.subheader("Yêu cầu khai báo tồn kho chờ duyệt")
        pending_reqs = get_holding_requests_dataframe(status_filter='Pending')
        if pending_reqs.empty:
            st.info("Không có yêu cầu khai báo tồn kho.")
        else:
            for _, req in pending_reqs.iterrows():
                with st.expander(f"Yêu cầu ID {req['id']} - {req['asset_name']} | {req['room_name']} | Cơ số {req['baseline_qty']}"):
                    st.write(f"Tài sản: {req['asset_name']} ({req['asset_type']})")
                    st.write(f"Khoa: {req['department_name']}, Phòng: {req['room_name']}")
                    st.write(f"Số serial: {req['serial_number'] if req['serial_number'] else 'N/A'}")
                    st.write(f"Cơ số khai báo: {req['baseline_qty']}")
                    st.write(f"Người khai báo: {req['requested_by']}")
                    st.write(f"Thời gian gửi: {req['date_requested']}")
                    col_a, col_b = st.columns(2)
                    if col_a.button("Duyệt", key=f"approve_hr_{req['id']}"):
                        if approve_holding_request(req['id'], st.session_state.username):
                            st.success("Đã duyệt yêu cầu.")
                            st.rerun()
                    if col_b.button("Từ chối", key=f"reject_hr_{req['id']}"):
                        reject_holding_request(req['id'], st.session_state.username)
                        st.warning("Đã từ chối yêu cầu.")
                        st.rerun()


def page_transactions():
    st.header("Giao/Nhận tài sản")
    # Show form to create a transaction
    assets_df = get_assets()
    rooms_df = get_rooms()
    if assets_df.empty or rooms_df.empty:
        st.info("Cần có Tài sản và Phòng trước khi tạo giao dịch.")
        return
    with st.form("transaction_form"):
        asset_option = st.selectbox("Tài sản", options=assets_df['name'].tolist())
        txn_type = st.selectbox("Loại giao dịch", options=['IN', 'OUT', 'ADJUST'])
        qty = st.number_input("Số lượng", min_value=1, step=1, value=1)
        serial = st.text_input("Số serial (tuỳ chọn)")
        date_txn = st.date_input("Ngày giao dịch", value=date.today())
        delivered_by = st.text_input("Người giao")
        received_by = st.text_input("Người nhận")
        if txn_type == 'IN':
            to_room = st.selectbox("Phòng nhận", options=rooms_df['name'].tolist())
            from_room = None
        elif txn_type == 'OUT':
            from_room = st.selectbox("Phòng xuất", options=rooms_df['name'].tolist())
            to_room = None
        else:  # ADJUST
            adjust_room = st.selectbox("Phòng điều chỉnh", options=rooms_df['name'].tolist())
            from_room = adjust_room
            to_room = adjust_room
        submit_txn = st.form_submit_button("Lưu giao dịch")
        if submit_txn:
            asset_id = assets_df[assets_df['name'] == asset_option]['id'].values[0]
            from_id = None
            to_id = None
            if txn_type == 'IN':
                to_id = rooms_df[rooms_df['name'] == to_room]['id'].values[0]
            elif txn_type == 'OUT':
                from_id = rooms_df[rooms_df['name'] == from_room]['id'].values[0]
            else:  # ADJUST
                from_id = rooms_df[rooms_df['name'] == adjust_room]['id'].values[0]
                to_id = from_id
            add_transaction(asset_id, from_id, to_id, txn_type, int(qty), serial if serial else None, date_txn.isoformat(), delivered_by, received_by, st.session_state.username)
            st.success("Đã lưu giao dịch.")
    st.subheader("Lịch sử giao dịch")
    df_txn = get_transactions_dataframe()
    if df_txn.empty:
        st.info("Chưa có giao dịch.")
    else:
        st.dataframe(df_txn)


def page_transfer_requests():
    st.header("Phiếu điều chuyển")
    assets_df = get_assets()
    rooms_df = get_rooms()
    if assets_df.empty or rooms_df.empty:
        st.info("Cần có Tài sản và Phòng trước khi yêu cầu điều chuyển.")
        return
    # Employee view: create new transfer request
    if st.session_state.role == 'Nhân viên':
        with st.form("transfer_request_form"):
            asset_option = st.selectbox("Tài sản", options=assets_df['name'].tolist())
            from_room = st.selectbox("Chọn Phòng nguồn", options=rooms_df['name'].tolist())
            to_room = st.selectbox("Chọn Phòng đích", options=rooms_df['name'].tolist())
            qty = st.number_input("Số lượng điều chuyển", min_value=1, step=1, value=1)
            reason = st.text_input("Lý do")
            submit_req = st.form_submit_button("Gửi yêu cầu")
            if submit_req:
                asset_id = assets_df[assets_df['name'] == asset_option]['id'].values[0]
                from_id = rooms_df[rooms_df['name'] == from_room]['id'].values[0]
                to_id = rooms_df[rooms_df['name'] == to_room]['id'].values[0]
                if from_id == to_id:
                    st.error("Phòng nguồn và Phòng đích không được trùng nhau.")
                else:
                    add_transfer_request(asset_id, from_id, to_id, int(qty), reason, st.session_state.username)
                    st.success("Đã gửi yêu cầu điều chuyển.")
        # Show user's requests
        st.subheader("Yêu cầu của tôi")
        my_requests = get_transfer_requests_dataframe(include_all=False, username=st.session_state.username)
        if my_requests.empty:
            st.info("Bạn chưa gửi yêu cầu nào.")
        else:
            st.dataframe(my_requests)
    else:
        # Manager view: list all requests
        st.subheader("Danh sách yêu cầu điều chuyển")
        df_reqs = get_transfer_requests_dataframe(include_all=True)
        if df_reqs.empty:
            st.info("Không có yêu cầu điều chuyển.")
        else:
            for _, row in df_reqs.iterrows():
                with st.expander(f"Yêu cầu ID {row['id']} - {row['asset_name']} ({row['qty']}): {row['from_room']} -> {row['to_room']} | Trạng thái: {row['status']}"):
                    st.write(f"Tài sản: {row['asset_name']} ({row['asset_type']})")
                    st.write(f"Từ phòng: {row['from_room']} → Đến phòng: {row['to_room']}")
                    st.write(f"Số lượng: {row['qty']}")
                    st.write(f"Lý do: {row['reason']}")
                    st.write(f"Người gửi: {row['requested_by']}")
                    st.write(f"Thời gian gửi: {row['requested_on']}")
                    if row['status'] == 'Pending':
                        col_a, col_b = st.columns(2)
                        if col_a.button("Duyệt", key=f"approve_req_{row['id']}"):
                            approve_transfer_request(row['id'], st.session_state.username)
                            st.success("Đã duyệt yêu cầu.")
                            st.rerun()
                        if col_b.button("Từ chối", key=f"reject_req_{row['id']}"):
                            reject_transfer_request(row['id'], st.session_state.username)
                            st.warning("Đã từ chối yêu cầu.")
                            st.rerun()
                    else:
                        st.write(f"Người duyệt: {row['approved_by']} lúc {row['approved_on']}")

# Page for withdrawal requests (Phiếu lãnh) for stationery
def page_withdrawal_requests():
    """Handle creation and approval of withdrawal slips for Văn phòng phẩm (stationery)."""
    st.header("Phiếu lãnh")
    # Load assets and rooms
    assets_df = get_assets()
    rooms_df = get_rooms()
    if assets_df.empty or rooms_df.empty:
        st.info("Cần có Tài sản và Phòng trước khi tạo phiếu lãnh.")
        return
    # Filter assets to only those with category 'Văn phòng phẩm'
    types_df = get_asset_types()
    vpp_type_ids = types_df[types_df['category'] == 'Văn phòng phẩm']['id'].tolist()
    assets_vpp = assets_df[assets_df['type_id'].isin(vpp_type_ids)] if vpp_type_ids else pd.DataFrame()
    if assets_vpp.empty:
        st.info("Không có tài sản thuộc danh mục Văn phòng phẩm.")
        return
    if st.session_state.role == 'Nhân viên':
        # Employee can create a new withdrawal request
        with st.form("withdrawal_request_form"):
            asset_option = st.selectbox("Chọn tài sản (Văn phòng phẩm)", options=assets_vpp['name'].tolist())
            room_option = st.selectbox("Chọn Phòng", options=rooms_df['name'].tolist())
            qty = st.number_input("Số lượng", min_value=1, step=1, value=1)
            note = st.text_area("Ghi chú (tuỳ chọn)")
            submit_req = st.form_submit_button("Gửi phiếu lãnh")
            if submit_req:
                asset_id = assets_vpp[assets_vpp['name'] == asset_option]['id'].values[0]
                room_id = rooms_df[rooms_df['name'] == room_option]['id'].values[0]
                add_withdrawal_request(
                    asset_id,
                    room_id,
                    int(qty),
                    st.session_state.username,
                    note if note else None,
                )
                st.success("Đã gửi phiếu lãnh.")
        # Display user's own withdrawal requests
        st.subheader("Phiếu lãnh của tôi")
        my_reqs = get_withdrawal_requests_dataframe(include_all=False, username=st.session_state.username)
        if my_reqs.empty:
            st.info("Bạn chưa gửi phiếu lãnh nào.")
        else:
            # Rename columns for display
            display_df = my_reqs.rename(columns={
                'id': 'Mã',
                'requested_on': 'Ngày gửi',
                'asset_name': 'Tài sản',
                'asset_type': 'Loại',
                'room_name': 'Phòng',
                'department_name': 'Khoa',
                'qty': 'Số lượng',
                'note': 'Ghi chú',
                'status': 'Trạng thái',
                'approved_by': 'Người duyệt',
                'approved_on': 'Ngày duyệt',
            })
            st.dataframe(display_df[['Mã','Ngày gửi','Tài sản','Loại','Khoa','Phòng','Số lượng','Ghi chú','Trạng thái','Người duyệt','Ngày duyệt']])
    else:
        # Manager or nursing view: list all requests and approve/reject
        st.subheader("Danh sách phiếu lãnh")
        all_reqs = get_withdrawal_requests_dataframe(include_all=True)
        if all_reqs.empty:
            st.info("Không có phiếu lãnh.")
        else:
            for _, row in all_reqs.iterrows():
                exp_title = f"Phiếu ID {row['id']} - {row['asset_name']} ({row['qty']}) | {row['room_name']} | Trạng thái: {row['status']}"
                with st.expander(exp_title):
                    st.write(f"Tài sản: {row['asset_name']} ({row['asset_type']})")
                    st.write(f"Khoa: {row['department_name']}, Phòng: {row['room_name']}")
                    st.write(f"Số lượng: {row['qty']}")
                    st.write(f"Ghi chú: {row['note'] if row['note'] else 'Không' }")
                    st.write(f"Người gửi: {row['requested_by']}")
                    st.write(f"Thời gian gửi: {row['requested_on']}")
                    if row['status'] == 'Pending':
                        col_a, col_b = st.columns(2)
                        if col_a.button("Duyệt", key=f"approve_wr_{row['id']}"):
                            if approve_withdrawal_request(row['id'], st.session_state.username):
                                st.success("Đã duyệt phiếu.")
                                st.rerun()
                        if col_b.button("Từ chối", key=f"reject_wr_{row['id']}"):
                            reject_withdrawal_request(row['id'], st.session_state.username)
                            st.warning("Đã từ chối phiếu.")
                            st.rerun()
                    else:
                        st.write(f"Người duyệt: {row['approved_by']} lúc {row['approved_on']}")


def page_daily_counts():
    st.header("Kiểm đếm tài sản")
    rooms_df = get_rooms()
    assets_df = get_assets()
    if rooms_df.empty or assets_df.empty:
        st.info("Cần có Phòng và Tài sản trước khi kiểm đếm.")
        return
    if st.session_state.role == 'Nhân viên':
        # Employee selects a room to count
        room_option = st.selectbox("Chọn phòng để kiểm đếm", options=rooms_df['name'].tolist())
        room_id = rooms_df[rooms_df['name'] == room_option]['id'].values[0]
        # Get holdings for this room
        holdings_df = get_holdings_dataframe(filter_room_id=room_id)
        if holdings_df.empty:
            st.info("Phòng này chưa có bản ghi Tồn kho.")
        else:
            # Map asset type IDs to their categories
            types_df = get_asset_types()
            type_category = dict(zip(types_df['id'], types_df['category']))
            st.subheader("Nhập dữ liệu cơ số theo từng loại tài sản")
            entries = []
            with st.form("count_form"):
                for _, row in holdings_df.iterrows():
                    asset_id = row['asset_id']
                    holding_id = row['holding_id']
                    baseline_qty = row['baseline_qty']
                    asset_name = row['asset_name']
                    type_id = row['type_id']
                    category = type_category.get(type_id, "")
                    if category in ["Đồ vải", "CSSD"]:
                        col1, col2 = st.columns(2)
                        qty_given = col1.number_input(f"{asset_name} - Số giao", min_value=0, step=1, key=f"given_{holding_id}")
                        qty_received = col2.number_input(f"{asset_name} - Số nhận", min_value=0, step=1, key=f"recv_{holding_id}")
                        current_qty = baseline_qty - qty_given + qty_received
                        # Determine status for display
                        if current_qty == baseline_qty:
                            status_txt = "Đủ"
                        elif current_qty < baseline_qty:
                            status_txt = "Thiếu"
                        else:
                            status_txt = "Thừa"
                        st.markdown(f"*Hiện tại:* **{current_qty}**  |  *Tình trạng:* **{status_txt}**")
                        entries.append({
                            'holding_id': holding_id,
                            'asset_id': asset_id,
                            'qty_counted': current_qty,
                            'qty_given': qty_given,
                            'qty_received': qty_received,
                            'used_qty': None,
                            'withdraw_qty': None,
                            'equipment_status': None,
                        })
                    elif category == "Văn phòng phẩm":
                        col1, col2 = st.columns(2)
                        used_qty = col1.number_input(f"{asset_name} - Sử dụng", min_value=0, step=1, key=f"used_{holding_id}")
                        withdraw_qty = col2.number_input(f"{asset_name} - Lãnh", min_value=0, step=1, key=f"with_{holding_id}")
                        current_qty = baseline_qty - used_qty + withdraw_qty
                        status_txt = "Trong định mức" if used_qty <= baseline_qty else "Vượt định mức"
                        st.markdown(f"*Tồn cuối:* **{current_qty}**  |  *Tình trạng:* **{status_txt}**")
                        entries.append({
                            'holding_id': holding_id,
                            'asset_id': asset_id,
                            'qty_counted': current_qty,
                            'qty_given': None,
                            'qty_received': None,
                            'used_qty': used_qty,
                            'withdraw_qty': withdraw_qty,
                            'equipment_status': None,
                        })
                    else:
                        # Thiết bị/Công cụ
                        status_option = st.selectbox(
                            f"{asset_name} - Tình trạng",
                            options=["Chưa sử dụng", "Hoạt động tốt", "Hư hỏng"],
                            key=f"status_{holding_id}",
                        )
                        current_qty = baseline_qty
                        st.markdown(f"*Cơ số:* **{baseline_qty}**")
                        entries.append({
                            'holding_id': holding_id,
                            'asset_id': asset_id,
                            'qty_counted': current_qty,
                            'qty_given': None,
                            'qty_received': None,
                            'used_qty': None,
                            'withdraw_qty': None,
                            'equipment_status': status_option,
                        })
                note = st.text_area("Ghi chú", key="count_note")
                if st.form_submit_button("Lưu kiểm đếm"):
                    # Save counts for each asset
                    for entry in entries:
                        add_daily_count(
                            entry['asset_id'],
                            room_id,
                            int(entry['qty_counted']),
                            st.session_state.username,
                            note,
                            qty_given=entry.get('qty_given'),
                            qty_received=entry.get('qty_received'),
                            used_qty=entry.get('used_qty'),
                            withdraw_qty=entry.get('withdraw_qty'),
                            equipment_status=entry.get('equipment_status'),
                        )
                    st.success("Đã lưu kiểm đếm.")
                    st.experimental_rerun()
    else:
        # Manager view: list counts and review
        st.subheader("Danh sách kiểm đếm")
        df_counts = get_daily_counts_dataframe()
        if df_counts.empty:
            st.info("Chưa có kiểm đếm.")
        else:
            for _, row in df_counts.iterrows():
                with st.expander(f"ID {row['id']} - {row['department_name']} / {row['room_name']} - {row['asset_name']} ({row['qty_counted']})"):
                    st.write(f"Phòng: {row['department_name']} / {row['room_name']}")
                    st.write(f"Tài sản: {row['asset_name']} ({row['asset_type']})")
                    st.write(f"Số đếm: {row['qty_counted']}")
                    st.write(f"Chênh lệch: {row['variance']}")
                    st.write(f"Người đếm: {row['counted_by']}")
                    st.write(f"Ngày đếm: {row['count_date']}")
                    st.write(f"Ghi chú: {row['note']}")
                    if row['reviewed_by_manager'] == 0:
                        if st.button("Đánh dấu đã xem", key=f"review_{row['id']}"):
                            review_daily_count(row['id'], st.session_state.username)
                            st.success("Đã đánh dấu đã xem.")
                            st.rerun()
                    else:
                        st.write(f"Đã xem bởi {row['manager_reviewed_by']} lúc {row['manager_reviewed_on']}")


def page_alerts():
    st.header("Cảnh báo tồn kho")
    df_alerts = get_alerts_dataframe(only_unacknowledged=False)
    if df_alerts.empty:
        st.info("Không có cảnh báo.")
    else:
        for _, row in df_alerts.iterrows():
            title = f"Alert ID {row['id']} - {row['department_name']}/{row['room_name']} - {row['asset_name']} ({row['variance']})"
            with st.expander(title):
                st.write(f"Mức độ: {row['severity']}")
                st.write(f"Chênh lệch: {row['variance']}")
                st.write(f"Ngày kiểm đếm: {row['count_date']}")
                st.write(f"Đã xem: {'Có' if row['acknowledged'] else 'Chưa'}")
                if row['acknowledged']:
                    st.write(f"Người xem: {row['acknowledged_by']} lúc {row['acknowledged_on']}")
                else:
                    if st.button("Đánh dấu đã xem", key=f"ack_{row['id']}"):
                        acknowledge_alert(row['id'], st.session_state.username)
                        st.success("Đã đánh dấu đã xem.")
                        st.rerun()


def page_export():
    st.header("Xuất báo cáo")
    df_holdings = get_holdings_dataframe()
    if df_holdings.empty:
        st.info("Không có dữ liệu để xuất.")
    else:
        data = df_holdings.copy()
        data = data.rename(columns={
            'department': 'Khoa', 'room': 'Phòng', 'asset_type': 'Loại tài sản',
            'asset_name': 'Tên tài sản', 'unit': 'ĐVT',
            'serial_number': 'Số serial', 'date_received': 'Ngày nhận',
            'received_by': 'Người nhận', 'manager_in_charge': 'Người quản lý',
            'baseline_qty': 'Cơ số ban đầu', 'qty_on_hand': 'Tồn hệ thống',
            'last_count': 'Số kiểm đếm gần nhất', 'variance': 'Chênh lệch'
        })
        excel_bytes = export_holdings_to_excel(data)
        st.download_button(label="Tải xuống Excel", data=excel_bytes, file_name="bao_cao_ton_kho.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.success("Đã tạo file báo cáo.")

# Page for user management (accessible by Phòng điều dưỡng role)
def page_user_management():
    st.header("Quản lý người dùng")
    st.info("Chức năng này dành cho người dùng Phòng điều dưỡng để tạo và phân quyền người dùng mới.")
    # Form to add a new user
    with st.form("add_user_form"):
        new_username = st.text_input("Tên đăng nhập mới")
        new_role = st.selectbox("Chọn vai trò", options=['Quản lý', 'Nhân viên', 'Phòng điều dưỡng'])
        submit_user = st.form_submit_button("Thêm người dùng")
        if submit_user:
            if not new_username.strip():
                st.error("Tên đăng nhập không được để trống.")
            else:
                if add_user(new_username, new_role):
                    st.success("Đã tạo người dùng mới.")
                else:
                    st.error("Tên đăng nhập đã tồn tại.")
    # Display existing users with role editing and delete option
    users_df = get_users()
    st.subheader("Danh sách người dùng")
    if users_df.empty:
        st.info("Chưa có người dùng.")
    else:
        for _, row in users_df.iterrows():
            cols = st.columns([0.4, 0.3, 0.2, 0.1])
            cols[0].markdown(f"**{row['username']}**")
            current_role = row['role']
            new_role_value = cols[1].selectbox(
                "Vai trò",
                options=['Quản lý', 'Nhân viên', 'Phòng điều dưỡng'],
                index=['Quản lý', 'Nhân viên', 'Phòng điều dưỡng'].index(current_role),
                key=f"role_{row['id']}"
            )
            if cols[2].button("Cập nhật", key=f"update_user_{row['id']}"):
                if new_role_value != current_role:
                    update_user_role(row['id'], new_role_value)
                    st.success(f"Đã cập nhật vai trò cho {row['username']}")
                    st.experimental_rerun()
                else:
                    st.info("Không có thay đổi về vai trò.")
            if cols[3].button("Xoá", key=f"delete_user_{row['id']}"):
                if row['username'] == st.session_state.username:
                    st.warning("Không thể xoá chính bạn.")
                else:
                    delete_user(row['id'])
                    st.success(f"Đã xoá người dùng {row['username']}")
                    st.experimental_rerun()

# Page for declaring inventory (for staff)
def page_declare_holdings():
    st.header("Khai báo tồn kho")
    st.info("Nhập thông tin cơ số ban đầu cho tài sản và gửi duyệt.")
    assets_df = get_assets()
    rooms_df = get_rooms()
    if assets_df.empty or rooms_df.empty:
        st.info("Cần có Tài sản và Phòng trước khi khai báo.")
        return
    with st.form("declare_holding_form"):
        asset_option = st.selectbox("Chọn tài sản", options=assets_df['name'].tolist())
        room_option = st.selectbox("Chọn Phòng", options=rooms_df['name'].tolist())
        serial = st.text_input("Số serial (có thể để trống nếu không có)")
        origin = st.text_input("Nơi sản xuất (tuỳ chọn)")
        baseline = st.number_input("Cơ số ban đầu", min_value=0, step=1, value=0)
        submit_declare = st.form_submit_button("Gửi khai báo")
        if submit_declare:
            asset_id = assets_df[assets_df['name'] == asset_option]['id'].values[0]
            room_id = rooms_df[rooms_df['name'] == room_option]['id'].values[0]
            add_holding_request(
                asset_id,
                room_id,
                serial if serial else None,
                int(baseline),
                st.session_state.username,
                origin=origin if origin else None,
            )
            st.success("Đã gửi yêu cầu khai báo tồn kho. Chờ phê duyệt.")
    # Show user's requests
    st.subheader("Các yêu cầu của bạn")
    req_df = pd.read_sql_query(
        "SELECT id, status, date_requested, baseline_qty, asset_id, room_id FROM holding_requests WHERE requested_by=? ORDER BY date_requested DESC",
        conn,
        params=(st.session_state.username,),
    )
    if not req_df.empty:
        # Add asset and room names
        req_df['Tên tài sản'] = req_df['asset_id'].map(assets_df.set_index('id')['name'])
        req_df['Phòng'] = req_df['room_id'].map(rooms_df.set_index('id')['name'])
        display_df = req_df.rename(columns={'id': 'Mã', 'status': 'Trạng thái', 'date_requested': 'Ngày gửi', 'baseline_qty': 'Cơ số khai báo'})
        st.dataframe(display_df[['Mã','Ngày gửi','Tên tài sản','Phòng','Cơ số khai báo','Trạng thái']])

# Run the app
if __name__ == "__main__":
    main()
