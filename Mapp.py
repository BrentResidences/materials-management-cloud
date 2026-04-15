
from __future__ import annotations

from contextlib import closing
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Optional
import hashlib
import hmac
import secrets
import re

import pandas as pd
import psycopg
from pypdf import PdfReader
from psycopg.rows import dict_row
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


# -----------------------------
# Database helpers
# -----------------------------

def _connect_new() -> psycopg.Connection:
    return psycopg.connect(
        host=st.secrets["connections"]["postgresql"]["host"],
        dbname=st.secrets["connections"]["postgresql"]["database"],
        user=st.secrets["connections"]["postgresql"]["username"],
        password=st.secrets["connections"]["postgresql"]["password"],
        port=st.secrets["connections"]["postgresql"]["port"],
        sslmode=st.secrets["connections"]["postgresql"].get("sslmode", "require"),
        row_factory=dict_row,
        autocommit=False,
    )


def get_conn() -> psycopg.Connection:
    try:
        conn = st.session_state.get("_db_conn")
        if conn is None or getattr(conn, "closed", False):
            conn = _connect_new()
            st.session_state["_db_conn"] = conn
        else:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _connect_new()
                st.session_state["_db_conn"] = conn
        return conn
    except Exception as exc:
        st.error("Could not connect to Neon / PostgreSQL. Check your Streamlit secrets.")
        raise exc


def _normalize_params(params: tuple | list | None = ()) -> tuple:
    if params is None:
        return ()
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    return (params,)


def execute(sql: str, params: tuple = ()) -> None:
    params = _normalize_params(params)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    cached_query_df.clear()
    cached_query_one.clear()
    get_lookup_data.clear()
    get_dashboard_data.clear()


@st.cache_data(ttl=120, show_spinner=False)
def cached_query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    params = _normalize_params(params)
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc.name if hasattr(desc, "name") else desc[0] for desc in (cur.description or [])]
        if not rows:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(rows, columns=columns)


def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    return cached_query_df(sql, _normalize_params(params)).copy()


@st.cache_data(ttl=120, show_spinner=False)
def cached_query_one(sql: str, params: tuple = ()) -> Optional[dict[str, Any]]:
    params = _normalize_params(params)
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def query_one(sql: str, params: tuple = ()) -> Optional[dict[str, Any]]:
    return cached_query_one(sql, _normalize_params(params))


def init_db() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS company_categories (
        category_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        category_name TEXT NOT NULL UNIQUE,
        active INTEGER DEFAULT 1,
        sort_order INTEGER,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS company_subcategories (
        subcategory_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        category_id INTEGER NOT NULL REFERENCES company_categories(category_id) ON DELETE CASCADE,
        subcategory_name TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        sort_order INTEGER,
        notes TEXT,
        UNIQUE (category_id, subcategory_name)
    );

    CREATE TABLE IF NOT EXISTS units_of_measure (
        unit_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        unit_name TEXT NOT NULL UNIQUE,
        unit_abbreviation TEXT,
        measurement_system TEXT,
        unit_type TEXT,
        active INTEGER DEFAULT 1,
        sort_order INTEGER,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS materials (
        material_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        internal_material_code TEXT,
        material_name TEXT NOT NULL,
        full_description TEXT,
        category_id INTEGER REFERENCES company_categories(category_id),
        subcategory_id INTEGER REFERENCES company_subcategories(subcategory_id),
        default_unit_id INTEGER REFERENCES units_of_measure(unit_id),
        manufacturer TEXT,
        model_number TEXT,
        dimension_display TEXT,
        notes TEXT,
        active INTEGER DEFAULT 1,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        date_modified TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS material_vendor_current (
        material_vendor_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        material_id INTEGER NOT NULL REFERENCES materials(material_id) ON DELETE CASCADE,
        vendor_name TEXT NOT NULL,
        vendor_item_number TEXT,
        vendor_store_number TEXT,
        store_aisle TEXT,
        latest_retail_price NUMERIC(12,2),
        latest_retail_price_date DATE,
        latest_quoted_price NUMERIC(12,2),
        latest_quoted_price_date DATE,
        vendor_notes TEXT,
        active INTEGER DEFAULT 1,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        date_modified TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS projects (
        project_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        project_name TEXT NOT NULL,
        property_name TEXT,
        unit_or_location TEXT,
        project_description TEXT,
        status TEXT,
        notes TEXT,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        date_modified TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS project_work_items (
        work_item_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
        work_item_name TEXT NOT NULL,
        work_item_description TEXT,
        sort_order INTEGER,
        notes TEXT,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        date_modified TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS work_item_materials (
        work_item_material_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        work_item_id INTEGER NOT NULL REFERENCES project_work_items(work_item_id) ON DELETE CASCADE,
        material_id INTEGER REFERENCES materials(material_id),
        material_vendor_id INTEGER REFERENCES material_vendor_current(material_vendor_id),
        line_material_name_snapshot TEXT,
        line_description_snapshot TEXT,
        line_category_snapshot TEXT,
        line_subcategory_snapshot TEXT,
        line_vendor_name_snapshot TEXT,
        line_vendor_item_number_snapshot TEXT,
        quantity NUMERIC(12,2) NOT NULL,
        unit_id INTEGER REFERENCES units_of_measure(unit_id),
        unit_price NUMERIC(12,2) NOT NULL,
        line_total NUMERIC(14,2) NOT NULL,
        notes TEXT,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        date_modified TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_materials_name ON materials(material_name);
    CREATE INDEX IF NOT EXISTS idx_materials_category ON materials(category_id);
    CREATE INDEX IF NOT EXISTS idx_vendor_item_number ON material_vendor_current(vendor_item_number);
    CREATE INDEX IF NOT EXISTS idx_work_item_materials_work_item ON work_item_materials(work_item_id);
    CREATE INDEX IF NOT EXISTS idx_projects_name ON projects(project_name);
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    seed_defaults()


def seed_defaults() -> None:
    categories = [
        "Appliances", "Bath Fixtures", "Cabinets - Kitchen", "Cabinets - Bath",
        "Clean out and Demo", "Cleaning Materials", "Concrete", "Concrete Block",
        "Concrete Forms", "Countertop", "Décor & Privacy", "Doors", "Drainage",
        "Drywall Supplies", "Electrical", "Fasteners", "Fencing", "Flooring",
        "Framing", "Guttering", "Hardware", "House Wrap", "HVAC", "Insulation",
        "Internet & Wifi", "Landscaping", "Lumber", "Mailboxes", "Other Items",
        "Paint Materials", "Plumbing", "Roofing", "Safety", "Shelving & Closet Rods",
        "Siding & Fascia", "Smoke Detectors", "Tile", "Tools & Equipment",
        "Trim & Base", "Windows"
    ]
    units = [
        ("Each", "EA", "Count", "Count"),
        ("Box", "BX", "Package", "Count"),
        ("Bag", "BAG", "Package", "Count"),
        ("Bundle", "BDL", "Package", "Count"),
        ("Roll", "ROLL", "Package", "Count"),
        ("Sheet", "SHT", "Count", "Count"),
        ("Stick", "STK", "Count", "Count"),
        ("Gallon", "GAL", "Imperial", "Volume"),
        ("Quart", "QT", "Imperial", "Volume"),
        ("Linear Foot", "LF", "Imperial", "Length"),
        ("Square Foot", "SF", "Imperial", "Area"),
        ("Cubic Yard", "CY", "Imperial", "Volume"),
        ("Inch", "IN", "Imperial", "Length"),
        ("Foot", "FT", "Imperial", "Length"),
        ("Millimeter", "MM", "Metric", "Length"),
        ("Centimeter", "CM", "Metric", "Length"),
        ("Meter", "M", "Metric", "Length"),
        ("Square Meter", "M2", "Metric", "Area"),
        ("Liter", "L", "Metric", "Volume"),
        ("Kilogram", "KG", "Metric", "Weight"),
    ]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for name in categories:
                cur.execute(
                    "INSERT INTO company_categories (category_name, active) VALUES (%s, 1) ON CONFLICT (category_name) DO NOTHING",
                    (name,),
                )
            for row in units:
                cur.execute(
                    """
                    INSERT INTO units_of_measure
                    (unit_name, unit_abbreviation, measurement_system, unit_type, active)
                    VALUES (%s, %s, %s, %s, 1)
                    ON CONFLICT (unit_name) DO NOTHING
                    """,
                    row,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


@st.cache_resource(show_spinner=False)
def ensure_db_ready() -> bool:
    init_db()
    return True


@st.cache_data(ttl=300, show_spinner=False)
def get_lookup_data() -> dict[str, pd.DataFrame]:
    return {
        "categories": query_df("SELECT category_id, category_name, active, sort_order, notes FROM company_categories ORDER BY category_name"),
        "subcategories": query_df(
            """
            SELECT s.subcategory_id, s.category_id, c.category_name, s.subcategory_name, s.active, s.sort_order, s.notes
            FROM company_subcategories s
            JOIN company_categories c ON s.category_id = c.category_id
            ORDER BY c.category_name, s.subcategory_name
            """
        ),
        "units": query_df("SELECT * FROM units_of_measure ORDER BY unit_name"),
        "active_units": query_df("SELECT unit_id, unit_name FROM units_of_measure WHERE active = 1 ORDER BY unit_name"),
        "active_categories": query_df("SELECT category_id, category_name FROM company_categories WHERE active = 1 ORDER BY category_name"),
        "active_materials": query_df(
            """
            SELECT m.material_id, m.material_name, u.unit_id AS default_unit_id
            FROM materials m
            LEFT JOIN units_of_measure u ON m.default_unit_id = u.unit_id
            WHERE m.active = 1
            ORDER BY m.material_name
            """
        ),
        "projects": query_df("SELECT project_id, project_name FROM projects ORDER BY project_name"),
    }


@st.cache_data(ttl=120, show_spinner=False)
def get_dashboard_data() -> dict[str, Any]:
    counts = query_one(
        """
        SELECT
            (SELECT COUNT(*) FROM company_categories WHERE active = 1) AS categories,
            (SELECT COUNT(*) FROM materials WHERE active = 1) AS materials,
            (SELECT COUNT(*) FROM projects) AS projects,
            (SELECT COUNT(*) FROM project_work_items) AS work_items
        """
    ) or {"categories": 0, "materials": 0, "projects": 0, "work_items": 0}
    recent_materials = format_dates(query_df(
        "SELECT material_id, material_name, manufacturer, model_number, dimension_display, date_created FROM materials ORDER BY material_id DESC LIMIT 10"
    ))
    recent_projects = format_dates(query_df(
        "SELECT project_id, project_name, property_name, unit_or_location, status, date_created FROM projects ORDER BY project_id DESC LIMIT 10"
    ))
    return {
        "counts": counts,
        "recent_materials": recent_materials,
        "recent_projects": recent_projects,
    }


@st.cache_data(ttl=120, show_spinner=False)
def search_material_selector(search_text: str, limit: int = 50) -> pd.DataFrame:
    term = (search_text or "").strip()
    if not term:
        return pd.DataFrame(columns=[
            "material_id", "material_name", "vendor_item_number", "vendor_name",
            "full_description", "default_unit_id", "default_unit_name", "latest_price", "display_label"
        ])

    like = f"%{term}%"
    digits_only = re.sub(r"\D", "", term)
    exact_numeric = digits_only if digits_only else ""
    exact_text = term

    sql = """
        SELECT *
        FROM (
            SELECT
                m.material_id,
                m.material_name,
                mvc.vendor_item_number,
                mvc.vendor_name,
                m.full_description,
                m.default_unit_id,
                u.unit_name AS default_unit_name,
                COALESCE(NULLIF(mvc.latest_quoted_price, 0), mvc.latest_retail_price, 0) AS latest_price,
                ROW_NUMBER() OVER (
                    PARTITION BY m.material_id
                    ORDER BY
                        CASE
                            WHEN %s <> '' AND COALESCE(mvc.vendor_item_number, '') = %s THEN 0
                            WHEN LOWER(COALESCE(m.material_name, '')) = LOWER(%s) THEN 1
                            WHEN COALESCE(mvc.vendor_item_number, '') ILIKE %s THEN 2
                            WHEN COALESCE(m.material_name, '') ILIKE %s THEN 3
                            WHEN COALESCE(m.full_description, '') ILIKE %s THEN 4
                            ELSE 5
                        END,
                        COALESCE(mvc.vendor_name, ''),
                        COALESCE(mvc.vendor_item_number, ''),
                        m.material_name
                ) AS rn,
                CASE
                    WHEN COALESCE(mvc.vendor_item_number, '') <> '' AND COALESCE(mvc.vendor_name, '') <> ''
                        THEN m.material_name || ' | Item ' || mvc.vendor_item_number || ' | ' || mvc.vendor_name
                    WHEN COALESCE(mvc.vendor_item_number, '') <> ''
                        THEN m.material_name || ' | Item ' || mvc.vendor_item_number
                    WHEN COALESCE(mvc.vendor_name, '') <> ''
                        THEN m.material_name || ' | ' || mvc.vendor_name
                    ELSE m.material_name
                END AS display_label
            FROM materials m
            LEFT JOIN material_vendor_current mvc
                ON mvc.material_id = m.material_id
               AND mvc.active = 1
            LEFT JOIN units_of_measure u
                ON m.default_unit_id = u.unit_id
            WHERE m.active = 1
              AND (
                    m.material_name ILIKE %s
                 OR COALESCE(m.full_description, '') ILIKE %s
                 OR COALESCE(mvc.vendor_item_number, '') ILIKE %s
                 OR COALESCE(mvc.vendor_name, '') ILIKE %s
              )
        ) ranked
        WHERE rn = 1
        ORDER BY
            CASE
                WHEN %s <> '' AND COALESCE(vendor_item_number, '') = %s THEN 0
                WHEN LOWER(COALESCE(material_name, '')) = LOWER(%s) THEN 1
                WHEN COALESCE(vendor_item_number, '') ILIKE %s THEN 2
                WHEN COALESCE(material_name, '') ILIKE %s THEN 3
                WHEN COALESCE(full_description, '') ILIKE %s THEN 4
                ELSE 5
            END,
            material_name,
            vendor_item_number NULLS LAST
        LIMIT %s
    """
    params = (
        exact_numeric, exact_numeric, exact_text, like, like, like,
        like, like, like, like,
        exact_numeric, exact_numeric, exact_text, like, like, like, limit,
    )
    return query_df(sql, params)


# -----------------------------
# Utility functions
# -----------------------------

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_vendor_item_number(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    s = str(value).strip()
    if s.lower() in {"nan", "none", "<na>"}:
        return ""
    m = re.fullmatch(r"([0-9]+)\.0+", s)
    if m:
        return m.group(1)
    m = re.fullmatch(r"([0-9]+)\.([0]+)", s)
    if m:
        return m.group(1)
    return s


def clean_all_vendor_item_numbers() -> int:
    df = query_df("SELECT material_vendor_id, vendor_item_number FROM material_vendor_current")
    if df.empty:
        return 0
    changed = 0
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                original = row.get("vendor_item_number")
                cleaned = normalize_vendor_item_number(original)
                original_str = "" if original is None else str(original).strip()
                if cleaned != original_str:
                    cur.execute(
                        "UPDATE material_vendor_current SET vendor_item_number = %s, date_modified = %s WHERE material_vendor_id = %s",
                        (cleaned or None, now_ts(), int(row["material_vendor_id"])),
                    )
                    changed += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    cached_query_df.clear()
    cached_query_one.clear()
    get_lookup_data.clear()
    get_dashboard_data.clear()
    return changed


def money(v: float | int | None) -> str:
    return f"${float(v or 0):,.2f}"


def format_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if "date" in col.lower():
            try:
                out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%m/%d/%Y")
            except Exception:
                pass
    return out


def item_sort_key(val: Any) -> tuple:
    s = "" if pd.isna(val) else str(val).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        try:
            return (0, int(digits), s)
        except Exception:
            return (1, s)
    return (1, s)


def sort_vendor_df_numeric(df: pd.DataFrame, item_col: str) -> pd.DataFrame:
    if df.empty or item_col not in df.columns:
        return df
    out = df.copy()
    out["_sort_key"] = out[item_col].apply(item_sort_key)
    out = out.sort_values(by="_sort_key").drop(columns=["_sort_key"])
    return out



PBKDF2_ITERATIONS = 200_000
ROLE_OPTIONS = ["Owner", "Materials Manager", "Contractor", "Other"]


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), PBKDF2_ITERATIONS)
    return f"{salt}${digest.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, expected = stored_hash.split("$", 1)
    except ValueError:
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), PBKDF2_ITERATIONS).hex()
    return hmac.compare_digest(digest, expected)


def count_users() -> int:
    row = query_one("SELECT COUNT(*) AS c FROM users")
    return int(row["c"] if row else 0)


def get_user_by_username(username: str) -> Optional[dict[str, Any]]:
    return query_one("SELECT * FROM users WHERE LOWER(username) = LOWER(%s)", (username.strip(),))


def ensure_session_defaults() -> None:
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("user", None)


def logout() -> None:
    conn = st.session_state.pop("_db_conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    st.session_state["logged_in"] = False
    st.session_state["user"] = None
    st.rerun()


def show_bootstrap_owner() -> None:
    st.title("Materials Management System")
    st.subheader("Create Owner Account")
    st.info("No users exist yet. Create the first Owner account to start using the system.")
    with st.form("bootstrap_owner_form"):
        username = st.text_input("Owner username")
        password = st.text_input("Password", type="password")
        confirm = st.text_input("Confirm password", type="password")
        submitted = st.form_submit_button("Create Owner")
        if submitted:
            if not username.strip() or not password:
                st.error("Username and password are required.")
            elif password != confirm:
                st.error("Passwords do not match.")
            else:
                execute(
                    "INSERT INTO users (username, password_hash, role, active, updated_at) VALUES (%s, %s, %s, %s, %s)",
                    (username.strip(), hash_password(password), "Owner", 1, now_ts()),
                )
                st.success("Owner account created. Please log in.")
                st.rerun()


def show_login() -> None:
    st.title("Materials Management System")
    st.subheader("Login")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in")
        if submitted:
            user = get_user_by_username(username)
            if not user:
                st.error("Invalid username or password.")
            elif int(user.get("active", 0)) != 1:
                st.error("This user is inactive.")
            elif not verify_password(password, user["password_hash"]):
                st.error("Invalid username or password.")
            else:
                st.session_state["logged_in"] = True
                st.session_state["user"] = dict(user)
                st.rerun()


def require_login() -> bool:
    ensure_session_defaults()
    if count_users() == 0:
        show_bootstrap_owner()
        return False
    if not st.session_state.get("logged_in") or not st.session_state.get("user"):
        show_login()
        return False
    current = get_user_by_username(st.session_state["user"]["username"])
    if not current or int(current.get("active", 0)) != 1:
        logout()
        return False
    st.session_state["user"] = dict(current)
    return True


def can_manage_users() -> bool:
    user = st.session_state.get("user") or {}
    return user.get("role") == "Owner"


def page_admin(admin_section: str | None = None) -> None:
    st.header("Admin")
    if not can_manage_users():
        st.warning("Only the Owner can access this page.")
        return

    if admin_section is None:
        admin_section = st.radio("Admin Section", ["Create User", "Manage Users"], horizontal=True, key="admin_section")

    if admin_section == "Create User":
        with st.form("create_user_form"):
            c1, c2 = st.columns(2)
            username = c1.text_input("Username")
            role = c2.selectbox("User type", ROLE_OPTIONS)
            password = st.text_input("Password", type="password")
            confirm = st.text_input("Confirm password", type="password")
            active = st.checkbox("Active", value=True)
            submitted = st.form_submit_button("Create User")
            if submitted:
                existing = get_user_by_username(username)
                if not username.strip() or not password:
                    st.error("Username and password are required.")
                elif existing:
                    st.error("That username already exists.")
                elif password != confirm:
                    st.error("Passwords do not match.")
                else:
                    execute(
                        "INSERT INTO users (username, password_hash, role, active, updated_at) VALUES (%s, %s, %s, %s, %s)",
                        (username.strip(), hash_password(password), role, 1 if active else 0, now_ts()),
                    )
                    st.success("User created.")
                    st.rerun()

    if admin_section == "Manage Users":
        users_df = query_df("SELECT user_id, username, role, active, created_at, updated_at FROM users ORDER BY username")
        users_df = format_dates(users_df)
        st.dataframe(users_df, use_container_width=True)
        if not users_df.empty:
            options = {f"{row['username']} | {row['role']}": int(row['user_id']) for _, row in users_df.iterrows()}
            selected = st.selectbox("Select user", list(options.keys()))
            user_row = query_one("SELECT * FROM users WHERE user_id = %s", (options[selected],))
            with st.form("manage_user_form"):
                c1, c2 = st.columns(2)
                role = c1.selectbox("User type", ROLE_OPTIONS, index=ROLE_OPTIONS.index(user_row['role']) if user_row['role'] in ROLE_OPTIONS else 0)
                active = c2.checkbox("Active", value=bool(user_row['active']))
                new_password = st.text_input("New password (leave blank to keep current)", type="password")
                save = st.form_submit_button("Save User Changes")
                if save:
                    if new_password:
                        execute(
                            "UPDATE users SET role = %s, active = %s, password_hash = %s, updated_at = %s WHERE user_id = %s",
                            (role, 1 if active else 0, hash_password(new_password), now_ts(), user_row['user_id']),
                        )
                    else:
                        execute(
                            "UPDATE users SET role = %s, active = %s, updated_at = %s WHERE user_id = %s",
                            (role, 1 if active else 0, now_ts(), user_row['user_id']),
                        )
                    st.success("User updated.")
                    if st.session_state.get('user', {}).get('user_id') == user_row['user_id']:
                        st.session_state['user'] = dict(query_one("SELECT * FROM users WHERE user_id = %s", (user_row['user_id'],)))
                    st.rerun()

def add_material_line_from_master(
    work_item_id: int,
    material_id: int,
    quantity: float,
    unit_id: int,
    unit_price: float,
    vendor_id: Optional[int],
    notes: str = "",
) -> None:
    material = query_one(
        """
        SELECT m.material_id, m.material_name, m.full_description,
               c.category_name, s.subcategory_name
        FROM materials m
        LEFT JOIN company_categories c ON m.category_id = c.category_id
        LEFT JOIN company_subcategories s ON m.subcategory_id = s.subcategory_id
        WHERE m.material_id = %s
        """,
        (material_id,),
    )
    if material is None:
        raise ValueError("Material not found.")

    vendor_name = None
    vendor_item_number = None
    snapshot_vendor_id = vendor_id

    if vendor_id:
        vendor = query_one(
            "SELECT vendor_name, vendor_item_number FROM material_vendor_current WHERE material_vendor_id = %s",
            (vendor_id,),
        )
        if vendor:
            vendor_name = vendor["vendor_name"]
            vendor_item_number = vendor["vendor_item_number"]
    else:
        fallback_vendor = query_one(
            """
            SELECT material_vendor_id, vendor_name, vendor_item_number
            FROM material_vendor_current
            WHERE material_id = %s
              AND active = 1
            ORDER BY
                CASE WHEN COALESCE(vendor_item_number, '') <> '' THEN 0 ELSE 1 END,
                material_vendor_id
            LIMIT 1
            """,
            (material_id,),
        )
        if fallback_vendor:
            snapshot_vendor_id = fallback_vendor["material_vendor_id"]
            vendor_name = fallback_vendor["vendor_name"]
            vendor_item_number = fallback_vendor["vendor_item_number"]

    execute(
        """
        INSERT INTO work_item_materials (
            work_item_id, material_id, material_vendor_id,
            line_material_name_snapshot, line_description_snapshot,
            line_category_snapshot, line_subcategory_snapshot,
            line_vendor_name_snapshot, line_vendor_item_number_snapshot,
            quantity, unit_id, unit_price, line_total, notes, date_modified
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            work_item_id,
            material_id,
            snapshot_vendor_id,
            material["material_name"],
            material["full_description"],
            material["category_name"],
            material["subcategory_name"],
            vendor_name,
            vendor_item_number,
            quantity,
            unit_id,
            unit_price,
            quantity * unit_price,
            notes,
            now_ts(),
        ),
    )

def update_material_line_from_master(
    work_item_material_id: int,
    material_id: int,
    quantity: float,
    unit_id: int,
    unit_price: float,
    vendor_id: Optional[int],
    notes: str = "",
) -> None:
    line_row = query_one(
        "SELECT work_item_id FROM work_item_materials WHERE work_item_material_id = %s",
        (work_item_material_id,),
    )
    if line_row is None:
        raise ValueError("Material line not found.")

    material = query_one(
        """
        SELECT m.material_id, m.material_name, m.full_description,
               c.category_name, s.subcategory_name
        FROM materials m
        LEFT JOIN company_categories c ON m.category_id = c.category_id
        LEFT JOIN company_subcategories s ON m.subcategory_id = s.subcategory_id
        WHERE m.material_id = %s
        """,
        (material_id,),
    )
    if material is None:
        raise ValueError("Material not found.")

    vendor_name = None
    vendor_item_number = None
    if vendor_id:
        vendor = query_one(
            "SELECT vendor_name, vendor_item_number FROM material_vendor_current WHERE material_vendor_id = %s",
            (vendor_id,),
        )
        if vendor:
            vendor_name = vendor["vendor_name"]
            vendor_item_number = vendor["vendor_item_number"]

    execute(
        """
        UPDATE work_item_materials
        SET material_id = %s,
            material_vendor_id = %s,
            line_material_name_snapshot = %s,
            line_description_snapshot = %s,
            line_category_snapshot = %s,
            line_subcategory_snapshot = %s,
            line_vendor_name_snapshot = %s,
            line_vendor_item_number_snapshot = %s,
            quantity = %s,
            unit_id = %s,
            unit_price = %s,
            line_total = %s,
            notes = %s,
            date_modified = %s
        WHERE work_item_material_id = %s
        """,
        (
            material_id,
            vendor_id,
            material["material_name"],
            material["full_description"],
            material["category_name"],
            material["subcategory_name"],
            vendor_name,
            vendor_item_number,
            quantity,
            unit_id,
            unit_price,
            quantity * unit_price,
            notes,
            now_ts(),
            work_item_material_id,
        ),
    )


def copy_material_lines_between_subprojects(
    source_work_item_id: int,
    target_work_item_id: int,
    selected_line_ids: Optional[list[int]] = None,
) -> int:
    if source_work_item_id == target_work_item_id:
        return 0

    params: list[Any] = [source_work_item_id]
    filter_sql = ""
    if selected_line_ids:
        placeholders = ", ".join(["%s"] * len(selected_line_ids))
        filter_sql = f" AND work_item_material_id IN ({placeholders})"
        params.extend(selected_line_ids)

    rows = query_df(
        f"""
        SELECT material_id, material_vendor_id,
               line_material_name_snapshot, line_description_snapshot,
               line_category_snapshot, line_subcategory_snapshot,
               line_vendor_name_snapshot, line_vendor_item_number_snapshot,
               quantity, unit_id, unit_price, line_total, notes
        FROM work_item_materials
        WHERE work_item_id = %s{filter_sql}
        ORDER BY work_item_material_id
        """,
        tuple(params),
    )
    if rows.empty:
        return 0

    conn = get_conn()
    inserted = 0
    try:
        with conn.cursor() as cur:
            for _, row in rows.iterrows():
                cur.execute(
                    """
                    INSERT INTO work_item_materials (
                        work_item_id, material_id, material_vendor_id,
                        line_material_name_snapshot, line_description_snapshot,
                        line_category_snapshot, line_subcategory_snapshot,
                        line_vendor_name_snapshot, line_vendor_item_number_snapshot,
                        quantity, unit_id, unit_price, line_total, notes, date_modified
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        target_work_item_id,
                        None if pd.isna(row["material_id"]) else int(row["material_id"]),
                        None if pd.isna(row["material_vendor_id"]) else int(row["material_vendor_id"]),
                        row["line_material_name_snapshot"],
                        row["line_description_snapshot"],
                        row["line_category_snapshot"],
                        row["line_subcategory_snapshot"],
                        row["line_vendor_name_snapshot"],
                        row["line_vendor_item_number_snapshot"],
                        float(row["quantity"] or 0),
                        None if pd.isna(row["unit_id"]) else int(row["unit_id"]),
                        float(row["unit_price"] or 0),
                        float(row["line_total"] or 0),
                        row["notes"] or "",
                        now_ts(),
                    ),
                )
                inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    cached_query_df.clear()
    cached_query_one.clear()
    get_lookup_data.clear()
    get_dashboard_data.clear()
    return inserted


# -----------------------------
# Report builders
# -----------------------------

def build_project_report_pdf(project_id: int) -> BytesIO:
    project = query_one("SELECT * FROM projects WHERE project_id = %s", (project_id,))
    if project is None:
        raise ValueError("Project not found.")

    rows = query_df(
        """
        SELECT wi.work_item_name,
               wim.line_category_snapshot,
               wim.line_subcategory_snapshot,
               wim.line_material_name_snapshot,
               wim.line_description_snapshot,
               u.unit_abbreviation,
               wim.quantity,
               wim.unit_price,
               wim.line_total
        FROM work_item_materials wim
        JOIN project_work_items wi ON wim.work_item_id = wi.work_item_id
        LEFT JOIN units_of_measure u ON wim.unit_id = u.unit_id
        WHERE wi.project_id = %s
        ORDER BY wi.sort_order NULLS LAST, wi.work_item_name,
                 wim.line_category_snapshot, wim.line_subcategory_snapshot,
                 wim.line_material_name_snapshot
        """,
        (project_id,),
    )

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Project Materials Report: {project['project_name']}", styles["Title"]))
    story.append(Paragraph(f"Property: {project.get('property_name') or ''}", styles["Normal"]))
    story.append(Paragraph(f"Location: {project.get('unit_or_location') or ''}", styles["Normal"]))
    story.append(Spacer(1, 12))

    grand_total = 0.0
    for work_item_name, wi_df in rows.groupby("work_item_name", dropna=False):
        story.append(Paragraph(f"Sub-Project: {work_item_name}", styles["Heading2"]))
        work_item_total = 0.0

        for category_name, cat_df in wi_df.groupby("line_category_snapshot", dropna=False):
            cat_label = category_name if pd.notna(category_name) else "Uncategorized"
            story.append(Paragraph(f"Category: {cat_label}", styles["Heading3"]))

            data = [["Sub-category", "Material", "Description", "Qty", "Unit", "Unit Price", "Line Total"]]
            cat_total = 0.0
            for _, row in cat_df.iterrows():
                data.append([
                    row["line_subcategory_snapshot"] or "",
                    row["line_material_name_snapshot"] or "",
                    row["line_description_snapshot"] or "",
                    f"{float(row['quantity']):,.2f}",
                    row["unit_abbreviation"] or "",
                    money(row["unit_price"]),
                    money(row["line_total"]),
                ])
                cat_total += float(row["line_total"] or 0.0)

            table = Table(data, repeatRows=1, colWidths=[80, 110, 150, 45, 45, 60, 65])
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]))
            story.append(table)
            story.append(Paragraph(f"Category Total: {money(cat_total)}", styles["Normal"]))
            story.append(Spacer(1, 8))
            work_item_total += cat_total

        story.append(Paragraph(f"Sub-Project Total: {money(work_item_total)}", styles["Heading3"]))
        story.append(Spacer(1, 12))
        grand_total += work_item_total

    story.append(Paragraph(f"Project Grand Total: {money(grand_total)}", styles["Title"]))
    doc.build(story)
    buffer.seek(0)
    return buffer


def _vendor_project_df(project_id: int) -> pd.DataFrame:
    df = query_df(
        """
        SELECT COALESCE(wim.line_vendor_name_snapshot, 'No Vendor Assigned') AS vendor_name,
               COALESCE(wim.line_vendor_item_number_snapshot, '') AS item_number,
               COALESCE(wim.line_material_name_snapshot, '') AS item_name,
               COALESCE(wim.line_description_snapshot, '') AS description,
               COALESCE(wim.unit_price, 0) AS latest_price
        FROM work_item_materials wim
        JOIN project_work_items wi ON wim.work_item_id = wi.work_item_id
        WHERE wi.project_id = %s
        """,
        (project_id,),
    )
    return sort_vendor_df_numeric(df, "item_number")


def build_vendor_report_pdf(project_id: int) -> BytesIO:
    project = query_one("SELECT * FROM projects WHERE project_id = %s", (project_id,))
    if project is None:
        raise ValueError("Project not found.")
    rows = _vendor_project_df(project_id)

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), rightMargin=24, leftMargin=24, topMargin=24, bottomMargin=24)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Vendor Purchasing Report: {project['project_name']}", styles["Title"]))
    story.append(Paragraph(f"Property: {project.get('property_name') or ''}", styles["Normal"]))
    story.append(Paragraph(f"Location: {project.get('unit_or_location') or ''}", styles["Normal"]))
    story.append(Spacer(1, 12))

    for vendor_name, vendor_df in rows.groupby("vendor_name", dropna=False):
        story.append(Paragraph(f"Vendor: {vendor_name}", styles["Heading2"]))
        data = [["Item Number", "Item Name", "Description", "Latest Price"]]
        for _, row in vendor_df.iterrows():
            data.append([
                row["item_number"] or "",
                row["item_name"] or "",
                row["description"] or "",
                money(row["latest_price"]),
            ])

        table = Table(data, repeatRows=1, colWidths=[100, 180, 430, 90])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("WORDWRAP", (0, 0), (-1, -1), "LTR"),
        ]))
        story.append(table)
        story.append(Spacer(1, 12))

    doc.build(story)
    buffer.seek(0)
    return buffer


def build_vendor_report_excel(project_id: int) -> BytesIO:
    df = _vendor_project_df(project_id).rename(
        columns={
            "vendor_name": "Vendor",
            "item_number": "Item Number",
            "item_name": "Item Name",
            "description": "Description",
            "latest_price": "Latest Price",
        }
    )

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Vendor Report")
        ws = writer.book["Vendor Report"]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        widths = {"A": 18, "B": 28, "C": 65, "D": 14, "E": 18}
        for col, width in widths.items():
            ws.column_dimensions[col].width = width
        for cell in ws[1]:
            cell.font = cell.font.copy(bold=True)
        for row in ws.iter_rows(min_row=2, min_col=4, max_col=4):
            for cell in row:
                cell.number_format = "$#,##0.00"
        for row in ws.iter_rows(min_row=2, min_col=3, max_col=3):
            for cell in row:
                cell.alignment = cell.alignment.copy(wrap_text=True)
    buffer.seek(0)
    return buffer


def _vendor_master_df(selected_vendor: str) -> pd.DataFrame:
    df = query_df(
        """
        SELECT mvc.vendor_item_number AS item_number,
               m.material_name AS item_name,
               COALESCE(m.full_description, '') AS description,
               COALESCE(mvc.latest_retail_price, 0) AS latest_price
        FROM material_vendor_current mvc
        JOIN materials m ON mvc.material_id = m.material_id
        WHERE mvc.active = 1 AND mvc.vendor_name = %s
        """,
        (selected_vendor,),
    )
    return sort_vendor_df_numeric(df, "item_number")


def build_vendor_master_pdf(selected_vendor: str) -> BytesIO:
    rows = _vendor_master_df(selected_vendor)
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), rightMargin=24, leftMargin=24, topMargin=24, bottomMargin=24)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Vendor Master Report: {selected_vendor}", styles["Title"]))
    story.append(Spacer(1, 12))

    data = [["Item Number", "Item Name", "Description", "Latest Price"]]
    for _, row in rows.iterrows():
        data.append([
            row["item_number"] or "",
            row["item_name"] or "",
            row["description"] or "",
            money(row["latest_price"]),
        ])

    table = Table(data, repeatRows=1, colWidths=[100, 180, 430, 90])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("WORDWRAP", (0, 0), (-1, -1), "LTR"),
    ]))
    story.append(table)
    doc.build(story)
    buffer.seek(0)
    return buffer


def build_vendor_master_excel(selected_vendor: str) -> BytesIO:
    df = _vendor_master_df(selected_vendor).rename(
        columns={
            "item_number": "Item Number",
            "item_name": "Item Name",
            "description": "Description",
            "latest_price": "Latest Price",
        }
    )
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Vendor Master")
        ws = writer.book["Vendor Master"]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        widths = {"A": 18, "B": 28, "C": 70, "D": 14}
        for col, width in widths.items():
            ws.column_dimensions[col].width = width
        for cell in ws[1]:
            cell.font = cell.font.copy(bold=True)
        for row in ws.iter_rows(min_row=2, min_col=4, max_col=4):
            for cell in row:
                cell.number_format = "$#,##0.00"
        for row in ws.iter_rows(min_row=2, min_col=3, max_col=3):
            for cell in row:
                cell.alignment = cell.alignment.copy(wrap_text=True)
    buffer.seek(0)
    return buffer

@st.cache_data(show_spinner=False)
def parse_vendor_checklist_pdf(file_bytes: bytes) -> tuple[pd.DataFrame, str | None]:
    rows: list[dict[str, Any]] = []
    checklist_date: str | None = None

    def clean_text(value: Any) -> str:
        if value is None:
            return ""
        return " ".join(str(value).replace("\n", " ").split()).strip()

    def parse_price(value: str) -> float | None:
        value = clean_text(value)
        if not value:
            return None
        try:
            return float(value.replace("$", "").replace(",", ""))
        except Exception:
            return None

    reader = PdfReader(BytesIO(file_bytes))
    pattern = re.compile(r'^\s*(?P<item>-|\d+)\s+(?P<desc>.+?)\s{2,}(?P<name>.+?)\s+(?P<price>\$?[\d,]+(?:\.\d{2})?)\s*$')

    for page in reader.pages:
        page_text = page.extract_text() or ""
        if checklist_date is None:
            m = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})", page_text)
            if m:
                try:
                    checklist_date = datetime.strptime(m.group(1), "%B %d, %Y").date().isoformat()
                except Exception:
                    checklist_date = None

        pending: dict[str, Any] | None = None
        for raw_line in page_text.splitlines():
            line = clean_text(raw_line)
            if not line:
                continue
            low = line.lower()
            if low.startswith('vendor material checklist') or low.startswith('item # description name retail price') or low.startswith('page '):
                continue
            if ' item(s)' in low:
                continue
            if re.fullmatch(r'[A-Z][a-z]+ \d{1,2}, \d{4}', line):
                continue

            m = pattern.match(line)
            if m:
                item_no = m.group('item').strip()
                description = clean_text(m.group('desc'))
                material_name = clean_text(m.group('name'))
                retail_price = parse_price(m.group('price'))
                normalized_item = '' if item_no == '-' else item_no
                pending = {
                    'vendor_item_number': normalized_item,
                    'description': description,
                    'material_name': material_name or description,
                    'latest_retail_price': retail_price,
                }
                rows.append(pending)
                continue

            if pending is not None:
                if pending['latest_retail_price'] is None:
                    tail = re.search(r'(\$?[\d,]+(?:\.\d{2})?)\s*$', line)
                    if tail:
                        pending['latest_retail_price'] = parse_price(tail.group(1))
                        line = clean_text(line[:tail.start()])
                if line:
                    pending['description'] = (pending['description'] + ' ' + line).strip()
                    if not pending.get('material_name'):
                        pending['material_name'] = pending['description']

    df = pd.DataFrame(rows)
    if df.empty:
        return df, checklist_date

    df["vendor_item_number"] = df["vendor_item_number"].fillna("").astype(str).str.strip()
    df["description"] = df["description"].fillna("").astype(str).str.strip()
    df["material_name"] = df["material_name"].fillna("").astype(str).str.strip()
    df = df[(df["description"] != "") | (df["material_name"] != "")].copy()
    df["dedupe_key"] = df.apply(
        lambda r: (r["vendor_item_number"].lower(), r["material_name"].lower(), r["description"].lower()), axis=1
    )
    df = df.drop_duplicates(subset=["dedupe_key"]).drop(columns=["dedupe_key"])
    return df.reset_index(drop=True), checklist_date


def import_vendor_checklist_df(
    import_df: pd.DataFrame,
    vendor_name: str,
    retail_price_date: str | None = None,
    category_id: int | None = None,
    unit_id: int | None = None,
    update_existing: bool = True,
) -> dict[str, int]:
    results = {"inserted": 0, "updated": 0, "skipped": 0}
    if import_df is None or import_df.empty:
        return results

    conn = get_conn()
    with conn.cursor() as cur:
        for _, row in import_df.iterrows():
            item_number = normalize_vendor_item_number(row.get("vendor_item_number"))
            description = str(row.get("description") or "").strip()
            material_name = str(row.get("material_name") or description or "").strip()
            latest_retail_price = row.get("latest_retail_price")
            if not material_name:
                results["skipped"] += 1
                continue

            existing_vendor = None
            if item_number:
                cur.execute(
                    """
                    SELECT material_vendor_id, material_id
                    FROM material_vendor_current
                    WHERE LOWER(vendor_name) = LOWER(%s)
                      AND COALESCE(vendor_item_number, '') = %s
                    ORDER BY material_vendor_id
                    LIMIT 1
                    """,
                    (vendor_name, item_number),
                )
                existing_vendor = cur.fetchone()
            else:
                cur.execute(
                    """
                    SELECT mvc.material_vendor_id, mvc.material_id
                    FROM material_vendor_current mvc
                    JOIN materials m ON mvc.material_id = m.material_id
                    WHERE LOWER(mvc.vendor_name) = LOWER(%s)
                      AND LOWER(COALESCE(m.material_name, '')) = LOWER(%s)
                      AND LOWER(COALESCE(m.full_description, '')) = LOWER(%s)
                    ORDER BY mvc.material_vendor_id
                    LIMIT 1
                    """,
                    (vendor_name, material_name, description),
                )
                existing_vendor = cur.fetchone()

            if existing_vendor:
                if update_existing:
                    cur.execute(
                        """
                        UPDATE materials
                        SET material_name = %s,
                            full_description = %s,
                            category_id = COALESCE(category_id, %s),
                            default_unit_id = COALESCE(default_unit_id, %s),
                            date_modified = %s
                        WHERE material_id = %s
                        """,
                        (material_name, description, category_id, unit_id, now_ts(), existing_vendor["material_id"]),
                    )
                    cur.execute(
                        """
                        UPDATE material_vendor_current
                        SET latest_retail_price = %s,
                            latest_retail_price_date = %s,
                            vendor_item_number = %s,
                            date_modified = %s
                        WHERE material_vendor_id = %s
                        """,
                        (latest_retail_price, retail_price_date, item_number or None, now_ts(), existing_vendor["material_vendor_id"]),
                    )
                    results["updated"] += 1
                else:
                    results["skipped"] += 1
                continue

            cur.execute(
                """
                INSERT INTO materials (
                    internal_material_code, material_name, full_description,
                    category_id, subcategory_id, default_unit_id,
                    manufacturer, model_number, dimension_display,
                    notes, active, date_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING material_id
                """,
                (
                    item_number or None,
                    material_name,
                    description,
                    category_id,
                    None,
                    unit_id,
                    vendor_name,
                    None,
                    None,
                    f"Imported from {vendor_name} checklist PDF",
                    1,
                    now_ts(),
                ),
            )
            material_id = cur.fetchone()["material_id"]

            cur.execute(
                """
                INSERT INTO material_vendor_current (
                    material_id, vendor_name, vendor_item_number,
                    vendor_store_number, store_aisle,
                    latest_retail_price, latest_retail_price_date,
                    latest_quoted_price, latest_quoted_price_date,
                    vendor_notes, active, date_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    material_id,
                    vendor_name,
                    item_number or None,
                    None,
                    None,
                    latest_retail_price,
                    retail_price_date,
                    None,
                    None,
                    "Imported from vendor checklist PDF",
                    1,
                    now_ts(),
                ),
            )
            results["inserted"] += 1

    conn.commit()
    cached_query_df.clear()
    cached_query_one.clear()
    get_lookup_data.clear()
    get_dashboard_data.clear()
    return results



def standardize_vendor_import_df(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, str | None, str | None]:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=["vendor_item_number", "description", "material_name", "latest_retail_price"]), None, None

    df = raw_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def norm_col(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())

    col_lookup = {norm_col(c): c for c in df.columns}

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            key = norm_col(candidate)
            if key in col_lookup:
                return col_lookup[key]
        return None

    item_col = pick("vendor_item_number", "vendor item number", "item #", "item#", "item number", "item")
    desc_col = pick("description", "full_description", "full description")
    name_col = pick("material_name", "material name", "name", "item_name", "item name")
    price_col = pick("latest_retail_price", "latest retail price", "retail price", "price")
    vendor_col = pick("vendor_name", "vendor name", "vendor")
    date_col = pick("latest_retail_price_date", "latest retail price date", "retail price date", "price date")

    out = pd.DataFrame()
    out["vendor_item_number"] = df[item_col].apply(normalize_vendor_item_number) if item_col else ""
    out["description"] = df[desc_col].astype(str).str.strip() if desc_col else ""
    out["material_name"] = df[name_col].astype(str).str.strip() if name_col else ""
    out["latest_retail_price"] = pd.to_numeric(
        df[price_col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False),
        errors="coerce",
    ) if price_col else pd.Series([None] * len(df))

    out["vendor_item_number"] = out["vendor_item_number"].replace({"nan": "", "None": "", "<NA>": ""})
    out["description"] = out["description"].replace({"nan": "", "None": "", "<NA>": ""})
    out["material_name"] = out["material_name"].replace({"nan": "", "None": "", "<NA>": ""})

    out = out[(out["material_name"].astype(str).str.strip() != "") | (out["description"].astype(str).str.strip() != "")]
    out = out.reset_index(drop=True)

    vendor_default = None
    if vendor_col:
        vendor_values = [str(v).strip() for v in df[vendor_col].dropna().tolist() if str(v).strip()]
        unique_vendors = sorted(set(vendor_values))
        if len(unique_vendors) == 1:
            vendor_default = unique_vendors[0]

    price_date_default = None
    if date_col:
        date_values = [str(v).strip() for v in df[date_col].dropna().tolist() if str(v).strip()]
        if date_values:
            price_date_default = date_values[0]

    return out, vendor_default, price_date_default


def _safe_date_value(value: Any):
    if value in (None, "", pd.NaT):
        return None
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def parse_vendor_checklist_upload(file_bytes: bytes, file_name: str) -> tuple[pd.DataFrame, str | None, str | None]:
    suffix = Path(file_name or "").suffix.lower()
    if suffix == ".pdf":
        parsed_df, parsed_price_date = parse_vendor_checklist_pdf(file_bytes)
        return parsed_df, "Lowes", parsed_price_date

    if suffix == ".csv":
        raw_df = pd.read_csv(BytesIO(file_bytes))
    elif suffix in {".xlsx", ".xls"}:
        raw_df = pd.read_excel(BytesIO(file_bytes))
    else:
        raise ValueError("Unsupported file type. Use PDF, CSV, or Excel.")

    standardized_df, vendor_default, price_date_default = standardize_vendor_import_df(raw_df)
    return standardized_df, vendor_default, price_date_default



def page_materials_import_vendor_pdf() -> None:
    st.subheader("Import Vendor Checklist")
    st.caption("Upload a vendor checklist as PDF, CSV, or Excel. Expected columns are Item #, Description, Name, and Retail Price, or the equivalent import column names.")

    lookups = get_lookup_data()
    categories = lookups["active_categories"].copy()
    units = lookups["active_units"].copy()

    uploaded_file = st.file_uploader(
        "Vendor checklist file",
        type=["pdf", "csv", "xlsx", "xls"],
        key="vendor_checklist_upload",
    )
    if not uploaded_file:
        st.info("Upload a PDF, CSV, or Excel file to preview and import items.")
        return

    try:
        parsed_df, suggested_vendor_name, parsed_price_date = parse_vendor_checklist_upload(
            uploaded_file.getvalue(),
            uploaded_file.name,
        )
    except Exception as exc:
        st.error(f"Could not read that file: {exc}")
        return

    if parsed_df.empty:
        st.error("No importable rows were found in that file.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Parsed Items", len(parsed_df))
    c2.metric("With Item #", int((parsed_df["vendor_item_number"].astype(str).str.strip() != "").sum()))
    c3.metric("With Price", int(parsed_df["latest_retail_price"].notna().sum()))
    c4.metric("Date Found", parsed_price_date or "Not found")

    category_options = {"No Category": None}
    for _, row in categories.iterrows():
        category_options[str(row["category_name"])] = int(row["category_id"])

    unit_options = {"No Default Unit": None}
    for _, row in units.iterrows():
        unit_options[str(row["unit_name"])] = int(row["unit_id"])

    with st.form("import_vendor_checklist_form"):
        v1, v2, v3, v4 = st.columns(4)
        vendor_name = v1.text_input("Vendor Name", value=suggested_vendor_name or "Lowes")
        price_date_value = v2.text_input("Retail Price Date (YYYY-MM-DD)", value=parsed_price_date or "")
        category_name = v3.selectbox("Assign Category", list(category_options.keys()), index=list(category_options.keys()).index("No Category"))
        unit_name = v4.selectbox("Default Unit", list(unit_options.keys()), index=0)
        update_existing = st.checkbox("Update existing vendor items if they already exist", value=True)
        import_now = st.form_submit_button("Import Items Into Materials Database")

        if import_now:
            if not vendor_name.strip():
                st.error("Vendor name is required.")
            else:
                results = import_vendor_checklist_df(
                    parsed_df,
                    vendor_name=vendor_name.strip(),
                    retail_price_date=price_date_value.strip() or None,
                    category_id=category_options[category_name],
                    unit_id=unit_options[unit_name],
                    update_existing=update_existing,
                )
                st.success(
                    f"Import complete. Inserted {results['inserted']} items, updated {results['updated']} items, skipped {results['skipped']} items."
                )

    st.markdown("**Preview**")
    st.dataframe(parsed_df.head(100), use_container_width=True)
    if len(parsed_df) > 100:
        st.caption(f"Showing first 100 of {len(parsed_df)} parsed rows.")



def render_material_master_editor(material_id: int, lookups: dict[str, pd.DataFrame], key_prefix: str, expanded: bool = True, title: str = "Material Details") -> None:
    material_row = query_one("SELECT * FROM materials WHERE material_id = %s", (material_id,)) or {}
    cats_all = lookups["active_categories"].copy()
    subs_all = lookups["subcategories"].copy()
    units_all = lookups["units"].copy()

    category_options_edit = {"No Category": None}
    for _, row in cats_all.iterrows():
        category_options_edit[str(row["category_name"])] = int(row["category_id"])
    category_labels = list(category_options_edit.keys())
    current_category_name = next((name for name, cid in category_options_edit.items() if cid == material_row.get("category_id")), "No Category")
    default_category_index = category_labels.index(current_category_name) if current_category_name in category_labels else 0

    with st.expander(title, expanded=expanded):
        selected_edit_category_name = st.selectbox(
            "Category",
            category_labels,
            index=default_category_index,
            key=f"{key_prefix}_category",
        )
        selected_category_id = category_options_edit[selected_edit_category_name]

        sub_options_edit = {"No Sub-category": None}
        if selected_category_id is not None and not subs_all.empty:
            filtered_subs = subs_all[subs_all["category_id"] == selected_category_id]
        else:
            filtered_subs = subs_all.iloc[0:0] if not subs_all.empty else pd.DataFrame()
        for _, row in filtered_subs.iterrows():
            sub_options_edit[str(row["subcategory_name"])] = int(row["subcategory_id"])
        sub_labels = list(sub_options_edit.keys())
        current_sub_name = next((name for name, sid in sub_options_edit.items() if sid == material_row.get("subcategory_id")), "No Sub-category")
        default_sub_index = sub_labels.index(current_sub_name) if current_sub_name in sub_labels else 0

        unit_options_edit = {"No Default Unit": None}
        for _, row in units_all.iterrows():
            unit_options_edit[str(row["unit_name"])] = int(row["unit_id"])
        unit_labels = list(unit_options_edit.keys())
        current_unit_name = next((name for name, uid in unit_options_edit.items() if uid == material_row.get("default_unit_id")), "No Default Unit")
        default_unit_edit_index = unit_labels.index(current_unit_name) if current_unit_name in unit_labels else 0

        e1, e2 = st.columns(2)
        edit_material_name = e1.text_input(
            "Material name",
            value=material_row.get("material_name") or "",
            key=f"{key_prefix}_material_name",
        )
        edit_internal_code = e2.text_input(
            "Internal material code",
            value=material_row.get("internal_material_code") or "",
            key=f"{key_prefix}_material_code",
        )
        edit_full_description = st.text_area(
            "Full description",
            value=material_row.get("full_description") or "",
            key=f"{key_prefix}_material_desc",
        )
        e3, e4 = st.columns(2)
        edit_sub_name = e3.selectbox(
            "Sub-category",
            sub_labels,
            index=default_sub_index,
            key=f"{key_prefix}_sub",
        )
        edit_unit_name = e4.selectbox(
            "Default unit",
            unit_labels,
            index=default_unit_edit_index,
            key=f"{key_prefix}_unit",
        )
        e5, e6, e7 = st.columns(3)
        edit_manufacturer = e5.text_input(
            "Manufacturer",
            value=material_row.get("manufacturer") or "",
            key=f"{key_prefix}_manufacturer",
        )
        edit_model_number = e6.text_input(
            "Model number",
            value=material_row.get("model_number") or "",
            key=f"{key_prefix}_model",
        )
        edit_dimension_display = e7.text_input(
            "Dimensions display",
            value=material_row.get("dimension_display") or "",
            key=f"{key_prefix}_dimensions",
        )
        edit_notes = st.text_area(
            "Notes",
            value=material_row.get("notes") or "",
            key=f"{key_prefix}_notes",
        )
        edit_active = st.checkbox(
            "Active",
            value=bool(material_row.get("active", 1)),
            key=f"{key_prefix}_active",
        )
        save_material_changes = st.button(
            "Save Material Changes",
            key=f"{key_prefix}_save",
        )
        if save_material_changes:
            if not edit_material_name.strip():
                st.error("Material name is required.")
            else:
                execute(
                    """
                    UPDATE materials
                    SET internal_material_code = %s,
                        material_name = %s,
                        full_description = %s,
                        category_id = %s,
                        subcategory_id = %s,
                        default_unit_id = %s,
                        manufacturer = %s,
                        model_number = %s,
                        dimension_display = %s,
                        notes = %s,
                        active = %s,
                        date_modified = %s
                    WHERE material_id = %s
                    """,
                    (
                        edit_internal_code.strip() or None,
                        edit_material_name.strip(),
                        edit_full_description.strip() or None,
                        selected_category_id,
                        sub_options_edit[edit_sub_name],
                        unit_options_edit[edit_unit_name],
                        edit_manufacturer.strip() or None,
                        edit_model_number.strip() or None,
                        edit_dimension_display.strip() or None,
                        edit_notes.strip() or None,
                        1 if edit_active else 0,
                        now_ts(),
                        material_id,
                    ),
                )
                st.success("Material updated.")
                st.rerun()


# -----------------------------
# UI pages
# -----------------------------

def page_dashboard() -> None:
    st.header("Dashboard")
    data = get_dashboard_data()
    counts = data["counts"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Categories", int(counts["categories"]))
    c2.metric("Materials", int(counts["materials"]))
    c3.metric("Projects", int(counts["projects"]))
    c4.metric("Sub-Projects", int(counts["work_items"]))

    st.subheader("Recent Materials")
    st.dataframe(data["recent_materials"], use_container_width=True)

    st.subheader("Recent Projects")
    st.dataframe(data["recent_projects"], use_container_width=True)


def page_categories(category_section: str | None = None) -> None:
    st.header("Categories")
    lookups = get_lookup_data()
    if category_section is None:
        category_section = st.radio("Category Section", ["Categories", "Sub-categories", "Units"], horizontal=True, key="category_section")

    if category_section == "Categories":
        left, right = st.columns([1, 1])
        with left:
            with st.form("add_category_form"):
                category_name = st.text_input("Category name")
                sort_order = st.number_input("Sort order", min_value=0, step=1, value=0)
                notes = st.text_area("Notes")
                submitted = st.form_submit_button("Add Category")
                if submitted:
                    if not category_name.strip():
                        st.error("Category name is required.")
                    else:
                        try:
                            execute(
                                "INSERT INTO company_categories (category_name, sort_order, notes) VALUES (%s, %s, %s)",
                                (category_name.strip(), sort_order, notes.strip()),
                            )
                            st.success("Category added.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Could not add category: {exc}")
        with right:
            cats_df = lookups["categories"].copy()
            if not cats_df.empty:
                choice_map = {f"{row['category_name']} (ID {row['category_id']})": int(row['category_id']) for _, row in cats_df.iterrows()}
                selected = st.selectbox("Edit category", list(choice_map.keys()))
                row = query_one("SELECT * FROM company_categories WHERE category_id = %s", (choice_map[selected],))
                with st.form("edit_category_form"):
                    new_name = st.text_input("Category name", value=row['category_name'])
                    new_active = st.checkbox("Active", value=bool(row['active']))
                    new_sort = st.number_input("Sort order ", min_value=0, step=1, value=int(row['sort_order'] or 0))
                    new_notes = st.text_area("Notes ", value=row['notes'] or "")
                    c1, c2 = st.columns(2)
                    save = c1.form_submit_button("Save Category")
                    delete = c2.form_submit_button("Delete Category")
                    if save:
                        try:
                            execute(
                                "UPDATE company_categories SET category_name = %s, active = %s, sort_order = %s, notes = %s WHERE category_id = %s",
                                (new_name.strip(), 1 if new_active else 0, new_sort, new_notes.strip(), row['category_id']),
                            )
                            st.success("Category updated.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Could not update category: {exc}")
                    if delete:
                        try:
                            execute("DELETE FROM company_categories WHERE category_id = %s", (row['category_id'],))
                            st.success("Category deleted.")
                            st.rerun()
                        except Exception as exc:
                            st.error("Could not delete category. Remove dependent records first or mark it inactive.")
            st.dataframe(cats_df, use_container_width=True)

    if category_section == "Sub-categories":
        cats = lookups["active_categories"].copy()
        if cats.empty:
            st.info("Add a category first.")
        else:
            cat_map = dict(zip(cats["category_name"], cats["category_id"]))
            left, right = st.columns([1, 1])
            with left:
                with st.form("add_subcategory_form"):
                    selected_cat = st.selectbox("Category", list(cat_map.keys()))
                    subcategory_name = st.text_input("Sub-category name")
                    sort_order = st.number_input("Sort order", min_value=0, step=1, value=0, key="sub_sort")
                    notes = st.text_area("Notes", key="sub_notes")
                    submitted = st.form_submit_button("Add Sub-category")
                    if submitted:
                        if not subcategory_name.strip():
                            st.error("Sub-category name is required.")
                        else:
                            try:
                                execute(
                                    "INSERT INTO company_subcategories (category_id, subcategory_name, sort_order, notes) VALUES (%s, %s, %s, %s)",
                                    (cat_map[selected_cat], subcategory_name.strip(), sort_order, notes.strip()),
                                )
                                st.success("Sub-category added.")
                                st.rerun()
                            except Exception as exc:
                                st.error(f"Could not add sub-category: {exc}")
            with right:
                subs_df = lookups["subcategories"].copy()
                if not subs_df.empty:
                    sub_choice = {
                        f"{row['category_name']} | {row['subcategory_name']} (ID {row['subcategory_id']})": int(row['subcategory_id'])
                        for _, row in subs_df.iterrows()
                    }
                    selected_sub = st.selectbox("Edit sub-category", list(sub_choice.keys()))
                    sub_row = query_one("SELECT * FROM company_subcategories WHERE subcategory_id = %s", (sub_choice[selected_sub],))
                    cats_all = lookups["active_categories"].copy()
                    cats_all_map = dict(zip(cats_all['category_name'], cats_all['category_id']))
                    current_cat_name = next((n for n, cid in cats_all_map.items() if cid == sub_row['category_id']), list(cats_all_map.keys())[0])
                    with st.form("edit_subcategory_form"):
                        new_cat = st.selectbox("Category", list(cats_all_map.keys()), index=list(cats_all_map.keys()).index(current_cat_name))
                        new_name = st.text_input("Sub-category name", value=sub_row['subcategory_name'])
                        new_active = st.checkbox("Active", value=bool(sub_row['active']))
                        new_sort = st.number_input("Sort order ", min_value=0, step=1, value=int(sub_row['sort_order'] or 0), key='edit_sub_sort')
                        new_notes = st.text_area("Notes ", value=sub_row['notes'] or "")
                        c1, c2 = st.columns(2)
                        save = c1.form_submit_button("Save Sub-category")
                        delete = c2.form_submit_button("Delete Sub-category")
                        if save:
                            try:
                                execute(
                                    "UPDATE company_subcategories SET category_id = %s, subcategory_name = %s, active = %s, sort_order = %s, notes = %s WHERE subcategory_id = %s",
                                    (cats_all_map[new_cat], new_name.strip(), 1 if new_active else 0, new_sort, new_notes.strip(), sub_row['subcategory_id']),
                                )
                                st.success("Sub-category updated.")
                                st.rerun()
                            except Exception as exc:
                                st.error(f"Could not update sub-category: {exc}")
                        if delete:
                            try:
                                execute("DELETE FROM company_subcategories WHERE subcategory_id = %s", (sub_row['subcategory_id'],))
                                st.success("Sub-category deleted.")
                                st.rerun()
                            except Exception:
                                st.error("Could not delete sub-category. Remove dependent materials first or mark it inactive.")
                st.dataframe(subs_df, use_container_width=True)

    if category_section == "Units":
        st.info("Units remain editable here for Owner or manager use.")
        with st.form("add_unit_form"):
            c1, c2, c3, c4 = st.columns(4)
            unit_name = c1.text_input("Unit name")
            unit_abbreviation = c2.text_input("Abbreviation")
            measurement_system = c3.text_input("Measurement system")
            unit_type = c4.text_input("Unit type")
            notes = st.text_area("Notes", key="unit_notes")
            submitted = st.form_submit_button("Add Unit")
            if submitted and unit_name.strip():
                try:
                    execute(
                        "INSERT INTO units_of_measure (unit_name, unit_abbreviation, measurement_system, unit_type, notes) VALUES (%s, %s, %s, %s, %s)",
                        (unit_name.strip(), unit_abbreviation.strip(), measurement_system.strip(), unit_type.strip(), notes.strip()),
                    )
                    st.success("Unit added.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not add unit: {exc}")
        units_df = lookups["units"].copy()
        st.dataframe(units_df, use_container_width=True)


def page_materials(materials_section: str | None = None) -> None:
    st.header("Materials")
    lookups = get_lookup_data()
    if materials_section is None:
        materials_section = st.radio("Materials Section", ["Add Material", "Add Vendor Info", "Search / Review", "Vendor Master Reports", "Import Vendor Checklist PDF"], horizontal=True, key="materials_section")

    if materials_section == "Add Material":
        cats = lookups["active_categories"].copy()
        units = lookups["active_units"].copy()
        if cats.empty or units.empty:
            st.warning("Please add categories and units first.")
        else:
            cat_map = {str(name): int(cat_id) for name, cat_id in zip(cats["category_name"], cats["category_id"])}
            selected_cat = st.selectbox("Category", list(cat_map.keys()), key="mat_cat")
            selected_cat_id = int(cat_map[selected_cat])
            sub_df = query_df(
                "SELECT subcategory_id, subcategory_name FROM company_subcategories WHERE category_id = %s AND active = 1 ORDER BY subcategory_name",
                (selected_cat_id,),
            )
            sub_map = {str(name): int(sid) for name, sid in zip(sub_df["subcategory_name"], sub_df["subcategory_id"])} if not sub_df.empty else {}
            unit_map = {str(name): int(uid) for name, uid in zip(units["unit_name"], units["unit_id"])}

            with st.form("add_material_form"):
                c1, c2 = st.columns(2)
                material_name = c1.text_input("Material name")
                internal_material_code = c2.text_input("Internal material code")
                full_description = st.text_area("Full description")
                c3, c4, c5 = st.columns(3)
                selected_sub = c3.selectbox("Sub-category", list(sub_map.keys()) if sub_map else [""], key="mat_sub")
                default_unit = c4.selectbox("Default unit", list(unit_map.keys()))
                dimension_display = c5.text_input("Dimensions display")
                c6, c7 = st.columns(2)
                manufacturer = c6.text_input("Manufacturer")
                model_number = c7.text_input("Model number")
                notes = st.text_area("Notes", key="mat_notes")
                submitted = st.form_submit_button("Add material")
                if submitted and material_name.strip():
                    execute(
                        """
                        INSERT INTO materials (
                            internal_material_code, material_name, full_description,
                            category_id, subcategory_id, default_unit_id,
                            manufacturer, model_number, dimension_display,
                            notes, date_modified
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            internal_material_code.strip(),
                            material_name.strip(),
                            full_description.strip(),
                            selected_cat_id,
                            sub_map.get(selected_sub),
                            unit_map[default_unit],
                            manufacturer.strip(),
                            model_number.strip(),
                            dimension_display.strip(),
                            notes.strip(),
                            now_ts(),
                        ),
                    )
                    st.success("Material added.")

    if materials_section == "Add Vendor Info":
        mats = lookups["active_materials"][["material_id", "material_name"]].copy()
        if mats.empty:
            st.info("Add a material first.")
        else:
            mat_map = {str(name): int(mid) for name, mid in zip(mats["material_name"], mats["material_id"])}
            with st.form("add_vendor_form"):
                material_name = st.selectbox("Material", list(mat_map.keys()))
                c1, c2, c3 = st.columns(3)
                vendor_name = c1.text_input("Vendor name")
                vendor_item_number = c2.text_input("Vendor item number")
                vendor_store_number = c3.text_input("Store number")
                c4, c5 = st.columns(2)
                store_aisle = c4.text_input("Store aisle")
                vendor_notes = c5.text_input("Vendor notes")
                c6, c7 = st.columns(2)
                latest_retail_price = c6.number_input("Latest retail price", min_value=0.0, step=0.01)
                latest_retail_price_date = c7.date_input("Retail price date", format="MM/DD/YYYY")
                c8, c9 = st.columns(2)
                latest_quoted_price = c8.number_input("Latest quoted price", min_value=0.0, step=0.01)
                latest_quoted_price_date = c9.date_input("Quoted price date", format="MM/DD/YYYY")
                submitted = st.form_submit_button("Add vendor info")
                if submitted and vendor_name.strip():
                    execute(
                        """
                        INSERT INTO material_vendor_current (
                            material_id, vendor_name, vendor_item_number,
                            vendor_store_number, store_aisle,
                            latest_retail_price, latest_retail_price_date,
                            latest_quoted_price, latest_quoted_price_date,
                            vendor_notes, date_modified
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            mat_map[material_name],
                            vendor_name.strip(),
                            vendor_item_number.strip(),
                            vendor_store_number.strip(),
                            store_aisle.strip(),
                            latest_retail_price,
                            latest_retail_price_date,
                            latest_quoted_price,
                            latest_quoted_price_date,
                            vendor_notes.strip(),
                            now_ts(),
                        ),
                    )
                    st.success("Vendor info added.")

    if materials_section == "Search / Review":
        with st.form("materials_search_form"):
            c1, c2, c3 = st.columns(3)
            search = c1.text_input("Search materials")
            categories = lookups["active_categories"].copy()
            category_options = ["All"] + categories["category_name"].tolist()
            selected_category = c2.selectbox("Category filter", category_options)

            selected_category_id = None
            if selected_category != "All":
                selected_category_id = int(categories.loc[categories["category_name"] == selected_category, "category_id"].iloc[0])

            if selected_category_id is not None:
                subcats = query_df(
                    "SELECT subcategory_id, subcategory_name FROM company_subcategories WHERE active = 1 AND category_id = %s ORDER BY subcategory_name",
                    (selected_category_id,),
                )
            else:
                subcats = query_df("SELECT subcategory_id, subcategory_name FROM company_subcategories WHERE active = 1 ORDER BY subcategory_name")
            subcat_names = subcats["subcategory_name"].tolist() if "subcategory_name" in subcats.columns else []
            subcat_options = ["All"] + subcat_names
            selected_subcategory = c3.selectbox("Sub-category filter", subcat_options)
            run_search = st.form_submit_button("Load Search Results")

        if run_search:
            sql = """
                SELECT m.material_id, m.material_name, c.category_name, s.subcategory_name,
                       u.unit_name AS default_unit, m.manufacturer, m.model_number,
                       m.dimension_display, m.full_description
                FROM materials m
                LEFT JOIN company_categories c ON m.category_id = c.category_id
                LEFT JOIN company_subcategories s ON m.subcategory_id = s.subcategory_id
                LEFT JOIN units_of_measure u ON m.default_unit_id = u.unit_id
                WHERE m.active = 1
            """
            params: list[Any] = []

            if search.strip():
                sql += " AND (m.material_name ILIKE %s OR m.full_description ILIKE %s OR m.manufacturer ILIKE %s OR m.model_number ILIKE %s OR m.dimension_display ILIKE %s)"
                like = f"%{search.strip()}%"
                params.extend([like, like, like, like, like])

            if selected_category != "All":
                sql += " AND c.category_name = %s"
                params.append(selected_category)

            if selected_subcategory != "All":
                sql += " AND s.subcategory_name = %s"
                params.append(selected_subcategory)

            sql += " ORDER BY m.material_name"
            df_mat_search = query_df(sql, tuple(params))
            st.dataframe(df_mat_search, use_container_width=True)
        else:
            st.caption("Search results load only after you click Load Search Results.")

        if st.checkbox("Load Current Vendor Records", key="load_vendor_records_table"):
            st.subheader("Current Vendor Records")
            df_vendor = query_df("SELECT * FROM material_vendor_current ORDER BY vendor_name, vendor_item_number")
            if not df_vendor.empty and "vendor_item_number" in df_vendor.columns:
                df_vendor["vendor_item_number"] = df_vendor["vendor_item_number"].apply(normalize_vendor_item_number)
            df_vendor = format_dates(df_vendor)
            st.dataframe(df_vendor, use_container_width=True)

        st.subheader("Material Maintenance")
        m1, m2 = st.columns([1, 1])
        if m1.button("Remove .0 from all vendor item numbers", key="clean_vendor_item_numbers_btn"):
            changed = clean_all_vendor_item_numbers()
            st.success(f"Cleaned {changed} vendor item number(s).")
            st.rerun()
        m2.caption("Use this once after spreadsheet imports that turned item numbers into decimals.")

        if st.checkbox("Load Material Editor", key="load_material_editor"):
            edit_materials_df = query_df(
                """
                SELECT m.material_id, m.material_name, COALESCE(c.category_name, '') AS category_name
                FROM materials m
                LEFT JOIN company_categories c ON m.category_id = c.category_id
                ORDER BY m.material_name, m.material_id
                """
            )
            if edit_materials_df.empty:
                st.info("No materials found.")
            else:
                material_options = {
                    f"{row['material_name']} | ID {int(row['material_id'])}" + (f" | {row['category_name']}" if str(row['category_name']).strip() else ""): int(row['material_id'])
                    for _, row in edit_materials_df.iterrows()
                }
                selected_material_label = st.selectbox("Select material to edit", list(material_options.keys()), key="edit_material_select")
                selected_material_id = material_options[selected_material_label]
                material_row = query_one("SELECT * FROM materials WHERE material_id = %s", (selected_material_id,))

                cats_all = lookups["active_categories"].copy()
                subs_all = lookups["subcategories"].copy()
                units_all = lookups["units"].copy()

                category_options_edit = {"No Category": None}
                for _, row in cats_all.iterrows():
                    category_options_edit[str(row["category_name"])] = int(row["category_id"])
                current_category_name = next((name for name, cid in category_options_edit.items() if cid == material_row.get("category_id")), "No Category")

                sub_options_edit = {"No Sub-category": None}
                current_cat_id = material_row.get("category_id")
                if current_cat_id is not None and not subs_all.empty:
                    filtered_subs = subs_all[subs_all["category_id"] == current_cat_id]
                else:
                    filtered_subs = subs_all.iloc[0:0] if not subs_all.empty else pd.DataFrame()
                for _, row in filtered_subs.iterrows():
                    sub_options_edit[str(row["subcategory_name"])] = int(row["subcategory_id"])
                current_sub_name = next((name for name, sid in sub_options_edit.items() if sid == material_row.get("subcategory_id")), "No Sub-category")

                unit_options_edit = {"No Default Unit": None}
                for _, row in units_all.iterrows():
                    unit_options_edit[str(row["unit_name"])] = int(row["unit_id"])
                current_unit_name = next((name for name, uid in unit_options_edit.items() if uid == material_row.get("default_unit_id")), "No Default Unit")

                with st.form("edit_material_form"):
                    a1, a2 = st.columns(2)
                    edit_material_name = a1.text_input("Material name", value=material_row.get("material_name") or "")
                    edit_internal_code = a2.text_input("Internal material code", value=material_row.get("internal_material_code") or "")
                    edit_full_description = st.text_area("Full description", value=material_row.get("full_description") or "")
                    b1, b2, b3 = st.columns(3)
                    edit_category_name = b1.selectbox("Category", list(category_options_edit.keys()), index=list(category_options_edit.keys()).index(current_category_name), key="edit_mat_category")
                    edit_sub_name = b2.selectbox("Sub-category", list(sub_options_edit.keys()), index=list(sub_options_edit.keys()).index(current_sub_name), key="edit_mat_sub")
                    edit_unit_name = b3.selectbox("Default unit", list(unit_options_edit.keys()), index=list(unit_options_edit.keys()).index(current_unit_name), key="edit_mat_unit")
                    c1, c2, c3 = st.columns(3)
                    edit_manufacturer = c1.text_input("Manufacturer", value=material_row.get("manufacturer") or "")
                    edit_model_number = c2.text_input("Model number", value=material_row.get("model_number") or "")
                    edit_dimension_display = c3.text_input("Dimensions display", value=material_row.get("dimension_display") or "")
                    edit_notes = st.text_area("Notes", value=material_row.get("notes") or "", key="edit_material_notes")
                    edit_active = st.checkbox("Active", value=bool(material_row.get("active", 1)))
                    save_material = st.form_submit_button("Save Material Changes")
                    if save_material:
                        execute(
                            """
                            UPDATE materials
                            SET internal_material_code = %s,
                                material_name = %s,
                                full_description = %s,
                                category_id = %s,
                                subcategory_id = %s,
                                default_unit_id = %s,
                                manufacturer = %s,
                                model_number = %s,
                                dimension_display = %s,
                                notes = %s,
                                active = %s,
                                date_modified = %s
                            WHERE material_id = %s
                            """,
                            (
                                edit_internal_code.strip() or None,
                                edit_material_name.strip(),
                                edit_full_description.strip() or None,
                                category_options_edit[edit_category_name],
                                sub_options_edit[edit_sub_name],
                                unit_options_edit[edit_unit_name],
                                edit_manufacturer.strip() or None,
                                edit_model_number.strip() or None,
                                edit_dimension_display.strip() or None,
                                edit_notes.strip() or None,
                                1 if edit_active else 0,
                                now_ts(),
                                selected_material_id,
                            ),
                        )
                        st.success("Material updated.")
                        st.rerun()

                vendor_edit_df = query_df(
                    "SELECT * FROM material_vendor_current WHERE material_id = %s ORDER BY vendor_name, vendor_item_number, material_vendor_id",
                    (selected_material_id,),
                )
                if vendor_edit_df.empty:
                    st.info("This material has no vendor records yet.")
                else:
                    vendor_options_edit = {
                        f"{row['vendor_name']} | {normalize_vendor_item_number(row.get('vendor_item_number')) or 'No Item #'} | Vendor ID {int(row['material_vendor_id'])}": int(row['material_vendor_id'])
                        for _, row in vendor_edit_df.iterrows()
                    }
                    selected_vendor_label = st.selectbox("Select vendor record to edit", list(vendor_options_edit.keys()), key="edit_vendor_record_select")
                    selected_vendor_id = vendor_options_edit[selected_vendor_label]
                    vendor_row = query_one("SELECT * FROM material_vendor_current WHERE material_vendor_id = %s", (selected_vendor_id,))
                    with st.form("edit_vendor_record_form"):
                        v1, v2, v3 = st.columns(3)
                        edit_vendor_name = v1.text_input("Vendor name", value=vendor_row.get("vendor_name") or "")
                        edit_vendor_item_number = v2.text_input("Vendor item number", value=normalize_vendor_item_number(vendor_row.get("vendor_item_number")))
                        edit_vendor_store_number = v3.text_input("Store number", value=vendor_row.get("vendor_store_number") or "")
                        v4, v5 = st.columns(2)
                        edit_store_aisle = v4.text_input("Store aisle", value=vendor_row.get("store_aisle") or "")
                        edit_vendor_notes = v5.text_input("Vendor notes", value=vendor_row.get("vendor_notes") or "")
                        v6, v7 = st.columns(2)
                        edit_latest_retail_price = v6.number_input("Latest retail price", min_value=0.0, step=0.01, value=float(vendor_row.get("latest_retail_price") or 0.0))
                        retail_date_default = _safe_date_value(vendor_row.get("latest_retail_price_date")) or datetime.now().date()
                        edit_latest_retail_price_date = v7.date_input("Retail price date", value=retail_date_default, format="MM/DD/YYYY")
                        v8, v9 = st.columns(2)
                        edit_latest_quoted_price = v8.number_input("Latest quoted price", min_value=0.0, step=0.01, value=float(vendor_row.get("latest_quoted_price") or 0.0))
                        quoted_date_default = _safe_date_value(vendor_row.get("latest_quoted_price_date")) or datetime.now().date()
                        edit_latest_quoted_price_date = v9.date_input("Quoted price date", value=quoted_date_default, format="MM/DD/YYYY")
                        edit_vendor_active = st.checkbox("Vendor record active", value=bool(vendor_row.get("active", 1)))
                        save_vendor = st.form_submit_button("Save Vendor Record Changes")
                        if save_vendor:
                            execute(
                                """
                                UPDATE material_vendor_current
                                SET vendor_name = %s,
                                    vendor_item_number = %s,
                                    vendor_store_number = %s,
                                    store_aisle = %s,
                                    latest_retail_price = %s,
                                    latest_retail_price_date = %s,
                                    latest_quoted_price = %s,
                                    latest_quoted_price_date = %s,
                                    vendor_notes = %s,
                                    active = %s,
                                    date_modified = %s
                                WHERE material_vendor_id = %s
                                """,
                                (
                                    edit_vendor_name.strip(),
                                    normalize_vendor_item_number(edit_vendor_item_number) or None,
                                    edit_vendor_store_number.strip() or None,
                                    edit_store_aisle.strip() or None,
                                    edit_latest_retail_price,
                                    edit_latest_retail_price_date,
                                    edit_latest_quoted_price,
                                    edit_latest_quoted_price_date,
                                    edit_vendor_notes.strip() or None,
                                    1 if edit_vendor_active else 0,
                                    now_ts(),
                                    selected_vendor_id,
                                ),
                            )
                            st.success("Vendor record updated.")
                            st.rerun()

    if materials_section == "Vendor Master Reports":
        st.subheader("Vendor Master Reports")
        vendors_df = query_df(
            "SELECT DISTINCT vendor_name FROM material_vendor_current WHERE active = 1 AND TRIM(COALESCE(vendor_name,'')) <> '' ORDER BY vendor_name"
        )
        if vendors_df.empty:
            st.info("No vendor records yet.")
        else:
            vendor_names = vendors_df["vendor_name"].tolist()
            selected_vendor = st.selectbox("Select Vendor", vendor_names, key="vendor_master_select")
            vendor_master_df = _vendor_master_df(selected_vendor)
            st.caption("Sorted numerically by item number where possible.")
            st.dataframe(vendor_master_df.rename(
                columns={"item_number": "Item Number", "item_name": "Item Name", "description": "Description", "latest_price": "Latest Price"}
            ), use_container_width=True)

            col1, col2 = st.columns(2)
            vendor_pdf_key = f"vendor_master_pdf_bytes_{selected_vendor}"
            vendor_xlsx_key = f"vendor_master_xlsx_bytes_{selected_vendor}"

            if col1.button("Prepare Vendor Master PDF", key=f"prep_vendor_master_pdf_{selected_vendor}"):
                st.session_state[vendor_pdf_key] = build_vendor_master_pdf(selected_vendor).getvalue()
            if vendor_pdf_key in st.session_state:
                col1.download_button(
                    "Download Vendor Master PDF",
                    data=st.session_state[vendor_pdf_key],
                    file_name=f"vendor_master_{selected_vendor.replace(' ', '_')}.pdf",
                    mime="application/pdf",
                    key=f"download_vendor_master_pdf_{selected_vendor}",
                )

            if col2.button("Prepare Vendor Master Excel", key=f"prep_vendor_master_xlsx_{selected_vendor}"):
                st.session_state[vendor_xlsx_key] = build_vendor_master_excel(selected_vendor).getvalue()
            if vendor_xlsx_key in st.session_state:
                col2.download_button(
                    "Download Vendor Master Excel",
                    data=st.session_state[vendor_xlsx_key],
                    file_name=f"vendor_master_{selected_vendor.replace(' ', '_')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_vendor_master_xlsx_{selected_vendor}",
                )

    if materials_section == "Import Vendor Checklist PDF":
        page_materials_import_vendor_pdf()


def page_projects(project_section: str | None = None) -> None:
    st.header("Projects")
    lookups = get_lookup_data()
    if project_section is None:
        project_section = st.radio(
            "Project Section",
            ["Create Project", "Add Sub-Project", "Add Materials to Sub-Project", "Project Detail / Reports"],
            horizontal=True,
            key="project_section",
        )

    if project_section == "Create Project":
        create_tab, edit_tab = st.tabs(["Create Project", "Edit Project"])

        with create_tab:
            with st.form("create_project_form"):
                project_name = st.text_input("Project name")
                c1, c2 = st.columns(2)
                property_name = c1.text_input("Property name")
                unit_or_location = c2.text_input("Unit or location")
                project_description = st.text_area("Project description")
                c3, c4 = st.columns(2)
                status = c3.text_input("Status", value="Open")
                notes = c4.text_input("Notes")
                submitted = st.form_submit_button("Create project")
                if submitted:
                    if not project_name.strip():
                        st.error("Project name is required.")
                    else:
                        execute(
                            """
                            INSERT INTO projects (project_name, property_name, unit_or_location, project_description, status, notes, date_modified)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                project_name.strip(),
                                property_name.strip(),
                                unit_or_location.strip(),
                                project_description.strip(),
                                status.strip(),
                                notes.strip(),
                                now_ts(),
                            ),
                        )
                        st.success("Project created.")
                        st.rerun()

        with edit_tab:
            projects_df = query_df(
                """
                SELECT project_id, project_name, property_name, unit_or_location, status, project_description, notes
                FROM projects
                ORDER BY project_name, project_id
                """
            )
            if projects_df.empty:
                st.info("No projects available to edit yet.")
            else:
                project_options = {
                    f"{row['project_name']} | ID {int(row['project_id'])}": int(row['project_id'])
                    for _, row in projects_df.iterrows()
                }
                selected_project_label = st.selectbox(
                    "Select project to edit",
                    list(project_options.keys()),
                    key="edit_project_select",
                )
                selected_project_id = project_options[selected_project_label]
                project_edit_row = query_one("SELECT * FROM projects WHERE project_id = %s", (selected_project_id,))

                preview_cols = ["project_name", "property_name", "unit_or_location", "status"]
                st.dataframe(projects_df[preview_cols], use_container_width=True, hide_index=True)

                with st.form("edit_project_form"):
                    ep1, ep2 = st.columns(2)
                    edit_project_name = ep1.text_input("Project name", value=project_edit_row.get("project_name") or "")
                    edit_property_name = ep2.text_input("Property name", value=project_edit_row.get("property_name") or "")
                    ep3, ep4 = st.columns(2)
                    edit_location = ep3.text_input("Unit or location", value=project_edit_row.get("unit_or_location") or "")
                    edit_status = ep4.text_input("Status", value=project_edit_row.get("status") or "")
                    edit_description = st.text_area("Project description", value=project_edit_row.get("project_description") or "")
                    edit_notes = st.text_area("Notes", value=project_edit_row.get("notes") or "")
                    save_col, delete_col = st.columns(2)
                    save_project = save_col.form_submit_button("Save Project Changes")
                    delete_project = delete_col.form_submit_button("Delete Project")
                    if save_project:
                        if not edit_project_name.strip():
                            st.error("Project name is required.")
                        else:
                            execute(
                                """
                                UPDATE projects
                                SET project_name = %s,
                                    property_name = %s,
                                    unit_or_location = %s,
                                    project_description = %s,
                                    status = %s,
                                    notes = %s,
                                    date_modified = %s
                                WHERE project_id = %s
                                """,
                                (
                                    edit_project_name.strip(),
                                    edit_property_name.strip(),
                                    edit_location.strip(),
                                    edit_description.strip(),
                                    edit_status.strip(),
                                    edit_notes.strip(),
                                    now_ts(),
                                    selected_project_id,
                                ),
                            )
                            st.success("Project updated.")
                            st.rerun()
                    if delete_project:
                        execute("DELETE FROM projects WHERE project_id = %s", (selected_project_id,))
                        st.success("Project deleted.")
                        st.rerun()

    if project_section == "Add Sub-Project":
        projects = lookups["projects"].copy()
        if projects.empty:
            st.info("Create a project first.")
        else:
            proj_map = dict(zip(projects["project_name"], projects["project_id"]))
            add_tab, edit_tab = st.tabs(["Add Sub-Project", "Edit Sub-Project"])

            with add_tab:
                with st.form("add_work_item_form"):
                    project_name = st.selectbox("Project", list(proj_map.keys()))
                    work_item_name = st.text_input("Sub-Project name")
                    work_item_description = st.text_area("Sub-Project description")
                    sort_order = st.number_input("Sort order", min_value=0, step=1, value=0, key="wi_sort")
                    notes = st.text_input("Notes", key="wi_notes")
                    submitted = st.form_submit_button("Add Sub-Project")
                    if submitted:
                        if not work_item_name.strip():
                            st.error("Sub-Project name is required.")
                        else:
                            execute(
                                """
                                INSERT INTO project_work_items (project_id, work_item_name, work_item_description, sort_order, notes, date_modified)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """,
                                (proj_map[project_name], work_item_name.strip(), work_item_description.strip(), sort_order, notes.strip(), now_ts()),
                            )
                            st.success("Sub-Project added.")
                            st.rerun()

            with edit_tab:
                subprojects_df = query_df(
                    """
                    SELECT wi.work_item_id, wi.project_id, wi.work_item_name, wi.work_item_description, wi.sort_order, wi.notes,
                           p.project_name
                    FROM project_work_items wi
                    JOIN projects p ON wi.project_id = p.project_id
                    ORDER BY p.project_name, wi.sort_order NULLS LAST, wi.work_item_name
                    """
                )
                if subprojects_df.empty:
                    st.info("No sub-projects available to edit yet.")
                else:
                    subproject_options = {
                        f"{row['project_name']} | {row['work_item_name']} | ID {int(row['work_item_id'])}": int(row['work_item_id'])
                        for _, row in subprojects_df.iterrows()
                    }
                    selected_subproject_label = st.selectbox(
                        "Select sub-project to edit",
                        list(subproject_options.keys()),
                        key="edit_subproject_select",
                    )
                    selected_subproject_id = subproject_options[selected_subproject_label]
                    subproject_row = query_one("SELECT * FROM project_work_items WHERE work_item_id = %s", (selected_subproject_id,))
                    current_project_id = int(subproject_row["project_id"])
                    current_project_name = next((name for name, pid in proj_map.items() if pid == current_project_id), list(proj_map.keys())[0])

                    st.dataframe(
                        subprojects_df[["project_name", "work_item_name", "sort_order"]].rename(
                            columns={"project_name": "Project", "work_item_name": "Sub-Project", "sort_order": "Sort Order"}
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

                    with st.form("edit_subproject_form"):
                        sp1, sp2 = st.columns(2)
                        edit_parent_project = sp1.selectbox("Project", list(proj_map.keys()), index=list(proj_map.keys()).index(current_project_name))
                        edit_subproject_name = sp2.text_input("Sub-Project name", value=subproject_row.get("work_item_name") or "")
                        edit_subproject_description = st.text_area("Sub-Project description", value=subproject_row.get("work_item_description") or "")
                        sp3, sp4 = st.columns(2)
                        edit_sort_order = sp3.number_input("Sort order", min_value=0, step=1, value=int(subproject_row.get("sort_order") or 0), key="edit_subproject_sort")
                        edit_subproject_notes = sp4.text_input("Notes", value=subproject_row.get("notes") or "", key="edit_subproject_notes")
                        save_col, delete_col = st.columns(2)
                        save_subproject = save_col.form_submit_button("Save Sub-Project Changes")
                        delete_subproject = delete_col.form_submit_button("Delete Sub-Project")
                        if save_subproject:
                            if not edit_subproject_name.strip():
                                st.error("Sub-Project name is required.")
                            else:
                                execute(
                                    """
                                    UPDATE project_work_items
                                    SET project_id = %s,
                                        work_item_name = %s,
                                        work_item_description = %s,
                                        sort_order = %s,
                                        notes = %s,
                                        date_modified = %s
                                    WHERE work_item_id = %s
                                    """,
                                    (
                                        proj_map[edit_parent_project],
                                        edit_subproject_name.strip(),
                                        edit_subproject_description.strip(),
                                        edit_sort_order,
                                        edit_subproject_notes.strip(),
                                        now_ts(),
                                        selected_subproject_id,
                                    ),
                                )
                                st.success("Sub-Project updated.")
                                st.rerun()
                        if delete_subproject:
                            execute("DELETE FROM project_work_items WHERE work_item_id = %s", (selected_subproject_id,))
                            st.success("Sub-Project deleted.")
                            st.rerun()

    if project_section == "Add Materials to Sub-Project":
        projects_df = lookups["projects"].copy()
        units = lookups["active_units"].copy()
        if projects_df.empty or units.empty:
            st.info("You need at least one project, sub-project, material, and unit.")
        else:
            project_map = {str(row["project_name"]): int(row["project_id"]) for _, row in projects_df.iterrows()}
            project_names = list(project_map.keys())
            selected_project_name = st.selectbox("Project", project_names, key="subproject_materials_project_select")
            selected_project_id = project_map[selected_project_name]

            subprojects_df = query_df(
                """
                SELECT work_item_id, work_item_name, work_item_description, sort_order
                FROM project_work_items
                WHERE project_id = %s
                ORDER BY sort_order NULLS LAST, work_item_name
                """,
                (selected_project_id,),
            )
            if subprojects_df.empty:
                st.info("This project does not have any Sub-Projects yet.")
            else:
                subproject_map = {str(row["work_item_name"]): int(row["work_item_id"]) for _, row in subprojects_df.iterrows()}
                subproject_names = list(subproject_map.keys())
                selected_subproject_name = st.selectbox("Sub-Project", subproject_names, key="subproject_materials_subproject_select")
                work_item_id = subproject_map[selected_subproject_name]
                work_item_row = query_one(
                    """
                    SELECT wi.work_item_name, p.project_name, wi.work_item_description
                    FROM project_work_items wi
                    JOIN projects p ON wi.project_id = p.project_id
                    WHERE wi.work_item_id = %s
                    """,
                    (work_item_id,),
                )
                if work_item_row and work_item_row.get("work_item_description"):
                    st.caption(f"Description: {work_item_row['work_item_description']}")

                action_mode = st.radio(
                    "Choose Action",
                    ["Add New Material", "Edit Existing Material", "Copy Materials From Another Sub-Project"],
                    horizontal=True,
                    key="subproject_material_action_mode",
                )

                unit_map = {str(name): int(uid) for name, uid in zip(units["unit_name"], units["unit_id"])}
                unit_name_by_id = {int(uid): str(name) for uid, name in zip(units["unit_id"], units["unit_name"])}

                current_lines_df = format_dates(query_df(
                    """
                    SELECT wim.work_item_material_id,
                           wim.line_material_name_snapshot AS material,
                           wim.line_vendor_item_number_snapshot AS item_number,
                           wim.line_vendor_name_snapshot AS vendor,
                           wim.line_category_snapshot AS category,
                           wim.line_subcategory_snapshot AS subcategory,
                           wim.quantity,
                           u.unit_abbreviation,
                           wim.unit_price,
                           wim.line_total,
                           COALESCE(wim.notes, '') AS notes
                    FROM work_item_materials wim
                    LEFT JOIN units_of_measure u ON wim.unit_id = u.unit_id
                    WHERE wim.work_item_id = %s
                    ORDER BY wim.work_item_material_id DESC
                    """,
                    (work_item_id,),
                ))
                current_total = float(current_lines_df["line_total"].sum()) if not current_lines_df.empty else 0.0
                st.markdown("#### Current Items In This Sub-Project")
                st.caption(f"{len(current_lines_df)} item(s) | Total {money(current_total)}")
                if current_lines_df.empty:
                    st.info("No items have been entered into this Sub-Project yet.")
                else:
                    current_items_display_df = current_lines_df[[
                        "item_number", "material", "vendor", "quantity", "unit_price"
                    ]].rename(columns={
                        "item_number": "Vendor Item Number",
                        "material": "Description",
                        "vendor": "Vendor",
                        "quantity": "Quantity",
                        "unit_price": "Price",
                    })
                    st.dataframe(current_items_display_df, use_container_width=True, hide_index=True, height=260)

                if action_mode == "Add New Material":
                    search_value = st.text_input(
                        "Enter item number or material name",
                        key="subproject_material_search_add",
                        placeholder="Example: 12345 or drywall screw",
                    )
                    search_df = pd.DataFrame()
                    selected_search_row = None
                    if search_value.strip():
                        search_df = search_material_selector(search_value.strip(), limit=75)
                        if search_df.empty:
                            st.warning("No matching materials found.")
                        else:
                            search_display = search_df[["display_label", "vendor_item_number", "vendor_name", "default_unit_name", "latest_price"]].rename(
                                columns={
                                    "display_label": "Material",
                                    "vendor_item_number": "Item Number",
                                    "vendor_name": "Vendor",
                                    "default_unit_name": "Default Unit",
                                    "latest_price": "Latest Price",
                                }
                            )
                            st.dataframe(search_display, use_container_width=True, hide_index=True, height=260)
                            search_option_labels = search_df["display_label"].tolist()
                            selected_search_label = st.selectbox(
                                "Select material result",
                                search_option_labels,
                                key="material_search_result_select_add",
                            )
                            selected_search_row = search_df.loc[search_df["display_label"] == selected_search_label].iloc[0].to_dict()
                    else:
                        st.info("Enter an item number or part of a material name to load matching materials.")

                    if selected_search_row is not None:
                        selected_material_id = int(selected_search_row["material_id"])
                        selected_material_name = str(selected_search_row["material_name"])
                        vendor_df = query_df(
                            """
                            SELECT material_vendor_id,
                                   vendor_name || COALESCE(' | ' || vendor_item_number, '') AS label,
                                   latest_retail_price,
                                   latest_quoted_price
                            FROM material_vendor_current
                            WHERE material_id = %s AND active = 1
                            ORDER BY vendor_name, vendor_item_number
                            """,
                            (selected_material_id,),
                        )
                        vendor_options = {"None": None}
                        vendor_price_lookup = {"None": float(selected_search_row.get("latest_price") or 0.0)}
                        default_vendor_label = "None"
                        if not vendor_df.empty:
                            for _, row in vendor_df.iterrows():
                                label = row["label"]
                                vendor_options[label] = row["material_vendor_id"]
                                preferred_price = row["latest_quoted_price"] if pd.notna(row["latest_quoted_price"]) and float(row["latest_quoted_price"] or 0) > 0 else row["latest_retail_price"]
                                vendor_price_lookup[label] = float(preferred_price or 0.0)
                                if str(label) == str(selected_search_row.get("display_label")):
                                    default_vendor_label = label

                        default_unit_id = selected_search_row.get("default_unit_id")
                        default_unit_name = unit_name_by_id.get(int(default_unit_id)) if pd.notna(default_unit_id) else None
                        unit_choices = list(unit_map.keys())
                        default_unit_index = unit_choices.index(default_unit_name) if default_unit_name in unit_choices else 0
                        vendor_choice_labels = list(vendor_options.keys())
                        vendor_index = vendor_choice_labels.index(default_vendor_label) if default_vendor_label in vendor_choice_labels else 0
                        default_price = vendor_price_lookup.get(default_vendor_label, float(selected_search_row.get("latest_price") or 0.0))

                        st.markdown("#### Selected Material")
                        info1, info2, info3 = st.columns(3)
                        info1.write(f"**Material:** {selected_material_name}")
                        info2.write(f"**Item #:** {selected_search_row.get('vendor_item_number') or ''}")
                        info3.write(f"**Vendor:** {selected_search_row.get('vendor_name') or ''}")
                        if selected_search_row.get("full_description"):
                            st.caption(selected_search_row["full_description"])

                        render_material_master_editor(
                            selected_material_id,
                            lookups,
                            key_prefix=f"subproject_add_material_{selected_material_id}",
                            expanded=True,
                            title="Material Details",
                        )

                        with st.form("add_material_to_work_item_form"):
                            selected_vendor = st.selectbox("Vendor record", vendor_choice_labels, index=vendor_index)
                            c1, c2, c3 = st.columns(3)
                            quantity = c1.number_input("Quantity", min_value=0.0, step=1.0, value=1.0)
                            unit_name = c2.selectbox("Unit", unit_choices, index=default_unit_index)
                            unit_price = c3.number_input("Unit price", min_value=0.0, step=0.01, value=float(default_price))
                            notes = st.text_input("Line notes")
                            submitted = st.form_submit_button("Add Material Line")
                            if submitted:
                                add_material_line_from_master(
                                    work_item_id,
                                    selected_material_id,
                                    quantity,
                                    unit_map[unit_name],
                                    unit_price,
                                    vendor_options[selected_vendor],
                                    notes.strip(),
                                )
                                st.success("Material line added.")
                                st.rerun()

                if action_mode == "Edit Existing Material":
                    if current_lines_df.empty:
                        st.info("There are no existing material entries in this Sub-Project to edit yet.")
                    else:
                        edit_line_options = {
                            f"{row['material']} | Item {row['item_number'] or ''} | Qty {row['quantity']} | Line ID {int(row['work_item_material_id'])}": int(row['work_item_material_id'])
                            for _, row in current_lines_df.iterrows()
                        }
                        selected_line_label = st.selectbox(
                            "Select existing entry",
                            list(edit_line_options.keys()),
                            key="subproject_edit_line_select",
                        )
                        selected_line_id = edit_line_options[selected_line_label]
                        line_row = query_one("SELECT * FROM work_item_materials WHERE work_item_material_id = %s", (selected_line_id,)) or {}
                        current_material_id = int(line_row.get("material_id")) if line_row.get("material_id") is not None else None

                        selected_material_id = current_material_id
                        selected_material_name = None
                        selected_vendor_id = line_row.get("material_vendor_id")

                        st.markdown("#### Replace Material (optional)")
                        replace_search_value = st.text_input(
                            "Search by item number or material name",
                            key="subproject_material_search_edit",
                            placeholder="Leave as-is or search to replace this entry with another material",
                        )
                        if replace_search_value.strip():
                            replace_df = search_material_selector(replace_search_value.strip(), limit=75)
                            if replace_df.empty:
                                st.warning("No matching materials found.")
                            else:
                                replace_display = replace_df[["display_label", "vendor_item_number", "vendor_name", "default_unit_name", "latest_price"]].rename(
                                    columns={
                                        "display_label": "Material",
                                        "vendor_item_number": "Item Number",
                                        "vendor_name": "Vendor",
                                        "default_unit_name": "Default Unit",
                                        "latest_price": "Latest Price",
                                    }
                                )
                                st.dataframe(replace_display, use_container_width=True, hide_index=True, height=220)
                                replace_labels = replace_df["display_label"].tolist()
                                selected_replace_label = st.selectbox(
                                    "Select replacement material",
                                    replace_labels,
                                    key="material_search_result_select_edit",
                                )
                                selected_replace_row = replace_df.loc[replace_df["display_label"] == selected_replace_label].iloc[0].to_dict()
                                selected_material_id = int(selected_replace_row["material_id"])
                                selected_material_name = str(selected_replace_row["material_name"])
                                selected_vendor_id = None
                        
                        if selected_material_id is None:
                            st.error("This line does not have a valid linked material. Use Project Detail / Reports if you need to inspect old snapshot-only lines.")
                        else:
                            material_row = query_one("SELECT * FROM materials WHERE material_id = %s", (selected_material_id,)) or {}
                            if selected_material_name is None:
                                selected_material_name = str(material_row.get("material_name") or "")

                            vendor_df = query_df(
                                """
                                SELECT material_vendor_id,
                                       vendor_name || COALESCE(' | ' || vendor_item_number, '') AS label,
                                       latest_retail_price,
                                       latest_quoted_price
                                FROM material_vendor_current
                                WHERE material_id = %s AND active = 1
                                ORDER BY vendor_name, vendor_item_number
                                """,
                                (selected_material_id,),
                            )
                            vendor_options = {"None": None}
                            vendor_price_lookup = {"None": float(line_row.get("unit_price") or 0.0)}
                            default_vendor_label = "None"
                            if not vendor_df.empty:
                                for _, row in vendor_df.iterrows():
                                    label = row["label"]
                                    vendor_options[label] = row["material_vendor_id"]
                                    preferred_price = row["latest_quoted_price"] if pd.notna(row["latest_quoted_price"]) and float(row["latest_quoted_price"] or 0) > 0 else row["latest_retail_price"]
                                    vendor_price_lookup[label] = float(preferred_price or 0.0)
                                    if selected_vendor_id is not None and int(row["material_vendor_id"]) == int(selected_vendor_id):
                                        default_vendor_label = label

                            unit_choices = list(unit_map.keys())
                            line_unit_id = line_row.get("unit_id")
                            default_unit_name = unit_name_by_id.get(int(line_unit_id)) if line_unit_id is not None and pd.notna(line_unit_id) else None
                            if default_unit_name not in unit_choices:
                                material_default_unit_id = material_row.get("default_unit_id")
                                default_unit_name = unit_name_by_id.get(int(material_default_unit_id)) if material_default_unit_id is not None and pd.notna(material_default_unit_id) else unit_choices[0]
                            default_unit_index = unit_choices.index(default_unit_name) if default_unit_name in unit_choices else 0
                            vendor_choice_labels = list(vendor_options.keys())
                            vendor_index = vendor_choice_labels.index(default_vendor_label) if default_vendor_label in vendor_choice_labels else 0

                            st.markdown("#### Edit Existing Entry")
                            info1, info2 = st.columns(2)
                            info1.write(f"**Material:** {selected_material_name}")
                            info2.write(f"**Current Line ID:** {selected_line_id}")

                            render_material_master_editor(
                                selected_material_id,
                                lookups,
                                key_prefix=f"subproject_edit_material_{selected_material_id}_{selected_line_id}",
                                expanded=True,
                                title="Material Details",
                            )

                            with st.form("edit_material_line_form"):
                                selected_vendor = st.selectbox("Vendor record", vendor_choice_labels, index=vendor_index)
                                c1, c2, c3 = st.columns(3)
                                quantity = c1.number_input("Quantity", min_value=0.0, step=1.0, value=float(line_row.get("quantity") or 0.0))
                                unit_name = c2.selectbox("Unit", unit_choices, index=default_unit_index)
                                unit_price = c3.number_input("Unit price", min_value=0.0, step=0.01, value=float(line_row.get("unit_price") or 0.0))
                                notes = st.text_input("Line notes", value=line_row.get("notes") or "")
                                save_col, delete_col = st.columns(2)
                                save_line = save_col.form_submit_button("Save Existing Entry")
                                delete_line = delete_col.form_submit_button("Delete Item")
                                if save_line:
                                    update_material_line_from_master(
                                        selected_line_id,
                                        selected_material_id,
                                        quantity,
                                        unit_map[unit_name],
                                        unit_price,
                                        vendor_options[selected_vendor],
                                        notes.strip(),
                                    )
                                    st.success("Existing entry updated.")
                                    st.rerun()
                                if delete_line:
                                    execute("DELETE FROM work_item_materials WHERE work_item_material_id = %s", (selected_line_id,))
                                    st.success("Item deleted.")
                                    st.rerun()

                if action_mode == "Copy Materials From Another Sub-Project":
                    source_projects_df = lookups["projects"].copy()
                    source_project_options = {str(row["project_name"]): int(row["project_id"]) for _, row in source_projects_df.iterrows()}
                    default_source_project_index = list(source_project_options.values()).index(selected_project_id) if selected_project_id in source_project_options.values() else 0
                    cp1, cp2 = st.columns(2)
                    source_project_name = cp1.selectbox(
                        "Source Project",
                        list(source_project_options.keys()),
                        index=default_source_project_index,
                        key="copy_materials_source_project",
                    )
                    source_project_id = source_project_options[source_project_name]
                    source_subprojects_df = query_df(
                        """
                        SELECT work_item_id, work_item_name, sort_order
                        FROM project_work_items
                        WHERE project_id = %s
                        ORDER BY sort_order NULLS LAST, work_item_name
                        """,
                        (source_project_id,),
                    )
                    if source_subprojects_df.empty:
                        st.info("The selected source project does not have any Sub-Projects yet.")
                    else:
                        source_subproject_options = {str(row["work_item_name"]): int(row["work_item_id"]) for _, row in source_subprojects_df.iterrows()}
                        default_source_subproject_index = 0
                        if source_project_id == selected_project_id and work_item_id in source_subproject_options.values():
                            default_source_subproject_index = list(source_subproject_options.values()).index(work_item_id)
                        source_subproject_name = cp2.selectbox(
                            "Source Sub-Project",
                            list(source_subproject_options.keys()),
                            index=default_source_subproject_index,
                            key="copy_materials_source_subproject",
                        )
                        source_work_item_id = source_subproject_options[source_subproject_name]

                        if source_work_item_id == work_item_id:
                            st.info("Choose a different source Sub-Project than the current target Sub-Project.")
                        else:
                            source_lines_df = query_df(
                                """
                                SELECT work_item_material_id,
                                       COALESCE(line_vendor_item_number_snapshot, '') AS item_number,
                                       COALESCE(line_material_name_snapshot, '') AS material,
                                       COALESCE(line_vendor_name_snapshot, '') AS vendor,
                                       quantity,
                                       unit_price,
                                       line_total
                                FROM work_item_materials
                                WHERE work_item_id = %s
                                ORDER BY work_item_material_id DESC
                                """,
                                (source_work_item_id,),
                            )
                            if source_lines_df.empty:
                                st.info("The selected source Sub-Project does not have any materials to copy.")
                            else:
                                st.markdown("#### Source Materials")
                                source_display_df = source_lines_df[["item_number", "material", "vendor", "quantity", "unit_price"]].rename(columns={
                                    "item_number": "Vendor Item Number",
                                    "material": "Description",
                                    "vendor": "Vendor",
                                    "quantity": "Quantity",
                                    "unit_price": "Price",
                                })
                                st.dataframe(source_display_df, use_container_width=True, hide_index=True, height=260)

                                copy_mode = st.radio(
                                    "Copy Mode",
                                    ["Copy Entire List", "Copy Partial List"],
                                    horizontal=True,
                                    key="copy_materials_mode",
                                )
                                selected_copy_line_ids: list[int] = []
                                if copy_mode == "Copy Partial List":
                                    partial_options = {
                                        f"{row['material']} | Item {row['item_number'] or ''} | Qty {row['quantity']} | Line ID {int(row['work_item_material_id'])}": int(row['work_item_material_id'])
                                        for _, row in source_lines_df.iterrows()
                                    }
                                    selected_labels = st.multiselect(
                                        "Select materials to copy",
                                        list(partial_options.keys()),
                                        key="copy_materials_partial_select",
                                    )
                                    selected_copy_line_ids = [partial_options[label] for label in selected_labels]

                                with st.form("copy_materials_to_subproject_form"):
                                    st.caption(f"Copying from {source_project_name} / {source_subproject_name} to {selected_project_name} / {selected_subproject_name}")
                                    submit_copy = st.form_submit_button("Copy Materials To This Sub-Project")
                                    if submit_copy:
                                        if copy_mode == "Copy Partial List" and not selected_copy_line_ids:
                                            st.error("Select at least one material to copy.")
                                        else:
                                            copied_count = copy_material_lines_between_subprojects(
                                                source_work_item_id,
                                                work_item_id,
                                                selected_copy_line_ids if copy_mode == "Copy Partial List" else None,
                                            )
                                            if copied_count == 0:
                                                st.warning("No materials were copied.")
                                            else:
                                                st.success(f"Copied {copied_count} material line(s) into this Sub-Project.")
                                                st.rerun()

    if project_section == "Project Detail / Reports":
        projects = lookups["projects"].copy()
        if projects.empty:
            st.info("No projects yet.")
            return

        project_map = dict(zip(projects["project_name"], projects["project_id"]))
        selected_project = st.selectbox("Select Project", list(project_map.keys()), key="detail_project")
        project_id = project_map[selected_project]
        project_row = query_one("SELECT * FROM projects WHERE project_id = %s", (project_id,))

        st.subheader("Project Info")
        top1, top2, top3 = st.columns(3)
        top1.write(f"**Project:** {project_row['project_name']}")
        top2.write(f"**Property:** {project_row.get('property_name') or ''}")
        top3.write(f"**Location:** {project_row.get('unit_or_location') or ''}")
        st.write(f"**Status:** {project_row.get('status') or ''}")
        if project_row.get("project_description"):
            st.write(f"**Description:** {project_row['project_description']}")
        if project_row.get("notes"):
            st.write(f"**Notes:** {project_row['notes']}")

        work_items_df = query_df(
            "SELECT * FROM project_work_items WHERE project_id = %s ORDER BY sort_order NULLS LAST, work_item_name",
            (project_id,),
        )
        if work_items_df.empty:
            st.info("This project has no Sub-Projects yet.")
            return

        all_lines = query_df(
            """
            SELECT wim.work_item_material_id, wi.work_item_id, wi.work_item_name,
                   wim.line_category_snapshot AS category,
                   wim.line_subcategory_snapshot AS subcategory,
                   wim.line_material_name_snapshot AS material,
                   wim.line_description_snapshot AS description,
                   wim.line_vendor_name_snapshot AS vendor,
                   wim.line_vendor_item_number_snapshot AS vendor_item_number,
                   wim.quantity,
                   u.unit_name,
                   u.unit_abbreviation,
                   wim.unit_price,
                   wim.line_total,
                   wim.notes,
                   wim.date_created,
                   wim.date_modified
            FROM work_item_materials wim
            JOIN project_work_items wi ON wim.work_item_id = wi.work_item_id
            LEFT JOIN units_of_measure u ON wim.unit_id = u.unit_id
            WHERE wi.project_id = %s
            ORDER BY wi.sort_order NULLS LAST, wi.work_item_name, category, subcategory, material
            """,
            (project_id,),
        )
        all_lines = format_dates(all_lines)

        project_total = float(all_lines["line_total"].sum()) if not all_lines.empty else 0.0
        st.metric("Project Total", money(project_total))

        st.subheader("Sub-Projects")
        for _, wi in work_items_df.iterrows():
            wi_id = int(wi["work_item_id"])
            wi_name = wi["work_item_name"]
            wi_lines = all_lines[all_lines["work_item_id"] == wi_id].copy() if not all_lines.empty else pd.DataFrame()
            wi_total = float(wi_lines["line_total"].sum()) if not wi_lines.empty else 0.0

            with st.expander(f"{wi_name} — {money(wi_total)}", expanded=False):
                if wi["work_item_description"]:
                    st.write(f"**Description:** {wi['work_item_description']}")
                if wi["notes"]:
                    st.write(f"**Notes:** {wi['notes']}")

                if wi_lines.empty:
                    st.info("No material lines yet.")
                else:
                    display_cols = [
                        "work_item_material_id", "category", "subcategory", "material", "description",
                        "vendor", "vendor_item_number", "quantity", "unit_abbreviation", "unit_price", "line_total", "notes"
                    ]
                    st.dataframe(wi_lines[display_cols], use_container_width=True)

                    cat_rollup = wi_lines.groupby(["category", "subcategory"], dropna=False)["line_total"].sum().reset_index()
                    st.caption("Category / Sub-category Totals")
                    st.dataframe(cat_rollup, use_container_width=True)

                    line_options = {
                        f"{int(r['work_item_material_id'])} | {r['material']} | {money(float(r['line_total']))}": int(r["work_item_material_id"])
                        for _, r in wi_lines.iterrows()
                    }
                    selected_line_label = st.selectbox(
                        f"Select line for {wi_name}",
                        list(line_options.keys()),
                        key=f"line_select_{wi_id}",
                    )
                    selected_line_id = line_options[selected_line_label]
                    line_row = query_one("SELECT * FROM work_item_materials WHERE work_item_material_id = %s", (selected_line_id,))
                    units_df = query_df("SELECT unit_id, unit_name FROM units_of_measure WHERE active = 1 ORDER BY unit_name")
                    unit_map_edit = dict(zip(units_df["unit_name"], units_df["unit_id"]))
                    unit_name_by_id_edit = dict(zip(units_df["unit_id"], units_df["unit_name"]))
                    current_unit_name = unit_name_by_id_edit.get(line_row["unit_id"], list(unit_map_edit.keys())[0])
                    unit_choices_edit = list(unit_map_edit.keys())
                    current_unit_index = unit_choices_edit.index(current_unit_name) if current_unit_name in unit_choices_edit else 0

                    st.markdown("**Edit Selected Line**")
                    current_search_text = st.text_input(
                        "Change material by item number or material name",
                        value=line_row.get("line_vendor_item_number_snapshot") or line_row.get("line_material_name_snapshot") or "",
                        key=f"edit_line_search_{wi_id}",
                        placeholder="Example: 12345 or drywall screw",
                    )
                    edit_search_df = pd.DataFrame()
                    selected_edit_search_row = None
                    if current_search_text.strip():
                        edit_search_df = search_material_selector(current_search_text.strip(), limit=50)
                        if not edit_search_df.empty:
                            edit_search_display = edit_search_df[["display_label", "vendor_item_number", "vendor_name", "default_unit_name", "latest_price"]].rename(
                                columns={
                                    "display_label": "Material",
                                    "vendor_item_number": "Item Number",
                                    "vendor_name": "Vendor",
                                    "default_unit_name": "Default Unit",
                                    "latest_price": "Latest Price",
                                }
                            )
                            st.dataframe(edit_search_display, use_container_width=True, hide_index=True, height=220)
                            current_material_id = line_row.get("material_id")
                            matching_labels = edit_search_df.loc[edit_search_df["material_id"] == current_material_id, "display_label"].tolist()
                            edit_option_labels = edit_search_df["display_label"].tolist()
                            default_edit_index = edit_option_labels.index(matching_labels[0]) if matching_labels else 0
                            selected_edit_label = st.selectbox(
                                "Select replacement material",
                                edit_option_labels,
                                index=default_edit_index,
                                key=f"edit_line_result_{wi_id}",
                            )
                            selected_edit_search_row = edit_search_df.loc[edit_search_df["display_label"] == selected_edit_label].iloc[0].to_dict()
                        else:
                            st.warning("No matching materials found for replacement.")

                    selected_edit_material_id = int(selected_edit_search_row["material_id"]) if selected_edit_search_row else int(line_row["material_id"])
                    selected_default_price = float(line_row.get("unit_price") or 0.0)
                    selected_default_unit_index = current_unit_index
                    selected_vendor_options = {"None": None}
                    default_vendor_label_edit = "None"

                    if selected_edit_search_row is not None:
                        selected_vendor_df = query_df(
                            """
                            SELECT material_vendor_id,
                                   vendor_name || COALESCE(' | ' || vendor_item_number, '') AS label,
                                   latest_retail_price,
                                   latest_quoted_price
                            FROM material_vendor_current
                            WHERE material_id = %s AND active = 1
                            ORDER BY vendor_name, vendor_item_number
                            """,
                            (selected_edit_material_id,),
                        )
                        selected_vendor_price_lookup = {"None": float(selected_edit_search_row.get("latest_price") or 0.0)}
                        if not selected_vendor_df.empty:
                            for _, vendor_row in selected_vendor_df.iterrows():
                                vendor_label = vendor_row["label"]
                                selected_vendor_options[vendor_label] = vendor_row["material_vendor_id"]
                                preferred_price = vendor_row["latest_quoted_price"] if pd.notna(vendor_row["latest_quoted_price"]) and float(vendor_row["latest_quoted_price"] or 0) > 0 else vendor_row["latest_retail_price"]
                                selected_vendor_price_lookup[vendor_label] = float(preferred_price or 0.0)
                                if line_row.get("material_vendor_id") and int(vendor_row["material_vendor_id"]) == int(line_row["material_vendor_id"]):
                                    default_vendor_label_edit = vendor_label
                        selected_default_price = float(selected_vendor_price_lookup.get(default_vendor_label_edit, selected_edit_search_row.get("latest_price") or line_row.get("unit_price") or 0.0))
                        selected_default_unit_id = selected_edit_search_row.get("default_unit_id")
                        if pd.notna(selected_default_unit_id):
                            selected_default_unit_name = unit_name_by_id_edit.get(int(selected_default_unit_id))
                            if selected_default_unit_name in unit_choices_edit:
                                selected_default_unit_index = unit_choices_edit.index(selected_default_unit_name)
                    else:
                        if line_row.get("material_vendor_id"):
                            current_vendor = query_one(
                                "SELECT vendor_name, vendor_item_number FROM material_vendor_current WHERE material_vendor_id = %s",
                                (line_row["material_vendor_id"],),
                            )
                            if current_vendor:
                                default_vendor_label_edit = current_vendor["vendor_name"] + (f" | {current_vendor['vendor_item_number']}" if current_vendor.get("vendor_item_number") else "")
                                selected_vendor_options[default_vendor_label_edit] = line_row["material_vendor_id"]

                    with st.form(f"edit_line_form_{wi_id}"):
                        vendor_choice_edit = st.selectbox(
                            "Vendor record",
                            list(selected_vendor_options.keys()),
                            index=list(selected_vendor_options.keys()).index(default_vendor_label_edit) if default_vendor_label_edit in selected_vendor_options else 0,
                            key=f"edit_vendor_{wi_id}",
                        )
                        e1, e2, e3 = st.columns(3)
                        edit_quantity = e1.number_input("Quantity", min_value=0.0, step=1.0, value=float(line_row["quantity"]), key=f"edit_qty_{wi_id}")
                        edit_unit_name = e2.selectbox("Unit", unit_choices_edit, index=selected_default_unit_index, key=f"edit_unit_{wi_id}")
                        edit_unit_price = e3.number_input("Unit price", min_value=0.0, step=0.01, value=float(selected_default_price), key=f"edit_price_{wi_id}")
                        edit_notes = st.text_input("Notes", value=line_row["notes"] or "", key=f"edit_notes_{wi_id}")
                        col_save, col_delete = st.columns(2)
                        save_clicked = col_save.form_submit_button("Save Line Changes")
                        delete_clicked = col_delete.form_submit_button("Delete Line")

                        if save_clicked:
                            update_material_line_from_master(
                                selected_line_id,
                                selected_edit_material_id,
                                edit_quantity,
                                unit_map_edit[edit_unit_name],
                                edit_unit_price,
                                selected_vendor_options[vendor_choice_edit],
                                edit_notes.strip(),
                            )
                            st.success("Line updated.")
                            st.rerun()

                        if delete_clicked:
                            execute(
                                "DELETE FROM work_item_materials WHERE work_item_material_id = %s",
                                (selected_line_id,),
                            )
                            st.success("Line deleted.")
                            st.rerun()

        st.subheader("Project Summary Table")
        st.dataframe(all_lines, use_container_width=True)
        if not all_lines.empty:
            category_rollup = all_lines.groupby(["work_item_name", "category"], dropna=False)["line_total"].sum().reset_index()
            category_rollup["line_total"] = category_rollup["line_total"].map(lambda x: round(float(x), 2))
            st.subheader("Category Rollup")
            st.dataframe(category_rollup, use_container_width=True)

            col_pdf1, col_pdf2, col_xlsx = st.columns(3)
            project_pdf_key = f"project_pdf_bytes_{project_id}"
            vendor_pdf_key = f"project_vendor_pdf_bytes_{project_id}"
            vendor_xlsx_key = f"project_vendor_xlsx_bytes_{project_id}"

            if col_pdf1.button("Prepare Project PDF", key=f"prep_project_pdf_{project_id}"):
                st.session_state[project_pdf_key] = build_project_report_pdf(project_id).getvalue()
            if project_pdf_key in st.session_state:
                col_pdf1.download_button(
                    "Download Project PDF",
                    data=st.session_state[project_pdf_key],
                    file_name=f"project_report_{project_id}.pdf",
                    mime="application/pdf",
                    key=f"download_project_pdf_{project_id}",
                )

            if col_pdf2.button("Prepare Vendor PDF", key=f"prep_vendor_pdf_{project_id}"):
                st.session_state[vendor_pdf_key] = build_vendor_report_pdf(project_id).getvalue()
            if vendor_pdf_key in st.session_state:
                col_pdf2.download_button(
                    "Download Vendor PDF",
                    data=st.session_state[vendor_pdf_key],
                    file_name=f"vendor_report_{project_id}.pdf",
                    mime="application/pdf",
                    key=f"download_vendor_pdf_{project_id}",
                )

            if col_xlsx.button("Prepare Vendor Excel", key=f"prep_vendor_xlsx_{project_id}"):
                st.session_state[vendor_xlsx_key] = build_vendor_report_excel(project_id).getvalue()
            if vendor_xlsx_key in st.session_state:
                col_xlsx.download_button(
                    "Download Vendor Excel",
                    data=st.session_state[vendor_xlsx_key],
                    file_name=f"vendor_report_{project_id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_vendor_xlsx_{project_id}",
                )


# -----------------------------
# Main app
# -----------------------------

def main() -> None:
    st.set_page_config(page_title="Materials Management System", layout="wide")
    ensure_db_ready()

    if not require_login():
        return

    current_user = st.session_state.get("user", {})

    st.title("Materials Management System")
    st.caption("Materials database + project and sub-project materials management")

    page_labels = [
        "Dashboard",
        "Categories - Categories",
        "Categories - Sub-categories",
        "Categories - Units",
        "Materials - Add Material",
        "Materials - Add Vendor Info",
        "Materials - Search / Review",
        "Materials - Vendor Master Reports",
        "Materials - Import Vendor Checklist PDF",
        "Projects - Create Project",
        "Projects - Add Sub-Project",
        "Projects - Add Materials to Sub-Project",
        "Projects - Project Detail / Reports",
    ]
    if can_manage_users():
        page_labels.extend(["Admin - Create User", "Admin - Manage Users"])

    page_dispatch = {
        "Dashboard": (page_dashboard, None),
        "Categories - Categories": (page_categories, "Categories"),
        "Categories - Sub-categories": (page_categories, "Sub-categories"),
        "Categories - Units": (page_categories, "Units"),
        "Materials - Add Material": (page_materials, "Add Material"),
        "Materials - Add Vendor Info": (page_materials, "Add Vendor Info"),
        "Materials - Search / Review": (page_materials, "Search / Review"),
        "Materials - Vendor Master Reports": (page_materials, "Vendor Master Reports"),
        "Materials - Import Vendor Checklist PDF": (page_materials, "Import Vendor Checklist PDF"),
        "Projects - Create Project": (page_projects, "Create Project"),
        "Projects - Add Sub-Project": (page_projects, "Add Sub-Project"),
        "Projects - Add Materials to Sub-Project": (page_projects, "Add Materials to Sub-Project"),
        "Projects - Project Detail / Reports": (page_projects, "Project Detail / Reports"),
        "Admin - Create User": (page_admin, "Create User"),
        "Admin - Manage Users": (page_admin, "Manage Users"),
    }

    with st.sidebar:
        st.markdown("### Navigation")
        page = st.radio("Go to", page_labels)
        st.markdown("---")
        st.success("Connected to Cloud Database ✅")
        st.markdown("---")
        st.write(f"**User:** {current_user.get('username', '')}")
        st.write(f"**Type:** {current_user.get('role', '')}")
        if st.button("Log Out"):
            logout()

    handler, section = page_dispatch[page]
    if section is None:
        handler()
    else:
        handler(section)


if __name__ == "__main__":
    main()
