import html
import json
import os
import re
import uuid
import zipfile
from contextlib import closing
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import psycopg
import streamlit as st
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings
from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
try:
    from PIL import Image
except Exception:
    Image = None

DEFAULT_DATABASE_URL_ENV = "DATABASE_URL"
TASK_CATALOG_CANDIDATES = [
    Path("Updated Task & Trade List.xlsx"),
    Path("Cleaned Final List With Measurements.xlsx"),
    Path("final_task_list_ready.xlsx"),
]


APP_USERS = {
    "brent": "change123",
    "user2": "change123",
    "user3": "change123",
}

# Build 18F: expanded user roles for Quality Control assignment.
USER_ROLE_OPTIONS = [
    "Owner",
    "Renovation Manager",
    "Property Manager",
    "Maintenance",
    "Lawn & Landscape",
    "Contractor",
    "Other",
]
QC_USER_ASSIGNMENT_TYPES = [
    "Unassigned",
    "Property Manager",
    "Maintenance",
    "Lawn & Landscape",
    "Renovation Manager",
    "Owner",
    "Other",
    "Contractor",
]


# -----------------------------
# Database helpers
# -----------------------------
def get_pg_conninfo() -> str:
    if "database_url" in st.secrets:
        return str(st.secrets["database_url"])

    if "connections" in st.secrets and "postgresql" in st.secrets["connections"]:
        cfg = st.secrets["connections"]["postgresql"]
        if "url" in cfg:
            return str(cfg["url"])
        host = cfg.get("host", "")
        dbname = cfg.get("database", "")
        user = cfg.get("username", cfg.get("user", ""))
        password = cfg.get("password", "")
        port = cfg.get("port", 5432)
        sslmode = cfg.get("sslmode", "require")
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"

    if "postgresql" in st.secrets:
        cfg = st.secrets["postgresql"]
        if "url" in cfg:
            return str(cfg["url"])
        host = cfg.get("host", "")
        dbname = cfg.get("database", "")
        user = cfg.get("username", cfg.get("user", ""))
        password = cfg.get("password", "")
        port = cfg.get("port", 5432)
        sslmode = cfg.get("sslmode", "require")
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"

    env_url = os.environ.get(DEFAULT_DATABASE_URL_ENV, "").strip()
    if env_url:
        return env_url

    raise RuntimeError(
        "PostgreSQL connection not found. Add a Neon/Postgres connection string to Streamlit secrets or set DATABASE_URL."
    )



def get_azure_blob_connection_string() -> str:
    if "AZURE_STORAGE_CONNECTION_STRING" in st.secrets:
        return str(st.secrets["AZURE_STORAGE_CONNECTION_STRING"])
    if "azure_storage_connection_string" in st.secrets:
        return str(st.secrets["azure_storage_connection_string"])
    if "azure_blob" in st.secrets and "connection_string" in st.secrets["azure_blob"]:
        return str(st.secrets["azure_blob"]["connection_string"])

    env_value = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    return env_value


def get_azure_container_name() -> str:
    if "AZURE_CONTAINER_NAME" in st.secrets:
        return str(st.secrets["AZURE_CONTAINER_NAME"])
    if "azure_container_name" in st.secrets:
        return str(st.secrets["azure_container_name"])
    if "azure_blob" in st.secrets and "container_name" in st.secrets["azure_blob"]:
        return str(st.secrets["azure_blob"]["container_name"])

    env_value = os.environ.get("AZURE_CONTAINER_NAME", "").strip()
    return env_value


def azure_blob_enabled() -> bool:
    return bool(get_azure_blob_connection_string() and get_azure_container_name())


@st.cache_resource(show_spinner=False)
def get_blob_service_client():
    connection_string = get_azure_blob_connection_string()
    if not connection_string:
        return None
    return BlobServiceClient.from_connection_string(connection_string)


def get_blob_container_client():
    service_client = get_blob_service_client()
    container_name = get_azure_container_name()
    if service_client is None or not container_name:
        return None
    container_client = service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass
    return container_client


@st.cache_data(show_spinner=False, ttl=1800)
def cached_download_blob_bytes(blob_name: str) -> bytes | None:
    if not blob_name or not azure_blob_enabled():
        return None
    container_client = get_blob_container_client()
    if container_client is None:
        return None
    try:
        return container_client.get_blob_client(blob_name).download_blob().readall()
    except ResourceNotFoundError:
        return None
    except Exception:
        return None


def sanitize_blob_component(value: str) -> str:
    value = str(value or "").strip().replace("\\", "/")
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value)
    value = value.strip(".-")
    return value or "file"


def upload_bytes_to_blob(data: bytes, filename: str, content_type: str, folder: str = "repair-photos") -> dict:
    if not data:
        return {
            "filename": filename,
            "content_type": content_type,
            "storage_mode": "database",
            "blob_url": "",
            "blob_name": "",
            "bytes": data,
        }

    if not azure_blob_enabled():
        return {
            "filename": filename,
            "content_type": content_type,
            "storage_mode": "database",
            "blob_url": "",
            "blob_name": "",
            "bytes": data,
        }

    container_client = get_blob_container_client()
    if container_client is None:
        return {
            "filename": filename,
            "content_type": content_type,
            "storage_mode": "database",
            "blob_url": "",
            "blob_name": "",
            "bytes": data,
        }

    safe_name = sanitize_blob_component(filename)
    safe_folder = "/".join([sanitize_blob_component(part) for part in str(folder).split("/") if part.strip()])
    blob_name = f"{safe_folder}/{uuid.uuid4().hex}_{safe_name}" if safe_folder else f"{uuid.uuid4().hex}_{safe_name}"

    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type or "application/octet-stream"),
        )
        return {
            "filename": filename,
            "content_type": content_type or "application/octet-stream",
            "storage_mode": "azure_blob",
            "blob_url": blob_client.url,
            "blob_name": blob_name,
            "bytes": None,
        }
    except Exception:
        return {
            "filename": filename,
            "content_type": content_type,
            "storage_mode": "database",
            "blob_url": "",
            "blob_name": "",
            "bytes": data,
        }


def download_blob_bytes(blob_name: str) -> bytes | None:
    if not blob_name or not azure_blob_enabled():
        return None
    container_client = get_blob_container_client()
    if container_client is None:
        return None
    try:
        return container_client.get_blob_client(blob_name).download_blob().readall()
    except ResourceNotFoundError:
        return None
    except Exception:
        return None


def sql_params(query: str) -> str:
    return query.replace("?", "%s")


def get_conn():
    return psycopg.connect(get_pg_conninfo())


def init_db():
    required_tables = [
        "trades",
        "tasks",
        "contractors",
        "scope_templates",
        "estimates",
        "estimate_lines",
        "schedule_entries",
        "estimate_notes_log",
        "punch_list_projects",
        "punch_list_items",
        "punch_list_item_photos",
        "project_status_entries",
        "project_status_photos",
        "project_registry",
        "work_groups",
        "work_group_photos",
        "work_group_contractor_notes",
        "renovation_pipeline_items",
        "renovation_pipeline_files",
        "renovation_pipeline_cash_flows",
        "manager_repair_requests",
        "manager_repair_request_files",
        "manager_repair_request_comments",
        "quality_control_items",
        "quality_control_files",
        "quality_control_comments",
        "portfolio_properties",
        "portfolio_addresses",
        "renovation_master_records",
        "renovation_master_record_files",
        "renovation_master_record_history",
        "rmr_groups",
        "rmr_group_members",
    ]
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                notes TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS estimate_line_photos (
                id BIGSERIAL PRIMARY KEY,
                estimate_id BIGINT REFERENCES estimates(id) ON DELETE CASCADE,
                estimate_line_id BIGINT REFERENCES estimate_lines(id) ON DELETE CASCADE,
                photo_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                photo_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS allowed_portfolio TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE estimates ADD COLUMN IF NOT EXISTS order_number TEXT")
        cur.execute("ALTER TABLE estimates ADD COLUMN IF NOT EXISTS category_name TEXT")
        cur.execute("ALTER TABLE estimates ADD COLUMN IF NOT EXISTS work_group_name TEXT")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS category_name TEXT")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS work_group_name TEXT")
        cur.execute("ALTER TABLE punch_list_projects ADD COLUMN IF NOT EXISTS order_number TEXT")
        cur.execute("ALTER TABLE punch_list_items ADD COLUMN IF NOT EXISTS order_number TEXT")
        cur.execute("UPDATE estimates SET order_number = 'Est' || id::text WHERE COALESCE(order_number, '') = ''")
        cur.execute("UPDATE punch_list_projects SET order_number = 'PL' || id::text WHERE COALESCE(order_number, '') = ''")
        cur.execute("UPDATE punch_list_items SET order_number = 'PLWG' || id::text WHERE COALESCE(order_number, '') = ''")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS manual_repair_amount NUMERIC(12,2)")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS approved_final_cost NUMERIC(12,2)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_accounts (
                id BIGSERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'Other',
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS punch_list_projects (
                id BIGSERIAL PRIMARY KEY,
                project_name TEXT NOT NULL,
                project_address TEXT,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                status TEXT NOT NULL DEFAULT 'Open',
                inspection_date DATE,
                deadline_date DATE,
                notes TEXT,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS punch_list_items (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT NOT NULL REFERENCES punch_list_projects(id) ON DELETE CASCADE,
                item_title TEXT NOT NULL,
                trade_name TEXT,
                scope_description TEXT,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                item_status TEXT NOT NULL DEFAULT 'Open',
                identified_date DATE,
                deadline_date DATE,
                completed_date DATE,
                quote_requested BOOLEAN NOT NULL DEFAULT FALSE,
                manager_notes TEXT,
                contractor_notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS punch_list_item_photos (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT REFERENCES punch_list_projects(id) ON DELETE CASCADE,
                punch_list_item_id BIGINT REFERENCES punch_list_items(id) ON DELETE CASCADE,
                photo_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                photo_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS project_status_entries (
                id BIGSERIAL PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id BIGINT NOT NULL,
                project_name TEXT NOT NULL,
                entry_date DATE NOT NULL,
                note_text TEXT,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS project_status_photos (
                id BIGSERIAL PRIMARY KEY,
                status_entry_id BIGINT REFERENCES project_status_entries(id) ON DELETE CASCADE,
                photo_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                photo_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS project_registry (
                id BIGSERIAL PRIMARY KEY,
                project_name TEXT NOT NULL,
                project_address TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE estimates ADD COLUMN IF NOT EXISTS project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE punch_list_projects ADD COLUMN IF NOT EXISTS project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE project_status_entries ADD COLUMN IF NOT EXISTS project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS notes TEXT")
        cur.execute("ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS project_code TEXT")
        cur.execute("ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS activated_at TIMESTAMPTZ")
        cur.execute("ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS final_project_cost NUMERIC(12,2)")
        cur.execute("ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS materials_notes TEXT")
        cur.execute("ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS work_item_code TEXT")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS project_material_files (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
                file_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                file_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quote_requests (
                id BIGSERIAL PRIMARY KEY,
                estimate_line_id BIGINT NOT NULL REFERENCES estimate_lines(id) ON DELETE CASCADE,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                quote_status TEXT NOT NULL DEFAULT 'Requested',
                quote_amount NUMERIC(12,2),
                quote_notes TEXT,
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                submitted_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_item_costs (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT REFERENCES project_registry(id) ON DELETE CASCADE,
                estimate_line_id BIGINT REFERENCES estimate_lines(id) ON DELETE SET NULL,
                task_name TEXT NOT NULL DEFAULT '',
                trade_name TEXT NOT NULL DEFAULT '',
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                agreed_price NUMERIC(12,2) NOT NULL DEFAULT 0,
                entered_date DATE,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_groups (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
                estimate_line_id BIGINT REFERENCES estimate_lines(id) ON DELETE SET NULL,
                task_name TEXT NOT NULL DEFAULT '',
                trade_name TEXT NOT NULL DEFAULT '',
                scope_description TEXT,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                agreed_price NUMERIC(12,2),
                estimated_price NUMERIC(12,2),
                due_date DATE,
                status TEXT NOT NULL DEFAULT 'Open',
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS work_group_address TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS work_group_unit_number TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS order_number TEXT")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS contractor_requested_price NUMERIC(12,2)")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS amount_to_be_paid NUMERIC(12,2)")
        cur.execute("UPDATE work_groups SET amount_to_be_paid = agreed_price WHERE amount_to_be_paid IS NULL AND COALESCE(agreed_price, 0) > 0")
        cur.execute("UPDATE work_groups SET order_number = 'WG' || id::text WHERE COALESCE(order_number, '') = ''")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS category_name TEXT")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS work_group_name TEXT")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS cashflow_export_status TEXT NOT NULL DEFAULT 'Not Exported'")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS cashflow_last_exported_at TIMESTAMPTZ")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS cashflow_export_signature TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS contractor_priority TEXT NOT NULL DEFAULT '3 - Quote Only'")
        cur.execute("ALTER TABLE work_groups ADD COLUMN IF NOT EXISTS owner_intent TEXT NOT NULL DEFAULT 'Quote Only'")
        cur.execute("UPDATE estimate_lines SET category_name = trade_name WHERE COALESCE(category_name, '') = ''")
        cur.execute("UPDATE work_groups SET category_name = trade_name WHERE COALESCE(category_name, '') = ''")
        cur.execute("UPDATE work_groups SET work_group_name = task_name WHERE COALESCE(work_group_name, '') = ''")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_group_photos (
                id BIGSERIAL PRIMARY KEY,
                work_group_id BIGINT NOT NULL REFERENCES work_groups(id) ON DELETE CASCADE,
                photo_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                photo_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_group_contractor_notes (
                id BIGSERIAL PRIMARY KEY,
                work_group_id BIGINT NOT NULL REFERENCES work_groups(id) ON DELETE CASCADE,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                note_text TEXT NOT NULL DEFAULT '',
                entered_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        # One-time compatibility migration: if an older database still has the old work_orders table,
        # copy any existing rows into the new work_groups table only when work_groups is empty.
        try:
            cur.execute("SELECT to_regclass(%s)", ("public.work_orders",))
            old_work_groups_table_exists = cur.fetchone()[0] is not None
            if old_work_groups_table_exists:
                cur.execute("SELECT COUNT(*) FROM work_groups")
                new_work_group_count = int(cur.fetchone()[0] or 0)
                if new_work_group_count == 0:
                    cur.execute(
                        """
                        INSERT INTO work_groups (
                            id, project_id, estimate_line_id, task_name, trade_name, scope_description,
                            contractor_id, agreed_price, estimated_price, due_date, status, notes,
                            created_at, modified_at
                        )
                        SELECT
                            id, project_id, estimate_line_id, task_name, trade_name, scope_description,
                            contractor_id, agreed_price, estimated_price, due_date, status, notes,
                            created_at, modified_at
                        FROM work_orders
                        """
                    )
                    cur.execute(
                        "SELECT setval(pg_get_serial_sequence('work_groups', 'id'), COALESCE(MAX(id), 1), COALESCE(MAX(id), 0) > 0) FROM work_groups"
                    )
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contractor_weekly_schedules (
                id BIGSERIAL PRIMARY KEY,
                contractor_id BIGINT NOT NULL REFERENCES contractors(id) ON DELETE CASCADE,
                contractor_name TEXT NOT NULL DEFAULT '',
                week_start_date DATE NOT NULL,
                day_name TEXT NOT NULL,
                am_project_name TEXT NOT NULL DEFAULT '',
                am_crew_members TEXT NOT NULL DEFAULT '',
                pm_project_name TEXT NOT NULL DEFAULT '',
                pm_crew_members TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                submitted_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(contractor_id, week_start_date, day_name)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS renovation_pipeline_items (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL,
                project_name TEXT NOT NULL DEFAULT '',
                project_address TEXT NOT NULL DEFAULT '',
                category_name TEXT NOT NULL DEFAULT '',
                work_group_name TEXT NOT NULL DEFAULT '',
                work_item_name TEXT NOT NULL DEFAULT '',
                priority TEXT NOT NULL DEFAULT 'Medium',
                status TEXT NOT NULL DEFAULT 'Idea',
                target_timing TEXT NOT NULL DEFAULT '',
                rough_budget NUMERIC(12,2) NOT NULL DEFAULT 0,
                rough_labor_hours NUMERIC(12,2) NOT NULL DEFAULT 0,
                rough_duration TEXT NOT NULL DEFAULT '',
                cash_flow_notes TEXT NOT NULL DEFAULT '',
                scope_description TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                promoted_project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL,
                promoted_estimate_id BIGINT REFERENCES estimates(id) ON DELETE SET NULL,
                promoted_work_group_id BIGINT REFERENCES work_groups(id) ON DELETE SET NULL,
                archived BOOLEAN NOT NULL DEFAULT FALSE,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS renovation_pipeline_files (
                id BIGSERIAL PRIMARY KEY,
                pipeline_item_id BIGINT NOT NULL REFERENCES renovation_pipeline_items(id) ON DELETE CASCADE,
                file_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                file_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS renovation_pipeline_cash_flows (
                id BIGSERIAL PRIMARY KEY,
                pipeline_item_id BIGINT NOT NULL REFERENCES renovation_pipeline_items(id) ON DELETE CASCADE,
                scheduled_date DATE,
                amount NUMERIC(12,2) NOT NULL DEFAULT 0,
                payment_type TEXT NOT NULL DEFAULT 'Planned',
                status TEXT NOT NULL DEFAULT 'Draft',
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manager_repair_requests (
                id BIGSERIAL PRIMARY KEY,
                manager_user_id BIGINT REFERENCES user_accounts(id) ON DELETE SET NULL,
                manager_username TEXT NOT NULL DEFAULT '',
                date_requested DATE,
                property_name TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                unit_number TEXT NOT NULL DEFAULT '',
                repair_description TEXT NOT NULL DEFAULT '',
                priority TEXT NOT NULL DEFAULT '2. When you can schedule it in',
                status TEXT NOT NULL DEFAULT 'New Request',
                owner_response TEXT NOT NULL DEFAULT '',
                archived BOOLEAN NOT NULL DEFAULT FALSE,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manager_repair_request_files (
                id BIGSERIAL PRIMARY KEY,
                request_id BIGINT NOT NULL REFERENCES manager_repair_requests(id) ON DELETE CASCADE,
                file_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                file_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manager_repair_request_comments (
                id BIGSERIAL PRIMARY KEY,
                request_id BIGINT NOT NULL REFERENCES manager_repair_requests(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES user_accounts(id) ON DELETE SET NULL,
                username TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT '',
                comment_text TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_control_items (
                id BIGSERIAL PRIMARY KEY,
                qc_code TEXT UNIQUE,
                entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
                property_name TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                unit_number TEXT NOT NULL DEFAULT '',
                location_identifier TEXT NOT NULL DEFAULT '',
                work_item_name TEXT NOT NULL DEFAULT '',
                category_name TEXT NOT NULL DEFAULT '',
                issue_description TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                priority TEXT NOT NULL DEFAULT '2 - Normal',
                status TEXT NOT NULL DEFAULT 'Open',
                due_date DATE,
                follow_up_date DATE,
                assignee_type TEXT NOT NULL DEFAULT '',
                assignee_name TEXT NOT NULL DEFAULT '',
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                assigned_user_id BIGINT REFERENCES user_accounts(id) ON DELETE SET NULL,
                completed_date DATE,
                verified_date DATE,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_control_files (
                id BIGSERIAL PRIMARY KEY,
                qc_item_id BIGINT NOT NULL REFERENCES quality_control_items(id) ON DELETE CASCADE,
                file_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                file_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_control_comments (
                id BIGSERIAL PRIMARY KEY,
                qc_item_id BIGINT NOT NULL REFERENCES quality_control_items(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES user_accounts(id) ON DELETE SET NULL,
                username TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT '',
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                comment_text TEXT NOT NULL DEFAULT '',
                status_update TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS qc_code TEXT UNIQUE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS entry_date DATE NOT NULL DEFAULT CURRENT_DATE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS property_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS address TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS unit_number TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS location_identifier TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS work_item_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS category_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS issue_description TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT '2 - Normal'")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'Open'")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS due_date DATE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS follow_up_date DATE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS assignee_type TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS assignee_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS assigned_user_id BIGINT REFERENCES user_accounts(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS completed_date DATE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS verified_date DATE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("UPDATE quality_control_items SET qc_code = 'QC-' || LPAD(id::text, 6, '0') WHERE COALESCE(qc_code, '') = ''")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_properties (
                id BIGSERIAL PRIMARY KEY,
                portfolio_name TEXT NOT NULL DEFAULT '',
                property_name TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                active BOOLEAN NOT NULL DEFAULT TRUE,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_addresses (
                id BIGSERIAL PRIMARY KEY,
                portfolio_property_id BIGINT REFERENCES portfolio_properties(id) ON DELETE CASCADE,
                portfolio_name TEXT NOT NULL DEFAULT '',
                property_name TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                unit_number TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                active BOOLEAN NOT NULL DEFAULT TRUE,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS renovation_master_records (
                id BIGSERIAL PRIMARY KEY,
                rmr_code TEXT UNIQUE,
                entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
                portfolio_name TEXT NOT NULL DEFAULT '',
                property_name TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                unit_number TEXT NOT NULL DEFAULT '',
                location_identifier TEXT NOT NULL DEFAULT '',
                work_item_name TEXT NOT NULL DEFAULT '',
                category_name TEXT NOT NULL DEFAULT '',
                scope_description TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                materials_notes TEXT NOT NULL DEFAULT '',
                scope_complete BOOLEAN NOT NULL DEFAULT FALSE,
                ai_estimated_hours NUMERIC(12,2),
                user_estimated_hours NUMERIC(12,2),
                labor_budget NUMERIC(12,2),
                materials_budget NUMERIC(12,2),
                budget_timeframe TEXT NOT NULL DEFAULT 'No Timeframe Yet',
                budget_start_date DATE,
                budget_end_date DATE,
                budget_status TEXT NOT NULL DEFAULT 'Active',
                info_status TEXT NOT NULL DEFAULT 'Open',
                project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL,
                work_group_id BIGINT REFERENCES work_groups(id) ON DELETE SET NULL,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS renovation_master_record_files (
                id BIGSERIAL PRIMARY KEY,
                rmr_id BIGINT NOT NULL REFERENCES renovation_master_records(id) ON DELETE CASCADE,
                file_filename TEXT,
                content_type TEXT,
                storage_mode TEXT NOT NULL DEFAULT 'database',
                blob_url TEXT,
                blob_name TEXT,
                file_bytes BYTEA,
                sort_order INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS renovation_master_record_history (
                id BIGSERIAL PRIMARY KEY,
                rmr_id BIGINT NOT NULL REFERENCES renovation_master_records(id) ON DELETE CASCADE,
                action_type TEXT NOT NULL DEFAULT '',
                action_notes TEXT NOT NULL DEFAULT '',
                changed_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rmr_communications (
                id BIGSERIAL PRIMARY KEY,
                rmr_id BIGINT NOT NULL REFERENCES renovation_master_records(id) ON DELETE CASCADE,
                quote_request_id BIGINT REFERENCES quote_requests(id) ON DELETE SET NULL,
                contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL,
                author_type TEXT NOT NULL DEFAULT '',
                author_name TEXT NOT NULL DEFAULT '',
                message_text TEXT NOT NULL DEFAULT '',
                is_unread_for_owner BOOLEAN NOT NULL DEFAULT FALSE,
                is_unread_for_contractor BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rmr_groups (
                id BIGSERIAL PRIMARY KEY,
                group_name TEXT NOT NULL DEFAULT '',
                property_name TEXT NOT NULL DEFAULT '',
                project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL,
                notes TEXT NOT NULL DEFAULT '',
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rmr_group_members (
                id BIGSERIAL PRIMARY KEY,
                rmr_group_id BIGINT NOT NULL REFERENCES rmr_groups(id) ON DELETE CASCADE,
                rmr_id BIGINT NOT NULL REFERENCES renovation_master_records(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(rmr_group_id, rmr_id)
            )
            """
        )
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS rmr_code TEXT UNIQUE")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS entry_date DATE NOT NULL DEFAULT CURRENT_DATE")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS portfolio_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS property_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS address TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS unit_number TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS location_identifier TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS work_item_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS category_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS scope_description TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS materials_notes TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS scope_complete BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS ai_estimated_hours NUMERIC(12,2)")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS user_estimated_hours NUMERIC(12,2)")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS labor_budget NUMERIC(12,2)")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS materials_budget NUMERIC(12,2)")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS budget_timeframe TEXT NOT NULL DEFAULT 'No Timeframe Yet'")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS budget_start_date DATE")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS budget_end_date DATE")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS budget_status TEXT NOT NULL DEFAULT 'Active'")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS info_status TEXT NOT NULL DEFAULT 'Open'")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS work_group_id BIGINT REFERENCES work_groups(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS cashflow_export_status TEXT NOT NULL DEFAULT 'Not Exported'")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS cashflow_last_exported_at TIMESTAMPTZ")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS cashflow_export_signature TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS contractor_priority TEXT NOT NULL DEFAULT '3 - Quote Only'")
        cur.execute("ALTER TABLE renovation_master_records ADD COLUMN IF NOT EXISTS owner_intent TEXT NOT NULL DEFAULT 'Quote Only'")
        cur.execute("ALTER TABLE estimates ADD COLUMN IF NOT EXISTS source_rmr_id BIGINT")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS source_rmr_id BIGINT")
        cur.execute("ALTER TABLE estimate_lines ADD COLUMN IF NOT EXISTS contractor_id BIGINT REFERENCES contractors(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE quote_requests ADD COLUMN IF NOT EXISTS rmr_id BIGINT")
        cur.execute("UPDATE renovation_master_records SET budget_status = 'Cancelled' WHERE COALESCE(budget_status, '') = 'Deleted'")
        cur.execute("UPDATE renovation_master_records SET rmr_code = 'RMR-' || LPAD(id::text, 6, '0') WHERE COALESCE(rmr_code, '') = ''")
        cur.execute("ALTER TABLE manager_repair_requests ADD COLUMN IF NOT EXISTS portfolio_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE manager_repair_requests ADD COLUMN IF NOT EXISTS manager_user_id BIGINT REFERENCES user_accounts(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE manager_repair_requests ADD COLUMN IF NOT EXISTS manager_username TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE manager_repair_requests ADD COLUMN IF NOT EXISTS owner_response TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE manager_repair_requests ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE manager_repair_requests ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS project_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS project_address TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS category_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS work_group_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS work_item_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'Medium'")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'Idea'")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS target_timing TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS rough_budget NUMERIC(12,2) NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS rough_labor_hours NUMERIC(12,2) NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS rough_duration TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS scope_description TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS promoted_project_id BIGINT REFERENCES project_registry(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS promoted_estimate_id BIGINT REFERENCES estimates(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS promoted_work_group_id BIGINT REFERENCES work_groups(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS promoted_rmr_id BIGINT REFERENCES renovation_master_records(id) ON DELETE SET NULL")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS cash_flow_notes TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE renovation_pipeline_items ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("UPDATE project_registry SET project_code = 'PRJ-' || LPAD(id::text, 6, '0') WHERE COALESCE(project_code, '') = ''")
        cur.execute("UPDATE tasks SET work_item_code = 'WI-' || LPAD(id::text, 6, '0') WHERE COALESCE(work_item_code, '') = ''")
        conn.commit()

        missing = []
        for table_name in required_tables:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            if cur.fetchone()[0] is None:
                missing.append(table_name)

        if missing:
            raise RuntimeError(
                "Missing required PostgreSQL tables: " + ", ".join(missing) + ". Run renovation_estimator_postgres_schema.sql first."
            )


def seed_defaults():
    seed_default_user_accounts()
    return


def fetch_df(query, params=()):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_params(query), params)
            rows = cur.fetchall()
            cols = [desc.name if hasattr(desc, 'name') else desc[0] for desc in cur.description] if cur.description else []
            return pd.DataFrame(rows, columns=cols)


def execute(query, params=()):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_params(query), params)
        conn.commit()
    st.cache_data.clear()
    return None


def set_order_number(table_name: str, record_id: int, prefix: str):
    if not record_id:
        return
    allowed = {
        "estimates": "Est",
        "work_groups": "WG",
        "punch_list_projects": "PL",
        "punch_list_items": "PLWG",
    }
    if table_name not in allowed or allowed[table_name] != prefix:
        return
    execute(
        f"UPDATE {table_name} SET order_number = ? WHERE id = ? AND COALESCE(order_number, '') = ''",
        (f"{prefix}{int(record_id)}", int(record_id)),
    )


def execute_returning_id(query, params=()):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            sql = sql_params(query)
            if "RETURNING" not in sql.upper():
                sql = sql.rstrip().rstrip(';') + " RETURNING id"
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
    st.cache_data.clear()
    return row[0] if row else None


def delete_estimate(estimate_id: int):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            line_ids_df = fetch_df("SELECT id FROM estimate_lines WHERE estimate_id = ?", (estimate_id,))
            line_ids = [int(v) for v in line_ids_df["id"].tolist()] if not line_ids_df.empty else []

            if line_ids:
                placeholders = ", ".join(["%s"] * len(line_ids))
                cur.execute(f"DELETE FROM work_item_costs WHERE estimate_line_id IN ({placeholders})", tuple(line_ids))
                cur.execute(f"DELETE FROM work_groups WHERE estimate_line_id IN ({placeholders})", tuple(line_ids))
                cur.execute(f"DELETE FROM quote_requests WHERE estimate_line_id IN ({placeholders})", tuple(line_ids))

            cur.execute(sql_params("DELETE FROM estimate_line_photos WHERE estimate_id = ?"), (estimate_id,))
            cur.execute(sql_params("DELETE FROM estimate_lines WHERE estimate_id = ?"), (estimate_id,))
            cur.execute(sql_params("DELETE FROM estimates WHERE id = ?"), (estimate_id,))
        conn.commit()
    st.cache_data.clear()


def delete_work_group(work_group_id: int):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_params("DELETE FROM work_group_contractor_notes WHERE work_group_id = ?"), (work_group_id,))
            cur.execute(sql_params("DELETE FROM work_group_photos WHERE work_group_id = ?"), (work_group_id,))
            cur.execute(sql_params("DELETE FROM work_groups WHERE id = ?"), (work_group_id,))
        conn.commit()
    st.cache_data.clear()


def work_group_duplicate_exists(
    project_id: int,
    task_name: str,
    trade_name: str,
    exclude_work_group_id: int | None = None,
    category_name: str = "",
    work_group_name: str = "",
) -> bool:
    query = """
        SELECT id
        FROM work_groups
        WHERE COALESCE(project_id, 0) = ?
          AND LOWER(TRIM(COALESCE(task_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(trade_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(category_name, trade_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(work_group_name, task_name, ''))) = LOWER(TRIM(?))
    """
    params = [
        project_id,
        str(task_name or "").strip(),
        str(trade_name or "").strip(),
        str(category_name or trade_name or "").strip(),
        str(work_group_name or task_name or "").strip(),
    ]
    if exclude_work_group_id:
        query += " AND id <> ?"
        params.append(exclude_work_group_id)
    query += " LIMIT 1"
    df = fetch_df(query, tuple(params))
    return not df.empty


def delete_punch_list_project(project_id: int):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_params("DELETE FROM punch_list_item_photos WHERE project_id = ?"), (project_id,))
            cur.execute(sql_params("DELETE FROM punch_list_items WHERE project_id = ?"), (project_id,))
            cur.execute(sql_params("DELETE FROM punch_list_projects WHERE id = ?"), (project_id,))
        conn.commit()
    st.cache_data.clear()


@st.cache_data(show_spinner=False, ttl=300)
def work_group_contractor_notes_df(work_group_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            won.id,
            won.work_group_id,
            COALESCE(won.contractor_id, 0) AS contractor_id,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(won.note_text, '') AS note_text,
            COALESCE(won.entered_by, '') AS entered_by,
            won.created_at
        FROM work_group_contractor_notes won
        LEFT JOIN contractors c ON c.id = won.contractor_id
        WHERE won.work_group_id = ?
        ORDER BY won.created_at DESC, won.id DESC
        """,
        (work_group_id,),
    )


def add_work_group_contractor_note(work_group_id: int, contractor_id: int | None, note_text: str, entered_by: str = ""):
    execute(
        """
        INSERT INTO work_group_contractor_notes (
            work_group_id, contractor_id, note_text, entered_by, created_at
        ) VALUES (?, ?, ?, ?, NOW())
        """,
        (
            work_group_id,
            contractor_id,
            str(note_text or "").strip(),
            str(entered_by or "").strip(),
        ),
    )


def render_work_group_contractor_notes(work_group_id: int):
    notes_df = work_group_contractor_notes_df(work_group_id)
    st.markdown("### Contractor Notes / Updates")
    if notes_df.empty:
        st.info("No contractor notes have been entered yet.")
        return

    for row in notes_df.itertuples():
        created_display = pd.to_datetime(row.created_at, errors="coerce")
        created_text = created_display.strftime("%m-%d-%Y %I:%M %p") if pd.notna(created_display) else ""
        contractor_label = str(getattr(row, "contractor_name", "") or getattr(row, "entered_by", "") or "Contractor")
        st.markdown(f"**{created_text} — {contractor_label}**")
        st.write(str(getattr(row, "note_text", "") or ""))
        st.markdown("---")


def work_group_photo_row_to_dict(row) -> dict:
    photo_bytes = row.get("photo_bytes")
    if photo_bytes is not None and not isinstance(photo_bytes, (bytes, bytearray)):
        try:
            photo_bytes = bytes(photo_bytes)
        except Exception:
            photo_bytes = None
    return {
        "id": int(row["id"]) if pd.notna(row.get("id")) else None,
        "filename": str(row.get("photo_filename") or "photo"),
        "content_type": str(row.get("content_type") or "image/jpeg"),
        "storage_mode": str(row.get("storage_mode") or "database"),
        "blob_url": str(row.get("blob_url") or ""),
        "blob_name": str(row.get("blob_name") or ""),
        "bytes": photo_bytes,
        "sort_order": int(row.get("sort_order") or 0),
        "uploaded_by": str(row.get("uploaded_by") or ""),
    }


@st.cache_data(show_spinner=False, ttl=300)
def work_group_photos_df(work_group_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            work_group_id,
            COALESCE(photo_filename, '') AS photo_filename,
            COALESCE(content_type, 'image/jpeg') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            photo_bytes,
            COALESCE(sort_order, 0) AS sort_order,
            COALESCE(uploaded_by, '') AS uploaded_by,
            created_at
        FROM work_group_photos
        WHERE work_group_id = ?
        ORDER BY sort_order, id
        """,
        (work_group_id,),
    )


def save_work_group_photos(work_group_id: int, uploaded_files, uploaded_by: str = ""):
    photos = normalize_uploaded_photos(uploaded_files)
    if not photos:
        return
    existing_df = work_group_photos_df(work_group_id)
    existing_count = len(existing_df) if existing_df is not None else 0
    for offset, photo in enumerate(photos):
        execute(
            """
            INSERT INTO work_group_photos (
                work_group_id, photo_filename, content_type, storage_mode, blob_url, blob_name,
                photo_bytes, sort_order, uploaded_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            """,
            (
                work_group_id,
                photo.get("filename"),
                photo.get("content_type"),
                photo.get("storage_mode"),
                photo.get("blob_url"),
                photo.get("blob_name"),
                photo.get("bytes"),
                existing_count + offset,
                uploaded_by or "",
            ),
        )


def delete_work_group_photo(photo_id: int):
    execute("DELETE FROM work_group_photos WHERE id = ?", (photo_id,))


def render_work_group_photos_section(work_group_id: int, section_key: str):
    photos_df = work_group_photos_df(work_group_id)
    if photos_df.empty:
        st.info('No Work Group photos saved yet.')
        return

    st.caption(f"{len(photos_df)} Work Group photo(s) saved.")
    if st.checkbox('Load Work Group photo previews', key=f"load_work_group_photos_{section_key}_{work_group_id}", value=False):
        photos = [work_group_photo_row_to_dict(row) for _, row in photos_df.iterrows()]
        cols = st.columns(min(4, max(1, len(photos))))
        for idx, photo in enumerate(photos):
            with cols[idx % len(cols)]:
                render_photo_item(photo)

    delete_labels = [
        f"{int(row.id)} | {row.photo_filename or 'photo'}"
        for row in photos_df.itertuples()
    ]
    if delete_labels:
        selected_photo_label = st.selectbox(
            'Choose Work Group Photo To Delete',
            delete_labels,
            key=f"delete_work_group_photo_select_{section_key}_{work_group_id}",
        )
        selected_photo_id = int(selected_photo_label.split(" | ", 1)[0])
        confirm_key = f"confirm_delete_work_group_photo_{section_key}_{selected_photo_id}"
        if confirm_key not in st.session_state:
            st.session_state[confirm_key] = False

        if not st.session_state[confirm_key]:
            if st.button("Delete Selected Work Group Photo", type="secondary", key=f"delete_work_group_photo_btn_{section_key}_{selected_photo_id}"):
                st.session_state[confirm_key] = True
                st.rerun()
        else:
            st.warning("Delete this Work Group photo permanently?")
            p1, p2 = st.columns(2)
            if p1.button("Yes, Delete Photo", type="primary", key=f"confirm_delete_work_group_photo_yes_{section_key}_{selected_photo_id}"):
                delete_work_group_photo(selected_photo_id)
                st.session_state[confirm_key] = False
                st.success('Work Group photo deleted.')
                st.rerun()
            if p2.button("Cancel", key=f"confirm_delete_work_group_photo_cancel_{section_key}_{selected_photo_id}"):
                st.session_state[confirm_key] = False
                st.rerun()


def render_work_group_photos_readonly(work_group_id: int, section_key: str):
    photos_df = work_group_photos_df(work_group_id)
    if photos_df.empty:
        return

    st.markdown('### Work Group Photos')
    st.caption(f"{len(photos_df)} Work Group photo(s) attached.")
    if st.checkbox('Load Work Group photo previews', key=f"load_work_group_photos_readonly_{section_key}_{work_group_id}", value=False):
        photos = [work_group_photo_row_to_dict(row) for _, row in photos_df.iterrows()]
        cols = st.columns(min(4, max(1, len(photos))))
        for idx, photo in enumerate(photos):
            with cols[idx % len(cols)]:
                render_photo_item(photo)


def photo_row_to_dict(row) -> dict:
    photo_bytes = row.get("photo_bytes")
    if photo_bytes is not None and not isinstance(photo_bytes, (bytes, bytearray)):
        try:
            photo_bytes = bytes(photo_bytes)
        except Exception:
            photo_bytes = None
    return {
        "id": int(row["id"]) if pd.notna(row.get("id")) else None,
        "filename": str(row.get("photo_filename") or "photo"),
        "content_type": str(row.get("content_type") or "image/jpeg"),
        "storage_mode": str(row.get("storage_mode") or "database"),
        "blob_url": str(row.get("blob_url") or ""),
        "blob_name": str(row.get("blob_name") or ""),
        "bytes": photo_bytes,
        "sort_order": int(row.get("sort_order") or 0),
    }


@st.cache_data(show_spinner=False, ttl=300)
def line_photo_records_df(estimate_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            estimate_id,
            estimate_line_id,
            COALESCE(photo_filename, '') AS photo_filename,
            COALESCE(content_type, 'image/jpeg') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            photo_bytes,
            COALESCE(sort_order, 0) AS sort_order
        FROM estimate_line_photos
        WHERE estimate_id = ?
        ORDER BY estimate_line_id, sort_order, id
        """,
        (estimate_id,),
    )


@st.cache_data(show_spinner=False, ttl=300)
def line_photo_map(estimate_id: int) -> dict[int, list[dict]]:
    df = line_photo_records_df(estimate_id)
    photo_map: dict[int, list[dict]] = {}
    if df.empty:
        return photo_map
    for _, row in df.iterrows():
        line_id = int(row["estimate_line_id"])
        photo_map.setdefault(line_id, []).append(photo_row_to_dict(row))
    return photo_map


MAX_UPLOAD_IMAGE_DIMENSION = 1800
JPEG_UPLOAD_QUALITY = 82


def optimize_image_bytes_for_upload(raw_bytes: bytes, filename: str = ""):
    if not raw_bytes:
        return raw_bytes, "image/jpeg", filename or "image.jpg"
    if Image is None:
        return raw_bytes, "image/jpeg", filename or "image.jpg"
    try:
        image = Image.open(BytesIO(raw_bytes))
        image = image.convert("RGB")
        width, height = image.size
        max_dim = max(width, height)
        if max_dim > MAX_UPLOAD_IMAGE_DIMENSION:
            scale = MAX_UPLOAD_IMAGE_DIMENSION / float(max_dim)
            new_size = (max(1, int(width * scale)), max(1, int(height * scale)))
            image = image.resize(new_size, Image.LANCZOS)
        out = BytesIO()
        image.save(out, format="JPEG", quality=JPEG_UPLOAD_QUALITY, optimize=True)
        optimized_name = (Path(filename).stem + ".jpg") if filename else "image.jpg"
        return out.getvalue(), "image/jpeg", optimized_name
    except Exception:
        return raw_bytes, "image/jpeg", filename or "image.jpg"


def normalize_uploaded_photos(uploaded_files) -> list[dict]:
    photos = []
    for sort_order, uploaded in enumerate(uploaded_files or []):
        if uploaded is None:
            continue
        raw_bytes = uploaded.getvalue()
        if not raw_bytes:
            continue
        optimized_bytes, content_type, filename = optimize_image_bytes_for_upload(
            raw_bytes,
            getattr(uploaded, "name", "image.jpg"),
        )
        stored_photo = upload_bytes_to_blob(
            data=optimized_bytes,
            filename=filename,
            content_type=content_type or getattr(uploaded, "type", None) or "image/jpeg",
            folder="renovation-estimator/repair-photos",
        )
        stored_photo["sort_order"] = sort_order
        photos.append(stored_photo)
    return photos


def render_photo_item(photo: dict, caption_prefix: str = ""):
    caption = caption_prefix + str(photo.get("filename") or "Photo")
    display_bytes = photo.get("bytes")
    if not display_bytes and photo.get("blob_name"):
        display_bytes = cached_download_blob_bytes(str(photo.get("blob_name")))

    try:
        if display_bytes:
            st.image(display_bytes, caption=caption, use_container_width=True)
        elif photo.get("blob_url"):
            st.image(photo["blob_url"], caption=caption, use_container_width=True)
    except Exception:
        st.warning(f"Could not preview image: {caption}. The file may be damaged or in an unsupported image format.")


def render_line_photo_sections(
    cart_items: list[dict],
    section_title: str = "Photos By Repair",
    gallery_title: str = "All Pictures For This Job",
    load_key_prefix: str = "line_photos",
):
    line_items_with_photos = [
        (idx, line) for idx, line in enumerate(cart_items, start=1) if line.get("photos")
    ]
    if not line_items_with_photos:
        return

    all_photos = []
    for idx, line in line_items_with_photos:
        for photo in line.get("photos", []):
            all_photos.append((idx, line, photo))

    st.markdown("---")
    st.subheader(section_title)
    st.caption(f"{len(line_items_with_photos)} repair item(s) have photos attached. {len(all_photos)} total photo(s).")

    toggle_key = f"{load_key_prefix}_{section_title}_{gallery_title}".lower().replace(" ", "_").replace("/", "_")
    load_photos = st.checkbox(
        f"Load photo previews for this section",
        key=toggle_key,
        value=False,
        help="Photos are lazy-loaded for speed. Leave this off unless you want to review images right now.",
    )

    if not load_photos:
        return

    max_repairs_to_render = 10
    max_gallery_photos = 24

    for idx, line in line_items_with_photos[:max_repairs_to_render]:
        st.markdown(f"**Repair {idx}: {line['task_name']} | {line['trade_name']}**")
        line_photos = line.get("photos", [])
        preview_photos = line_photos[:8]
        cols = st.columns(min(4, max(1, len(preview_photos))))
        for photo_idx, photo in enumerate(preview_photos):
            with cols[photo_idx % len(cols)]:
                render_photo_item(photo)
        if len(line_photos) > len(preview_photos):
            st.caption(f"{len(line_photos) - len(preview_photos)} more photo(s) attached to this repair.")
    if len(line_items_with_photos) > max_repairs_to_render:
        st.caption(f"Showing first {max_repairs_to_render} repair photo groups for speed.")

    if all_photos:
        st.markdown("---")
        st.subheader(gallery_title)
        gallery_cols = st.columns(4)
        for pos, (idx, line, photo) in enumerate(all_photos[:max_gallery_photos]):
            with gallery_cols[pos % 4]:
                render_photo_item(photo, caption_prefix=f"Repair {idx}: ")
        if len(all_photos) > max_gallery_photos:
            st.caption(f"Showing first {max_gallery_photos} photos in the gallery for speed.")


def get_database_file():
    table_names = [
        "app_meta",
        "trades",
        "contractors",
        "user_accounts",
        "tasks",
        "scope_templates",
        "project_registry",
        "estimates",
        "estimate_lines",
        "estimate_line_photos",
        "schedule_entries",
        "punch_list_projects",
        "punch_list_items",
        "punch_list_item_photos",
        "project_status_entries",
        "project_status_photos",
        "project_material_files",
        "quote_requests",
        "work_groups",
        "work_group_photos",
        "work_group_contractor_notes",
        "contractor_weekly_schedules",
        "renovation_pipeline_items",
        "renovation_pipeline_files",
        "renovation_pipeline_cash_flows",
        "manager_repair_requests",
        "manager_repair_request_files",
        "manager_repair_request_comments",
        "quality_control_items",
        "quality_control_files",
        "quality_control_comments",
        "portfolio_properties",
        "portfolio_addresses",
        "renovation_master_records",
        "renovation_master_record_files",
        "renovation_master_record_history",
        "rmr_groups",
        "rmr_group_members",
    ]
    from io import BytesIO
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table_name in table_names:
            try:
                df = fetch_df(f"SELECT * FROM {table_name}")
                zf.writestr(f"{table_name}.csv", df.to_csv(index=False))
            except Exception:
                pass
    buffer.seek(0)
    return buffer.getvalue()


def _restore_parse_value(raw_value):
    import ast

    if raw_value is None:
        return None
    if isinstance(raw_value, float) and pd.isna(raw_value):
        return None

    value = str(raw_value)
    if value == "" or value.lower() == "nan":
        return None

    if value.startswith("b'") or value.startswith('b"'):
        try:
            return ast.literal_eval(value)
        except Exception:
            return None

    return value


def restore_database_from_zip(zip_bytes: bytes) -> tuple[bool, str]:
    import io

    restore_order = [
        "app_meta",
        "trades",
        "contractors",
        "user_accounts",
        "tasks",
        "scope_templates",
        "project_registry",
        "estimates",
        "estimate_lines",
        "estimate_line_photos",
        "schedule_entries",
        "punch_list_projects",
        "punch_list_items",
        "punch_list_item_photos",
        "project_status_entries",
        "project_status_photos",
        "project_material_files",
        "quote_requests",
        "work_groups",
        "work_group_photos",
        "work_group_contractor_notes",
        "contractor_weekly_schedules",
        "renovation_pipeline_items",
        "renovation_pipeline_files",
        "renovation_pipeline_cash_flows",
        "manager_repair_requests",
        "manager_repair_request_files",
        "manager_repair_request_comments",
        "quality_control_items",
        "quality_control_files",
        "quality_control_comments",
        "portfolio_properties",
        "portfolio_addresses",
        "renovation_master_records",
        "renovation_master_record_files",
        "renovation_master_record_history",
        "rmr_groups",
        "rmr_group_members",
    ]
    truncate_order = list(reversed(restore_order))

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), mode="r") as zf:
            csv_names = {name for name in zf.namelist() if name.endswith(".csv")}
            with closing(get_conn()) as conn:
                with conn.cursor() as cur:
                    for table_name in truncate_order:
                        try:
                            cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')
                        except Exception:
                            pass

                    for table_name in restore_order:
                        csv_name = f"{table_name}.csv"
                        if csv_name not in csv_names:
                            continue

                        with zf.open(csv_name) as f:
                            df = pd.read_csv(f, dtype=str, keep_default_na=False)

                        if df.empty and len(df.columns) == 0:
                            continue

                        columns = list(df.columns)
                        if not columns:
                            continue

                        quoted_cols = ", ".join([f'"{col}"' for col in columns])
                        placeholders = ", ".join(["%s"] * len(columns))
                        sql = f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders})'

                        for _, row in df.iterrows():
                            values = [_restore_parse_value(row[col]) for col in columns]
                            cur.execute(sql, values)

                    for table_name in restore_order:
                        try:
                            cur.execute(
                                f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), COALESCE(MAX(id), 1), COALESCE(MAX(id), 0) > 0) FROM {table_name}"
                            )
                        except Exception:
                            pass

                conn.commit()

        st.cache_data.clear()
        st.cache_resource.clear()
        return True, "Backup restored successfully."
    except Exception as e:
        return False, f"Restore failed: {e}"


def get_meta(key: str, default: str = "") -> str:
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_params("SELECT value FROM app_meta WHERE key = ?"), (key,))
            row = cur.fetchone()
            return row[0] if row else default


def set_meta(key: str, value: str):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_meta (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, modified_at = NOW()
                """,
                (key, value),
            )
        conn.commit()


def get_user_accounts_df(active_only: bool = False) -> pd.DataFrame:
    query = """
        SELECT
            ua.id,
            ua.username,
            ua.password,
            COALESCE(ua.role, 'Other') AS role,
            COALESCE(ua.allowed_portfolio, '') AS allowed_portfolio,
            COALESCE(ua.contractor_id, 0) AS contractor_id,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(ua.active, TRUE) AS active,
            ua.created_at,
            ua.modified_at
        FROM user_accounts ua
        LEFT JOIN contractors c ON c.id = ua.contractor_id
    """
    if active_only:
        query += " WHERE COALESCE(ua.active, TRUE) = TRUE"
    query += " ORDER BY LOWER(ua.username), ua.id"
    return fetch_df(query)


def seed_default_user_accounts():
    existing = fetch_df("SELECT COUNT(*) AS cnt FROM user_accounts")
    existing_count = int(existing.iloc[0]["cnt"]) if not existing.empty else 0
    if existing_count > 0:
        return

    default_users = APP_USERS.copy()
    for username, password in default_users.items():
        role = "Owner" if username.lower() == "brent" else "Other"
        execute(
            """
            INSERT INTO user_accounts (username, password, role, contractor_id, active, created_at, modified_at)
            VALUES (?, ?, ?, ?, TRUE, NOW(), NOW())
            """,
            (username, password, role, None),
        )


def get_user_account(username: str):
    if not username:
        return None
    df = fetch_df(
        """
        SELECT
            ua.id,
            ua.username,
            ua.password,
            COALESCE(ua.role, 'Other') AS role,
            COALESCE(ua.allowed_portfolio, '') AS allowed_portfolio,
            COALESCE(ua.contractor_id, 0) AS contractor_id,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(ua.active, TRUE) AS active
        FROM user_accounts ua
        LEFT JOIN contractors c ON c.id = ua.contractor_id
        WHERE LOWER(ua.username) = LOWER(?)
        LIMIT 1
        """,
        (username,),
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    row["contractor_id"] = int(row.get("contractor_id") or 0)
    row["active"] = bool(row.get("active", True))
    return row


def authenticate_user(username: str, password: str):
    user = get_user_account(username)
    if not user:
        return None
    if not user.get("active", True):
        return None
    if str(user.get("password") or "") != str(password or ""):
        return None
    return user


def update_user_password(username: str, new_password: str):
    execute(
        "UPDATE user_accounts SET password = ?, modified_at = NOW() WHERE LOWER(username) = LOWER(?)",
        (new_password, username),
    )


def add_user_account(username: str, password: str, role: str, contractor_id: int | None = None, allowed_portfolio: str = ""):
    execute(
        """
        INSERT INTO user_accounts (username, password, role, contractor_id, allowed_portfolio, active, created_at, modified_at)
        VALUES (?, ?, ?, ?, ?, TRUE, NOW(), NOW())
        """,
        (username, password, role, contractor_id, allowed_portfolio),
    )


def update_user_account(user_id: int, username: str, role: str, contractor_id: int | None, active: bool, password: str | None = None, allowed_portfolio: str = ""):
    if password is not None and str(password).strip():
        execute(
            """
            UPDATE user_accounts
            SET username = ?, password = ?, role = ?, contractor_id = ?, allowed_portfolio = ?, active = ?, modified_at = NOW()
            WHERE id = ?
            """,
            (username, password, role, contractor_id, allowed_portfolio, active, user_id),
        )
    else:
        execute(
            """
            UPDATE user_accounts
            SET username = ?, role = ?, contractor_id = ?, allowed_portfolio = ?, active = ?, modified_at = NOW()
            WHERE id = ?
            """,
            (username, role, contractor_id, allowed_portfolio, active, user_id),
        )


def delete_user_account(user_id: int):
    execute("DELETE FROM user_accounts WHERE id = ?", (user_id,))


def user_has_role(*allowed_roles: str) -> bool:
    current_role = str(st.session_state.get("logged_in_role", "") or "")
    return current_role in allowed_roles


def can_access_admin_page() -> bool:
    return user_has_role("Owner")


def can_access_full_app() -> bool:
    return user_has_role("Owner", "Renovation Manager", "Other")


def format_project_label(project_row) -> str:
    project_name = str(project_row.get("project_name") or "")
    project_code = str(project_row.get("project_code") or "")
    if st.session_state.get("show_shared_ids") and project_code:
        return f"{project_code} | {project_name}"
    return project_name


def format_work_item_label(work_item_row) -> str:
    work_item_name = str(work_item_row.get("name") or work_item_row.get("task_name") or "")
    work_item_code = str(work_item_row.get("work_item_code") or "")
    if st.session_state.get("show_shared_ids") and work_item_code:
        return f"{work_item_code} | {work_item_name}"
    return work_item_name

@st.cache_data(show_spinner=False, ttl=300)
def get_trade_names():
    df = fetch_df("SELECT name FROM trades ORDER BY LOWER(name)")
    return df["name"].tolist()


def get_category_names():
    return get_trade_names()


MANAGER_REPAIR_PRIORITY_OPTIONS = [
    "1. Needs to be done as soon as possible",
    "2. When you can schedule it in",
    "3. Not critical but at some point if you can",
]

MANAGER_REPAIR_STATUS_OPTIONS = [
    "New Request",
    "Under Review",
    "Need More Information",
    "Added To Project Ideas",
    "Sent To Estimate",
    'Sent To Work Group',
    "Scheduled",
    "Completed",
    "Deferred",
    "Denied",
    "Archived",
]


@st.cache_data(show_spinner=False, ttl=300)
def manager_repair_requests_df(manager_user_id: int | None = None, include_archived: bool = False, include_deleted: bool = False, portfolio_name: str | None = None) -> pd.DataFrame:
    query = """
        SELECT
            mrr.id,
            COALESCE(mrr.manager_user_id, 0) AS manager_user_id,
            COALESCE(mrr.manager_username, '') AS manager_username,
            COALESCE(mrr.portfolio_name, '') AS portfolio_name,
            mrr.date_requested,
            COALESCE(mrr.property_name, '') AS property_name,
            COALESCE(mrr.address, '') AS address,
            COALESCE(mrr.unit_number, '') AS unit_number,
            COALESCE(mrr.repair_description, '') AS repair_description,
            COALESCE(mrr.priority, '2. When you can schedule it in') AS priority,
            COALESCE(mrr.status, 'New Request') AS status,
            COALESCE(mrr.owner_response, '') AS owner_response,
            COALESCE(mrr.archived, FALSE) AS archived,
            COALESCE(mrr.deleted, FALSE) AS deleted,
            mrr.created_at,
            mrr.modified_at
        FROM manager_repair_requests mrr
        WHERE 1 = 1
    """
    params = []
    if manager_user_id:
        query += " AND COALESCE(mrr.manager_user_id, 0) = ?"
        params.append(int(manager_user_id))
    if portfolio_name:
        query += " AND COALESCE(mrr.portfolio_name, '') = ?"
        params.append(str(portfolio_name))
    if not include_archived:
        query += " AND COALESCE(mrr.archived, FALSE) = FALSE"
    if not include_deleted:
        query += " AND COALESCE(mrr.deleted, FALSE) = FALSE"
    query += """
        ORDER BY
            CASE COALESCE(mrr.priority, '2. When you can schedule it in')
                WHEN '1. Needs to be done as soon as possible' THEN 1
                WHEN '2. When you can schedule it in' THEN 2
                WHEN '3. Not critical but at some point if you can' THEN 3
                ELSE 4
            END,
            mrr.date_requested DESC NULLS LAST,
            mrr.modified_at DESC,
            mrr.id DESC
    """
    return fetch_df(query, tuple(params))


@st.cache_data(show_spinner=False, ttl=300)
def manager_repair_request_files_df(request_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            request_id,
            COALESCE(file_filename, '') AS file_filename,
            COALESCE(content_type, 'application/octet-stream') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            file_bytes,
            COALESCE(sort_order, 0) AS sort_order,
            COALESCE(uploaded_by, '') AS uploaded_by,
            created_at
        FROM manager_repair_request_files
        WHERE request_id = ?
        ORDER BY sort_order, id
        """,
        (request_id,),
    )


@st.cache_data(show_spinner=False, ttl=300)
def manager_repair_request_comments_df(request_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            request_id,
            COALESCE(user_id, 0) AS user_id,
            COALESCE(username, '') AS username,
            COALESCE(role, '') AS role,
            COALESCE(comment_text, '') AS comment_text,
            created_at
        FROM manager_repair_request_comments
        WHERE request_id = ?
        ORDER BY created_at, id
        """,
        (request_id,),
    )


def save_manager_repair_request_files(request_id: int, uploaded_files, uploaded_by: str = ""):
    files = []
    for sort_order, uploaded in enumerate(uploaded_files or []):
        if uploaded is None:
            continue
        data = uploaded.getvalue()
        if not data:
            continue
        filename = getattr(uploaded, "name", "file")
        content_type = getattr(uploaded, "type", None) or "application/octet-stream"
        stored_file = upload_bytes_to_blob(
            data=data,
            filename=filename,
            content_type=content_type,
            folder="renovation-estimator/manager-repair-requests",
        )
        stored_file["sort_order"] = sort_order
        files.append(stored_file)

    if not files:
        return

    existing_df = manager_repair_request_files_df(request_id)
    existing_count = len(existing_df) if existing_df is not None else 0
    for offset, stored_file in enumerate(files):
        execute(
            """
            INSERT INTO manager_repair_request_files (
                request_id, file_filename, content_type, storage_mode, blob_url, blob_name,
                file_bytes, sort_order, uploaded_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            """,
            (
                int(request_id),
                stored_file.get("filename"),
                stored_file.get("content_type"),
                stored_file.get("storage_mode"),
                stored_file.get("blob_url"),
                stored_file.get("blob_name"),
                stored_file.get("bytes"),
                existing_count + offset,
                uploaded_by or "",
            ),
        )


def delete_manager_repair_request_file(file_id: int):
    execute("DELETE FROM manager_repair_request_files WHERE id = ?", (file_id,))


def add_manager_repair_request_comment(request_id: int, comment_text: str):
    if not str(comment_text or "").strip():
        return
    execute(
        """
        INSERT INTO manager_repair_request_comments (
            request_id, user_id, username, role, comment_text, created_at
        ) VALUES (?, ?, ?, ?, ?, NOW())
        """,
        (
            int(request_id),
            int(st.session_state.get("logged_in_user_id", 0) or 0) or None,
            str(st.session_state.get("logged_in_user", "") or ""),
            str(st.session_state.get("logged_in_role", "") or ""),
            str(comment_text or "").strip(),
        ),
    )


def delete_manager_repair_request(request_id: int):
    execute(
        "UPDATE manager_repair_requests SET deleted = TRUE, modified_at = NOW() WHERE id = ?",
        (int(request_id),),
    )


def update_manager_repair_request(
    request_id: int,
    date_requested,
    property_name: str,
    address: str,
    unit_number: str,
    repair_description: str,
    priority: str,
    status: str,
    owner_response: str,
):
    execute(
        """
        UPDATE manager_repair_requests
        SET date_requested = ?, property_name = ?, address = ?, unit_number = ?,
            repair_description = ?, priority = ?, status = ?, owner_response = ?, modified_at = NOW()
        WHERE id = ?
        """,
        (
            date_requested,
            str(property_name or "").strip(),
            str(address or "").strip(),
            str(unit_number or "").strip(),
            str(repair_description or "").strip(),
            str(priority or "").strip(),
            str(status or "").strip(),
            str(owner_response or "").strip(),
            int(request_id),
        ),
    )


PORTFOLIO_NAMES = ["Residences Portfolio", "Sandstone Portfolio"]


@st.cache_data(show_spinner=False, ttl=300)
def portfolio_properties_df(portfolio_name: str | None = None, include_inactive: bool = True, include_deleted: bool = False) -> pd.DataFrame:
    query = """
        SELECT
            id,
            COALESCE(portfolio_name, '') AS portfolio_name,
            COALESCE(property_name, '') AS property_name,
            COALESCE(notes, '') AS notes,
            COALESCE(active, TRUE) AS active,
            COALESCE(deleted, FALSE) AS deleted,
            created_at,
            modified_at
        FROM portfolio_properties
        WHERE 1 = 1
    """
    params = []
    if portfolio_name:
        query += " AND COALESCE(portfolio_name, '') = ?"
        params.append(str(portfolio_name))
    if not include_inactive:
        query += " AND COALESCE(active, TRUE) = TRUE"
    if not include_deleted:
        query += " AND COALESCE(deleted, FALSE) = FALSE"
    query += " ORDER BY portfolio_name, LOWER(property_name), id"
    return fetch_df(query, tuple(params))


@st.cache_data(show_spinner=False, ttl=300)
def portfolio_addresses_df(portfolio_name: str | None = None, property_name: str | None = None, include_inactive: bool = True, include_deleted: bool = False) -> pd.DataFrame:
    query = """
        SELECT
            pa.id,
            COALESCE(pa.portfolio_property_id, 0) AS portfolio_property_id,
            COALESCE(pa.portfolio_name, '') AS portfolio_name,
            COALESCE(pa.property_name, '') AS property_name,
            COALESCE(pa.address, '') AS address,
            COALESCE(pa.unit_number, '') AS unit_number,
            COALESCE(pa.notes, '') AS notes,
            COALESCE(pa.active, TRUE) AS active,
            COALESCE(pa.deleted, FALSE) AS deleted,
            pa.created_at,
            pa.modified_at
        FROM portfolio_addresses pa
        WHERE 1 = 1
    """
    params = []
    if portfolio_name:
        query += " AND COALESCE(pa.portfolio_name, '') = ?"
        params.append(str(portfolio_name))
    if property_name:
        query += " AND COALESCE(pa.property_name, '') = ?"
        params.append(str(property_name))
    if not include_inactive:
        query += " AND COALESCE(pa.active, TRUE) = TRUE"
    if not include_deleted:
        query += " AND COALESCE(pa.deleted, FALSE) = FALSE"
    query += " ORDER BY pa.portfolio_name, LOWER(pa.property_name), LOWER(pa.address), LOWER(pa.unit_number), pa.id"
    return fetch_df(query, tuple(params))


def portfolio_property_labels(portfolio_name: str | None = None) -> list[str]:
    df = portfolio_properties_df(portfolio_name=portfolio_name, include_inactive=False)
    labels = []
    if not df.empty:
        labels = [f"{int(row.id)} | {row.property_name}" for row in df.itertuples()]

    existing_names = {label.split(" | ", 1)[1].strip().lower() for label in labels if " | " in label}

    # Fallback property choices if database seed has not been run or missed a property.
    if portfolio_name == "Sandstone Portfolio":
        if "sandstone apartments" not in existing_names:
            labels.append("SANDSTONE-PROP-FALLBACK | Sandstone Apartments")
        if "plaza apartments" not in existing_names:
            labels.append("PLAZA-PROP-FALLBACK | Plaza Apartments")
        if "vineyards townhomes" not in existing_names:
            labels.append("VINEYARDS-PROP-FALLBACK | Vineyards Townhomes")

    if portfolio_name == "Residences Portfolio":
        for fallback_name in ["Residences at Linwood", "Stafford Cottages", "Westgate Apartments", "Comfort West Apartments", "ReVest Rentals"]:
            if fallback_name.lower() not in existing_names:
                labels.append(f"{fallback_name.upper().replace(' ', '-')}-PROP-FALLBACK | {fallback_name}")

    return labels


def portfolio_address_labels(portfolio_name: str | None = None, property_name: str | None = None) -> list[str]:
    df = portfolio_addresses_df(portfolio_name=portfolio_name, property_name=property_name, include_inactive=False)
    if df.empty:
        return []
    labels = []
    for row in df.itertuples():
        unit_text = f" | Unit {row.unit_number}" if str(row.unit_number or "").strip() else ""
        labels.append(f"{int(row.id)} | {row.property_name} | {row.address}{unit_text}")
    return labels


def portfolio_address_labels_for_property_id(portfolio_property_id: int | None) -> list[str]:
    if not portfolio_property_id:
        return []

    property_df = portfolio_properties_df(include_inactive=True, include_deleted=False)
    property_match = property_df[property_df["id"].fillna(0).astype(int) == int(portfolio_property_id)].copy()
    property_name = str(property_match.iloc[0]["property_name"] or "") if not property_match.empty else ""
    portfolio_name = str(property_match.iloc[0]["portfolio_name"] or "") if not property_match.empty else ""

    df = portfolio_addresses_df(include_inactive=False)
    direct_df = pd.DataFrame()

    if not df.empty:
        direct_df = df[df["portfolio_property_id"].fillna(0).astype(int) == int(portfolio_property_id)].copy()
        if direct_df.empty and property_name:
            direct_df = df[
                (df["portfolio_name"].astype(str) == portfolio_name)
                & (df["property_name"].astype(str).str.lower().str.strip() == property_name.lower().strip())
            ].copy()

    labels = []
    if not direct_df.empty:
        for row in direct_df.itertuples():
            unit_text = f" | Unit {row.unit_number}" if str(row.unit_number or "").strip() else ""
            labels.append(f"{int(row.id)} | {row.property_name} | {row.address}{unit_text}")

    # Fallback: if ReVest Rentals exists as a property but the database has not yet been populated,
    # show the hard-coded ReVest addresses in the dropdown anyway.
    if not labels and property_name.strip().lower() == "revest rentals":
        fallback_addresses = ['905 W. 54th', '907 W. 54th', '3907 E. Skinner', '931 W 54th', '933 W 54th', '915 W 54th', '917 W 54th', '1003 W 54th', '1005 W 54th', '1015 W. 54th', '1017 W. 54th', '1021 W. 54th', '1023 W. 54th', '1004 W 54th', '1006 W 54th', '930 W 54th', '932 W 54th', '5556 S. Handley', '5558 S. Handley', '5522 S. Handley', '5524 S. Handley', '1022 W. 54th', '1024 W. 54th', '1016 W. 54th', '1018 W. 54th', '1010 W. 54th', '1012 W. 54th']
        labels = [f"{address} | ReVest Rentals" for address in fallback_addresses]

    return labels


def portfolio_address_row_from_label(label: str):
    if not label:
        return None
    label_text = str(label)

    # Fallback labels are displayed as normal address text, e.g. "905 W. 54th | ReVest Rentals".
    # They are not database IDs, but should still populate the request form.
    if label_text.endswith(" | ReVest Rentals") and not label_text.split(" | ", 1)[0].isdigit():
        address = label_text.split(" | ", 1)[0].strip()
        return pd.Series({
            "id": 0,
            "portfolio_property_id": 0,
            "portfolio_name": "Residences Portfolio",
            "property_name": "ReVest Rentals",
            "address": address,
            "unit_number": "",
            "notes": "",
            "active": True,
            "deleted": False,
        })

    if label_text.endswith(" | Plaza Apartments") and not label_text.split(" | ", 1)[0].isdigit():
        address = label_text.split(" | ", 1)[0].strip()
        return pd.Series({
            "id": 0,
            "portfolio_property_id": 0,
            "portfolio_name": "Sandstone Portfolio",
            "property_name": "Plaza Apartments",
            "address": address,
            "unit_number": "",
            "notes": "",
            "active": True,
            "deleted": False,
        })

    try:
        address_id = int(label_text.split(" | ", 1)[0])
    except Exception:
        return None
    df = portfolio_addresses_df(include_deleted=False)
    match = df[df["id"] == address_id]
    if match.empty:
        return None
    return match.iloc[0]


def master_property_labels() -> list[str]:
    return ["No Property / General Address"] + portfolio_property_labels(None)


def master_address_labels(selected_property_label: str = "") -> list[str]:
    selected_property_label = str(selected_property_label or "")
    if not selected_property_label or selected_property_label == "No Property / General Address":
        return portfolio_address_labels(None, None)

    try:
        property_token, property_name = selected_property_label.split(" | ", 1)
    except Exception:
        return portfolio_address_labels(None, None)

    property_token = str(property_token).strip()
    property_name = str(property_name).strip()

    if property_token.isdigit():
        labels = portfolio_address_labels_for_property_id(int(property_token))
    else:
        # Fallback property labels have non-numeric IDs.
        portfolio_name = "Residences Portfolio" if property_name in ["Residences at Linwood", "Stafford Cottages", "Westgate Apartments", "Comfort West Apartments", "ReVest Rentals"] else "Sandstone Portfolio"
        labels = portfolio_address_options_for_property(portfolio_name, selected_property_label)

    if labels:
        return labels
    return portfolio_address_labels(None, property_name)


def render_shared_address_picker(label: str, key_prefix: str, default_address: str = "", default_unit_number: str = ""):
    st.markdown(f"#### {label}")
    selected_property = st.selectbox(
        "Property (optional)",
        master_property_labels(),
        key=f"{key_prefix}_property",
        help="Optional. Choose a property to narrow the address list, or leave it as a general address.",
    )

    address_options = master_address_labels(selected_property)
    selected_address = st.selectbox(
        "Choose Address",
        ["Type New Address"] + address_options,
        key=f"{key_prefix}_address_choice",
        help="This list pulls from the master address database. You can also type a new address manually.",
    )

    selected_row = None
    if selected_address != "Type New Address":
        selected_row = portfolio_address_row_from_label(selected_address)

    if selected_row is not None:
        address = str(selected_row.get("address") or "")
        unit = str(selected_row.get("unit_number") or "")
        d1, d2 = st.columns(2)
        d1.text_input("Selected Address", value=address, disabled=True, key=f"{key_prefix}_selected_address")
        d2.text_input("Selected Unit Number", value=unit, disabled=True, key=f"{key_prefix}_selected_unit")
        return address, unit

    a1, a2 = st.columns(2)
    address = a1.text_input("Type New Address", value=str(default_address or ""), key=f"{key_prefix}_new_address")
    unit = a2.text_input("Type New Unit Number", value=str(default_unit_number or ""), key=f"{key_prefix}_new_unit")
    return str(address or "").strip(), str(unit or "").strip()


def portfolio_address_options_for_property(portfolio_name: str, selected_property_label: str) -> list[str]:
    if not selected_property_label or selected_property_label.startswith("Type"):
        return []

    try:
        property_token, property_name = selected_property_label.split(" | ", 1)
    except Exception:
        return []

    property_token = str(property_token).strip()
    property_name = str(property_name).strip()

    if property_token.isdigit():
        return portfolio_address_labels_for_property_id(int(property_token))

    if property_name.lower() == "revest rentals":
        return [f"{address} | ReVest Rentals" for address in [
            "905 W. 54th", "907 W. 54th", "3907 E. Skinner", "931 W 54th", "933 W 54th",
            "915 W 54th", "917 W 54th", "1003 W 54th", "1005 W 54th", "1015 W. 54th",
            "1017 W. 54th", "1021 W. 54th", "1023 W. 54th", "1004 W 54th", "1006 W 54th",
            "930 W 54th", "932 W 54th", "5556 S. Handley", "5558 S. Handley",
            "5522 S. Handley", "5524 S. Handley", "1022 W. 54th", "1024 W. 54th",
            "1016 W. 54th", "1018 W. 54th", "1010 W. 54th", "1012 W. 54th",
        ]]

    if property_name.lower() == "plaza apartments":
        return [f"{address} | Plaza Apartments" for address in [
            "1701 George Washington", "1703 George Washington", "1705 George Washington", "1707 George Washington",
            "1709 George Washington", "1711 George Washington", "1715 George Washington", "1717 George Washington",
            "1719 George Washington", "1721 George Washington", "1723 George Washington", "1725 George Washington",
            "1727 George Washington", "1729 George Washington", "1731 George Washington", "1733 George Washington",
            "1735 George Washington", "1737 George Washington", "1739 George Washington", "1741 George Washington",
            "1743 George Washington", "1745 George Washington", "1747 George Washington", "1749 George Washington",
            "1751 George Washington", "1753 George Washington", "3001 Osie", "3003 Osie", "3005 Osie",
            "3007 Osie", "3009 Osie", "3011 Osie", "3013 Osie", "3015 Osie", "3017 Osie",
            "3019 Osie", "3021 Osie", "3023 Osie", "3002 Schrader", "3004 Schrader", "3006 Schrader",
            "3008 Schrader", "3010 Schrader", "3012 Schrader", "3014 Schrader", "3016 Schrader",
            "3018 Schrader", "3020 Schrader", "3022 Schrader", "3024 Schrader", "3026 Schrader",
            "3028 Schrader", "3030 Schrader", "3001 Schrader", "3005 Schrader", "3007 Schrader",
            "3009 Schrader", "3011 Schrader", "3013 Schrader", "3015 Schrader", "3017 Schrader",
            "3019 Schrader", "3021 Schrader", "3023 Schrader", "3025 Schrader", "3027 Schrader",
            "3029 Schrader", "3031 Schrader", "3033 Schrader", "3035 Schrader", "3037 Schrader",
            "3039 Schrader", "3002 Funston", "3004 Funston", "3006 Funston", "3008 Funston",
            "3010 Funston", "3012 Funston", "3014 Funston", "3016 Funston", "3018 Funston",
            "3020 Funston", "3022 Funston", "3024 Funston", "3025 Funston", "3028 Funston",
            "3030 Funston", "3032 Funston", "3034 Funston", "3036 Funston", "3038 Funston",
            "3040 Funston", "3102 Funston", "3104 Funston", "3106 Funston", "3108 Funston",
            "3110 Funston", "3112 Funston", "3114 Funston", "3116 Funston",
        ]]

    return portfolio_address_labels(portfolio_name, property_name)


DEFAULT_PORTFOLIO_ADDRESS_SEED_DATA = [{'portfolio': 'Residences Portfolio',
  'property': 'Westgate Apartments',
  'addresses': [{'address': '1448 N. Westgate', 'unit_number': '101'},
                {'address': '1448 N. Westgate', 'unit_number': '102'},
                {'address': '1448 N. Westgate', 'unit_number': '103'},
                {'address': '1448 N. Westgate', 'unit_number': '104'},
                {'address': '1448 N. Westgate', 'unit_number': '201'},
                {'address': '1448 N. Westgate', 'unit_number': '202'},
                {'address': '1448 N. Westgate', 'unit_number': '203'},
                {'address': '1448 N. Westgate', 'unit_number': '301'},
                {'address': '1448 N. Westgate', 'unit_number': '302'},
                {'address': '1448 N. Westgate', 'unit_number': '401'},
                {'address': '1448 N. Westgate', 'unit_number': '402'},
                {'address': '1448 N. Westgate', 'unit_number': '403'},
                {'address': '1448 N. Westgate', 'unit_number': '404'}]},
 {'portfolio': 'Residences Portfolio',
  'property': 'Stafford Cottages',
  'addresses': [{'address': '1863 E. Stafford', 'unit_number': ''},
                {'address': '1867 E. Stafford', 'unit_number': ''},
                {'address': '1871 E. Stafford', 'unit_number': ''},
                {'address': '1875 E. Stafford', 'unit_number': ''},
                {'address': '1883 E. Stafford', 'unit_number': ''},
                {'address': '1879 E. Stafford', 'unit_number': ''},
                {'address': '1887 E. Stafford', 'unit_number': ''},
                {'address': '1891 E. Stafford', 'unit_number': ''},
                {'address': '1895 E. Stafford', 'unit_number': ''},
                {'address': '1899 E. Stafford', 'unit_number': ''},
                {'address': '1903 E. Stafford', 'unit_number': ''},
                {'address': '1907 E. Stafford', 'unit_number': ''}]},
 {'portfolio': 'Sandstone Portfolio',
  'property': 'Vineyards Townhomes',
  'addresses': [{'address': '3737 N. Rushwood', 'unit_number': '101'},
                {'address': '3737 N. Rushwood', 'unit_number': '102'},
                {'address': '3737 N. Rushwood', 'unit_number': '103'},
                {'address': '3737 N. Rushwood', 'unit_number': '104'},
                {'address': '3737 N. Rushwood', 'unit_number': '105'},
                {'address': '3737 N. Rushwood', 'unit_number': '201'},
                {'address': '3737 N. Rushwood', 'unit_number': '202'},
                {'address': '3737 N. Rushwood', 'unit_number': '203'},
                {'address': '3737 N. Rushwood', 'unit_number': '204'},
                {'address': '3737 N. Rushwood', 'unit_number': '205'},
                {'address': '3737 N. Rushwood', 'unit_number': '206'},
                {'address': '3737 N. Rushwood', 'unit_number': '207'},
                {'address': '3737 N. Rushwood', 'unit_number': '208'},
                {'address': '3737 N. Rushwood', 'unit_number': '301'},
                {'address': '3737 N. Rushwood', 'unit_number': '302'},
                {'address': '3737 N. Rushwood', 'unit_number': '303'},
                {'address': '3737 N. Rushwood', 'unit_number': '304'},
                {'address': '3737 N. Rushwood', 'unit_number': '401'},
                {'address': '3737 N. Rushwood', 'unit_number': '402'},
                {'address': '3737 N. Rushwood', 'unit_number': '403'},
                {'address': '3737 N. Rushwood', 'unit_number': '404'},
                {'address': '3737 N. Rushwood', 'unit_number': '405'},
                {'address': '3737 N. Rushwood', 'unit_number': '501'},
                {'address': '3737 N. Rushwood', 'unit_number': '502'},
                {'address': '3737 N. Rushwood', 'unit_number': '503'},
                {'address': '3737 N. Rushwood', 'unit_number': '504'},
                {'address': '3737 N. Rushwood', 'unit_number': '505'},
                {'address': '3737 N. Rushwood', 'unit_number': '602'},
                {'address': '3737 N. Rushwood', 'unit_number': '603'},
                {'address': '3737 N. Rushwood', 'unit_number': '701'},
                {'address': '3737 N. Rushwood', 'unit_number': '702'},
                {'address': '3737 N. Rushwood', 'unit_number': '703'},
                {'address': '3737 N. Rushwood', 'unit_number': '704'},
                {'address': '3737 N. Rushwood', 'unit_number': '705'},
                {'address': '3737 N. Rushwood', 'unit_number': '801'},
                {'address': '3737 N. Rushwood', 'unit_number': '802'},
                {'address': '3737 N. Rushwood', 'unit_number': '803'},
                {'address': '3737 N. Rushwood', 'unit_number': '804'},
                {'address': '3737 N. Rushwood', 'unit_number': '805'},
                {'address': '3737 N. Rushwood', 'unit_number': '806'},
                {'address': '3737 N. Rushwood', 'unit_number': '901'},
                {'address': '3737 N. Rushwood', 'unit_number': '902'},
                {'address': '3737 N. Rushwood', 'unit_number': '903'},
                {'address': '3737 N. Rushwood', 'unit_number': '904'},
                {'address': '3737 N. Rushwood', 'unit_number': '905'},
                {'address': '3737 N. Rushwood', 'unit_number': '906'},
                {'address': '3737 N. Rushwood', 'unit_number': '1001'},
                {'address': '3737 N. Rushwood', 'unit_number': '1002'},
                {'address': '3737 N. Rushwood', 'unit_number': '1003'},
                {'address': '3737 N. Rushwood', 'unit_number': '1004'},
                {'address': '3737 N. Rushwood', 'unit_number': '1005'},
                {'address': '3737 N. Rushwood', 'unit_number': '1006'},
                {'address': '3737 N. Rushwood', 'unit_number': '1007'},
                {'address': '3737 N. Rushwood', 'unit_number': '1201'},
                {'address': '3737 N. Rushwood', 'unit_number': '1202'},
                {'address': '3737 N. Rushwood', 'unit_number': '1203'},
                {'address': '3737 N. Rushwood', 'unit_number': '1204'},
                {'address': '3737 N. Rushwood', 'unit_number': '1205'},
                {'address': '3737 N. Rushwood', 'unit_number': '1206'},
                {'address': '3737 N. Rushwood', 'unit_number': '1207'},
                {'address': '3737 N. Rushwood', 'unit_number': '1401'},
                {'address': '3737 N. Rushwood', 'unit_number': '1402'},
                {'address': '3737 N. Rushwood', 'unit_number': '1403'},
                {'address': '3737 N. Rushwood', 'unit_number': '1404'},
                {'address': '3737 N. Rushwood', 'unit_number': '1405'},
                {'address': '3737 N. Rushwood', 'unit_number': '1406'}]},
 {'portfolio': 'Residences Portfolio',
  'property': 'Residences at Linwood',
  'addresses': [{'address': '2010 S Hydraulic', 'unit_number': ''},
                {'address': '2012 S Hydraulic', 'unit_number': ''},
                {'address': '2014 S Hydraulic', 'unit_number': ''},
                {'address': '2016 S Hydraulic', 'unit_number': ''},
                {'address': '2018 S Hydraulic', 'unit_number': ''},
                {'address': '2020 S Hydraulic', 'unit_number': ''},
                {'address': '2022 S Hydraulic', 'unit_number': ''},
                {'address': '2024 S Hydraulic', 'unit_number': ''},
                {'address': '2026 S Hydraulic', 'unit_number': ''},
                {'address': '2028 S Hydraulic', 'unit_number': ''},
                {'address': '2030 S Hydraulic', 'unit_number': ''},
                {'address': '2032 S Hydraulic', 'unit_number': ''},
                {'address': '2034 S Hydraulic', 'unit_number': ''},
                {'address': '2036 S Hydraulic', 'unit_number': ''},
                {'address': '2038 S Hydraulic', 'unit_number': ''},
                {'address': '2040 S Hydraulic', 'unit_number': ''},
                {'address': '2042 S Hydraulic', 'unit_number': ''},
                {'address': '2044 S Hydraulic', 'unit_number': ''},
                {'address': '2046 S Hydraulic', 'unit_number': ''},
                {'address': '2048 S Hydraulic', 'unit_number': ''},
                {'address': '2050 S Hydraulic', 'unit_number': ''},
                {'address': '2052 S Hydraulic', 'unit_number': ''},
                {'address': '2054 S Hydraulic', 'unit_number': ''},
                {'address': '2056 S Hydraulc', 'unit_number': ''},
                {'address': '2102 S Hydraulic', 'unit_number': ''},
                {'address': '2104 S Hydraulic', 'unit_number': ''},
                {'address': '2106 S Hydraulic', 'unit_number': ''},
                {'address': '2108 S Hydraulic', 'unit_number': ''},
                {'address': '2110 S Hydraulic', 'unit_number': ''},
                {'address': '2112 S Hydraulic', 'unit_number': ''},
                {'address': '2114 S Hydraulic', 'unit_number': ''},
                {'address': '2116 S Hydraulic', 'unit_number': ''},
                {'address': '2118 S Hydraulic', 'unit_number': ''},
                {'address': '2120 S Hydraulic', 'unit_number': ''},
                {'address': '2122 S Hydraulic', 'unit_number': ''},
                {'address': '2124 S Hydraulic', 'unit_number': ''},
                {'address': '2126 S Hydraulic', 'unit_number': ''},
                {'address': '2128 S Hydraulic', 'unit_number': ''},
                {'address': '2130 S Hydraulic', 'unit_number': ''},
                {'address': '2132 S Hydraulic', 'unit_number': ''},
                {'address': '2134 S Hydraulic', 'unit_number': ''},
                {'address': '2136 S Hydraulic', 'unit_number': ''},
                {'address': '2138 S Hydraulic', 'unit_number': ''},
                {'address': '2140 S Hydraulic', 'unit_number': ''},
                {'address': '2142 S Hydraulic', 'unit_number': ''},
                {'address': '2144 S Hydraulic', 'unit_number': ''},
                {'address': '2146 S Hydraulic', 'unit_number': ''},
                {'address': '2148 S Hydraulic', 'unit_number': ''},
                {'address': '2150 S Hydraulic', 'unit_number': ''},
                {'address': '2152 S Hydraulic', 'unit_number': ''},
                {'address': '2154 S Hydraulic', 'unit_number': ''},
                {'address': '2156 S Hydraulic', 'unit_number': ''},
                {'address': '2010 S. Hydraulic', 'unit_number': ''},
                {'address': '2012 S. Hydraulic', 'unit_number': ''},
                {'address': '2014 S. Hydraulic', 'unit_number': ''},
                {'address': '2016 S. Hydraulic', 'unit_number': ''},
                {'address': '2018 S. Hydraulic', 'unit_number': ''},
                {'address': '2020 S. Hydraulic', 'unit_number': ''},
                {'address': '2022 S. Hydraulic', 'unit_number': ''},
                {'address': '2024 S. Hydraulic', 'unit_number': ''},
                {'address': '2026 S. Hydraulic', 'unit_number': ''},
                {'address': '2028 S. Hydraulic', 'unit_number': ''},
                {'address': '2030 S. Hydraulic', 'unit_number': ''},
                {'address': '2032 S. Hydraulic', 'unit_number': ''},
                {'address': '2034 S. Hydraulic', 'unit_number': ''},
                {'address': '2036 S. Hydraulic', 'unit_number': ''},
                {'address': '2038 S. Hydraulic', 'unit_number': ''},
                {'address': '2040 S. Hydraulic', 'unit_number': ''},
                {'address': '2042 S. Hydraulic', 'unit_number': ''},
                {'address': '2044 S. Hydraulic', 'unit_number': ''},
                {'address': '2046 S. Hydraulic', 'unit_number': ''},
                {'address': '2048 S. Hydraulic', 'unit_number': ''},
                {'address': '2050 S. Hydraulic', 'unit_number': ''},
                {'address': '2052 S. Hydraulic', 'unit_number': ''},
                {'address': '2054 S. Hydraulic', 'unit_number': ''},
                {'address': '2056 S. Hydraulic', 'unit_number': ''},
                {'address': '2102 S. Hydraulic', 'unit_number': ''},
                {'address': '2104 S. Hydraulic', 'unit_number': ''},
                {'address': '2106 S. Hydraulic', 'unit_number': ''},
                {'address': '2108 S. Hydraulic', 'unit_number': ''},
                {'address': '2110 S. Hydraulic', 'unit_number': ''},
                {'address': '2112 S. Hydraulic', 'unit_number': ''},
                {'address': '2114 S. Hydraulic', 'unit_number': ''},
                {'address': '2116 S. Hydraulic', 'unit_number': ''},
                {'address': '2118 S. Hydraulic', 'unit_number': ''},
                {'address': '2120 S. Hydraulic', 'unit_number': ''},
                {'address': '2122 S. Hydraulic', 'unit_number': ''},
                {'address': '2124 S. Hydraulic', 'unit_number': ''},
                {'address': '2126 S. Hydraulic', 'unit_number': ''},
                {'address': '2128 S. Hydraulic', 'unit_number': ''},
                {'address': '2130 S. Hydraulic', 'unit_number': ''},
                {'address': '2132 S. Hydraulic', 'unit_number': ''},
                {'address': '2134 S. Hydraulic', 'unit_number': ''},
                {'address': '2136 S. Hydraulic', 'unit_number': ''},
                {'address': '2138 S. Hydraulic', 'unit_number': ''},
                {'address': '2140 S. Hydraulic', 'unit_number': ''},
                {'address': '2142 S. Hydraulic', 'unit_number': ''},
                {'address': '2144 S. Hydraulic', 'unit_number': ''},
                {'address': '2146 S. Hydraulic', 'unit_number': ''},
                {'address': '2148 S. Hydraulic', 'unit_number': ''},
                {'address': '2150 S. Hydraulic', 'unit_number': ''},
                {'address': '2152 S. Hydraulic', 'unit_number': ''},
                {'address': '2154 S. Hydraulic', 'unit_number': ''},
                {'address': '2156 S. Hydraulic', 'unit_number': ''},
                {'address': '2001 S. Kansas', 'unit_number': ''},
                {'address': '2003 S. Kansas', 'unit_number': ''},
                {'address': '2005 S. Kansas', 'unit_number': ''},
                {'address': '2007 S. Kansas', 'unit_number': ''},
                {'address': '2011 S. Kansas', 'unit_number': ''},
                {'address': '2013 S. Kansas', 'unit_number': ''},
                {'address': '2015 S. Kansas', 'unit_number': ''},
                {'address': '2017 S. Kansas', 'unit_number': ''},
                {'address': '2021 S. Kansas', 'unit_number': ''},
                {'address': '2023 S. Kansas', 'unit_number': ''},
                {'address': '2025 S. Kansas', 'unit_number': ''},
                {'address': '2027 S. Kansas', 'unit_number': ''},
                {'address': '2031 S. Kansas', 'unit_number': ''},
                {'address': '2033 S. Kansas', 'unit_number': ''},
                {'address': '2035 S. Kansas', 'unit_number': ''},
                {'address': '2037 S. Kansas', 'unit_number': ''},
                {'address': '2041 S. Kansas', 'unit_number': ''},
                {'address': '2043 S. Kansas', 'unit_number': ''},
                {'address': '2045 S. Kansas', 'unit_number': ''},
                {'address': '2047 S. Kansas', 'unit_number': ''},
                {'address': '2051 S. Kansas', 'unit_number': ''},
                {'address': '2053 S. Kansas', 'unit_number': ''},
                {'address': '2055 S. Kansas', 'unit_number': ''},
                {'address': '2057 S. Kansas', 'unit_number': ''},
                {'address': '2101 S. Kansas', 'unit_number': ''},
                {'address': '2103 S. Kansas', 'unit_number': ''},
                {'address': '2105 S. Kansas', 'unit_number': ''},
                {'address': '2107 S. Kansas', 'unit_number': ''},
                {'address': '2111 S. Kansas', 'unit_number': ''},
                {'address': '2113 S. Kansas', 'unit_number': ''},
                {'address': '2115 S. Kansas', 'unit_number': ''},
                {'address': '2117 S. Kansas', 'unit_number': ''},
                {'address': '2121 S. Kansas', 'unit_number': ''},
                {'address': '2123 S. Kansas', 'unit_number': ''},
                {'address': '2125 S. Kansas', 'unit_number': ''},
                {'address': '2127 S. Kansas', 'unit_number': ''},
                {'address': '2131 S. Kansas', 'unit_number': ''},
                {'address': '2133 S. Kansas', 'unit_number': ''},
                {'address': '2135 S. Kansas', 'unit_number': ''},
                {'address': '2137 S. Kansas', 'unit_number': ''},
                {'address': '2141 S. Kansas', 'unit_number': ''},
                {'address': '2143 S. Kansas', 'unit_number': ''},
                {'address': '2145 S. Kansas', 'unit_number': ''},
                {'address': '2147 S. Kansas', 'unit_number': ''},
                {'address': '2151 S. Kansas', 'unit_number': ''},
                {'address': '2153 S. Kansas', 'unit_number': ''},
                {'address': '2155 S. Kansas', 'unit_number': ''},
                {'address': '2157 S. Kansas', 'unit_number': ''},
                {'address': '2022 S. Kansas', 'unit_number': ''},
                {'address': '2024 S. Kansas', 'unit_number': ''},
                {'address': '2026 S. Kansas', 'unit_number': ''},
                {'address': '2028 S. Kansas', 'unit_number': ''},
                {'address': '2032 S. Kansas', 'unit_number': ''},
                {'address': '2034 S. Kansas', 'unit_number': ''},
                {'address': '2036 S. Kansas', 'unit_number': ''},
                {'address': '2038 S. Kansas', 'unit_number': ''},
                {'address': '2042 S. Kansas', 'unit_number': ''},
                {'address': '2044 S. Kansas', 'unit_number': ''},
                {'address': '2046 S. Kansas', 'unit_number': ''},
                {'address': '2048 S. Kansas', 'unit_number': ''},
                {'address': '2052 S. Kansas', 'unit_number': ''},
                {'address': '2054 S. Kansas', 'unit_number': ''},
                {'address': '2056 S. Kansas', 'unit_number': ''},
                {'address': '2058 S. Kansas', 'unit_number': ''},
                {'address': '2102 S. Kansas', 'unit_number': ''},
                {'address': '2104 S. Kansas', 'unit_number': ''},
                {'address': '2106 S. Kansas', 'unit_number': ''},
                {'address': '2108 S. Kansas', 'unit_number': ''},
                {'address': '2112 S. Kansas', 'unit_number': ''},
                {'address': '2114 S. Kansas', 'unit_number': ''},
                {'address': '2116 S. Kansas', 'unit_number': ''},
                {'address': '2118 S. Kansas', 'unit_number': ''},
                {'address': '2122 S. Kansas', 'unit_number': ''},
                {'address': '2124 S. Kansas', 'unit_number': ''},
                {'address': '2126 S. Kansas', 'unit_number': ''},
                {'address': '2128 S. Kansas', 'unit_number': ''},
                {'address': '2132 S. Kansas', 'unit_number': ''},
                {'address': '2134 S. Kansas', 'unit_number': ''},
                {'address': '2136 S. Kansas', 'unit_number': ''},
                {'address': '2138 S. Kansas', 'unit_number': ''},
                {'address': '2142 S. Kansas', 'unit_number': ''},
                {'address': '2144 S. Kansas', 'unit_number': ''},
                {'address': '2146 S. Kansas', 'unit_number': ''},
                {'address': '2148 S. Kansas', 'unit_number': ''},
                {'address': '2152 S. Kansas', 'unit_number': ''},
                {'address': '2154 S. Kansas', 'unit_number': ''},
                {'address': '2156 S. Kansas', 'unit_number': ''},
                {'address': '2158 S. Kansas', 'unit_number': ''},
                {'address': '2101 S. Minneapolis', 'unit_number': ''},
                {'address': '2103 S. Minneapolis', 'unit_number': ''},
                {'address': '2105 S. Minneapolis', 'unit_number': ''},
                {'address': '2107 S. Minneapolis', 'unit_number': ''},
                {'address': '2111 S. Minneapolis', 'unit_number': ''},
                {'address': '2113 S. Minneapolis', 'unit_number': ''},
                {'address': '2115 S. Minneapolis', 'unit_number': ''},
                {'address': '2117 S. Minneapolis', 'unit_number': ''},
                {'address': '2121 S. Minneapolis', 'unit_number': ''},
                {'address': '2123 S. Minneapolis', 'unit_number': ''},
                {'address': '2125 S. Minneapolis', 'unit_number': ''},
                {'address': '2127 S. Minneapolis', 'unit_number': ''},
                {'address': '2131 S. Minneapolis', 'unit_number': ''},
                {'address': '2133 S. Minneapolis', 'unit_number': ''},
                {'address': '2135 S. Minneapolis', 'unit_number': ''},
                {'address': '2137 S. Minneapolis', 'unit_number': ''},
                {'address': '2141 S. Minneapolis', 'unit_number': ''},
                {'address': '2143 S. Minneapolis', 'unit_number': ''},
                {'address': '2145 S. Minneapolis', 'unit_number': ''},
                {'address': '2147 S. Minneapolis', 'unit_number': ''},
                {'address': '2151 S. Minneapolis', 'unit_number': ''},
                {'address': '2153 S. Minneapolis', 'unit_number': ''},
                {'address': '2155 S. Minneapolis', 'unit_number': ''},
                {'address': '2157 S. Minneapolis', 'unit_number': ''},
                {'address': '2161 S. Minneapolis', 'unit_number': ''},
                {'address': '2163 S. Minneapolis', 'unit_number': ''},
                {'address': '2165 S. Minneapolis', 'unit_number': ''},
                {'address': '2167 S. Minneapolis', 'unit_number': ''},
                {'address': '2171 S. Minneapolis', 'unit_number': ''},
                {'address': '2173 S. Minneapolis', 'unit_number': ''},
                {'address': '2175 S. Minneapolis', 'unit_number': ''},
                {'address': '2177 S. Minneapolis', 'unit_number': ''},
                {'address': '2181 S. Minneapolis', 'unit_number': ''},
                {'address': '2183 S. Minneapolis', 'unit_number': ''},
                {'address': '2185 S. Minneapolis', 'unit_number': ''},
                {'address': '2187 S. Minneapolis', 'unit_number': ''},
                {'address': '2112 S. Minneapolis', 'unit_number': ''},
                {'address': '2114 S. Minneapolis', 'unit_number': ''},
                {'address': '2116 S. Minneapolis', 'unit_number': ''},
                {'address': '2118 S. Minneapolis', 'unit_number': ''},
                {'address': '2122 S. Minneapolis', 'unit_number': ''},
                {'address': '2124 S. Minneapolis', 'unit_number': ''},
                {'address': '2126 S. Minneapolis', 'unit_number': ''},
                {'address': '2128 S. Minneapolis', 'unit_number': ''},
                {'address': '2132 S. Minneapolis', 'unit_number': ''},
                {'address': '2134 S. Minneapolis', 'unit_number': ''},
                {'address': '2136 S. Minneapolis', 'unit_number': ''},
                {'address': '2138 S. Minneapolis', 'unit_number': ''},
                {'address': '2142 S. Minneapolis', 'unit_number': ''},
                {'address': '2144 S. Minneapolis', 'unit_number': ''},
                {'address': '2146 S. Minneapolis', 'unit_number': ''},
                {'address': '2148 S. Minneapolis', 'unit_number': ''},
                {'address': '2152 S. Minneapolis', 'unit_number': ''},
                {'address': '2154 S. Minneapolis', 'unit_number': ''},
                {'address': '2156 S. Minneapolis', 'unit_number': ''},
                {'address': '2158 S. Minneapolis', 'unit_number': ''},
                {'address': '1902 E. Hodson', 'unit_number': ''},
                {'address': '1904 E. Hodson', 'unit_number': ''},
                {'address': '1906 E. Hodson', 'unit_number': ''},
                {'address': '1908 E. Hodson', 'unit_number': ''},
                {'address': '2021 S. Minnesota', 'unit_number': ''},
                {'address': '2023 S. Minnesota', 'unit_number': ''},
                {'address': '2025 S. Minnesota', 'unit_number': ''},
                {'address': '2027 S. Minnesota', 'unit_number': ''},
                {'address': '2101 S. Minnesota', 'unit_number': ''},
                {'address': '2103 S. Minnesota', 'unit_number': ''},
                {'address': '2105 S. Minnesota', 'unit_number': ''},
                {'address': '2107 S. Minnesota', 'unit_number': ''},
                {'address': '2115 S. Minnesota', 'unit_number': ''},
                {'address': '2117 S. Minnesota', 'unit_number': ''},
                {'address': '2119 S. Minnesota', 'unit_number': ''},
                {'address': '2121 S. Minnesota', 'unit_number': ''},
                {'address': '2123 S. Minnesota', 'unit_number': ''},
                {'address': '2125 S. Minnesota', 'unit_number': ''},
                {'address': '2127 S. Minnesota', 'unit_number': ''},
                {'address': '2129 S. Minnesota', 'unit_number': ''},
                {'address': '2133 S. Minnesota', 'unit_number': ''},
                {'address': '2135 S. Minnesota', 'unit_number': ''},
                {'address': '2137 S. Minnesota', 'unit_number': ''},
                {'address': '2139 S. Minnesota', 'unit_number': ''},
                {'address': '2143 S. Minnesota', 'unit_number': ''},
                {'address': '2145 S. Minnesota', 'unit_number': ''},
                {'address': '2147 S. Minnesota', 'unit_number': ''},
                {'address': '2149 S. Minnesota', 'unit_number': ''},
                {'address': '2151 S. Minnesota', 'unit_number': ''},
                {'address': '2153 S. Minnesota', 'unit_number': ''},
                {'address': '2155 S. Minnesota', 'unit_number': ''},
                {'address': '2157 S. Minnesota', 'unit_number': ''},
                {'address': '2159 S. Minnesota', 'unit_number': ''},
                {'address': '2161 S. Minnesota', 'unit_number': ''},
                {'address': '2163 S. Minnesota', 'unit_number': ''},
                {'address': '2165 S. Minnesota', 'unit_number': ''},
                {'address': '2167 S. Minnesota', 'unit_number': ''},
                {'address': '2169 S. Minnesota', 'unit_number': ''},
                {'address': '2171 S. Minnesota', 'unit_number': ''},
                {'address': '2173 S. Minnesota', 'unit_number': ''},
                {'address': '2217 S. Minnesota', 'unit_number': ''},
                {'address': '2219 S. Minnesota', 'unit_number': ''},
                {'address': '2221 S. Minnesota', 'unit_number': ''},
                {'address': '2223 S. Minnesota', 'unit_number': ''},
                {'address': '2225 S. Minnesota', 'unit_number': ''},
                {'address': '2227 S. Minnesota', 'unit_number': ''},
                {'address': '2229 S. Minnesota', 'unit_number': ''},
                {'address': '2231 S. Minnesota', 'unit_number': ''},
                {'address': '2233 S. Minnesota', 'unit_number': ''},
                {'address': '2235 S. Minnesota', 'unit_number': ''},
                {'address': '2237 S. Minnesota', 'unit_number': ''},
                {'address': '2239 S. Minnesota', 'unit_number': ''},
                {'address': '2241 S. Minnesota', 'unit_number': ''},
                {'address': '2243 S. Minnesota', 'unit_number': ''},
                {'address': '2245 S. Minnesota', 'unit_number': ''},
                {'address': '2247 S. Minnesota', 'unit_number': ''},
                {'address': '2249 S. Minnesota', 'unit_number': ''},
                {'address': '2251 S. Minnesota', 'unit_number': ''},
                {'address': '2253 S. Minnesota', 'unit_number': ''},
                {'address': '2255 S. Minnesota', 'unit_number': ''},
                {'address': '2257 S. Minnesota', 'unit_number': ''},
                {'address': '2259 S. Minnesota', 'unit_number': ''},
                {'address': '2261 S. Minnesota', 'unit_number': ''},
                {'address': '2263 S. Minnesota', 'unit_number': ''},
                {'address': '2265 S. Minnesota', 'unit_number': ''},
                {'address': '2267 S. Minnesota', 'unit_number': ''},
                {'address': '2269 S. Minnesota', 'unit_number': ''},
                {'address': '2271 S. Minnesota', 'unit_number': ''},
                {'address': '2273 S. Minnesota', 'unit_number': ''},
                {'address': '2275 S. Minnesota', 'unit_number': ''},
                {'address': '2277 S. Minnesota', 'unit_number': ''},
                {'address': '2279 S. Minnesota', 'unit_number': ''},
                {'address': '2118 S. Minnesota', 'unit_number': ''},
                {'address': '2120 S. Minnesota', 'unit_number': ''},
                {'address': '2122 S. Minnesota', 'unit_number': ''},
                {'address': '2124 S. Minnesota', 'unit_number': ''},
                {'address': '2126 S. Minnesota', 'unit_number': ''},
                {'address': '2128 S. Minnesota', 'unit_number': ''},
                {'address': '2130 S. Minnesota', 'unit_number': ''},
                {'address': '2132 S. Minnesota', 'unit_number': ''},
                {'address': '2134 S. Minnesota', 'unit_number': ''},
                {'address': '2136 S. Minnesota', 'unit_number': ''},
                {'address': '2138 S. Minnesota', 'unit_number': ''},
                {'address': '2140 S. Minnesota', 'unit_number': ''},
                {'address': '2202 S. Minnesota', 'unit_number': ''},
                {'address': '2204 S. Minnesota', 'unit_number': ''},
                {'address': '2206 S. Minnesota', 'unit_number': ''},
                {'address': '2208 S. Minnesota', 'unit_number': ''},
                {'address': '2210 S. Minnesota', 'unit_number': ''},
                {'address': '2212 S. Minnesota', 'unit_number': ''},
                {'address': '2214 S. Minnesota', 'unit_number': ''},
                {'address': '2216 S. Minnesota', 'unit_number': ''},
                {'address': '2218 S. Minnesota', 'unit_number': ''},
                {'address': '2220 S. Minnesota', 'unit_number': ''},
                {'address': '2222 S. Minnesota', 'unit_number': ''},
                {'address': '2224 S. Minnesota', 'unit_number': ''},
                {'address': '2226 S. Minnesota', 'unit_number': ''},
                {'address': '2228 S. Minnesota', 'unit_number': ''},
                {'address': '2230 S. Minnesota', 'unit_number': ''},
                {'address': '2232 S. Minnesota', 'unit_number': ''},
                {'address': '2234 S. Minnesota', 'unit_number': ''},
                {'address': '2236 S. Minnesota', 'unit_number': ''},
                {'address': '2238 S. Minnesota', 'unit_number': ''},
                {'address': '2240 S. Minnesota', 'unit_number': ''},
                {'address': '2242 S. Minnesota', 'unit_number': ''},
                {'address': '2244 S. Minnesota', 'unit_number': ''},
                {'address': '2246 S. Minnesota', 'unit_number': ''},
                {'address': '2248 S. Minnesota', 'unit_number': ''},
                {'address': '2250 S. Minnesota', 'unit_number': ''},
                {'address': '2252 S. Minnesota', 'unit_number': ''},
                {'address': '2254 S. Minnesota', 'unit_number': ''},
                {'address': '2256 S. Minnesota', 'unit_number': ''},
                {'address': '2258 S. Minnesota', 'unit_number': ''},
                {'address': '2260 S. Minnesota', 'unit_number': ''},
                {'address': '2262 S. Minnesota', 'unit_number': ''},
                {'address': '2264 S. Minnesota', 'unit_number': ''},
                {'address': '2266 S. Minnesota', 'unit_number': ''},
                {'address': '2268 S. Minnesota', 'unit_number': ''},
                {'address': '2270 S. Minnesota', 'unit_number': ''},
                {'address': '2272 S. Minnesota', 'unit_number': ''},
                {'address': '2274 S. Minnesota', 'unit_number': ''},
                {'address': '2276 S. Minnesota', 'unit_number': ''},
                {'address': '2278 S. Minnesota', 'unit_number': ''},
                {'address': '2280 S. Minnesota', 'unit_number': ''},
                {'address': '2282 S. Minnesota', 'unit_number': ''},
                {'address': '2284 S. Minnesota', 'unit_number': ''},
                {'address': '2286 S. Minnesota', 'unit_number': ''},
                {'address': '2288 S. Minnesota', 'unit_number': ''},
                {'address': '1902 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1904 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1906 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1908 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1910 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1912 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1914 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1916 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1918 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1920 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1922 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1924 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1926 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1928 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1930 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1932 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1934 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1936 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1938 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1940 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1942 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1944 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1946 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1948 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1950 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1952 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1954 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1956 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1958 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1960 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1962 E. Stafford Ct.', 'unit_number': ''},
                {'address': '1964 E. Stafford Ct.', 'unit_number': ''},
                {'address': '2114 S. Minnesota', 'unit_number': ''},
                {'address': '2116 S. Minnesota', 'unit_number': ''},
                {'address': '2160 S. Minneapolis', 'unit_number': ''},
                {'address': '2162 S. Minneapolis', 'unit_number': ''},
                {'address': '2175 S. Minnesota', 'unit_number': ''},
                {'address': '2177 S. Minnesota', 'unit_number': ''},
                {'address': '2179 S. Minnesota', 'unit_number': ''},
                {'address': '2201 S. Minnesota', 'unit_number': ''},
                {'address': '2203 S. Minnesota', 'unit_number': ''},
                {'address': '2205 S. Minnesota', 'unit_number': ''},
                {'address': '2209 S. Minnesota', 'unit_number': ''},
                {'address': '2211 S. Minnesota', 'unit_number': ''},
                {'address': '2213 S. Minnesota', 'unit_number': ''},
                {'address': '2215 S. Minnesota', 'unit_number': ''}]},
 {'portfolio': 'Sandstone Portfolio',
  'property': 'Sandstone Apartments',
  'addresses': [{'address': '610 S. Oliver', 'unit_number': '200'},
                {'address': '610 S. Oliver', 'unit_number': '300'},
                {'address': '610 S. Oliver', 'unit_number': '400'},
                {'address': '610 S. Oliver', 'unit_number': '500'},
                {'address': '610 S. Oliver', 'unit_number': '600'},
                {'address': '610 S. Oliver', 'unit_number': '700'},
                {'address': '616 S. Oliver', 'unit_number': 'A'},
                {'address': '616 S. Oliver', 'unit_number': 'B'},
                {'address': '616 S. Oliver', 'unit_number': 'C'},
                {'address': '616 S. Oliver', 'unit_number': 'D'},
                {'address': '618 S. Oliver', 'unit_number': 'A'},
                {'address': '618 S. Oliver', 'unit_number': 'B'},
                {'address': '620 S. Oliver', 'unit_number': 'A'},
                {'address': '620 S. Oliver', 'unit_number': 'B'},
                {'address': '620 S. Oliver', 'unit_number': 'C'},
                {'address': '620 S. Oliver', 'unit_number': 'D'},
                {'address': '622 S. Oliver', 'unit_number': 'A'},
                {'address': '622 S. Oliver', 'unit_number': 'B'},
                {'address': '624 S. Oliver', 'unit_number': 'A'},
                {'address': '624 S. Oliver', 'unit_number': 'B'},
                {'address': '626 S. Oliver', 'unit_number': 'A'},
                {'address': '626 S. Oliver', 'unit_number': 'B'},
                {'address': '626 S. Oliver', 'unit_number': 'C'},
                {'address': '626 S. Oliver', 'unit_number': 'D'},
                {'address': '4825 E. Eastwood', 'unit_number': 'A'},
                {'address': '4825 E. Eastwood', 'unit_number': 'B'},
                {'address': '4827 E. Eastwood', 'unit_number': 'A'},
                {'address': '4827 E. Eastwood', 'unit_number': 'B'},
                {'address': '4827 E. Eastwood', 'unit_number': 'C'},
                {'address': '4827 E. Eastwood', 'unit_number': 'D'},
                {'address': '4829 E. Eastwood', 'unit_number': 'A'},
                {'address': '4829 E. Eastwood', 'unit_number': 'B'},
                {'address': '4829 E. Eastwood', 'unit_number': 'C'},
                {'address': '4829 E. Eastwood', 'unit_number': 'D'},
                {'address': '4831 E. Eastwood', 'unit_number': 'A'},
                {'address': '4831 E. Eastwood', 'unit_number': 'B'},
                {'address': '4831 E. Eastwood', 'unit_number': 'C'},
                {'address': '4831 E. Eastwood', 'unit_number': 'D'},
                {'address': '4833 E. Eastwood', 'unit_number': 'A'},
                {'address': '4833 E. Eastwood', 'unit_number': 'B'},
                {'address': '4835 E. Eastwood', 'unit_number': 'A'},
                {'address': '4835 E. Eastwood', 'unit_number': 'B'},
                {'address': '4835 E. Eastwood', 'unit_number': 'C'},
                {'address': '4835 E. Eastwood', 'unit_number': 'D'},
                {'address': '4840 E. Eastwood', 'unit_number': 'A'},
                {'address': '4840 E. Eastwood', 'unit_number': 'B'},
                {'address': '4840 E. Eastwood', 'unit_number': 'C'},
                {'address': '4840 E. Eastwood', 'unit_number': 'D'},
                {'address': '4842 E. Eastwood', 'unit_number': 'A'},
                {'address': '4842 E. Eastwood', 'unit_number': 'B'},
                {'address': '4844 E. Eastwood', 'unit_number': 'A'},
                {'address': '4844 E. Eastwood', 'unit_number': 'B'},
                {'address': '4844 E. Eastwood', 'unit_number': 'C'},
                {'address': '4844 E. Eastwood', 'unit_number': 'D'},
                {'address': '4846 E. Eastwood', 'unit_number': 'A'},
                {'address': '4846 E. Eastwood', 'unit_number': 'B'},
                {'address': '4846 E. Eastwood', 'unit_number': 'C'},
                {'address': '4846 E. Eastwood', 'unit_number': 'D'},
                {'address': '4848 E. Eastwood', 'unit_number': 'A'},
                {'address': '4848 E. Eastwood', 'unit_number': 'B'},
                {'address': '4850 E. Eastwood', 'unit_number': 'A'},
                {'address': '4850 E. Eastwood', 'unit_number': 'B'},
                {'address': '4850 E. Eastwood', 'unit_number': 'C'},
                {'address': '4850 E. Eastwood', 'unit_number': 'D'},
                {'address': '4826 E. Eastwood', 'unit_number': 'A'},
                {'address': '4826 E. Eastwood', 'unit_number': 'B'},
                {'address': '4826 E. Eastwood', 'unit_number': 'C'},
                {'address': '4826 E. Eastwood', 'unit_number': 'D'},
                {'address': '4828 E. Eastwood', 'unit_number': 'A'},
                {'address': '4828 E. Eastwood', 'unit_number': 'B'},
                {'address': '4830 E. Eastwood', 'unit_number': 'A'},
                {'address': '4830 E. Eastwood', 'unit_number': 'B'},
                {'address': '4830 E. Eastwood', 'unit_number': 'C'},
                {'address': '4830 E. Eastwood', 'unit_number': 'D'},
                {'address': '4832 E. Eastwood', 'unit_number': 'A'},
                {'address': '4832 E. Eastwood', 'unit_number': 'B'},
                {'address': '4832 E. Eastwood', 'unit_number': 'C'},
                {'address': '4832 E. Eastwood', 'unit_number': 'D'},
                {'address': '4834 E. Eastwood', 'unit_number': 'A'},
                {'address': '4834 E. Eastwood', 'unit_number': 'B'},
                {'address': '4836 E. Eastwood', 'unit_number': 'A'},
                {'address': '4836 E. Eastwood', 'unit_number': 'B'},
                {'address': '4836 E. Eastwood', 'unit_number': 'C'},
                {'address': '4836 E. Eastwood', 'unit_number': 'D'},
                {'address': '4818 E. Eastwood', 'unit_number': 'A'},
                {'address': '4818 E. Eastwood', 'unit_number': 'B'},
                {'address': '4818 E. Eastwood', 'unit_number': 'C'},
                {'address': '4818 E. Eastwood', 'unit_number': 'D'},
                {'address': '4820 E. Eastwood', 'unit_number': 'A'},
                {'address': '4820 E. Eastwood', 'unit_number': 'B'},
                {'address': '4822 E. Eastwood', 'unit_number': 'A'},
                {'address': '4822 E. Eastwood', 'unit_number': 'B'},
                {'address': '4822 E. Eastwood', 'unit_number': 'C'},
                {'address': '4822 E. Eastwood', 'unit_number': 'D'},
                {'address': '4824 E. Eastwood', 'unit_number': 'A'},
                {'address': '4824 E. Eastwood', 'unit_number': 'B'},
                {'address': '4824 E. Eastwood', 'unit_number': 'C'},
                {'address': '4824 E. Eastwood', 'unit_number': 'D'},
                {'address': '4802 E. Eastwood', 'unit_number': 'A'},
                {'address': '4802 E. Eastwood', 'unit_number': 'B'},
                {'address': '4802 E. Eastwood', 'unit_number': 'C'},
                {'address': '4802 E. Eastwood', 'unit_number': 'D'},
                {'address': '4804 E. Eastwood', 'unit_number': 'A'},
                {'address': '4804 E. Eastwood', 'unit_number': 'B'},
                {'address': '4806 E. Eastwood', 'unit_number': 'A'},
                {'address': '4806 E. Eastwood', 'unit_number': 'B'},
                {'address': '4806 E. Eastwood', 'unit_number': 'C'},
                {'address': '4806 E. Eastwood', 'unit_number': 'D'},
                {'address': '4808 E. Eastwood', 'unit_number': 'A'},
                {'address': '4808 E. Eastwood', 'unit_number': 'B'},
                {'address': '4808 E. Eastwood', 'unit_number': 'C'},
                {'address': '4808 E. Eastwood', 'unit_number': 'D'},
                {'address': '4810 E. Eastwood', 'unit_number': 'A'},
                {'address': '4810 E. Eastwood', 'unit_number': 'B'},
                {'address': '4812 E. Eastwood', 'unit_number': 'A'},
                {'address': '4812 E. Eastwood', 'unit_number': 'B'},
                {'address': '4812 E. Eastwood', 'unit_number': 'C'},
                {'address': '4812 E. Eastwood', 'unit_number': 'D'},
                {'address': '4814 E. Eastwood', 'unit_number': 'A'},
                {'address': '4814 E. Eastwood', 'unit_number': 'B'},
                {'address': '4816 E. Eastwood', 'unit_number': 'A'},
                {'address': '4816 E. Eastwood', 'unit_number': 'B'},
                {'address': '4816 E. Eastwood', 'unit_number': 'C'},
                {'address': '4816 E. Eastwood', 'unit_number': 'D'}]},
 {'portfolio': 'Residences Portfolio',
 'property': 'ReVest Rentals',
 'addresses': [{'address': '905 W. 54th', 'unit_number': ''},
               {'address': '907 W. 54th', 'unit_number': ''},
               {'address': '3907 E. Skinner', 'unit_number': ''},
               {'address': '931 W 54th', 'unit_number': ''},
               {'address': '933 W 54th', 'unit_number': ''},
               {'address': '915 W 54th', 'unit_number': ''},
               {'address': '917 W 54th', 'unit_number': ''},
               {'address': '1003 W 54th', 'unit_number': ''},
               {'address': '1005 W 54th', 'unit_number': ''},
               {'address': '1015 W. 54th', 'unit_number': ''},
               {'address': '1017 W. 54th', 'unit_number': ''},
               {'address': '1021 W. 54th', 'unit_number': ''},
               {'address': '1023 W. 54th', 'unit_number': ''},
               {'address': '1004 W 54th', 'unit_number': ''},
               {'address': '1006 W 54th', 'unit_number': ''},
               {'address': '930 W 54th', 'unit_number': ''},
               {'address': '932 W 54th', 'unit_number': ''},
               {'address': '5556 S. Handley', 'unit_number': ''},
               {'address': '5558 S. Handley', 'unit_number': ''},
               {'address': '5522 S. Handley', 'unit_number': ''},
               {'address': '5524 S. Handley', 'unit_number': ''},
               {'address': '1022 W. 54th', 'unit_number': ''},
               {'address': '1024 W. 54th', 'unit_number': ''},
               {'address': '1016 W. 54th', 'unit_number': ''},
               {'address': '1018 W. 54th', 'unit_number': ''},
               {'address': '1010 W. 54th', 'unit_number': ''},
               {'address': '1012 W. 54th', 'unit_number': ''}]},
 {'portfolio': 'Residences Portfolio',
  'property': 'Comfort West Apartments',
  'addresses': [{'address': '4828 W. 2nd', 'unit_number': '11'},
                {'address': '4828 W. 2nd', 'unit_number': '12'},
                {'address': '4828 W. 2nd', 'unit_number': '13'},
                {'address': '4828 W. 2nd', 'unit_number': '14'},
                {'address': '4828 W. 2nd', 'unit_number': '15'},
                {'address': '4828 W. 2nd', 'unit_number': '16'},
                {'address': '4828 W. 2nd', 'unit_number': '17'},
                {'address': '4828 W. 2nd', 'unit_number': '18'},
                {'address': '4828 W. 2nd', 'unit_number': '21'},
                {'address': '4828 W. 2nd', 'unit_number': '22'},
                {'address': '4828 W. 2nd', 'unit_number': '23'},
                {'address': '4828 W. 2nd', 'unit_number': '24'},
                {'address': '4828 W. 2nd', 'unit_number': '25'},
                {'address': '4828 W. 2nd', 'unit_number': '26'},
                {'address': '4828 W. 2nd', 'unit_number': '27'},
                {'address': '4828 W. 2nd', 'unit_number': '28'},
                {'address': '4828 W. 2nd', 'unit_number': '31'},
                {'address': '4828 W. 2nd', 'unit_number': '32'},
                {'address': '4828 W. 2nd', 'unit_number': '33'},
                {'address': '4828 W. 2nd', 'unit_number': '34'},
                {'address': '4828 W. 2nd', 'unit_number': '35'},
                {'address': '4828 W. 2nd', 'unit_number': '36'},
                {'address': '4828 W. 2nd', 'unit_number': '37'},
                {'address': '4828 W. 2nd', 'unit_number': '38'}]},
 {'portfolio': 'Sandstone Portfolio',
 'property': 'Plaza Apartments',
 'addresses': [{'address': '1701 George Washington', 'unit_number': ''},
               {'address': '1703 George Washington', 'unit_number': ''},
               {'address': '1705 George Washington', 'unit_number': ''},
               {'address': '1707 George Washington', 'unit_number': ''},
               {'address': '1709 George Washington', 'unit_number': ''},
               {'address': '1711 George Washington', 'unit_number': ''},
               {'address': '1715 George Washington', 'unit_number': ''},
               {'address': '1717 George Washington', 'unit_number': ''},
               {'address': '1719 George Washington', 'unit_number': ''},
               {'address': '1721 George Washington', 'unit_number': ''},
               {'address': '1723 George Washington', 'unit_number': ''},
               {'address': '1725 George Washington', 'unit_number': ''},
               {'address': '1727 George Washington', 'unit_number': ''},
               {'address': '1729 George Washington', 'unit_number': ''},
               {'address': '1731 George Washington', 'unit_number': ''},
               {'address': '1733 George Washington', 'unit_number': ''},
               {'address': '1735 George Washington', 'unit_number': ''},
               {'address': '1737 George Washington', 'unit_number': ''},
               {'address': '1739 George Washington', 'unit_number': ''},
               {'address': '1741 George Washington', 'unit_number': ''},
               {'address': '1743 George Washington', 'unit_number': ''},
               {'address': '1745 George Washington', 'unit_number': ''},
               {'address': '1747 George Washington', 'unit_number': ''},
               {'address': '1749 George Washington', 'unit_number': ''},
               {'address': '1751 George Washington', 'unit_number': ''},
               {'address': '1753 George Washington', 'unit_number': ''},
               {'address': '3001 Osie', 'unit_number': ''},
               {'address': '3003 Osie', 'unit_number': ''},
               {'address': '3005 Osie', 'unit_number': ''},
               {'address': '3007 Osie', 'unit_number': ''},
               {'address': '3009 Osie', 'unit_number': ''},
               {'address': '3011 Osie', 'unit_number': ''},
               {'address': '3013 Osie', 'unit_number': ''},
               {'address': '3015 Osie', 'unit_number': ''},
               {'address': '3017 Osie', 'unit_number': ''},
               {'address': '3019 Osie', 'unit_number': ''},
               {'address': '3021 Osie', 'unit_number': ''},
               {'address': '3023 Osie', 'unit_number': ''},
               {'address': '3002 Schrader', 'unit_number': ''},
               {'address': '3004 Schrader', 'unit_number': ''},
               {'address': '3006 Schrader', 'unit_number': ''},
               {'address': '3008 Schrader', 'unit_number': ''},
               {'address': '3010 Schrader', 'unit_number': ''},
               {'address': '3012 Schrader', 'unit_number': ''},
               {'address': '3014 Schrader', 'unit_number': ''},
               {'address': '3016 Schrader', 'unit_number': ''},
               {'address': '3018 Schrader', 'unit_number': ''},
               {'address': '3020 Schrader', 'unit_number': ''},
               {'address': '3022 Schrader', 'unit_number': ''},
               {'address': '3024 Schrader', 'unit_number': ''},
               {'address': '3026 Schrader', 'unit_number': ''},
               {'address': '3028 Schrader', 'unit_number': ''},
               {'address': '3030 Schrader', 'unit_number': ''},
               {'address': '3001 Schrader', 'unit_number': ''},
               {'address': '3005 Schrader', 'unit_number': ''},
               {'address': '3007 Schrader', 'unit_number': ''},
               {'address': '3009 Schrader', 'unit_number': ''},
               {'address': '3011 Schrader', 'unit_number': ''},
               {'address': '3013 Schrader', 'unit_number': ''},
               {'address': '3015 Schrader', 'unit_number': ''},
               {'address': '3017 Schrader', 'unit_number': ''},
               {'address': '3019 Schrader', 'unit_number': ''},
               {'address': '3021 Schrader', 'unit_number': ''},
               {'address': '3023 Schrader', 'unit_number': ''},
               {'address': '3025 Schrader', 'unit_number': ''},
               {'address': '3027 Schrader', 'unit_number': ''},
               {'address': '3029 Schrader', 'unit_number': ''},
               {'address': '3031 Schrader', 'unit_number': ''},
               {'address': '3033 Schrader', 'unit_number': ''},
               {'address': '3035 Schrader', 'unit_number': ''},
               {'address': '3037 Schrader', 'unit_number': ''},
               {'address': '3039 Schrader', 'unit_number': ''},
               {'address': '3002 Funston', 'unit_number': ''},
               {'address': '3004 Funston', 'unit_number': ''},
               {'address': '3006 Funston', 'unit_number': ''},
               {'address': '3008 Funston', 'unit_number': ''},
               {'address': '3010 Funston', 'unit_number': ''},
               {'address': '3012 Funston', 'unit_number': ''},
               {'address': '3014 Funston', 'unit_number': ''},
               {'address': '3016 Funston', 'unit_number': ''},
               {'address': '3018 Funston', 'unit_number': ''},
               {'address': '3020 Funston', 'unit_number': ''},
               {'address': '3022 Funston', 'unit_number': ''},
               {'address': '3024 Funston', 'unit_number': ''},
               {'address': '3025 Funston', 'unit_number': ''},
               {'address': '3028 Funston', 'unit_number': ''},
               {'address': '3030 Funston', 'unit_number': ''},
               {'address': '3032 Funston', 'unit_number': ''},
               {'address': '3034 Funston', 'unit_number': ''},
               {'address': '3036 Funston', 'unit_number': ''},
               {'address': '3038 Funston', 'unit_number': ''},
               {'address': '3040 Funston', 'unit_number': ''},
               {'address': '3102 Funston', 'unit_number': ''},
               {'address': '3104 Funston', 'unit_number': ''},
               {'address': '3106 Funston', 'unit_number': ''},
               {'address': '3108 Funston', 'unit_number': ''},
               {'address': '3110 Funston', 'unit_number': ''},
               {'address': '3112 Funston', 'unit_number': ''},
               {'address': '3114 Funston', 'unit_number': ''},
               {'address': '3116 Funston', 'unit_number': ''}]}]


def seed_default_portfolio_properties_and_addresses():
    for item in DEFAULT_PORTFOLIO_ADDRESS_SEED_DATA:
        portfolio_name = str(item.get("portfolio") or "").strip()
        property_name = str(item.get("property") or "").strip()
        if not portfolio_name or not property_name:
            continue

        property_df = fetch_df(
            """
            SELECT id
            FROM portfolio_properties
            WHERE COALESCE(deleted, FALSE) = FALSE
              AND COALESCE(portfolio_name, '') = ?
              AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (portfolio_name, property_name),
        )
        if property_df.empty:
            property_id = execute_returning_id(
                """
                INSERT INTO portfolio_properties (
                    portfolio_name, property_name, notes, active, deleted, created_at, modified_at
                ) VALUES (?, ?, '', TRUE, FALSE, NOW(), NOW())
                """,
                (portfolio_name, property_name),
            )
        else:
            property_id = int(property_df.iloc[0]["id"])

        # Clean up old combined ReVest address if it exists; it is now split into 905 W. 54th and 907 W. 54th.
        if portfolio_name == "Residences Portfolio" and property_name == "ReVest Rentals":
            execute(
                """
                UPDATE portfolio_addresses
                SET deleted = TRUE, active = FALSE, modified_at = NOW()
                WHERE COALESCE(portfolio_name, '') = ?
                  AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
                  AND LOWER(REPLACE(TRIM(COALESCE(address, '')), '.', '')) IN ('905-907 w 54th', '905-907 w 54')
                """,
                (portfolio_name, property_name),
            )

        for row in item.get("addresses", []):
            address = str(row.get("address") or "").strip()
            unit_number = str(row.get("unit_number") or "").strip()
            if not address:
                continue
            existing_address_df = fetch_df(
                """
                SELECT id
                FROM portfolio_addresses
                WHERE COALESCE(deleted, FALSE) = FALSE
                  AND COALESCE(portfolio_name, '') = ?
                  AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
                  AND LOWER(TRIM(COALESCE(address, ''))) = LOWER(TRIM(?))
                  AND LOWER(TRIM(COALESCE(unit_number, ''))) = LOWER(TRIM(?))
                LIMIT 1
                """,
                (portfolio_name, property_name, address, unit_number),
            )
            if existing_address_df.empty:
                execute(
                    """
                    INSERT INTO portfolio_addresses (
                        portfolio_property_id, portfolio_name, property_name, address, unit_number,
                        notes, active, deleted, created_at, modified_at
                    ) VALUES (?, ?, ?, ?, ?, '', TRUE, FALSE, NOW(), NOW())
                    """,
                    (property_id, portfolio_name, property_name, address, unit_number),
                )
            else:
                execute(
                    """
                    UPDATE portfolio_addresses
                    SET portfolio_property_id = ?, portfolio_name = ?, property_name = ?, active = TRUE, deleted = FALSE, modified_at = NOW()
                    WHERE id = ?
                    """,
                    (property_id, portfolio_name, property_name, int(existing_address_df.iloc[0]["id"])),
                )


def manager_request_file_bytes(row) -> bytes | None:
    file_bytes = row.get("file_bytes")
    if file_bytes is not None and not isinstance(file_bytes, (bytes, bytearray)):
        try:
            file_bytes = bytes(file_bytes)
        except Exception:
            file_bytes = None
    if file_bytes:
        return file_bytes
    blob_name = str(row.get("blob_name") or "")
    if blob_name:
        return cached_download_blob_bytes(blob_name)
    return None


def render_manager_request_files(request_id: int, section_key: str, allow_delete: bool = True):
    files_df = manager_repair_request_files_df(request_id)
    if files_df.empty:
        st.info("No photos or documents uploaded yet.")
        return

    st.caption(f"{len(files_df)} file(s) uploaded.")
    for _, file_row in files_df.iterrows():
        file_id = int(file_row["id"])
        file_name = str(file_row.get("file_filename") or "file")
        content_type = str(file_row.get("content_type") or "application/octet-stream")
        file_bytes = manager_request_file_bytes(file_row)
        c1, c2 = st.columns([3, 1])
        c1.write(f"**{file_name}**")
        if content_type.startswith("image/"):
            if c1.checkbox(f"Preview {file_name}", key=f"preview_mgr_req_file_{section_key}_{request_id}_{file_id}", value=False):
                try:
                    if file_bytes:
                        c1.image(file_bytes, caption=file_name, use_container_width=True)
                    elif str(file_row.get("blob_url") or ""):
                        c1.image(str(file_row.get("blob_url")), caption=file_name, use_container_width=True)
                except Exception:
                    c1.warning(f"Could not preview {file_name}.")
        if file_bytes:
            c2.download_button(
                "Download",
                data=file_bytes,
                file_name=file_name,
                mime=content_type,
                key=f"download_mgr_req_file_{section_key}_{request_id}_{file_id}",
            )
        if allow_delete and c2.button("Delete", key=f"delete_mgr_req_file_{section_key}_{request_id}_{file_id}"):
            delete_manager_repair_request_file(file_id)
            st.success("File deleted.")
            st.rerun()


def render_manager_request_conversation(request_id: int):
    st.markdown("### Conversation History")
    comments_df = manager_repair_request_comments_df(request_id)
    if comments_df.empty:
        st.info("No comments yet.")
    else:
        for row in comments_df.itertuples():
            created_display = pd.to_datetime(row.created_at, errors="coerce")
            created_text = created_display.strftime("%m-%d-%Y %I:%M %p") if pd.notna(created_display) else ""
            label = f"{created_text} — {row.username or 'User'} ({row.role or ''})"
            st.markdown(f"**{label}**")
            st.write(str(row.comment_text or ""))
            st.markdown("---")


def promote_manager_request_to_pipeline(request_id: int) -> int | None:
    df = manager_repair_requests_df(include_archived=True)
    if df.empty:
        return None
    match = df[df["id"] == int(request_id)]
    if match.empty:
        return None
    row = match.iloc[0].to_dict()
    pipeline_id = execute_returning_id(
        """
        INSERT INTO renovation_pipeline_items (
            project_name, project_address, category_name, work_group_name, work_item_name,
            priority, status, target_timing, rough_budget, rough_labor_hours, rough_duration,
            cash_flow_notes, scope_description, notes, created_by, created_at, modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'Needs Review', '', 0, 0, '', '', ?, ?, ?, NOW(), NOW())
        """,
        (
            str(row.get("property_name") or "").strip(),
            str(row.get("address") or "").strip(),
            "",
            "",
            str(row.get("repair_description") or "").strip()[:250],
            "High" if str(row.get("priority") or "").startswith("1.") else "Medium",
            str(row.get("repair_description") or "").strip(),
            f"Manager request #{int(request_id)} from {row.get('manager_username', '')}. Unit: {row.get('unit_number', '')}. Owner response: {row.get('owner_response', '')}",
            str(st.session_state.get("logged_in_user", "") or ""),
        ),
    )
    if pipeline_id:
        execute(
            "UPDATE manager_repair_requests SET status = 'Added To Project Ideas', modified_at = NOW() WHERE id = ?",
            (int(request_id),),
        )
    return int(pipeline_id) if pipeline_id else None


PROJECT_IDEA_STATUS_OPTIONS = [
    "Idea",
    "Research",
    "Budget Review",
    "Converted To RMR",
    "Archived",
    "Converted To Estimate",
    "Converted To Work Group",
    "Converted To Project",
]
# Backward-compatible alias for older code and saved records.
PIPELINE_STATUS_OPTIONS = PROJECT_IDEA_STATUS_OPTIONS
ACTIVE_PROJECT_IDEA_STATUSES = ["Idea", "Research", "Budget Review"]

PIPELINE_PRIORITY_OPTIONS = ["High", "Medium", "Low", "Urgent"]


@st.cache_data(show_spinner=False, ttl=300)
def renovation_pipeline_items_df(include_archived: bool = False, include_deleted: bool = False) -> pd.DataFrame:
    query = """
        SELECT
            rpi.id,
            COALESCE(rpi.project_id, 0) AS project_id,
            COALESCE(pr.project_name, rpi.project_name, '') AS linked_project_name,
            COALESCE(rpi.project_name, '') AS project_name,
            COALESCE(rpi.project_address, '') AS project_address,
            COALESCE(rpi.category_name, '') AS category_name,
            COALESCE(rpi.work_group_name, '') AS work_group_name,
            COALESCE(rpi.work_item_name, '') AS work_item_name,
            COALESCE(rpi.priority, 'Medium') AS priority,
            COALESCE(rpi.status, 'Idea') AS status,
            COALESCE(rpi.target_timing, '') AS target_timing,
            COALESCE(rpi.rough_budget, 0) AS rough_budget,
            COALESCE(rpi.rough_labor_hours, 0) AS rough_labor_hours,
            COALESCE(rpi.rough_duration, '') AS rough_duration,
            COALESCE(rpi.cash_flow_notes, '') AS cash_flow_notes,
            COALESCE(rpi.scope_description, '') AS scope_description,
            COALESCE(rpi.notes, '') AS notes,
            COALESCE(rpi.promoted_project_id, 0) AS promoted_project_id,
            COALESCE(rpi.promoted_estimate_id, 0) AS promoted_estimate_id,
            COALESCE(rpi.promoted_work_group_id, 0) AS promoted_work_group_id,
            COALESCE(rpi.promoted_rmr_id, 0) AS promoted_rmr_id,
            COALESCE(rpi.archived, FALSE) AS archived,
            COALESCE(rpi.deleted, FALSE) AS deleted,
            COALESCE(rpi.created_by, '') AS created_by,
            rpi.created_at,
            rpi.modified_at
        FROM renovation_pipeline_items rpi
        LEFT JOIN project_registry pr ON pr.id = rpi.project_id
        WHERE 1 = 1
    """
    params = []
    if not include_archived:
        query += " AND COALESCE(rpi.archived, FALSE) = FALSE"
    if not include_deleted:
        query += " AND COALESCE(rpi.deleted, FALSE) = FALSE"
    query += """
        ORDER BY
            CASE COALESCE(rpi.priority, 'Medium')
                WHEN 'Urgent' THEN 1
                WHEN 'High' THEN 2
                WHEN 'Medium' THEN 3
                WHEN 'Low' THEN 4
                ELSE 5
            END,
            rpi.modified_at DESC,
            rpi.id DESC
    """
    return fetch_df(query, tuple(params))


@st.cache_data(show_spinner=False, ttl=300)
def renovation_pipeline_files_df(pipeline_item_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            pipeline_item_id,
            COALESCE(file_filename, '') AS file_filename,
            COALESCE(content_type, 'application/octet-stream') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            file_bytes,
            COALESCE(sort_order, 0) AS sort_order,
            COALESCE(uploaded_by, '') AS uploaded_by,
            created_at
        FROM renovation_pipeline_files
        WHERE pipeline_item_id = ?
        ORDER BY sort_order, id
        """,
        (pipeline_item_id,),
    )


def save_renovation_pipeline_files(pipeline_item_id: int, uploaded_files, uploaded_by: str = ""):
    files = []
    for sort_order, uploaded in enumerate(uploaded_files or []):
        if uploaded is None:
            continue
        data = uploaded.getvalue()
        if not data:
            continue
        filename = getattr(uploaded, "name", "file")
        content_type = getattr(uploaded, "type", None) or "application/octet-stream"
        stored_file = upload_bytes_to_blob(
            data=data,
            filename=filename,
            content_type=content_type,
            folder="renovation-estimator/pipeline-files",
        )
        stored_file["sort_order"] = sort_order
        files.append(stored_file)

    if not files:
        return

    existing_df = renovation_pipeline_files_df(pipeline_item_id)
    existing_count = len(existing_df) if existing_df is not None else 0
    for offset, stored_file in enumerate(files):
        execute(
            """
            INSERT INTO renovation_pipeline_files (
                pipeline_item_id, file_filename, content_type, storage_mode, blob_url, blob_name,
                file_bytes, sort_order, uploaded_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            """,
            (
                pipeline_item_id,
                stored_file.get("filename"),
                stored_file.get("content_type"),
                stored_file.get("storage_mode"),
                stored_file.get("blob_url"),
                stored_file.get("blob_name"),
                stored_file.get("bytes"),
                existing_count + offset,
                uploaded_by or "",
            ),
        )


def delete_renovation_pipeline_file(file_id: int):
    execute("DELETE FROM renovation_pipeline_files WHERE id = ?", (file_id,))


def delete_renovation_pipeline_item(pipeline_item_id: int):
    execute(
        "UPDATE renovation_pipeline_items SET deleted = TRUE, modified_at = NOW() WHERE id = ?",
        (pipeline_item_id,),
    )


def archive_renovation_pipeline_item(pipeline_item_id: int, archived: bool = True):
    execute(
        "UPDATE renovation_pipeline_items SET archived = ?, modified_at = NOW() WHERE id = ?",
        (archived, pipeline_item_id),
    )


def find_or_create_project_from_pipeline(row: dict, active: bool = False) -> int | None:
    project_name = str(row.get("project_name") or "").strip()
    project_address = str(row.get("project_address") or "").strip()
    existing_project_id = int(row.get("project_id") or 0)
    if existing_project_id:
        return existing_project_id
    if not project_name:
        return None

    existing = fetch_df(
        """
        SELECT id
        FROM project_registry
        WHERE LOWER(TRIM(COALESCE(project_name, ''))) = LOWER(TRIM(?))
          AND COALESCE(deleted, FALSE) = FALSE
        ORDER BY id
        LIMIT 1
        """,
        (project_name,),
    )
    if not existing.empty:
        return int(existing.iloc[0]["id"])

    new_project_id = execute_returning_id(
        """
        INSERT INTO project_registry (
            project_name, project_address, active, notes, activated_at, created_at, modified_at
        ) VALUES (?, ?, ?, ?, ?, NOW(), NOW())
        """,
        (
            project_name,
            project_address,
            bool(active),
            str(row.get("notes") or row.get("scope_description") or "").strip(),
            datetime.now() if active else None,
        ),
    )
    if new_project_id:
        execute(
            "UPDATE project_registry SET project_code = 'PRJ-' || LPAD(id::text, 6, '0') WHERE id = ? AND COALESCE(project_code, '') = ''",
            (int(new_project_id),),
        )
    return int(new_project_id) if new_project_id else None


def pipeline_item_to_estimate(pipeline_item_id: int) -> int | None:
    df = renovation_pipeline_items_df(include_archived=True)
    if df.empty:
        return None
    match = df[df["id"] == int(pipeline_item_id)]
    if match.empty:
        return None
    row = match.iloc[0].to_dict()
    project_id = find_or_create_project_from_pipeline(row, active=False)
    if not project_id:
        return None

    estimate_id = execute_returning_id(
        """
        INSERT INTO estimates (
            project_id, estimate_name, estimate_address, contractor_id, labor_rate, active, notes,
            category_name, work_group_name, estimate_mode, source_method, status, version_no,
            created_at, modified_at
        ) VALUES (?, ?, ?, ?, ?, TRUE, ?, ?, ?, 'manual', 'pipeline', 'draft', 1, NOW(), NOW())
        """,
        (
            project_id,
            str(row.get("project_name") or "").strip(),
            str(row.get("project_address") or "").strip(),
            None,
            0.0,
            str(row.get("notes") or "").strip(),
            str(row.get("category_name") or "").strip(),
            str(row.get("work_group_name") or "").strip(),
        ),
    )
    if estimate_id:
        set_order_number("estimates", int(estimate_id), "Est")
        execute(
            """
            INSERT INTO estimate_lines (
                estimate_id, category_name, work_group_name, trade_name, task_name, scope_description,
                repair_quantity, onsite_hours_each, travel_hours_each, total_hours_each,
                onsite_hours, travel_hours, total_hours, labor_rate, onsite_cost, travel_cost,
                manual_repair_amount, total_labor_cost, created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 0, ?, ?, 0, ?, 0, 0, 0, ?, ?, NOW(), NOW())
            """,
            (
                int(estimate_id),
                str(row.get("category_name") or "").strip(),
                str(row.get("work_group_name") or "").strip(),
                str(row.get("category_name") or "").strip(),
                str(row.get("work_item_name") or "").strip(),
                str(row.get("scope_description") or "").strip(),
                float(row.get("rough_labor_hours") or 0),
                float(row.get("rough_labor_hours") or 0),
                float(row.get("rough_labor_hours") or 0),
                float(row.get("rough_labor_hours") or 0),
                float(row.get("rough_budget") or 0),
                float(row.get("rough_budget") or 0),
            ),
        )
        execute(
            """
            UPDATE renovation_pipeline_items
            SET promoted_project_id = ?, promoted_estimate_id = ?, status = 'Converted To Estimate', modified_at = NOW()
            WHERE id = ?
            """,
            (project_id, int(estimate_id), int(pipeline_item_id)),
        )
    return int(estimate_id) if estimate_id else None


def pipeline_item_to_work_group(pipeline_item_id: int) -> int | None:
    df = renovation_pipeline_items_df(include_archived=True)
    if df.empty:
        return None
    match = df[df["id"] == int(pipeline_item_id)]
    if match.empty:
        return None
    row = match.iloc[0].to_dict()
    project_id = find_or_create_project_from_pipeline(row, active=True)
    if not project_id:
        return None

    work_group_id = execute_returning_id(
        """
        INSERT INTO work_groups (
            project_id, estimate_line_id, work_group_name, category_name, task_name, trade_name,
            scope_description, contractor_id, agreed_price, estimated_price, contractor_requested_price,
            amount_to_be_paid, due_date, status, notes, created_at, modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Open', ?, NOW(), NOW())
        """,
        (
            project_id,
            None,
            str(row.get("work_group_name") or row.get("work_item_name") or "").strip(),
            str(row.get("category_name") or "").strip(),
            str(row.get("work_item_name") or "").strip(),
            str(row.get("category_name") or "").strip(),
            str(row.get("scope_description") or "").strip(),
            None,
            None,
            float(row.get("rough_budget") or 0),
            None,
            None,
            None,
            str(row.get("notes") or "").strip(),
        ),
    )
    if work_group_id:
        set_order_number("work_groups", int(work_group_id), "WG")
        execute(
            """
            UPDATE renovation_pipeline_items
            SET promoted_project_id = ?, promoted_work_group_id = ?, status = 'Converted To Work Group', modified_at = NOW()
            WHERE id = ?
            """,
            (project_id, int(work_group_id), int(pipeline_item_id)),
        )
    return int(work_group_id) if work_group_id else None



def project_idea_priority_to_contractor_priority(priority: str) -> str:
    value = str(priority or "").strip().lower()
    if value in ["urgent", "high"]:
        return "1 - ASAP"
    if value in ["medium"]:
        return "2 - Planning"
    return "3 - Quote Only"


def project_idea_to_rmr(pipeline_item_id: int) -> int | None:
    df = renovation_pipeline_items_df(include_archived=True, include_deleted=False)
    if df.empty:
        return None
    match = df[df["id"] == int(pipeline_item_id)]
    if match.empty:
        return None
    row = match.iloc[0].to_dict()

    property_name = str(row.get("project_name") or "").strip()
    address = str(row.get("project_address") or "").strip()
    work_item = str(row.get("work_item_name") or property_name or "Project Idea").strip()
    category = str(row.get("category_name") or "").strip()
    scope = str(row.get("scope_description") or "").strip()
    notes_parts = []
    if str(row.get("notes") or "").strip():
        notes_parts.append(str(row.get("notes") or "").strip())
    if str(row.get("cash_flow_notes") or "").strip():
        notes_parts.append("Cash Flow Notes: " + str(row.get("cash_flow_notes") or "").strip())
    notes_parts.append(f"Converted from Project Idea #{int(pipeline_item_id)}.")

    target_timing = str(row.get("target_timing") or "No Timeframe Yet").strip() or "No Timeframe Yet"
    rough_hours = float(row.get("rough_labor_hours") or 0)
    rough_budget = float(row.get("rough_budget") or 0)
    entry_date = datetime.now().date()
    try:
        start_date, end_date = calculate_budget_dates(entry_date, target_timing)
    except Exception:
        start_date, end_date = None, None

    data = {
        "entry_date": entry_date,
        "portfolio_name": "General Portfolio",
        "property_name": property_name,
        "address": address,
        "unit_number": "",
        "location_identifier": address,
        "work_item_name": work_item,
        "category_name": category,
        "scope_description": scope,
        "notes": "\n\n".join(notes_parts),
        "materials_notes": "",
        "scope_complete": bool(scope),
        "ai_estimated_hours": rough_hours if rough_hours > 0 else None,
        "user_estimated_hours": rough_hours if rough_hours > 0 else None,
        "labor_budget": rough_budget,
        "materials_budget": 0,
        "budget_timeframe": target_timing,
        "budget_start_date": start_date,
        "budget_end_date": end_date,
        "budget_status": "Active",
        "info_status": "Open",
        "project_id": int(row.get("project_id") or 0) or None,
        "contractor_id": None,
        "contractor_priority": project_idea_priority_to_contractor_priority(row.get("priority")),
        "owner_intent": "Quote Only",
        "save_to_master": False,
        "save_work_item_to_master": False,
    }
    rmr_id = create_rmr_record(data)
    if not rmr_id:
        return None

    execute(
        """
        INSERT INTO renovation_master_record_files (
            rmr_id, file_filename, content_type, storage_mode, blob_url, blob_name,
            file_bytes, sort_order, uploaded_by, created_at
        )
        SELECT ?, file_filename, content_type, storage_mode, blob_url, blob_name,
               file_bytes, sort_order, uploaded_by, NOW()
        FROM renovation_pipeline_files
        WHERE pipeline_item_id = ?
        """,
        (int(rmr_id), int(pipeline_item_id)),
    )
    execute(
        """
        UPDATE renovation_pipeline_items
        SET promoted_rmr_id = ?, status = 'Converted To RMR', modified_at = NOW()
        WHERE id = ?
        """,
        (int(rmr_id), int(pipeline_item_id)),
    )
    add_rmr_history(int(rmr_id), "Converted From Project Idea", f"Created from Project Idea #{int(pipeline_item_id)}.")
    return int(rmr_id)


@st.cache_data(show_spinner=False, ttl=300)
def renovation_pipeline_cash_flows_df(pipeline_item_id: int | None = None, include_deleted_items: bool = False) -> pd.DataFrame:
    query = """
        SELECT
            rpcf.id,
            rpcf.pipeline_item_id,
            COALESCE(rpi.project_name, '') AS project_name,
            COALESCE(rpi.category_name, '') AS category_name,
            COALESCE(rpi.work_group_name, '') AS work_group_name,
            COALESCE(rpi.work_item_name, '') AS work_item_name,
            COALESCE(rpi.priority, '') AS priority,
            COALESCE(rpi.status, '') AS pipeline_status,
            rpcf.scheduled_date,
            COALESCE(rpcf.amount, 0) AS amount,
            COALESCE(rpcf.payment_type, 'Planned') AS payment_type,
            COALESCE(rpcf.status, 'Draft') AS status,
            COALESCE(rpcf.notes, '') AS notes,
            rpcf.created_at,
            rpcf.modified_at
        FROM renovation_pipeline_cash_flows rpcf
        JOIN renovation_pipeline_items rpi ON rpi.id = rpcf.pipeline_item_id
        WHERE 1 = 1
    """
    params = []
    if pipeline_item_id:
        query += " AND rpcf.pipeline_item_id = ?"
        params.append(int(pipeline_item_id))
    if not include_deleted_items:
        query += " AND COALESCE(rpi.deleted, FALSE) = FALSE"
    query += " ORDER BY rpcf.scheduled_date, rpcf.id"
    return fetch_df(query, tuple(params))


def save_pipeline_cash_flow_rows(pipeline_item_id: int, rows: list[dict]):
    if not pipeline_item_id or not rows:
        return
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            for row in rows:
                amount = float(row.get("amount") or 0)
                scheduled_date = row.get("scheduled_date")
                if amount <= 0 or not scheduled_date:
                    continue
                cur.execute(
                    """
                    INSERT INTO renovation_pipeline_cash_flows (
                        pipeline_item_id, scheduled_date, amount, payment_type, status, notes, created_at, modified_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """,
                    (
                        int(pipeline_item_id),
                        scheduled_date,
                        amount,
                        str(row.get("payment_type") or "Planned"),
                        str(row.get("status") or "Draft"),
                        str(row.get("notes") or ""),
                    ),
                )
        conn.commit()
    st.cache_data.clear()


def delete_pipeline_cash_flow_row(cash_flow_id: int):
    execute("DELETE FROM renovation_pipeline_cash_flows WHERE id = ?", (cash_flow_id,))


def build_cash_flow_rows_from_pattern(start_date, total_amount: float, pattern: str, number_of_payments: int, notes: str = "") -> list[dict]:
    import datetime as _dt
    rows = []
    total_amount = float(total_amount or 0)
    number_of_payments = int(number_of_payments or 1)
    if total_amount <= 0 or not start_date:
        return rows

    if pattern == "One Payment":
        rows.append({
            "scheduled_date": start_date,
            "amount": total_amount,
            "payment_type": "One Payment",
            "status": "Draft",
            "notes": notes,
        })
    elif pattern in ("Weekly Payments", "Every 2 Weeks"):
        spacing_days = 7 if pattern == "Weekly Payments" else 14
        amount_each = round(total_amount / max(number_of_payments, 1), 2)
        running_total = 0.0
        for i in range(max(number_of_payments, 1)):
            amount = amount_each
            if i == number_of_payments - 1:
                amount = round(total_amount - running_total, 2)
            rows.append({
                "scheduled_date": start_date + _dt.timedelta(days=spacing_days * i),
                "amount": amount,
                "payment_type": pattern,
                "status": "Draft",
                "notes": notes,
            })
            running_total += amount
    elif pattern == "Deposit / Completion":
        rows.append({
            "scheduled_date": start_date,
            "amount": round(total_amount * 0.5, 2),
            "payment_type": "Deposit",
            "status": "Draft",
            "notes": notes,
        })
        rows.append({
            "scheduled_date": start_date + _dt.timedelta(days=14),
            "amount": round(total_amount * 0.5, 2),
            "payment_type": "Completion",
            "status": "Draft",
            "notes": notes,
        })
    return rows


def pipeline_file_bytes(row) -> bytes | None:
    file_bytes = row.get("file_bytes")
    if file_bytes is not None and not isinstance(file_bytes, (bytes, bytearray)):
        try:
            file_bytes = bytes(file_bytes)
        except Exception:
            file_bytes = None
    if file_bytes:
        return file_bytes
    blob_name = str(row.get("blob_name") or "")
    if blob_name:
        return cached_download_blob_bytes(blob_name)
    return None


@st.cache_data(show_spinner=False, ttl=300)
def get_contractor_names():
    df = fetch_df("SELECT name FROM contractors WHERE COALESCE(active, TRUE) = TRUE ORDER BY LOWER(name)")
    return df["name"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=300)
def get_contractor_id_by_name(name: str | None):
    if not name or name == "None selected":
        return None
    df = fetch_df("SELECT id FROM contractors WHERE name = ? LIMIT 1", (name,))
    if df.empty:
        return None
    return int(df.iloc[0]["id"])


# -----------------------------
# Catalog sync helpers
# -----------------------------
def find_task_catalog_file():
    for path in TASK_CATALOG_CANDIDATES:
        if path.exists():
            return path
    return None


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_catalog_row(task_name, trade_name):
    task_name = normalize_text(task_name)
    trade_name = normalize_text(trade_name)
    return task_name, trade_name


def sync_task_catalog_from_excel_if_needed():
    catalog_path = find_task_catalog_file()
    if not catalog_path:
        return

    existing_tasks = fetch_df("SELECT COUNT(*) AS cnt FROM tasks")
    existing_count = int(existing_tasks.iloc[0]["cnt"]) if not existing_tasks.empty else 0
    if existing_count > 0:
        return

    df = pd.read_excel(catalog_path)
    col_map = {str(c).strip().lower(): c for c in df.columns}

    required = {"task name", "trade"}
    if not required.issubset(set(col_map.keys())):
        raise ValueError("Work item catalog file must contain columns: Task Name and Trade")

    df = df.rename(
        columns={
            col_map["task name"]: "task_name",
            col_map["trade"]: "trade",
        }
    )[["task_name", "trade"]].copy()

    cleaned_rows = []
    for _, row in df.iterrows():
        task_name, trade_name = normalize_catalog_row(row["task_name"], row["trade"])
        if not task_name or not trade_name:
            continue
        cleaned_rows.append((trade_name, task_name))

    if not cleaned_rows:
        return

    seen_trades = set()
    ordered_trades = []
    seen_task_keys = set()
    ordered_tasks = []

    for trade_name, task_name in cleaned_rows:
        if trade_name not in seen_trades:
            ordered_trades.append(trade_name)
            seen_trades.add(trade_name)
        key = (trade_name, task_name)
        if key not in seen_task_keys:
            ordered_tasks.append(key)
            seen_task_keys.add(key)

    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            for trade_name in ordered_trades:
                cur.execute(
                    "INSERT INTO trades (name, active, created_at, modified_at) VALUES (%s, TRUE, NOW(), NOW()) ON CONFLICT (name) DO NOTHING",
                    (trade_name,),
                )

            trades_df = fetch_df("SELECT id, name FROM trades")
            trade_map = dict(zip(trades_df["name"], trades_df["id"]))

            for trade_name, task_name in ordered_tasks:
                cur.execute(
                    """
                    INSERT INTO tasks (trade_id, name, active, notes, task_level, default_scope_mode, default_quantity, allow_quantity_edit, created_at, modified_at)
                    VALUES (%s, %s, TRUE, '', 'base', 'standalone', 1, TRUE, NOW(), NOW())
                    ON CONFLICT (trade_id, name) DO NOTHING
                    """,
                    (int(trade_map[trade_name]), task_name),
                )
        conn.commit()

    set_meta("task_catalog_mtime", str(catalog_path.stat().st_mtime_ns))
    set_meta("task_catalog_path", str(catalog_path))



# -----------------------------
# Renovation Master Record (RMR) helpers
# -----------------------------
RMR_INFO_STATUS_OPTIONS = ["Open", "Work Completed", "Paid", "Closed"]
RMR_BUDGET_TIMEFRAME_OPTIONS = [
    "No Timeframe Yet",
    "Next 30 Days",
    "30-60 Days",
    "60-90 Days",
    "90-180 Days",
    "By End Of Year",
    "Next Year",
    "Future / Undetermined",
    "Custom Dates",
]
RMR_BUDGET_STATUS_OPTIONS = ["Active", "Deferred", "Cancelled"]

CONTRACTOR_PRIORITY_OPTIONS = [
    "1 - ASAP",
    "2 - Planning",
    "3 - Quote Only",
]
CONTRACTOR_PRIORITY_DEFINITIONS = {
    "1 - ASAP": "Need quote, materials list, comments, and availability ASAP so we can move forward.",
    "2 - Planning": "Need quote and materials list; likely project but not immediate.",
    "3 - Quote Only": "Quote only for evaluation/budgeting; do not assume the work is approved yet.",
}
OWNER_INTENT_OPTIONS = [
    "Quote Only",
    "Quote + Materials",
    "Ready To Schedule",
    "Approved To Proceed",
    "In Progress",
    "Complete",
]

def contractor_priority_sort_value(value) -> int:
    text = str(value or "3 - Quote Only").strip()
    if text.startswith("1"):
        return 1
    if text.startswith("2"):
        return 2
    return 3

def render_contractor_priority_legend():
    st.markdown("**Priority Legend**")
    for priority, definition in CONTRACTOR_PRIORITY_DEFINITIONS.items():
        st.caption(f"{priority}: {definition}")


def format_money(value) -> str:
    try:
        return f"${float(value or 0):,.2f}"
    except Exception:
        return "$0.00"


@st.cache_data(show_spinner=False, ttl=300)
def rmr_work_item_options_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            t.id,
            COALESCE(t.name, '') AS work_item_name,
            COALESCE(tr.name, '') AS category_name,
            COALESCE(t.active, TRUE) AS active
        FROM tasks t
        LEFT JOIN trades tr ON tr.id = t.trade_id
        WHERE COALESCE(t.active, TRUE) = TRUE
        ORDER BY LOWER(COALESCE(t.name, '')), LOWER(COALESCE(tr.name, ''))
        """
    )


def rmr_work_item_labels() -> list[str]:
    df = rmr_work_item_options_df()
    if df.empty:
        return ["Add New Work Item"]
    labels = ["Add New Work Item"]
    for row in df.itertuples():
        category = str(row.category_name or "").strip()
        suffix = f" | {category}" if category else ""
        labels.append(f"{row.work_item_name}{suffix}")
    return labels


def parse_rmr_work_item_label(label: str) -> tuple[str, str]:
    label = str(label or "").strip()
    if not label or label == "Add New Work Item":
        return "", ""
    if " | " in label:
        work_item, category = label.split(" | ", 1)
        return work_item.strip(), category.strip()
    return label, ""


@st.cache_data(show_spinner=False, ttl=300)
def rmr_default_scope_for_work_item(work_item_name: str, category_name: str = "") -> str:
    work_item_name = str(work_item_name or "").strip()
    category_name = str(category_name or "").strip()
    if not work_item_name:
        return ""

    query = """
        SELECT COALESCE(st.scope_description, '') AS scope_description
        FROM scope_templates st
        JOIN tasks t ON t.id = st.task_id
        LEFT JOIN trades tr ON tr.id = t.trade_id
        WHERE COALESCE(st.active, TRUE) = TRUE
          AND LOWER(TRIM(COALESCE(t.name, ''))) = LOWER(TRIM(?))
    """
    params = [work_item_name]
    if category_name:
        query += " AND LOWER(TRIM(COALESCE(tr.name, ''))) = LOWER(TRIM(?))"
        params.append(category_name)
    query += """
        ORDER BY
            CASE LOWER(COALESCE(st.template_type, '')) WHEN 'detailed' THEN 1 ELSE 2 END,
            st.id
        LIMIT 1
    """
    df = fetch_df(query, tuple(params))
    if df.empty:
        return ""
    return str(df.iloc[0].get("scope_description") or "").strip()


def ensure_portfolio_property_exists(portfolio_name: str, property_name: str) -> int | None:
    portfolio_name = str(portfolio_name or "").strip()
    property_name = str(property_name or "").strip()
    if not property_name:
        return None
    if not portfolio_name:
        portfolio_name = "General Portfolio"

    existing = fetch_df(
        """
        SELECT id
        FROM portfolio_properties
        WHERE COALESCE(deleted, FALSE) = FALSE
          AND LOWER(TRIM(COALESCE(portfolio_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
        LIMIT 1
        """,
        (portfolio_name, property_name),
    )
    if not existing.empty:
        return int(existing.iloc[0]["id"])

    new_id = execute_returning_id(
        """
        INSERT INTO portfolio_properties (portfolio_name, property_name, notes, active, deleted, created_at, modified_at)
        VALUES (?, ?, '', TRUE, FALSE, NOW(), NOW())
        """,
        (portfolio_name, property_name),
    )
    return int(new_id) if new_id else None


def ensure_portfolio_address_exists(portfolio_name: str, property_name: str, address: str, unit_number: str = ""):
    portfolio_name = str(portfolio_name or "").strip()
    property_name = str(property_name or "").strip()
    address = str(address or "").strip()
    unit_number = str(unit_number or "").strip()
    if not property_name or not address:
        return

    property_id = ensure_portfolio_property_exists(portfolio_name, property_name)
    if not property_id:
        return

    existing = fetch_df(
        """
        SELECT id
        FROM portfolio_addresses
        WHERE COALESCE(deleted, FALSE) = FALSE
          AND LOWER(TRIM(COALESCE(portfolio_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(address, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(unit_number, ''))) = LOWER(TRIM(?))
        LIMIT 1
        """,
        (portfolio_name or "General Portfolio", property_name, address, unit_number),
    )
    if existing.empty:
        execute(
            """
            INSERT INTO portfolio_addresses (
                portfolio_property_id, portfolio_name, property_name, address, unit_number,
                notes, active, deleted, created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, '', TRUE, FALSE, NOW(), NOW())
            """,
            (property_id, portfolio_name or "General Portfolio", property_name, address, unit_number),
        )




def ensure_category_exists(category_name: str) -> int | None:
    category_name = str(category_name or "").strip()
    if not category_name:
        return None
    existing = fetch_df(
        """
        SELECT id FROM trades
        WHERE LOWER(TRIM(COALESCE(name, ''))) = LOWER(TRIM(?))
        LIMIT 1
        """,
        (category_name,),
    )
    if not existing.empty:
        return int(existing.iloc[0]["id"])
    new_id = execute_returning_id(
        """
        INSERT INTO trades (name, active, created_at, modified_at)
        VALUES (?, TRUE, NOW(), NOW())
        """,
        (category_name,),
    )
    return int(new_id) if new_id else None


def ensure_work_item_exists(work_item_name: str, category_name: str, scope_description: str = "") -> int | None:
    work_item_name = str(work_item_name or "").strip()
    category_name = str(category_name or "").strip()
    scope_description = str(scope_description or "").strip()
    if not work_item_name:
        return None
    trade_id = ensure_category_exists(category_name) if category_name else None
    if not trade_id:
        return None

    existing = fetch_df(
        """
        SELECT t.id
        FROM tasks t
        WHERE COALESCE(t.trade_id, 0) = ?
          AND LOWER(TRIM(COALESCE(t.name, ''))) = LOWER(TRIM(?))
        LIMIT 1
        """,
        (int(trade_id), work_item_name),
    )
    if not existing.empty:
        task_id = int(existing.iloc[0]["id"])
    else:
        task_id = execute_returning_id(
            """
            INSERT INTO tasks (
                trade_id, name, active, notes, task_level, default_scope_mode,
                default_quantity, allow_quantity_edit, created_at, modified_at
            ) VALUES (?, ?, TRUE, '', 'base', 'standalone', 1, TRUE, NOW(), NOW())
            """,
            (int(trade_id), work_item_name),
        )
        if task_id:
            execute(
                "UPDATE tasks SET work_item_code = 'WI-' || LPAD(id::text, 6, '0') WHERE id = ? AND COALESCE(work_item_code, '') = ''",
                (int(task_id),),
            )
    if task_id and scope_description:
        existing_scope = fetch_df(
            """
            SELECT id FROM scope_templates
            WHERE task_id = ?
              AND LOWER(TRIM(COALESCE(scope_description, ''))) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (int(task_id), scope_description),
        )
        if existing_scope.empty:
            execute(
                """
                INSERT INTO scope_templates (
                    task_id, scope_name, scope_description, template_type, active, created_at, modified_at
                ) VALUES (?, ?, ?, 'detailed', TRUE, NOW(), NOW())
                """,
                (int(task_id), f"Default - {work_item_name}", scope_description),
            )
    return int(task_id) if task_id else None


@st.cache_data(show_spinner=False, ttl=300)
def rmr_groups_df(include_deleted: bool = False, property_name: str = "") -> pd.DataFrame:
    query = """
        SELECT
            g.id,
            COALESCE(g.group_name, '') AS group_name,
            COALESCE(g.property_name, '') AS property_name,
            COALESCE(g.project_id, 0) AS project_id,
            COALESCE(p.project_name, '') AS project_name,
            COALESCE(g.notes, '') AS notes,
            COALESCE(g.deleted, FALSE) AS deleted,
            COALESCE(g.created_by, '') AS created_by,
            g.created_at,
            g.modified_at,
            (SELECT COUNT(*) FROM rmr_group_members gm WHERE gm.rmr_group_id = g.id) AS rmr_count
        FROM rmr_groups g
        LEFT JOIN project_registry p ON p.id = g.project_id
        WHERE 1 = 1
    """
    params = []
    if not include_deleted:
        query += " AND COALESCE(g.deleted, FALSE) = FALSE"
    if property_name and property_name != "All":
        query += " AND COALESCE(g.property_name, '') = ?"
        params.append(str(property_name))
    query += " ORDER BY LOWER(g.property_name), LOWER(g.group_name), g.id"
    return fetch_df(query, tuple(params))


def create_rmr_group(group_name: str, property_name: str = "", notes: str = "") -> int | None:
    group_name = str(group_name or "").strip()
    property_name = str(property_name or "").strip()
    if not group_name:
        return None
    existing = fetch_df(
        """
        SELECT id FROM rmr_groups
        WHERE COALESCE(deleted, FALSE) = FALSE
          AND LOWER(TRIM(COALESCE(group_name, ''))) = LOWER(TRIM(?))
          AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
        LIMIT 1
        """,
        (group_name, property_name),
    )
    if not existing.empty:
        return int(existing.iloc[0]["id"])
    new_id = execute_returning_id(
        """
        INSERT INTO rmr_groups (group_name, property_name, notes, created_by, created_at, modified_at)
        VALUES (?, ?, ?, ?, NOW(), NOW())
        """,
        (group_name, property_name, str(notes or "").strip(), str(st.session_state.get("logged_in_user", "") or "")),
    )
    return int(new_id) if new_id else None


def assign_rmrs_to_rmr_group(rmr_ids: list[int], rmr_group_id: int):
    if not rmr_ids or not rmr_group_id:
        return
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            for rmr_id in rmr_ids:
                cur.execute(
                    """
                    INSERT INTO rmr_group_members (rmr_group_id, rmr_id, created_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (rmr_group_id, rmr_id) DO NOTHING
                    """,
                    (int(rmr_group_id), int(rmr_id)),
                )
        conn.commit()
    st.cache_data.clear()
    for rmr_id in rmr_ids:
        add_rmr_history(int(rmr_id), "Assigned To Group", f"Assigned to RMR group ID {int(rmr_group_id)}.")


def rmr_group_labels(property_name: str = "") -> list[str]:
    df = rmr_groups_df(property_name=property_name)
    if df.empty:
        return []
    return [f"{int(row.id)} | {row.property_name} | {row.group_name} ({int(row.rmr_count)} RMRs)" for row in df.itertuples()]


def find_or_create_project_simple(project_name: str, project_address: str = "", notes: str = "") -> int | None:
    project_name = str(project_name or "").strip()
    if not project_name:
        return None
    existing = fetch_df(
        """
        SELECT id FROM project_registry
        WHERE COALESCE(deleted, FALSE) = FALSE
          AND LOWER(TRIM(COALESCE(project_name, ''))) = LOWER(TRIM(?))
        LIMIT 1
        """,
        (project_name,),
    )
    if not existing.empty:
        return int(existing.iloc[0]["id"])
    new_id = execute_returning_id(
        """
        INSERT INTO project_registry (project_name, project_address, active, notes, activated_at, created_at, modified_at)
        VALUES (?, ?, TRUE, ?, NOW(), NOW(), NOW())
        """,
        (project_name, str(project_address or "").strip(), str(notes or "").strip()),
    )
    if new_id:
        execute(
            "UPDATE project_registry SET project_code = 'PRJ-' || LPAD(id::text, 6, '0') WHERE id = ? AND COALESCE(project_code, '') = ''",
            (int(new_id),),
        )
        st.cache_data.clear()
        return int(new_id)
    return None


def create_work_group_from_rmrs(
    rmr_ids: list[int],
    project_id: int,
    work_group_name: str,
    contractor_id: int | None = None,
    due_date=None,
    status: str = "Open",
    notes: str = "",
    copy_photos: bool = True,
) -> int | None:
    if not rmr_ids or not project_id:
        return None
    selected = rmr_records_df(search_text="", property_name="All", status="All")
    selected = selected[selected["id"].astype(int).isin([int(x) for x in rmr_ids])].copy()
    if selected.empty:
        return None

    work_group_name = str(work_group_name or "").strip()
    if not work_group_name:
        prop = str(selected["property_name"].mode().iloc[0]) if not selected["property_name"].mode().empty else ""
        item = str(selected["work_item_name"].mode().iloc[0]) if not selected["work_item_name"].mode().empty else "Work"
        work_group_name = f"{prop} {item}".strip()

    task_name = str(selected["work_item_name"].mode().iloc[0]) if not selected["work_item_name"].mode().empty else work_group_name
    category_name = str(selected["category_name"].mode().iloc[0]) if not selected["category_name"].mode().empty else ""
    property_name = str(selected["property_name"].mode().iloc[0]) if not selected["property_name"].mode().empty else ""
    address = str(selected["address"].mode().iloc[0]) if not selected["address"].mode().empty else ""
    unit_number = "; ".join([x for x in selected["location_identifier"].astype(str).tolist() if x.strip()][:8])
    scopes = []
    for row in selected.itertuples():
        code = str(getattr(row, "rmr_code", "") or "").strip()
        loc = str(getattr(row, "location_identifier", "") or "").strip()
        scope = str(getattr(row, "scope_description", "") or "").strip()
        line = f"{code}"
        if loc:
            line += f" - {loc}"
        if scope:
            line += f": {scope}"
        scopes.append(line)
    scope_description = "\n".join(scopes)
    if notes:
        scope_description = (scope_description + "\n\nOwner Notes: " + str(notes).strip()).strip()

    estimated_price = float(pd.to_numeric(selected.get("labor_budget", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    contractor_priority = "3 - Quote Only"
    owner_intent = "Quote Only"
    if "contractor_priority" in selected.columns and not selected.empty:
        priority_values = selected["contractor_priority"].fillna("3 - Quote Only").astype(str).tolist()
        priority_values = sorted(priority_values, key=contractor_priority_sort_value)
        contractor_priority = priority_values[0] if priority_values else "3 - Quote Only"
    if "owner_intent" in selected.columns and not selected.empty:
        intents = [str(x).strip() for x in selected["owner_intent"].fillna("Quote Only").astype(str).tolist() if str(x).strip()]
        owner_intent = intents[0] if intents else "Quote Only"

    work_group_id = execute_returning_id(
        """
        INSERT INTO work_groups (
            project_id, estimate_line_id, work_group_name, category_name, task_name, trade_name,
            scope_description, contractor_id, agreed_price, estimated_price, contractor_requested_price,
            amount_to_be_paid, due_date, status, notes, work_group_address, work_group_unit_number, contractor_priority, owner_intent, created_at, modified_at
        ) VALUES (?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        """,
        (
            int(project_id),
            work_group_name,
            category_name,
            task_name,
            category_name,
            scope_description,
            int(contractor_id) if contractor_id else None,
            estimated_price,
            due_date,
            status or "Open",
            str(notes or "").strip(),
            address,
            unit_number,
            contractor_priority,
            owner_intent,
        ),
    )
    if not work_group_id:
        return None
    work_group_id = int(work_group_id)
    set_order_number("work_groups", work_group_id, "WG")

    placeholders = ",".join(["%s"] * len(rmr_ids))
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE renovation_master_records SET work_group_id = %s, project_id = %s, contractor_id = COALESCE(contractor_id, %s), modified_at = NOW() WHERE id IN ({placeholders})",
                tuple([work_group_id, int(project_id), int(contractor_id) if contractor_id else None] + [int(x) for x in rmr_ids]),
            )
            if copy_photos:
                cur.execute(
                    f"""
                    INSERT INTO work_group_photos (work_group_id, photo_filename, content_type, storage_mode, blob_url, blob_name, photo_bytes, sort_order, uploaded_by, created_at)
                    SELECT %s, file_filename, content_type, storage_mode, blob_url, blob_name, file_bytes, sort_order, uploaded_by, NOW()
                    FROM renovation_master_record_files
                    WHERE rmr_id IN ({placeholders})
                    """,
                    tuple([work_group_id] + [int(x) for x in rmr_ids]),
                )
        conn.commit()
    st.cache_data.clear()
    for rmr_id in rmr_ids:
        add_rmr_history(int(rmr_id), "Sent To Contractor Work Group", f"Created/assigned to contractor work group WG{work_group_id}.")
    return work_group_id


@st.cache_data(show_spinner=False, ttl=300)
def rmr_records_df(include_deleted: bool = False, search_text: str = "", property_name: str = "", status: str = "") -> pd.DataFrame:
    query = """
        SELECT
            r.id,
            COALESCE(r.rmr_code, 'RMR-' || LPAD(r.id::text, 6, '0')) AS rmr_code,
            r.entry_date,
            COALESCE(r.portfolio_name, '') AS portfolio_name,
            COALESCE(r.property_name, '') AS property_name,
            COALESCE(r.address, '') AS address,
            COALESCE(r.unit_number, '') AS unit_number,
            COALESCE(r.location_identifier, '') AS location_identifier,
            COALESCE(r.work_item_name, '') AS work_item_name,
            COALESCE(r.category_name, '') AS category_name,
            COALESCE(r.scope_description, '') AS scope_description,
            COALESCE(r.notes, '') AS notes,
            COALESCE(r.materials_notes, '') AS materials_notes,
            COALESCE(r.scope_complete, FALSE) AS scope_complete,
            COALESCE(r.ai_estimated_hours, 0) AS ai_estimated_hours,
            COALESCE(r.user_estimated_hours, 0) AS user_estimated_hours,
            COALESCE(r.labor_budget, 0) AS labor_budget,
            COALESCE(r.materials_budget, 0) AS materials_budget,
            COALESCE(r.budget_timeframe, 'No Timeframe Yet') AS budget_timeframe,
            r.budget_start_date,
            r.budget_end_date,
            COALESCE(r.budget_status, 'Active') AS budget_status,
            COALESCE(r.info_status, 'Open') AS info_status,
            COALESCE(r.project_id, 0) AS project_id,
            COALESCE(p.project_name, '') AS project_name,
            COALESCE(r.work_group_id, 0) AS work_group_id,
            COALESCE(wg.work_group_name, '') AS linked_work_group_name,
            COALESCE(r.contractor_id, 0) AS contractor_id,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(r.deleted, FALSE) AS deleted,
            COALESCE(r.cashflow_export_status, 'Not Exported') AS cashflow_export_status,
            r.cashflow_last_exported_at,
            COALESCE(r.cashflow_export_signature, '') AS cashflow_export_signature,
            COALESCE(r.contractor_priority, '3 - Quote Only') AS contractor_priority,
            COALESCE(r.owner_intent, 'Quote Only') AS owner_intent,
            COALESCE(r.created_by, '') AS created_by,
            r.created_at,
            r.modified_at,
            (SELECT COUNT(*) FROM renovation_master_record_files f WHERE f.rmr_id = r.id) AS photo_count,
            (SELECT COUNT(*) FROM rmr_communications rc WHERE rc.rmr_id = r.id AND COALESCE(rc.is_unread_for_owner, FALSE) = TRUE) AS unread_contractor_notes,
            (SELECT MAX(rc.created_at) FROM rmr_communications rc WHERE rc.rmr_id = r.id) AS last_contractor_update
        FROM renovation_master_records r
        LEFT JOIN project_registry p ON p.id = r.project_id
        LEFT JOIN work_groups wg ON wg.id = r.work_group_id
        LEFT JOIN contractors c ON c.id = r.contractor_id
        WHERE 1 = 1
    """
    params = []
    if not include_deleted:
        query += " AND COALESCE(r.deleted, FALSE) = FALSE"
    if property_name and property_name != "All":
        query += " AND COALESCE(r.property_name, '') = ?"
        params.append(str(property_name))
    if status and status != "All":
        query += " AND COALESCE(r.info_status, 'Open') = ?"
        params.append(str(status))
    if search_text:
        query += """
            AND (
                COALESCE(r.rmr_code, '') ILIKE ? OR COALESCE(r.property_name, '') ILIKE ? OR
                COALESCE(r.address, '') ILIKE ? OR COALESCE(r.unit_number, '') ILIKE ? OR
                COALESCE(r.location_identifier, '') ILIKE ? OR COALESCE(r.work_item_name, '') ILIKE ? OR
                COALESCE(r.category_name, '') ILIKE ? OR COALESCE(r.scope_description, '') ILIKE ? OR
                COALESCE(r.notes, '') ILIKE ? OR COALESCE(r.materials_notes, '') ILIKE ? OR
                COALESCE(r.contractor_priority, '') ILIKE ? OR COALESCE(r.owner_intent, '') ILIKE ?
            )
        """
        like_value = f"%{str(search_text).strip()}%"
        params.extend([like_value] * 12)
    query += " ORDER BY r.modified_at DESC, r.id DESC"
    return fetch_df(query, tuple(params))


def calculate_budget_dates(base_date, timeframe: str, custom_start=None, custom_end=None):
    import datetime as _dt
    try:
        base = pd.to_datetime(base_date).date()
    except Exception:
        base = datetime.now().date()

    timeframe = str(timeframe or "No Timeframe Yet")
    if timeframe == "Next 30 Days":
        return base, base + _dt.timedelta(days=30)
    if timeframe == "30-60 Days":
        return base + _dt.timedelta(days=30), base + _dt.timedelta(days=60)
    if timeframe == "60-90 Days":
        return base + _dt.timedelta(days=60), base + _dt.timedelta(days=90)
    if timeframe in ["90-120 Days", "90-180 Days"]:
        end_days = 180 if timeframe == "90-180 Days" else 120
        return base + _dt.timedelta(days=90), base + _dt.timedelta(days=end_days)
    if timeframe == "By End Of Year":
        return base, _dt.date(base.year, 12, 31)
    if timeframe == "Next Year":
        return _dt.date(base.year + 1, 1, 1), _dt.date(base.year + 1, 12, 31)
    if timeframe in ["Next 12 Months"]:
        return base, base + _dt.timedelta(days=365)
    if timeframe == "Custom Dates":
        return custom_start, custom_end
    return None, None


def update_rmr_budget_fields(rmr_id: int, labor_budget: float, materials_budget: float, budget_timeframe: str, budget_start_date, budget_end_date, budget_status: str):
    execute(
        """
        UPDATE renovation_master_records
        SET labor_budget = ?, materials_budget = ?, budget_timeframe = ?, budget_start_date = ?, budget_end_date = ?,
            budget_status = ?, modified_at = NOW()
        WHERE id = ?
        """,
        (
            float(labor_budget or 0),
            float(materials_budget or 0),
            str(budget_timeframe or "No Timeframe Yet"),
            budget_start_date,
            budget_end_date,
            str(budget_status or "Active"),
            int(rmr_id),
        ),
    )
    add_rmr_history(int(rmr_id), "Budget Updated", f"Budget set to {budget_timeframe}; status {budget_status}.")


def budget_summary_table(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_cols + ["RMR Count", "Labor Budget", "Materials Budget", "Total Budget"])
    work = df.copy()
    work["labor_budget"] = pd.to_numeric(work.get("labor_budget", 0), errors="coerce").fillna(0)
    work["materials_budget"] = pd.to_numeric(work.get("materials_budget", 0), errors="coerce").fillna(0)
    for col in group_cols:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str).replace("", "Unassigned")
    summary = work.groupby(group_cols, dropna=False).agg(
        **{
            "RMR Count": ("id", "count"),
            "Labor Budget": ("labor_budget", "sum"),
            "Materials Budget": ("materials_budget", "sum"),
        }
    ).reset_index()
    summary["Total Budget"] = summary["Labor Budget"] + summary["Materials Budget"]
    return summary.sort_values(group_cols).reset_index(drop=True)


# -----------------------------
# Build 15B: Budget Planner -> Cash Flow Cloud export bridge
# -----------------------------
CASH_FLOW_COLUMNS = [
    "Type", "Company Name", "Department", "Project", "Category", "Work Group ID", "Work Item",
    "Vendor/Source", "Description", "Amount", "Date", "Spread Method", "Start Date", "End Date",
]
CASH_FLOW_SOURCE_COLUMNS = ["Source Type", "Source ID", "Source Record", "Source App", "Last Exported At"]


def quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def get_cash_flow_pg_conninfo() -> str:
    """Return the Cash Flow Cloud database URL from Renovation app secrets.

    Add one of these to the Renovation Management Streamlit secrets:
      cash_flow_database_url = "postgresql://..."
      CASH_FLOW_DATABASE_URL = "postgresql://..."

    This is intentionally separate from the Renovation database URL so the two apps can remain separate.
    """
    for key in ["cash_flow_database_url", "CASH_FLOW_DATABASE_URL", "cashflow_database_url", "CASHFLOW_DATABASE_URL"]:
        try:
            if key in st.secrets and str(st.secrets[key]).strip():
                return str(st.secrets[key]).strip()
        except Exception:
            pass
    try:
        if "cash_flow" in st.secrets:
            cfg = st.secrets["cash_flow"]
            for key in ["database_url", "url", "DATABASE_URL"]:
                if key in cfg and str(cfg[key]).strip():
                    return str(cfg[key]).strip()
        if "cashflow" in st.secrets:
            cfg = st.secrets["cashflow"]
            for key in ["database_url", "url", "DATABASE_URL"]:
                if key in cfg and str(cfg[key]).strip():
                    return str(cfg[key]).strip()
    except Exception:
        pass
    return ""


def cash_flow_connection_available() -> bool:
    return bool(get_cash_flow_pg_conninfo())


def get_cash_flow_conn():
    conninfo = get_cash_flow_pg_conninfo()
    if not conninfo:
        raise RuntimeError("Cash Flow Cloud database URL is missing. Add cash_flow_database_url to the Renovation Management Streamlit secrets.")
    return psycopg.connect(conninfo)


def ensure_cash_flow_export_table():
    with closing(get_cash_flow_conn()) as conn:
        with conn.cursor() as cur:
            base_cols = ", ".join([f"{quote_identifier(col)} TEXT" for col in CASH_FLOW_COLUMNS])
            cur.execute(f"CREATE TABLE IF NOT EXISTS {quote_identifier('cashflow')} ({base_cols});")
            for col in CASH_FLOW_COLUMNS + CASH_FLOW_SOURCE_COLUMNS:
                cur.execute(f"ALTER TABLE {quote_identifier('cashflow')} ADD COLUMN IF NOT EXISTS {quote_identifier(col)} TEXT;")
        conn.commit()


def normalize_export_date(value):
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def money_float(value) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0])


def build_export_signature_from_values(*values) -> str:
    import hashlib
    payload = json.dumps([str(v) for v in values], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def rmr_export_signature(row) -> str:
    return build_export_signature_from_values(
        row.get("RMR ID") or row.get("rmr_code"),
        row.get("Property") or row.get("property_name"),
        row.get("Project") or row.get("project_name"),
        row.get("Work Group") or row.get("linked_work_group_name"),
        row.get("Work Item") or row.get("work_item_name"),
        row.get("Budget Timeframe") or row.get("budget_timeframe"),
        row.get("Budget Start Date") or row.get("budget_start_date"),
        row.get("Budget End Date") or row.get("budget_end_date"),
        row.get("Budget Status") or row.get("budget_status"),
        row.get("Contractor Priority") or row.get("contractor_priority"),
        row.get("Owner Intent") or row.get("owner_intent"),
        money_float(row.get("Labor Budget") if "Labor Budget" in row else row.get("labor_budget")),
        money_float(row.get("Materials Budget") if "Materials Budget" in row else row.get("materials_budget")),
    )


def display_export_status(raw_status: str, stored_signature: str, current_signature: str) -> str:
    status = str(raw_status or "Not Exported").strip() or "Not Exported"
    if status == "Exported" and stored_signature and current_signature and stored_signature != current_signature:
        return "Updated Since Export"
    return status


def selected_budget_rows_to_cash_flow_rows(rows_df: pd.DataFrame, export_level: str) -> tuple[list[dict], list[dict]]:
    """Return cashflow rows and source update rows.

    export_level:
      - Individual RMRs: each RMR exports as its own forecast source.
      - Work Groups: selected RMRs are grouped by Work Group where available; ungrouped RMRs remain individual.
    """
    if rows_df.empty:
        return [], []
    work = rows_df.copy()
    for col in ["Labor Budget", "Materials Budget"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0.0)
    export_rows = []
    source_updates = []

    def add_source_cashflow(source_type, source_id, source_record, row_for_context, labor_amount, materials_amount, work_item_label, work_group_label=""):
        if labor_amount <= 0 and materials_amount <= 0:
            return
        project_name = str(row_for_context.get("Project") or "").strip() or str(row_for_context.get("Property") or "").strip()
        property_name = str(row_for_context.get("Property") or "").strip()
        contractor = str(row_for_context.get("Contractor") or "").strip()
        contractor_priority = str(row_for_context.get("Contractor Priority") or row_for_context.get("contractor_priority") or "3 - Quote Only").strip()
        owner_intent = str(row_for_context.get("Owner Intent") or row_for_context.get("owner_intent") or "Quote Only").strip()
        start_date = row_for_context.get("Budget Start Date")
        end_date = row_for_context.get("Budget End Date")
        if not normalize_export_date(end_date):
            end_date = row_for_context.get("RMR Date")
        source_record_text = str(source_record or "").strip()
        common = {
            "Type": "Cash Out",
            "Company Name": property_name,
            "Department": "Renovation",
            "Project": project_name,
            "Work Group ID": work_group_label,
            "Work Item": work_item_label,
            "Date": normalize_export_date(end_date),
            "Spread Method": "RMR Budget Export",
            "Start Date": normalize_export_date(start_date),
            "End Date": normalize_export_date(end_date),
            "Source Type": source_type,
            "Source ID": str(source_id),
            "Source Record": source_record_text,
            "Source App": "Renovation Management",
            "Last Exported At": datetime.now().isoformat(timespec="seconds"),
        }
        if labor_amount > 0:
            export_rows.append({
                **common,
                "Category": "Labor",
                "Vendor/Source": contractor or "Renovation Budget Planner",
                "Description": f"{source_record_text} | Labor | {work_item_label} | {contractor_priority} | {owner_intent}",
                "Amount": f"{labor_amount:.2f}",
            })
        if materials_amount > 0:
            export_rows.append({
                **common,
                "Category": "Materials",
                "Vendor/Source": "Renovation Budget Planner",
                "Description": f"{source_record_text} | Materials | {work_item_label} | {contractor_priority} | {owner_intent}",
                "Amount": f"{materials_amount:.2f}",
            })

    if export_level == "Work Groups":
        # Group by work group where possible. Rows with no work group remain individual RMR exports.
        work["_wg"] = work.get("Work Group", "").fillna("").astype(str).str.strip()
        grouped = work[work["_wg"] != ""].copy()
        ungrouped = work[work["_wg"] == ""].copy()
        for wg_name, group in grouped.groupby("_wg", dropna=False):
            first = group.iloc[0]
            labor_amount = pd.to_numeric(group["Labor Budget"], errors="coerce").fillna(0).sum()
            materials_amount = pd.to_numeric(group["Materials Budget"], errors="coerce").fillna(0).sum()
            rmr_ids = [str(x) for x in group.get("RMR ID", pd.Series(dtype=str)).astype(str).tolist() if str(x).strip()]
            source_id = str(first.get("Work Group Internal ID") or wg_name).strip() or wg_name
            source_record = f"WG | {wg_name} | RMRs: {', '.join(rmr_ids[:20])}"
            work_item_label = f"{wg_name} ({len(group)} RMRs)"
            add_source_cashflow("Work Group", source_id, source_record, first, float(labor_amount), float(materials_amount), work_item_label, wg_name)
            sig = build_export_signature_from_values(source_id, wg_name, labor_amount, materials_amount, first.get("Budget Start Date"), first.get("Budget End Date"), ",".join(rmr_ids))
            source_updates.append({"source_type": "Work Group", "source_id": source_id, "signature": sig, "rmr_ids": [int(x) for x in group.get("RMR Internal ID", pd.Series(dtype=int)).tolist() if str(x).strip()]})
        work = ungrouped

    # Individual RMR export path, used for all rows in Individual mode and ungrouped rows in Work Group mode.
    for _, row in work.iterrows():
        rmr_internal_id = int(row.get("RMR Internal ID") or 0)
        rmr_code = str(row.get("RMR ID") or f"RMR-{rmr_internal_id}").strip()
        labor_amount = money_float(row.get("Labor Budget"))
        materials_amount = money_float(row.get("Materials Budget"))
        work_item = str(row.get("Work Item") or "").strip()
        work_group = str(row.get("Work Group") or "").strip()
        add_source_cashflow("RMR", str(rmr_internal_id), rmr_code, row, labor_amount, materials_amount, work_item, work_group)
        source_updates.append({"source_type": "RMR", "source_id": str(rmr_internal_id), "signature": rmr_export_signature(row), "rmr_ids": [rmr_internal_id] if rmr_internal_id else []})
    return export_rows, source_updates


def upsert_cash_flow_rows(export_rows: list[dict]):
    if not export_rows:
        return 0
    ensure_cash_flow_export_table()
    source_pairs = sorted({(str(r.get("Source Type") or ""), str(r.get("Source ID") or "")) for r in export_rows if str(r.get("Source Type") or "") and str(r.get("Source ID") or "")})
    with closing(get_cash_flow_conn()) as conn:
        with conn.cursor() as cur:
            for source_type, source_id in source_pairs:
                cur.execute(
                    f"DELETE FROM {quote_identifier('cashflow')} WHERE {quote_identifier('Source Type')} = %s AND {quote_identifier('Source ID')} = %s AND {quote_identifier('Source App')} = %s",
                    (source_type, source_id, "Renovation Management"),
                )
            columns = CASH_FLOW_COLUMNS + CASH_FLOW_SOURCE_COLUMNS
            col_sql = ", ".join([quote_identifier(c) for c in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {quote_identifier('cashflow')} ({col_sql}) VALUES ({placeholders})"
            values = []
            for row in export_rows:
                values.append(tuple(str(row.get(col, "") or "") for col in columns))
            cur.executemany(insert_sql, values)
        conn.commit()
    return len(export_rows)


def update_cashflow_export_status(source_updates: list[dict]):
    exported_at = datetime.now()
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            for item in source_updates:
                source_type = item.get("source_type")
                signature = str(item.get("signature") or "")
                if source_type == "RMR":
                    for rmr_id in item.get("rmr_ids", []):
                        cur.execute(
                            """
                            UPDATE renovation_master_records
                            SET cashflow_export_status = 'Exported', cashflow_last_exported_at = %s,
                                cashflow_export_signature = %s, modified_at = NOW()
                            WHERE id = %s
                            """,
                            (exported_at, signature, int(rmr_id)),
                        )
                elif source_type == "Work Group":
                    source_id = str(item.get("source_id") or "")
                    if source_id.isdigit():
                        cur.execute(
                            """
                            UPDATE work_groups
                            SET cashflow_export_status = 'Exported', cashflow_last_exported_at = %s,
                                cashflow_export_signature = %s, modified_at = NOW()
                            WHERE id = %s
                            """,
                            (exported_at, signature, int(source_id)),
                        )
                    for rmr_id in item.get("rmr_ids", []):
                        cur.execute(
                            """
                            UPDATE renovation_master_records
                            SET cashflow_export_status = 'Exported', cashflow_last_exported_at = %s,
                                cashflow_export_signature = %s, modified_at = NOW()
                            WHERE id = %s
                            """,
                            (exported_at, signature, int(rmr_id)),
                        )
        conn.commit()
    st.cache_data.clear()



@st.cache_data(show_spinner=False, ttl=300)
def rmr_files_df(rmr_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            rmr_id,
            COALESCE(file_filename, '') AS file_filename,
            COALESCE(content_type, 'application/octet-stream') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            file_bytes,
            COALESCE(sort_order, 0) AS sort_order,
            COALESCE(uploaded_by, '') AS uploaded_by,
            created_at
        FROM renovation_master_record_files
        WHERE rmr_id = ?
        ORDER BY sort_order, id
        """,
        (int(rmr_id),),
    )


@st.cache_data(show_spinner=False, ttl=300)
def rmr_history_df(rmr_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT id, rmr_id, COALESCE(action_type, '') AS action_type,
               COALESCE(action_notes, '') AS action_notes,
               COALESCE(changed_by, '') AS changed_by, created_at
        FROM renovation_master_record_history
        WHERE rmr_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (int(rmr_id),),
    )


def add_rmr_history(rmr_id: int, action_type: str, action_notes: str = ""):
    if not rmr_id:
        return
    execute(
        """
        INSERT INTO renovation_master_record_history (rmr_id, action_type, action_notes, changed_by, created_at)
        VALUES (?, ?, ?, ?, NOW())
        """,
        (int(rmr_id), str(action_type or ""), str(action_notes or ""), str(st.session_state.get("logged_in_user", "") or "")),
    )


def add_rmr_communication(rmr_id: int, message_text: str, author_type: str = "Owner", contractor_id: int | None = None, quote_request_id: int | None = None):
    """Add an owner/contractor communication item tied to one RMR."""
    text = str(message_text or "").strip()
    if not rmr_id or not text:
        return
    author_name = str(st.session_state.get("logged_in_user", "") or author_type or "")
    unread_owner = str(author_type or "").lower() == "contractor"
    unread_contractor = str(author_type or "").lower() != "contractor"
    execute(
        """
        INSERT INTO rmr_communications (
            rmr_id, quote_request_id, contractor_id, author_type, author_name, message_text,
            is_unread_for_owner, is_unread_for_contractor, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
        """,
        (
            int(rmr_id),
            int(quote_request_id) if quote_request_id else None,
            int(contractor_id) if contractor_id else None,
            str(author_type or ""),
            author_name,
            text,
            bool(unread_owner),
            bool(unread_contractor),
        ),
    )
    add_rmr_history(int(rmr_id), "Communication Added", f"{author_type}: {text[:120]}")
    st.cache_data.clear()


@st.cache_data(show_spinner=False, ttl=60)
def rmr_communications_df(rmr_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT rc.id, rc.rmr_id, rc.quote_request_id, COALESCE(rc.contractor_id, 0) AS contractor_id,
               COALESCE(c.name, '') AS contractor_name, COALESCE(rc.author_type, '') AS author_type,
               COALESCE(rc.author_name, '') AS author_name, COALESCE(rc.message_text, '') AS message_text,
               COALESCE(rc.is_unread_for_owner, FALSE) AS is_unread_for_owner,
               COALESCE(rc.is_unread_for_contractor, FALSE) AS is_unread_for_contractor,
               rc.created_at
        FROM rmr_communications rc
        LEFT JOIN contractors c ON c.id = rc.contractor_id
        WHERE rc.rmr_id = ?
        ORDER BY rc.created_at DESC, rc.id DESC
        """,
        (int(rmr_id),),
    )


def render_rmr_communication_thread(rmr_id: int, allow_owner_note: bool = False, allow_contractor_note: bool = False, contractor_id: int | None = None, section_key: str = ""):
    st.markdown("### RMR Communication Thread")
    st.caption("Owner and contractor notes tied directly to this RMR record.")
    if allow_owner_note:
        owner_note = st.text_area("Add Owner Note / Reply", height=90, key=f"owner_rmr_comm_{section_key}_{rmr_id}")
        if st.button("Save Owner Note", key=f"save_owner_rmr_comm_{section_key}_{rmr_id}"):
            if str(owner_note or "").strip():
                add_rmr_communication(rmr_id, owner_note, author_type="Owner", contractor_id=contractor_id)
                execute("UPDATE rmr_communications SET is_unread_for_owner = FALSE WHERE rmr_id = ?", (int(rmr_id),))
                st.success("Owner note saved.")
                st.rerun()
            else:
                st.error("Enter a note before saving.")
    if allow_contractor_note:
        contractor_note = st.text_area("Add Contractor Note / Reply", height=90, key=f"contractor_rmr_comm_{section_key}_{rmr_id}")
        if st.button("Save Contractor Note", key=f"save_contractor_rmr_comm_{section_key}_{rmr_id}"):
            if str(contractor_note or "").strip():
                add_rmr_communication(rmr_id, contractor_note, author_type="Contractor", contractor_id=contractor_id)
                execute("UPDATE rmr_communications SET is_unread_for_contractor = FALSE WHERE rmr_id = ? AND COALESCE(contractor_id, 0) = ?", (int(rmr_id), int(contractor_id or 0)))
                st.success("Contractor note saved.")
                st.rerun()
            else:
                st.error("Enter a note before saving.")

    comm_df = rmr_communications_df(int(rmr_id))
    if comm_df.empty:
        st.info("No owner/contractor notes have been entered for this RMR yet.")
    else:
        for row in comm_df.itertuples():
            created_display = pd.to_datetime(row.created_at, errors="coerce")
            created_text = created_display.strftime("%m-%d-%Y %I:%M %p") if pd.notna(created_display) else ""
            who = str(row.author_name or row.author_type or "")
            contractor_suffix = f" ({row.contractor_name})" if str(row.contractor_name or "").strip() and str(row.author_type).lower() == "contractor" else ""
            unread_flag = " — NEW" if bool(row.is_unread_for_owner) and not allow_contractor_note else ""
            st.markdown(f"**{created_text} — {row.author_type}: {who}{contractor_suffix}{unread_flag}**")
            st.write(row.message_text)
            st.markdown("---")


def create_rmr_record(data: dict) -> int | None:
    if data.get("save_to_master", False):
        ensure_portfolio_property_exists(data.get("portfolio_name", ""), data.get("property_name", ""))
        ensure_portfolio_address_exists(
            data.get("portfolio_name", ""),
            data.get("property_name", ""),
            data.get("address", ""),
            data.get("unit_number", ""),
        )

    if data.get("save_work_item_to_master", False):
        ensure_work_item_exists(
            data.get("work_item_name", ""),
            data.get("category_name", ""),
            data.get("scope_description", ""),
        )

    new_id = execute_returning_id(
        """
        INSERT INTO renovation_master_records (
            entry_date, portfolio_name, property_name, address, unit_number, location_identifier,
            work_item_name, category_name, scope_description, notes, materials_notes, scope_complete,
            ai_estimated_hours, user_estimated_hours, labor_budget, materials_budget, budget_timeframe,
            budget_start_date, budget_end_date, budget_status, info_status, project_id,
            contractor_id, contractor_priority, owner_intent, created_by, created_at, modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        """,
        (
            data.get("entry_date"), data.get("portfolio_name", ""), data.get("property_name", ""),
            data.get("address", ""), data.get("unit_number", ""), data.get("location_identifier", ""),
            data.get("work_item_name", ""), data.get("category_name", ""), data.get("scope_description", ""),
            data.get("notes", ""), data.get("materials_notes", ""), bool(data.get("scope_complete", False)),
            data.get("ai_estimated_hours"), data.get("user_estimated_hours"), data.get("labor_budget"),
            data.get("materials_budget", 0), data.get("budget_timeframe", "No Timeframe Yet"),
            data.get("budget_start_date"), data.get("budget_end_date"), data.get("budget_status", "Active"),
            data.get("info_status", "Open"), data.get("project_id"), data.get("contractor_id"),
            data.get("contractor_priority", "3 - Quote Only"), data.get("owner_intent", "Quote Only"),
            str(st.session_state.get("logged_in_user", "") or ""),
        ),
    )
    if new_id:
        execute(
            "UPDATE renovation_master_records SET rmr_code = 'RMR-' || LPAD(id::text, 6, '0') WHERE id = ? AND COALESCE(rmr_code, '') = ''",
            (int(new_id),),
        )
        add_rmr_history(int(new_id), "Created", "RMR created.")
    return int(new_id) if new_id else None


def update_rmr_record(rmr_id: int, data: dict):
    execute(
        """
        UPDATE renovation_master_records
        SET entry_date = ?, portfolio_name = ?, property_name = ?, address = ?, unit_number = ?,
            location_identifier = ?, work_item_name = ?, category_name = ?, scope_description = ?,
            notes = ?, materials_notes = ?, scope_complete = ?, ai_estimated_hours = ?,
            user_estimated_hours = ?, labor_budget = ?, materials_budget = ?, budget_timeframe = ?,
            budget_start_date = ?, budget_end_date = ?, budget_status = ?, info_status = ?, project_id = ?,
            contractor_id = ?, contractor_priority = ?, owner_intent = ?, modified_at = NOW()
        WHERE id = ?
        """,
        (
            data.get("entry_date"), data.get("portfolio_name", ""), data.get("property_name", ""),
            data.get("address", ""), data.get("unit_number", ""), data.get("location_identifier", ""),
            data.get("work_item_name", ""), data.get("category_name", ""), data.get("scope_description", ""),
            data.get("notes", ""), data.get("materials_notes", ""), bool(data.get("scope_complete", False)),
            data.get("ai_estimated_hours"), data.get("user_estimated_hours"), data.get("labor_budget"),
            data.get("materials_budget", 0), data.get("budget_timeframe", "No Timeframe Yet"),
            data.get("budget_start_date"), data.get("budget_end_date"), data.get("budget_status", "Active"),
            data.get("info_status", "Open"), data.get("project_id"), data.get("contractor_id"),
            data.get("contractor_priority", "3 - Quote Only"), data.get("owner_intent", "Quote Only"), int(rmr_id),
        ),
    )
    add_rmr_history(int(rmr_id), "Updated", "RMR fields updated.")


def save_rmr_files(rmr_id: int, uploaded_files, uploaded_by: str = ""):
    files = []
    for sort_order, uploaded in enumerate(uploaded_files or []):
        if uploaded is None:
            continue
        data = uploaded.getvalue()
        if not data:
            continue
        filename = getattr(uploaded, "name", "file")
        content_type = getattr(uploaded, "type", None) or "application/octet-stream"
        if str(content_type).startswith("image/"):
            data, content_type, filename = optimize_image_bytes_for_upload(data, filename)
        stored_file = upload_bytes_to_blob(
            data=data,
            filename=filename,
            content_type=content_type,
            folder="renovation-estimator/rmr-files",
        )
        stored_file["sort_order"] = sort_order
        files.append(stored_file)

    if not files:
        return
    existing_df = rmr_files_df(int(rmr_id))
    existing_count = len(existing_df) if existing_df is not None else 0
    for offset, stored_file in enumerate(files):
        execute(
            """
            INSERT INTO renovation_master_record_files (
                rmr_id, file_filename, content_type, storage_mode, blob_url, blob_name,
                file_bytes, sort_order, uploaded_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            """,
            (
                int(rmr_id), stored_file.get("filename"), stored_file.get("content_type"),
                stored_file.get("storage_mode"), stored_file.get("blob_url"), stored_file.get("blob_name"),
                stored_file.get("bytes"), existing_count + offset, uploaded_by or "",
            ),
        )
    add_rmr_history(int(rmr_id), "Files Added", f"{len(files)} file/photo(s) added.")


def rmr_file_bytes(row) -> bytes | None:
    file_bytes = row.get("file_bytes")
    if file_bytes is not None and not isinstance(file_bytes, (bytes, bytearray)):
        try:
            file_bytes = bytes(file_bytes)
        except Exception:
            file_bytes = None
    if file_bytes:
        return file_bytes
    blob_name = str(row.get("blob_name") or "")
    if blob_name:
        return cached_download_blob_bytes(blob_name)
    return None


def delete_rmr_file(file_id: int):
    execute("DELETE FROM renovation_master_record_files WHERE id = ?", (int(file_id),))


def delete_rmr_record(rmr_id: int):
    # Build 19C: delete means remove from active workflow and clear budget/cash-flow/work-group links.
    execute("DELETE FROM rmr_group_members WHERE rmr_id = ?", (int(rmr_id),))
    execute(
        """
        UPDATE renovation_master_records
        SET deleted = TRUE,
            budget_status = 'Cancelled',
            info_status = 'Closed',
            work_group_id = NULL,
            cashflow_export_status = 'Not Exported',
            cashflow_last_exported_at = NULL,
            cashflow_export_signature = '',
            modified_at = NOW()
        WHERE id = ?
        """,
        (int(rmr_id),),
    )
    add_rmr_history(int(rmr_id), "Deleted", "RMR removed from active use; budget/cash-flow links and group membership cleared.")


def rmr_row_from_id(rmr_id: int):
    df = rmr_records_df(include_deleted=True)
    if df.empty:
        return None
    match = df[df["id"].astype(int) == int(rmr_id)]
    if match.empty:
        return None
    return match.iloc[0].to_dict()


def render_rmr_file_section(rmr_id: int, section_key: str, allow_delete: bool = True):
    files_df = rmr_files_df(int(rmr_id))
    if files_df.empty:
        st.info("No RMR photos or files saved yet.")
        return
    st.caption(f"{len(files_df)} file/photo(s) attached to this RMR.")
    for _, file_row in files_df.iterrows():
        file_id = int(file_row["id"])
        file_name = str(file_row.get("file_filename") or "file")
        content_type = str(file_row.get("content_type") or "application/octet-stream")
        file_bytes = rmr_file_bytes(file_row)
        c1, c2 = st.columns([3, 1])
        c1.write(f"**{file_name}**")
        if content_type.startswith("image/"):
            if c1.checkbox(f"Preview {file_name}", key=f"preview_rmr_file_{section_key}_{rmr_id}_{file_id}", value=False):
                try:
                    if file_bytes:
                        c1.image(file_bytes, caption=file_name, use_container_width=True)
                    elif str(file_row.get("blob_url") or ""):
                        c1.image(str(file_row.get("blob_url")), caption=file_name, use_container_width=True)
                except Exception:
                    c1.warning(f"Could not preview {file_name}.")
        if file_bytes:
            c2.download_button("Download", data=file_bytes, file_name=file_name, mime=content_type, key=f"download_rmr_file_{section_key}_{rmr_id}_{file_id}")
        if allow_delete and c2.button("Delete", key=f"delete_rmr_file_{section_key}_{rmr_id}_{file_id}"):
            delete_rmr_file(file_id)
            add_rmr_history(int(rmr_id), "File Deleted", file_name)
            st.success("File deleted.")
            st.rerun()


def render_rmr_progress_panel(row: dict):
    checks = [
        ("Property", bool(str(row.get("property_name") or "").strip())),
        ("Location", bool(str(row.get("location_identifier") or row.get("address") or "").strip())),
        ("Work Item", bool(str(row.get("work_item_name") or "").strip())),
        ("Scope Complete", bool(row.get("scope_complete"))),
        ("AI Hours", float(row.get("ai_estimated_hours") or 0) > 0),
        ("My Hours", float(row.get("user_estimated_hours") or 0) > 0),
        ("Labor Budget", float(row.get("labor_budget") or 0) > 0),
        ("Materials Budget", float(row.get("materials_budget") or 0) > 0),
        ("Budget Timeframe", str(row.get("budget_timeframe") or "No Timeframe Yet") != "No Timeframe Yet"),
        ("Photos", int(row.get("photo_count") or 0) > 0),
    ]
    st.markdown("#### Information Progress")
    cols = st.columns(2)
    for idx, (label, ok) in enumerate(checks):
        cols[idx % 2].write(("✅ " if ok else "☐ ") + label)


def build_rmr_form_defaults(source_row: dict | None = None) -> dict:
    source_row = source_row or {}
    today = datetime.now().date()
    return {
        "entry_date": source_row.get("entry_date") or today,
        "portfolio_name": source_row.get("portfolio_name") or "",
        "property_name": source_row.get("property_name") or "",
        "address": source_row.get("address") or "",
        "unit_number": source_row.get("unit_number") or "",
        "location_identifier": source_row.get("location_identifier") or "",
        "work_item_name": source_row.get("work_item_name") or "",
        "category_name": source_row.get("category_name") or "",
        "scope_description": source_row.get("scope_description") or "",
        "notes": source_row.get("notes") or "",
        "materials_notes": source_row.get("materials_notes") or "",
        "scope_complete": bool(source_row.get("scope_complete", False)),
        "ai_estimated_hours": float(source_row.get("ai_estimated_hours") or 0),
        "user_estimated_hours": float(source_row.get("user_estimated_hours") or 0),
        "labor_budget": float(source_row.get("labor_budget") or 0),
        "materials_budget": float(source_row.get("materials_budget") or 0),
        "budget_timeframe": source_row.get("budget_timeframe") or "No Timeframe Yet",
        "budget_start_date": source_row.get("budget_start_date"),
        "budget_end_date": source_row.get("budget_end_date"),
        "budget_status": source_row.get("budget_status") or "Active",
        "info_status": source_row.get("info_status") or "Open",
        "project_id": int(source_row.get("project_id") or 0),
        "contractor_id": int(source_row.get("contractor_id") or 0),
        "contractor_priority": source_row.get("contractor_priority") or "3 - Quote Only",
        "owner_intent": source_row.get("owner_intent") or "Quote Only",
    }


def render_rmr_entry_form(mode: str = "create", existing_row: dict | None = None):
    defaults = build_rmr_form_defaults(existing_row)
    form_version = st.session_state.get("rmr_entry_form_version", 0) if mode == "create" else 0
    key_prefix = f"rmr_{mode}_{defaults.get('id', 'new')}_{form_version}"

    # Date is auto-filled but still editable in case the entry is being recorded later.
    entry_date = st.date_input(
        "Date",
        value=pd.to_datetime(defaults["entry_date"]).date() if defaults["entry_date"] else datetime.now().date(),
        key=f"{key_prefix}_entry_date",
    )

    # Property is the operating unit for RMR entry. Portfolio is kept behind the scenes only.
    property_options = ["Add New Property"] + master_property_labels()
    selected_property = st.selectbox("Property", property_options, key=f"{key_prefix}_property_select")
    selected_property_name = ""
    selected_portfolio_name = ""
    save_to_master = False

    if selected_property == "Add New Property":
        selected_property_name = st.text_input(
            "New Property Name",
            value=defaults["property_name"],
            key=f"{key_prefix}_new_property_name",
        )
        # Portfolio is not shown on the iPhone RMR entry page. Store a neutral value internally.
        selected_portfolio_name = defaults["portfolio_name"] or "General Portfolio"
        # Permanent master-list saves must be deliberate; default unchecked.
        save_to_master = False
    elif selected_property and selected_property != "No Property / General Address":
        try:
            property_token, selected_property_name = selected_property.split(" | ", 1)
        except Exception:
            property_token = ""
            selected_property_name = selected_property
        property_df = portfolio_properties_df(include_deleted=False)
        if property_token.isdigit() and not property_df.empty:
            pmatch = property_df[property_df["id"].astype(int) == int(property_token)]
            if not pmatch.empty:
                selected_portfolio_name = str(pmatch.iloc[0].get("portfolio_name") or "")
    else:
        selected_property_name = st.text_input(
            "Property Name / General Name",
            value=defaults["property_name"],
            key=f"{key_prefix}_property_manual",
            help="Use this only when the property is not yet in the master property list.",
        )
        selected_portfolio_name = defaults["portfolio_name"] or "General Portfolio"

    # Address dropdown chooses a saved address. If a new address is needed, only ask for Street Address.
    address_options = [] if selected_property == "Add New Property" else master_address_labels(selected_property)
    address_choice = st.selectbox(
        "Address",
        ["Add / Type New Address"] + address_options,
        key=f"{key_prefix}_address_choice",
    )
    address = defaults["address"]
    unit_number = ""  # Unit / building / area now belongs in Specific Location, not a separate field.
    if address_choice != "Add / Type New Address":
        row = portfolio_address_row_from_label(address_choice)
        if row is not None:
            address = str(row.get("address") or "")
            existing_unit = str(row.get("unit_number") or "").strip()
            if existing_unit and not str(defaults.get("location_identifier") or "").strip():
                # If a saved address has a unit, prefill it into Specific Location below.
                defaults["location_identifier"] = f"Unit {existing_unit}"
            selected_property_name = str(row.get("property_name") or selected_property_name)
            selected_portfolio_name = str(row.get("portfolio_name") or selected_portfolio_name)
    else:
        address = st.text_input(
            "Street Address",
            value=defaults["address"],
            key=f"{key_prefix}_address",
        )
        if str(address or "").strip() and str(selected_property_name or "").strip():
            save_to_master = st.checkbox(
                "Save this property/address to the master list",
                value=False,
                key=f"{key_prefix}_save_to_master",
            )

    location_identifier = st.text_input(
        "Specific Location",
        value=defaults["location_identifier"],
        help="Example: Unit 11, Building 4 Breezeway 33, south stairwell, laundry room, roof over office.",
        key=f"{key_prefix}_location_identifier",
    )

    work_item_labels = rmr_work_item_labels()
    default_work_label = "Add New Work Item"
    if defaults["work_item_name"]:
        possible = [lbl for lbl in work_item_labels if lbl.startswith(defaults["work_item_name"] + " |") or lbl == defaults["work_item_name"]]
        default_work_label = possible[0] if possible else "Add New Work Item"
    work_index = work_item_labels.index(default_work_label) if default_work_label in work_item_labels else 0
    selected_work_label = st.selectbox("Work Item", work_item_labels, index=work_index, key=f"{key_prefix}_work_item_select")
    work_item_name, category_name = parse_rmr_work_item_label(selected_work_label)
    save_work_item_to_master = False
    if selected_work_label == "Add New Work Item":
        st.info("New Work Item: choose or create a Category of Labor, then write the scope. When you save, this can be added to the permanent Work Item list.")
        w1, w2 = st.columns(2)
        work_item_name = w1.text_input("New Work Item Name", value=defaults["work_item_name"], key=f"{key_prefix}_new_work_item")
        category_options = ["Add New Category of Labor"] + get_category_names()
        default_category_index = category_options.index(defaults["category_name"]) if defaults["category_name"] in category_options else 0
        category_choice = w2.selectbox("Category of Labor", category_options, index=default_category_index, key=f"{key_prefix}_category")
        if category_choice == "Add New Category of Labor":
            category_name = st.text_input("New Category of Labor", value=defaults["category_name"], key=f"{key_prefix}_new_category")
        else:
            category_name = category_choice
        save_work_item_to_master = st.checkbox("Add this Work Item / Category / Scope to the permanent list", value=False, key=f"{key_prefix}_save_work_item_to_master")
    else:
        st.caption(f"Category of Labor: {category_name or 'Not assigned'}")

    default_scope = defaults["scope_description"]
    if not default_scope and work_item_name:
        default_scope = rmr_default_scope_for_work_item(work_item_name, category_name)

    scope_key = f"{key_prefix}_scope"
    scope_sig_key = f"{key_prefix}_scope_work_item_signature"
    scope_signature = f"{work_item_name}|{category_name}"
    if st.session_state.get(scope_sig_key) != scope_signature:
        if default_scope and not str(st.session_state.get(scope_key, "")).strip():
            st.session_state[scope_key] = default_scope
        st.session_state[scope_sig_key] = scope_signature

    scope_description = st.text_area("Scope", value=st.session_state.get(scope_key, default_scope), height=180, key=scope_key)

    scope_complete = st.checkbox("Scope Complete", value=defaults["scope_complete"], key=f"{key_prefix}_scope_complete")
    st.caption("AI estimated hours can be entered after scope is complete. Full AI calculation will be added in a later phase.")
    h1, h2 = st.columns(2)
    ai_estimated_hours = h1.number_input("AI Estimated Hours", min_value=0.0, value=float(defaults["ai_estimated_hours"] or 0), step=0.25, key=f"{key_prefix}_ai_hours")
    user_estimated_hours = h2.number_input("My Estimated Hours", min_value=0.0, value=float(defaults["user_estimated_hours"] or 0), step=0.25, key=f"{key_prefix}_user_hours")

    st.markdown("#### Budget / Cash Flow Planning")
    b1, b2 = st.columns(2)
    labor_budget = b1.number_input("Labor Budget", min_value=0.0, value=float(defaults["labor_budget"] or 0), step=25.0, key=f"{key_prefix}_labor_budget")
    materials_budget = b2.number_input("Materials Budget", min_value=0.0, value=float(defaults["materials_budget"] or 0), step=25.0, key=f"{key_prefix}_materials_budget")

    tf_default = defaults.get("budget_timeframe", "No Timeframe Yet") or "No Timeframe Yet"
    # Keep older saved values visible if they exist, but default new entries to the current 15C list.
    timeframe_options = list(RMR_BUDGET_TIMEFRAME_OPTIONS)
    if tf_default not in timeframe_options:
        timeframe_options.append(tf_default)
    t1, t2 = st.columns(2)
    budget_timeframe = t1.selectbox(
        "Budget Timeframe",
        timeframe_options,
        index=timeframe_options.index(tf_default) if tf_default in timeframe_options else 0,
        key=f"{key_prefix}_budget_timeframe",
        help="This controls where the item appears in Budget Planner and the Cash Flow export preview.",
    )
    budget_status = t2.selectbox(
        "Budget Status",
        RMR_BUDGET_STATUS_OPTIONS,
        index=RMR_BUDGET_STATUS_OPTIONS.index(defaults.get("budget_status", "Active")) if defaults.get("budget_status", "Active") in RMR_BUDGET_STATUS_OPTIONS else 0,
        key=f"{key_prefix}_budget_status",
    )

    custom_start = defaults.get("budget_start_date")
    custom_end = defaults.get("budget_end_date")
    if budget_timeframe == "Custom Dates":
        d1, d2 = st.columns(2)
        custom_start = d1.date_input(
            "Budget Start Date",
            value=pd.to_datetime(custom_start).date() if custom_start else entry_date,
            key=f"{key_prefix}_budget_start_date",
        )
        custom_end = d2.date_input(
            "Budget End Date",
            value=pd.to_datetime(custom_end).date() if custom_end else entry_date,
            key=f"{key_prefix}_budget_end_date",
        )
    else:
        custom_start, custom_end = calculate_budget_dates(entry_date, budget_timeframe)
        if custom_start and custom_end:
            st.caption(f"Cash Flow date range: {custom_start} to {custom_end}")

    if mode == "edit" and existing_row is not None:
        export_status = display_export_status(
            existing_row.get("cashflow_export_status", "Not Exported"),
            existing_row.get("cashflow_export_signature", ""),
            rmr_export_signature(existing_row),
        )
        st.caption(f"Cash Flow Export Status: {export_status}")

    notes = st.text_area("Notes", value=defaults["notes"], height=100, key=f"{key_prefix}_notes")
    materials_notes = st.text_area("Materials Notes", value=defaults["materials_notes"], height=100, key=f"{key_prefix}_materials_notes")

    info_status = "Open"

    contractor_options = ["None selected"] + get_contractor_names()
    current_contractor_name = "None selected"
    if defaults["contractor_id"]:
        contractors_df = fetch_df("SELECT id, name FROM contractors ORDER BY LOWER(name)")
        match = contractors_df[contractors_df["id"].astype(int) == int(defaults["contractor_id"])] if not contractors_df.empty else pd.DataFrame()
        if not match.empty:
            current_contractor_name = str(match.iloc[0]["name"])
    contractor_index = contractor_options.index(current_contractor_name) if current_contractor_name in contractor_options else 0
    selected_contractor_name = st.selectbox("Preferred Contractor (optional)", contractor_options, index=contractor_index, key=f"{key_prefix}_contractor")
    contractor_id = get_contractor_id_by_name(selected_contractor_name)

    st.markdown("#### Contractor Priority / Intent")
    render_contractor_priority_legend()
    cp1, cp2 = st.columns(2)
    current_priority = defaults.get("contractor_priority", "3 - Quote Only")
    if current_priority not in CONTRACTOR_PRIORITY_OPTIONS:
        current_priority = "3 - Quote Only"
    contractor_priority = cp1.selectbox(
        "Contractor Priority",
        CONTRACTOR_PRIORITY_OPTIONS,
        index=CONTRACTOR_PRIORITY_OPTIONS.index(current_priority),
        key=f"{key_prefix}_contractor_priority",
        help="This appears on the contractor portal and controls sorting for contractor work.",
    )
    current_intent = defaults.get("owner_intent", "Quote Only")
    if current_intent not in OWNER_INTENT_OPTIONS:
        current_intent = "Quote Only"
    owner_intent = cp2.selectbox(
        "Owner Intent",
        OWNER_INTENT_OPTIONS,
        index=OWNER_INTENT_OPTIONS.index(current_intent),
        key=f"{key_prefix}_owner_intent",
        help="This tells the contractor whether you only need pricing or whether the work is moving forward.",
    )

    project_labels = ["No Project"] + project_registry_select_labels(active_only=False)
    project_default_label = "No Project"
    if defaults["project_id"]:
        for label in project_labels:
            if label.startswith(f"{defaults['project_id']} |"):
                project_default_label = label
                break
    project_label = st.selectbox("Project (optional)", project_labels, index=project_labels.index(project_default_label) if project_default_label in project_labels else 0, key=f"{key_prefix}_project")
    project_id = None
    if project_label != "No Project":
        try:
            project_id = int(project_label.split(" | ", 1)[0])
        except Exception:
            project_id = None

    uploaded_files = st.file_uploader("Photos", type=["png", "jpg", "jpeg", "webp", "pdf"], accept_multiple_files=True, key=f"{key_prefix}_files")

    data = {
        "entry_date": entry_date,
        "portfolio_name": selected_portfolio_name,
        "property_name": selected_property_name,
        "address": str(address or "").strip(),
        "unit_number": str(unit_number or "").strip(),
        "location_identifier": str(location_identifier or "").strip(),
        "work_item_name": str(work_item_name or "").strip(),
        "category_name": str(category_name or "").strip(),
        "scope_description": str(scope_description or "").strip(),
        "notes": str(notes or "").strip(),
        "materials_notes": str(materials_notes or "").strip(),
        "scope_complete": bool(scope_complete),
        "ai_estimated_hours": float(ai_estimated_hours or 0) if ai_estimated_hours else None,
        "user_estimated_hours": float(user_estimated_hours or 0) if user_estimated_hours else None,
        "labor_budget": float(labor_budget or 0) if labor_budget else 0,
        "materials_budget": float(materials_budget or 0) if materials_budget else 0,
        "budget_timeframe": str(budget_timeframe or "No Timeframe Yet"),
        "budget_start_date": custom_start,
        "budget_end_date": custom_end,
        "budget_status": str(budget_status or "Active"),
        "info_status": str(info_status or "Open"),
        "project_id": project_id,
        "contractor_id": contractor_id,
        "contractor_priority": str(contractor_priority or "3 - Quote Only"),
        "owner_intent": str(owner_intent or "Quote Only"),
        "save_to_master": bool(save_to_master),
        "save_work_item_to_master": bool(save_work_item_to_master),
    }
    return data, uploaded_files

# -----------------------------
# PDF helpers
# -----------------------------
@st.cache_data(show_spinner=False, ttl=300)
def estimate_header_df(estimate_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            COALESCE(e.order_number, 'Est' || e.id::text) AS order_number,
            COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
            COALESCE(e.estimate_address, '') AS estimate_address,
            e.id AS estimate_id,
            e.created_at,
            e.modified_at,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(c.address, '') AS contractor_address,
            COALESCE(e.notes, '') AS notes,
            COALESCE(e.labor_rate, 0) AS labor_rate,
            COALESCE(SUM(el.onsite_hours), 0) AS total_onsite_hours,
            COALESCE(SUM(el.travel_hours), 0) AS total_travel_hours,
            COALESCE(SUM(el.total_hours), 0) AS total_hours,
            COALESCE(SUM(el.onsite_cost), 0) AS total_onsite_cost,
            COALESCE(SUM(el.travel_cost), 0) AS total_travel_cost,
            COALESCE(SUM(el.total_labor_cost), 0) AS total_labor_cost
        FROM estimates e
        LEFT JOIN contractors c ON c.id = e.contractor_id
        LEFT JOIN estimate_lines el ON el.estimate_id = e.id
        WHERE e.id = ?
        GROUP BY e.id, e.created_at, e.modified_at, e.estimate_name, e.estimate_address, c.name, c.address, e.notes, e.labor_rate
        """,
        (estimate_id,),
    )


@st.cache_data(show_spinner=False, ttl=300)
def estimate_lines_df(estimate_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            COALESCE(category_name, trade_name, '') AS category_name,
            COALESCE(work_group_name, '') AS work_group_name,
            trade_name,
            task_name,
            COALESCE(scope_description, '') AS scope_description,
            COALESCE(repair_quantity, 1) AS repair_quantity,
            CASE
                WHEN COALESCE(onsite_hours_each, 0) = 0 AND COALESCE(repair_quantity, 1) > 0
                    THEN onsite_hours / repair_quantity
                ELSE onsite_hours_each
            END AS onsite_hours_each,
            CASE
                WHEN COALESCE(travel_hours_each, 0) = 0 AND COALESCE(repair_quantity, 1) > 0
                    THEN travel_hours / repair_quantity
                ELSE travel_hours_each
            END AS travel_hours_each,
            CASE
                WHEN COALESCE(total_hours_each, 0) = 0 AND COALESCE(repair_quantity, 1) > 0
                    THEN total_hours / repair_quantity
                ELSE total_hours_each
            END AS total_hours_each,
            onsite_hours,
            travel_hours,
            total_hours,
            labor_rate,
            onsite_cost,
            travel_cost,
            COALESCE(manual_repair_amount, 0) AS manual_repair_amount,
            total_labor_cost,
            created_at,
            modified_at
        FROM estimate_lines
        WHERE estimate_id = ?
        ORDER BY id
        """,
        (estimate_id,),
    )


def draw_wrapped_text(pdf, text, x, y, max_width, line_height=10, font_name="Helvetica", font_size=8):
    words = str(text).split()
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        if pdf.stringWidth(test, font_name, font_size) <= max_width:
            current = test
        else:
            if current:
                pdf.drawString(x, y, current)
                y -= line_height
            current = word
    if current:
        pdf.drawString(x, y, current)
        y -= line_height
    return y


def build_estimate_pdf(estimate_id: int, report_type: str = "internal"):
    header_df = estimate_header_df(estimate_id)
    lines_df = estimate_lines_df(estimate_id)

    if header_df.empty:
        return None

    header = header_df.iloc[0]

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    left = 40
    y = height - 40

    if report_type == "internal":
        title = "Internal Renovation Estimate"
    else:
        title = "Contractor Renovation Estimate - Estimated Hours Only"

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(left, y, title)
    y -= 20

    pdf.setFont("Helvetica", 10)
    pdf.drawString(left, y, f"Estimate ID: {int(header['estimate_id'])}")
    y -= 14
    pdf.drawString(left, y, f"Estimate Name: {header['estimate_name']}")
    y -= 14
    pdf.drawString(left, y, f"Address: {header['estimate_address']}")
    y -= 14
    pdf.drawString(left, y, f"Contractor: {header['contractor_name']}")
    y -= 20

    if report_type == "internal":
        col_task = 40
        col_trade = 170
        col_hours = 390
        col_rate = 470
        col_cost = 560
        task_width = 115
        trade_width = 110
    else:
        col_task = 40
        col_trade = 210
        col_hours = 560
        task_width = 150
        trade_width = 140

    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(col_task, y, "Work Item")
    pdf.drawString(col_trade, y, 'Category of Labor')
    pdf.drawRightString(col_hours, y, "Hours")
    if report_type == "internal":
        pdf.drawRightString(col_rate, y, "Rate")
        pdf.drawRightString(col_cost, y, "Cost")

    y -= 10
    pdf.line(left, y, width - 40, y)
    y -= 12

    for _, row in lines_df.iterrows():
        if y < 90:
            pdf.showPage()
            y = height - 40

            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(col_task, y, "Work Item")
            pdf.drawString(col_trade, y, 'Category of Labor')
            pdf.drawRightString(col_hours, y, "Hours")
            if report_type == "internal":
                pdf.drawRightString(col_rate, y, "Rate")
                pdf.drawRightString(col_cost, y, "Cost")

            y -= 10
            pdf.line(left, y, width - 40, y)
            y -= 12

        start_y = y

        pdf.setFont("Helvetica", 8)
        y_task = draw_wrapped_text(
            pdf, row["task_name"], col_task, start_y, task_width,
            line_height=9, font_name="Helvetica", font_size=8
        )
        y_trade = draw_wrapped_text(
            pdf, row["trade_name"], col_trade, start_y, trade_width,
            line_height=9, font_name="Helvetica", font_size=8
        )

        pdf.drawRightString(col_hours, start_y, f"{row['total_hours']:.2f}")

        if report_type == "internal":
            pdf.drawRightString(col_rate, start_y, f"${row['labor_rate']:.2f}")
            pdf.drawRightString(col_cost, start_y, f"${row['total_labor_cost']:.2f}")

        y = min(y_task, y_trade) - 6

    y -= 6
    pdf.line(left, y, width - 40, y)
    y -= 14

    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(left, y, f"Total Hours: {header['total_hours']:.2f}")
    y -= 14

    if report_type == "internal":
        pdf.drawString(left, y, f"Total Cost: ${header['total_labor_cost']:.2f}")

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


@st.cache_data(show_spinner=False, ttl=300)
def schedule_entries_df(
    start_date: str | None = None,
    end_date: str | None = None,
    crew_name: str | None = None,
) -> pd.DataFrame:
    query = """
        SELECT
            id,
            COALESCE(project_name, '') AS project_name,
            COALESCE(estimate_address, '') AS estimate_address,
            scheduled_date,
            time_block,
            COALESCE(crew_name, '') AS crew_name,
            COALESCE(notes, '') AS notes,
            COALESCE(estimate_id, 0) AS estimate_id,
            created_at
        FROM schedule_entries
    """
    params = []
    clauses = []
    if start_date:
        clauses.append("scheduled_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("scheduled_date <= ?")
        params.append(end_date)
    if crew_name and crew_name != "All":
        clauses.append("COALESCE(crew_name, '') = ?")
        params.append(crew_name)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY scheduled_date, CASE time_block WHEN 'Full Day' THEN 1 WHEN 'Morning' THEN 2 ELSE 3 END, id"
    return fetch_df(query, tuple(params))


@st.cache_data(show_spinner=False, ttl=300)
def contractor_weekly_schedule_df(contractor_id: int | None = None, week_start_date: str | None = None) -> pd.DataFrame:
    query = """
        SELECT
            cws.id,
            cws.contractor_id,
            COALESCE(c.name, cws.contractor_name, '') AS contractor_name,
            cws.week_start_date,
            cws.day_name,
            COALESCE(cws.am_project_name, '') AS am_project_name,
            COALESCE(cws.am_crew_members, '') AS am_crew_members,
            COALESCE(cws.pm_project_name, '') AS pm_project_name,
            COALESCE(cws.pm_crew_members, '') AS pm_crew_members,
            COALESCE(cws.notes, '') AS notes,
            COALESCE(cws.submitted_by, '') AS submitted_by,
            cws.created_at,
            cws.modified_at
        FROM contractor_weekly_schedules cws
        LEFT JOIN contractors c ON c.id = cws.contractor_id
    """
    params = []
    clauses = []
    if contractor_id:
        clauses.append("cws.contractor_id = ?")
        params.append(contractor_id)
    if week_start_date:
        clauses.append("cws.week_start_date = ?")
        params.append(week_start_date)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += """
        ORDER BY
            cws.week_start_date DESC,
            LOWER(COALESCE(c.name, cws.contractor_name, '')),
            CASE cws.day_name
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
                ELSE 8
            END
    """
    return fetch_df(query, tuple(params))


def save_contractor_weekly_schedule(
    contractor_id: int,
    contractor_name: str,
    week_start_date,
    schedule_rows: list[dict],
    submitted_by: str = "",
):
    week_start_text = week_start_date.strftime("%Y-%m-%d") if hasattr(week_start_date, "strftime") else str(week_start_date)
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            for row in schedule_rows:
                cur.execute(
                    """
                    INSERT INTO contractor_weekly_schedules (
                        contractor_id,
                        contractor_name,
                        week_start_date,
                        day_name,
                        am_project_name,
                        am_crew_members,
                        pm_project_name,
                        pm_crew_members,
                        notes,
                        submitted_by,
                        created_at,
                        modified_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (contractor_id, week_start_date, day_name)
                    DO UPDATE SET
                        contractor_name = EXCLUDED.contractor_name,
                        am_project_name = EXCLUDED.am_project_name,
                        am_crew_members = EXCLUDED.am_crew_members,
                        pm_project_name = EXCLUDED.pm_project_name,
                        pm_crew_members = EXCLUDED.pm_crew_members,
                        notes = EXCLUDED.notes,
                        submitted_by = EXCLUDED.submitted_by,
                        modified_at = NOW()
                    """,
                    (
                        contractor_id,
                        contractor_name,
                        week_start_text,
                        row.get("day_name", ""),
                        row.get("am_project_name", ""),
                        row.get("am_crew_members", ""),
                        row.get("pm_project_name", ""),
                        row.get("pm_crew_members", ""),
                        row.get("notes", ""),
                        submitted_by or "",
                    ),
                )
        conn.commit()
    st.cache_data.clear()


def delete_contractor_weekly_schedule_day(contractor_id: int, week_start_date, day_name: str):
    week_start_text = week_start_date.strftime("%Y-%m-%d") if hasattr(week_start_date, "strftime") else str(week_start_date)
    execute(
        """
        DELETE FROM contractor_weekly_schedules
        WHERE contractor_id = ? AND week_start_date = ? AND day_name = ?
        """,
        (contractor_id, week_start_text, day_name),
    )


def delete_contractor_weekly_schedule_week(contractor_id: int, week_start_date):
    week_start_text = week_start_date.strftime("%Y-%m-%d") if hasattr(week_start_date, "strftime") else str(week_start_date)
    execute(
        """
        DELETE FROM contractor_weekly_schedules
        WHERE contractor_id = ? AND week_start_date = ?
        """,
        (contractor_id, week_start_text),
    )


def build_contractor_weekly_schedule_pdf(contractor_name: str, week_start, week_end, schedule_rows: list[dict]) -> bytes:
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    width, height = landscape(letter)

    left = 36
    right = width - 36
    y = height - 36

    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(left, y, "Contractor Weekly Schedule")
    y -= 22

    pdf.setFont("Helvetica", 10)
    pdf.drawString(left, y, f"Contractor: {contractor_name}")
    y -= 14
    pdf.drawString(left, y, f"Week: {week_start.strftime('%m-%d-%Y')} through {week_end.strftime('%m-%d-%Y')}")
    y -= 14
    pdf.drawString(left, y, f"Generated: {datetime.now().strftime('%m-%d-%Y %I:%M %p')}")
    y -= 18

    table_top = y
    # Reduced widths slightly so the table does not run to the edge of the page
    col_widths = [82, 130, 130, 130, 130, 110]
    headers = ["Day", "AM Project", "AM Crew", "PM Project", "PM Crew", "Daily Notes"]
    row_height = 64
    header_height = 20
    x_positions = [left]
    for w in col_widths[:-1]:
        x_positions.append(x_positions[-1] + w)

    pdf.setFont("Helvetica-Bold", 8)
    x = left
    for idx, header in enumerate(headers):
        pdf.rect(x, y - header_height, col_widths[idx], header_height, stroke=1, fill=0)
        pdf.drawString(x + 4, y - 13, header)
        x += col_widths[idx]
    y -= header_height

    pdf.setFont("Helvetica", 7)

    for row in schedule_rows:
        if y - row_height < 36:
            pdf.showPage()
            y = height - 36
            pdf.setFont("Helvetica-Bold", 8)
            x = left
            for idx, header in enumerate(headers):
                pdf.rect(x, y - header_height, col_widths[idx], header_height, stroke=1, fill=0)
                pdf.drawString(x + 4, y - 13, header)
                x += col_widths[idx]
            y -= header_height
            pdf.setFont("Helvetica", 7)

        day_label = str(row.get("day_label") or row.get("day_name") or "")
        values = [
            day_label,
            str(row.get("am_project_name") or ""),
            str(row.get("am_crew_members") or ""),
            str(row.get("pm_project_name") or ""),
            str(row.get("pm_crew_members") or ""),
            str(row.get("notes") or ""),
        ]

        x = left
        for idx, value in enumerate(values):
            pdf.rect(x, y - row_height, col_widths[idx], row_height, stroke=1, fill=0)
            text_y = y - 12
            pdf.setFont("Helvetica-Bold" if idx == 0 else "Helvetica", 7)
            draw_wrapped_text(
                pdf,
                value,
                x + 4,
                text_y,
                col_widths[idx] - 8,
                line_height=8,
                font_name="Helvetica-Bold" if idx == 0 else "Helvetica",
                font_size=7,
            )
            x += col_widths[idx]

        y -= row_height

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def render_contractor_weekly_schedule_form(contractor_id: int, contractor_name: str, owner_view: bool = False):
    st.markdown("### Contractor Weekly Plan")
    st.caption("Enter, edit, or delete where the contractor and crew plan to be for each day of the selected week.")

    selected_week_date = st.date_input(
        "Select Week",
        value=datetime.now().date(),
        key=f"contractor_weekly_schedule_week_{contractor_id}_{'owner' if owner_view else 'contractor'}",
    )
    week_start = selected_week_date - __import__("datetime").timedelta(days=selected_week_date.weekday())
    week_end = week_start + __import__("datetime").timedelta(days=6)

    st.write(f"**Week:** {week_start.strftime('%m-%d-%Y')} through {week_end.strftime('%m-%d-%Y')}")
    st.write(f"**Contractor:** {contractor_name}")

    existing_df = contractor_weekly_schedule_df(
        contractor_id=contractor_id,
        week_start_date=week_start.strftime("%Y-%m-%d"),
    )
    existing_by_day = {}
    if not existing_df.empty:
        for _, existing_row in existing_df.iterrows():
            existing_by_day[str(existing_row.get("day_name") or "")] = existing_row

    st.markdown("#### Weekly Schedule Controls")
    control_1, control_2 = st.columns(2)
    confirm_week_key = f"confirm_delete_contractor_week_{contractor_id}_{week_start}_{'owner' if owner_view else 'contractor'}"
    if confirm_week_key not in st.session_state:
        st.session_state[confirm_week_key] = False

    if existing_df.empty:
        control_1.info("No saved schedule exists for this week yet.")
    else:
        if not st.session_state[confirm_week_key]:
            if control_1.button(
                "Delete Entire Saved Week",
                type="secondary",
                key=f"delete_contractor_week_btn_{contractor_id}_{week_start}_{'owner' if owner_view else 'contractor'}",
            ):
                st.session_state[confirm_week_key] = True
                st.rerun()
        else:
            control_1.warning("Delete the full saved week?")
            w1, w2 = control_1.columns(2)
            if w1.button(
                "Yes, Delete Week",
                type="primary",
                key=f"confirm_delete_contractor_week_yes_{contractor_id}_{week_start}_{'owner' if owner_view else 'contractor'}",
            ):
                delete_contractor_weekly_schedule_week(contractor_id, week_start)
                st.session_state[confirm_week_key] = False
                st.success("Full saved week deleted.")
                st.rerun()
            if w2.button(
                "Cancel",
                key=f"confirm_delete_contractor_week_cancel_{contractor_id}_{week_start}_{'owner' if owner_view else 'contractor'}",
            ):
                st.session_state[confirm_week_key] = False
                st.rerun()

    control_2.caption("To edit a saved weekly schedule, change any fields below and click Save Weekly Contractor Schedule.")

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pdf_rows = []
    for day_offset, day_name in enumerate(day_names):
        day_date = week_start + __import__("datetime").timedelta(days=day_offset)
        existing = existing_by_day.get(day_name)
        pdf_rows.append(
            {
                "day_name": day_name,
                "day_label": f"{day_name} — {day_date.strftime('%m-%d-%Y')}",
                "am_project_name": str(existing.get("am_project_name") or "") if existing is not None else "",
                "am_crew_members": str(existing.get("am_crew_members") or "") if existing is not None else "",
                "pm_project_name": str(existing.get("pm_project_name") or "") if existing is not None else "",
                "pm_crew_members": str(existing.get("pm_crew_members") or "") if existing is not None else "",
                "notes": str(existing.get("notes") or "") if existing is not None else "",
            }
        )

    safe_contractor_name = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_"
        for ch in str(contractor_name).strip()
    ).strip("_") or "contractor"

    control_2.download_button(
        "Download Weekly Schedule PDF",
        data=build_contractor_weekly_schedule_pdf(
            contractor_name=contractor_name,
            week_start=week_start,
            week_end=week_end,
            schedule_rows=pdf_rows,
        ),
        file_name=f"{safe_contractor_name}_weekly_schedule_{week_start.strftime('%Y-%m-%d')}.pdf",
        mime="application/pdf",
        key=f"download_contractor_weekly_schedule_pdf_{contractor_id}_{week_start}_{'owner' if owner_view else 'contractor'}",
    )

    schedule_rows = []

    for day_offset, day_name in enumerate(day_names):
        day_date = week_start + __import__("datetime").timedelta(days=day_offset)
        existing = existing_by_day.get(day_name)
        st.markdown("---")
        day_title_cols = st.columns([4, 1.6])
        day_title_cols[0].markdown(f"#### {day_name} — {day_date.strftime('%m-%d-%Y')}")

        confirm_day_key = f"confirm_delete_contractor_day_{contractor_id}_{week_start}_{day_name}_{'owner' if owner_view else 'contractor'}"
        if confirm_day_key not in st.session_state:
            st.session_state[confirm_day_key] = False

        if existing is not None:
            if not st.session_state[confirm_day_key]:
                if day_title_cols[1].button(
                    "Delete This Day",
                    type="secondary",
                    key=f"delete_contractor_day_btn_{contractor_id}_{week_start}_{day_name}_{'owner' if owner_view else 'contractor'}",
                ):
                    st.session_state[confirm_day_key] = True
                    st.rerun()
            else:
                day_title_cols[1].warning("Delete day?")
                d1, d2 = day_title_cols[1].columns(2)
                if d1.button(
                    "Yes",
                    type="primary",
                    key=f"confirm_delete_contractor_day_yes_{contractor_id}_{week_start}_{day_name}_{'owner' if owner_view else 'contractor'}",
                ):
                    delete_contractor_weekly_schedule_day(contractor_id, week_start, day_name)
                    st.session_state[confirm_day_key] = False
                    st.success(f"{day_name} deleted.")
                    st.rerun()
                if d2.button(
                    "No",
                    key=f"confirm_delete_contractor_day_no_{contractor_id}_{week_start}_{day_name}_{'owner' if owner_view else 'contractor'}",
                ):
                    st.session_state[confirm_day_key] = False
                    st.rerun()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**AM**")
            am_project = st.text_input(
                "Project Name",
                value=str(existing.get("am_project_name") or "") if existing is not None else "",
                key=f"weekly_{contractor_id}_{week_start}_{day_name}_am_project",
            )
            am_crew = st.text_area(
                "Crew Members",
                value=str(existing.get("am_crew_members") or "") if existing is not None else "",
                height=70,
                key=f"weekly_{contractor_id}_{week_start}_{day_name}_am_crew",
            )
        with c2:
            st.markdown("**PM**")
            pm_project = st.text_input(
                "Project Name",
                value=str(existing.get("pm_project_name") or "") if existing is not None else "",
                key=f"weekly_{contractor_id}_{week_start}_{day_name}_pm_project",
            )
            pm_crew = st.text_area(
                "Crew Members",
                value=str(existing.get("pm_crew_members") or "") if existing is not None else "",
                height=70,
                key=f"weekly_{contractor_id}_{week_start}_{day_name}_pm_crew",
            )

        day_notes = st.text_input(
            "Daily Notes",
            value=str(existing.get("notes") or "") if existing is not None else "",
            key=f"weekly_{contractor_id}_{week_start}_{day_name}_notes",
        )

        schedule_rows.append(
            {
                "day_name": day_name,
                "am_project_name": am_project.strip(),
                "am_crew_members": am_crew.strip(),
                "pm_project_name": pm_project.strip(),
                "pm_crew_members": pm_crew.strip(),
                "notes": day_notes.strip(),
            }
        )

    if st.button("Save Weekly Contractor Schedule", type="primary", key=f"save_weekly_contractor_schedule_{contractor_id}_{week_start}_{'owner' if owner_view else 'contractor'}"):
        save_contractor_weekly_schedule(
            contractor_id=contractor_id,
            contractor_name=contractor_name,
            week_start_date=week_start,
            schedule_rows=schedule_rows,
            submitted_by=str(st.session_state.get("logged_in_user", "") or ""),
        )
        st.success("Weekly contractor schedule saved.")
        st.rerun()

    st.markdown("### Saved Weekly Plan Preview")
    preview_rows = []
    for row in schedule_rows:
        preview_rows.append(
            {
                "Day": row["day_name"],
                "AM Project": row["am_project_name"],
                "AM Crew": row["am_crew_members"],
                "PM Project": row["pm_project_name"],
                "PM Crew": row["pm_crew_members"],
                "Notes": row["notes"],
            }
        )
    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)


def build_week_schedule_grid(schedule_df: pd.DataFrame, week_start: datetime.date) -> pd.DataFrame:
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    blocks = ["Full Day", "Morning", "Afternoon"]
    grid = pd.DataFrame("", index=blocks, columns=day_names)

    if schedule_df.empty:
        return grid

    temp = schedule_df.copy()
    temp["scheduled_date"] = pd.to_datetime(temp["scheduled_date"], errors="coerce")
    temp = temp.dropna(subset=["scheduled_date"])

    for _, row in temp.iterrows():
        day_offset = (row["scheduled_date"].date() - week_start).days
        if 0 <= day_offset <= 6:
            col = day_names[day_offset]
            block = row["time_block"] if row["time_block"] in blocks else "Full Day"
            entry_text = str(row["project_name"])
            if str(row.get("crew_name", "")).strip():
                entry_text += f"\n{row['crew_name']}"
            if str(row.get("notes", "")).strip():
                entry_text += f"\n{row['notes']}"
            if grid.at[block, col]:
                grid.at[block, col] += "\n\n" + entry_text
            else:
                grid.at[block, col] = entry_text

    return grid


def render_week_schedule_html(
    schedule_df: pd.DataFrame,
    week_start: datetime.date,
    enlarged: bool = False,
    title: str = "Weekly Renovation Schedule",
) -> str:
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    blocks = ["Full Day", "Morning", "Afternoon"]
    grid = build_week_schedule_grid(schedule_df, week_start)

    cell_min_height = 260 if enlarged else 170
    font_size = 15 if enlarged else 12
    header_font_size = 16 if enlarged else 13
    title_font_size = 24 if enlarged else 20

    date_labels = {}
    for i, day in enumerate(day_names):
        date_value = week_start + __import__("datetime").timedelta(days=i)
        date_labels[day] = date_value.strftime("%m-%d")

    html_parts = [
        "<style>",
        ".schedule-wrap { width: 100%; overflow-x: auto; }",
        ".schedule-title { font-size: %dpx; font-weight: 700; margin-bottom: 8px; }" % title_font_size,
        ".schedule-subtitle { font-size: 14px; margin-bottom: 14px; }",
        ".schedule-table { width: 100%; border-collapse: collapse; table-layout: fixed; }",
        ".schedule-table th, .schedule-table td { border: 1px solid #cfcfcf; vertical-align: top; padding: 8px; }",
        ".schedule-table th { background: #f4f4f4; font-size: %dpx; }" % header_font_size,
        ".schedule-row-header { background: #fafafa; font-weight: 700; width: 110px; }",
        ".schedule-cell { min-height: %dpx; font-size: %dpx; line-height: 1.35; white-space: pre-wrap; }" % (cell_min_height, font_size),
        ".schedule-entry { background: #f8fbff; border: 1px solid #d6e6ff; border-radius: 6px; padding: 6px; margin-bottom: 8px; }",
        ".schedule-entry:last-child { margin-bottom: 0; }",
        ".schedule-print-note { margin-top: 12px; font-size: 12px; color: #555; }",
        "@media print { .schedule-wrap { overflow: visible; } .schedule-table th, .schedule-table td { border: 1px solid #999; } }",
        "</style>",
        '<div class="schedule-wrap">',
        f'<div class="schedule-title">{html.escape(title)}</div>',
        f'<div class="schedule-subtitle">Week of {week_start.strftime("%m-%d-%Y")} through {(week_start + __import__("datetime").timedelta(days=6)).strftime("%m-%d-%Y")}</div>',
        '<table class="schedule-table">',
        "<tr><th></th>",
    ]

    for day in day_names:
        html_parts.append(f"<th>{html.escape(day)}<br>{html.escape(date_labels[day])}</th>")
    html_parts.append("</tr>")

    for block in blocks:
        html_parts.append(f'<tr><td class="schedule-row-header">{html.escape(block)}</td>')
        for day in day_names:
            raw_text = str(grid.at[block, day]) if day in grid.columns else ""
            entries = [e.strip() for e in raw_text.split("\n\n") if e.strip()]
            if entries:
                entry_html = "".join(
                    f'<div class="schedule-entry">{html.escape(entry).replace(chr(10), "<br>")}</div>'
                    for entry in entries
                )
            else:
                entry_html = "&nbsp;"
            html_parts.append(f'<td><div class="schedule-cell">{entry_html}</div></td>')
        html_parts.append("</tr>")

    html_parts.append("</table></div>")
    return "".join(html_parts)


def build_printable_schedule_html(
    schedule_df: pd.DataFrame,
    week_start: datetime.date,
    enlarged: bool = True,
    title: str = "Printable Renovation Schedule",
) -> bytes:
    html_text = render_week_schedule_html(
        schedule_df=schedule_df,
        week_start=week_start,
        enlarged=enlarged,
        title=title,
    )
    printable = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>"
        + html.escape(title)
        + "</title></head><body>"
        + html_text
        + "<div class='schedule-print-note'>Open this file in a browser and use Print.</div>"
        + "</body></html>"
    )
    return printable.encode("utf-8")


def _wrap_pdf_text_lines(pdf, text_value, max_width, font_name="Helvetica", font_size=8):
    text_value = "" if text_value is None else str(text_value)
    raw_lines = text_value.split("\n")
    wrapped = []
    for raw_line in raw_lines:
        words = raw_line.split()
        if not words:
            wrapped.append("")
            continue
        current = ""
        for word in words:
            test = f"{current} {word}".strip()
            if pdf.stringWidth(test, font_name, font_size) <= max_width:
                current = test
            else:
                if current:
                    wrapped.append(current)
                current = word
        if current:
            wrapped.append(current)
    return wrapped or [""]


def build_printable_schedule_pdf(
    schedule_df: pd.DataFrame,
    week_start: datetime.date,
    enlarged: bool = True,
    title: str = "Printable Renovation Schedule",
) -> bytes:
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    blocks = ["Full Day", "Morning", "Afternoon"]
    grid = build_week_schedule_grid(schedule_df, week_start)

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    width, height = landscape(letter)

    left_margin = 28
    right_margin = 28
    top_margin = 34
    bottom_margin = 28
    row_header_width = 78
    col_width = (width - left_margin - right_margin - row_header_width) / 7.0
    header_height = 34
    row_height = (height - top_margin - bottom_margin - 66 - header_height) / 3.0

    def draw_page_header():
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(left_margin, height - top_margin, title)
        pdf.setFont("Helvetica", 10)
        subtitle = f"Week of {week_start.strftime('%m-%d-%Y')} through {(week_start + __import__('datetime').timedelta(days=6)).strftime('%m-%d-%Y')}"
        pdf.drawString(left_margin, height - top_margin - 16, subtitle)

    def draw_grid_frame(start_y):
        pdf.setFont("Helvetica-Bold", 9)
        pdf.rect(left_margin, start_y - header_height, row_header_width, header_height)
        for i, day in enumerate(day_names):
            x = left_margin + row_header_width + i * col_width
            pdf.rect(x, start_y - header_height, col_width, header_height)
            pdf.drawCentredString(x + col_width / 2, start_y - 14, day)
            date_value = week_start + __import__('datetime').timedelta(days=i)
            pdf.setFont("Helvetica", 8)
            pdf.drawCentredString(x + col_width / 2, start_y - 25, date_value.strftime("%m-%d"))
            pdf.setFont("Helvetica-Bold", 9)

        current_y = start_y - header_height
        for block in blocks:
            current_y -= row_height
            pdf.rect(left_margin, current_y, row_header_width, row_height)
            pdf.drawCentredString(left_margin + row_header_width / 2, current_y + row_height - 16, block)
            for i in range(7):
                x = left_margin + row_header_width + i * col_width
                pdf.rect(x, current_y, col_width, row_height)

    draw_page_header()
    grid_top_y = height - top_margin - 30
    draw_grid_frame(grid_top_y)

    usable_cell_width = col_width - 10
    line_height = 9

    for row_idx, block in enumerate(blocks):
        cell_y_top = grid_top_y - header_height - row_idx * row_height
        for col_idx, day in enumerate(day_names):
            x = left_margin + row_header_width + col_idx * col_width
            raw_text = str(grid.at[block, day]) if day in grid.columns else ""
            entries = [e.strip() for e in raw_text.split("\n\n") if e.strip()]
            current_y = cell_y_top - 12

            for entry in entries:
                wrapped_lines = _wrap_pdf_text_lines(pdf, entry, usable_cell_width, font_name="Helvetica", font_size=8)
                needed_height = len(wrapped_lines) * line_height + 8

                if current_y - needed_height < cell_y_top - row_height + 4:
                    break

                text_y = current_y
                pdf.setFont("Helvetica", 8)
                for line in wrapped_lines:
                    pdf.drawString(x + 8, text_y, line)
                    text_y -= line_height

                separator_y = text_y - 2
                pdf.line(x + 4, separator_y, x + col_width - 4, separator_y)

                current_y = separator_y - 8

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def reset_estimate_editor():
    st.session_state.pending_builder_reset = True


def load_estimate_into_editor(estimate_id: int):
    header_df = fetch_df(
        """
        SELECT
            e.id,
            COALESCE(e.estimate_name, '') AS estimate_name,
            COALESCE(e.estimate_address, '') AS estimate_address,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(c.address, '') AS contractor_address,
            COALESCE(e.notes, '') AS notes
        FROM estimates e
        LEFT JOIN contractors c ON c.id = e.contractor_id
        WHERE e.id = ?
        """,
        (estimate_id,),
    )
    if header_df.empty:
        return False

    header = header_df.iloc[0]
    lines_df = estimate_lines_df(estimate_id)
    photo_map = line_photo_map(estimate_id)

    cart = []
    for _, row in lines_df.iterrows():
        line_id = int(row["id"])
        cart.append(
            {
                "db_line_id": line_id,
                "trade_name": row["trade_name"],
                "task_name": row["task_name"],
                "scope_description": row["scope_description"],
                "repair_quantity": int(row["repair_quantity"]) if pd.notna(row["repair_quantity"]) else 1,
                "onsite_hours_each": float(row["onsite_hours_each"]),
                "travel_hours_each": float(row["travel_hours_each"]),
                "total_hours_each": float(row["total_hours_each"]),
                "onsite_hours": float(row["onsite_hours"]),
                "travel_hours": float(row["travel_hours"]),
                "total_hours": float(row["total_hours"]),
                "labor_rate": float(row["labor_rate"]),
                "onsite_cost": float(row["onsite_cost"]),
                "travel_cost": float(row["travel_cost"]),
                "manual_repair_amount": float(row["manual_repair_amount"]) if pd.notna(row["manual_repair_amount"]) else 0.0,
                "hourly_calculated_amount": float(row["onsite_cost"]) + float(row["travel_cost"]),
                "total_labor_cost": float(row["total_labor_cost"]),
                "created_at": row.get("created_at"),
                "modified_at": row.get("modified_at"),
                "photos": photo_map.get(line_id, []),
            }
        )

    contractor_name = str(header["contractor_name"]).strip()
    st.session_state.editing_estimate_id = int(header["id"])
    st.session_state.builder_estimate_name = str(header["estimate_name"])
    st.session_state.builder_estimate_address = str(header["estimate_address"])
    st.session_state.builder_project_id = int(header["project_id"]) if pd.notna(header.get("project_id")) else None
    st.session_state.builder_selected_contractor = contractor_name if contractor_name else "None selected"
    st.session_state.builder_contractor_address = str(header["contractor_address"])
    st.session_state.builder_estimate_notes = str(header["notes"])
    labels = project_registry_select_labels(active_only=True)
    if st.session_state.get("builder_project_id"):
        project_row = get_project_registry_row(int(st.session_state.get("builder_project_id")))
        if project_row is not None:
            st.session_state.builder_project_select = f"{int(project_row['id'])} | {project_row['project_name']}"
        else:
            st.session_state.builder_project_select = labels[0] if labels else ""
    else:
        st.session_state.builder_project_select = labels[0] if labels else ""
    st.session_state.builder_project_select_applied = st.session_state.builder_project_select
    st.session_state.estimate_cart = cart
    st.session_state.pending_page = "Estimate Builder"
    return True



@st.cache_data(show_spinner=False, ttl=300)
def punch_list_projects_df(contractor_id: int | None = None) -> pd.DataFrame:
    query = """
        SELECT
            p.id,
            COALESCE(p.order_number, 'PL' || p.id::text) AS order_number,
            COALESCE(p.project_name, '') AS project_name,
            COALESCE(p.project_address, '') AS project_address,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(p.contractor_id, 0) AS contractor_id,
            COALESCE(p.status, 'Open') AS status,
            p.inspection_date,
            p.deadline_date,
            COALESCE(p.notes, '') AS notes,
            COALESCE(p.created_by, '') AS created_by,
            p.created_at,
            p.modified_at
        FROM punch_list_projects p
        LEFT JOIN contractors c ON c.id = p.contractor_id
    """
    params = []
    if contractor_id:
        query += """
        WHERE COALESCE(p.contractor_id, 0) = ?
           OR EXISTS (
                SELECT 1 FROM punch_list_items i
                WHERE i.project_id = p.id AND COALESCE(i.contractor_id, 0) = ?
           )
        """
        params.extend([contractor_id, contractor_id])
    query += " ORDER BY p.modified_at DESC NULLS LAST, p.id DESC"
    return fetch_df(query, tuple(params))


@st.cache_data(show_spinner=False, ttl=300)
def punch_list_items_df(project_id: int, contractor_id: int | None = None) -> pd.DataFrame:
    query = """
        SELECT
            i.id,
            COALESCE(i.order_number, 'PLWG' || i.id::text) AS order_number,
            i.project_id,
            COALESCE(i.item_title, '') AS item_title,
            COALESCE(i.trade_name, '') AS trade_name,
            COALESCE(i.scope_description, '') AS scope_description,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(i.contractor_id, 0) AS contractor_id,
            COALESCE(i.item_status, 'Open') AS item_status,
            i.identified_date,
            i.deadline_date,
            i.completed_date,
            COALESCE(i.quote_requested, FALSE) AS quote_requested,
            COALESCE(i.manager_notes, '') AS manager_notes,
            COALESCE(i.contractor_notes, '') AS contractor_notes,
            i.created_at,
            i.modified_at
        FROM punch_list_items i
        LEFT JOIN contractors c ON c.id = i.contractor_id
        WHERE i.project_id = ?
    """
    params = [project_id]
    if contractor_id:
        query += " AND COALESCE(i.contractor_id, 0) = ?"
        params.append(contractor_id)
    query += " ORDER BY i.id"
    return fetch_df(query, tuple(params))


def build_punch_list_report_pdf(project_id: int, status_filter: str = "all"):
    project_df = fetch_df(
        """
        SELECT
            p.id,
            COALESCE(p.project_name, '') AS project_name,
            COALESCE(p.project_address, '') AS project_address,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(p.status, 'Open') AS status,
            p.inspection_date,
            p.deadline_date,
            COALESCE(p.notes, '') AS notes
        FROM punch_list_projects p
        LEFT JOIN contractors c ON c.id = p.contractor_id
        WHERE p.id = ?
        LIMIT 1
        """,
        (project_id,),
    )
    if project_df.empty:
        return None

    items_df = punch_list_items_df(project_id)
    if status_filter == "open":
        items_df = items_df[items_df["item_status"].fillna("").astype(str) != "Complete"].copy()
    elif status_filter == "completed":
        items_df = items_df[items_df["item_status"].fillna("").astype(str) == "Complete"].copy()

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    left = 40
    y = height - 40
    header = project_df.iloc[0]

    title = "Punch List Report"
    if status_filter == "open":
        title = "Open Punch List Report"
    elif status_filter == "completed":
        title = "Completed Punch List Report"

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(left, y, title)
    y -= 18
    pdf.setFont("Helvetica", 10)
    pdf.drawString(left, y, f"Project: {header['project_name']}")
    y -= 14
    pdf.drawString(left, y, f"Address: {header['project_address']}")
    y -= 14
    pdf.drawString(left, y, f"Contractor: {header['contractor_name']}")
    y -= 14
    inspection_text = pd.to_datetime(header["inspection_date"], errors="coerce")
    deadline_text = pd.to_datetime(header["deadline_date"], errors="coerce")
    pdf.drawString(left, y, f"Inspection Date: {inspection_text.strftime('%m-%d-%Y') if pd.notna(inspection_text) else ''}")
    y -= 14
    pdf.drawString(left, y, f"Deadline: {deadline_text.strftime('%m-%d-%Y') if pd.notna(deadline_text) else ''}")
    y -= 18

    if str(header["notes"]).strip():
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(left, y, "Project Notes:")
        y -= 12
        pdf.setFont("Helvetica", 9)
        y = draw_wrapped_text(pdf, header["notes"], left, y, width - 80, line_height=10, font_name="Helvetica", font_size=9) - 4

    pdf.line(left, y, width - 40, y)
    y -= 14

    if items_df.empty:
        pdf.setFont("Helvetica", 10)
        pdf.drawString(left, y, "No items found for this report.")
    else:
        for idx, (_, row) in enumerate(items_df.iterrows(), start=1):
            if y < 100:
                pdf.showPage()
                y = height - 40

            pdf.setFont("Helvetica-Bold", 10)
            pdf.drawString(left, y, f"Item {idx}: {row['item_title']}")
            y -= 12
            pdf.setFont("Helvetica", 9)
            pdf.drawString(left, y, f"Trade: {row['trade_name']}")
            pdf.drawString(left + 150, y, f"Status: {row['item_status']}")
            pdf.drawString(left + 290, y, f"Quote Requested: {'Yes' if bool(row['quote_requested']) else 'No'}")
            y -= 12

            ident = pd.to_datetime(row["identified_date"], errors="coerce")
            ddl = pd.to_datetime(row["deadline_date"], errors="coerce")
            comp = pd.to_datetime(row["completed_date"], errors="coerce")
            pdf.drawString(left, y, f"Date Reference: {ident.strftime('%m-%d-%Y') if pd.notna(ident) else ''}")
            pdf.drawString(left + 190, y, f"Deadline: {ddl.strftime('%m-%d-%Y') if pd.notna(ddl) else ''}")
            pdf.drawString(left + 350, y, f"Completed: {comp.strftime('%m-%d-%Y') if pd.notna(comp) else ''}")
            y -= 12

            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(left, y, "Punch Item:")
            y -= 11
            pdf.setFont("Helvetica", 8)
            y = draw_wrapped_text(pdf, row["scope_description"], left, y, width - 80, line_height=9, font_name="Helvetica", font_size=8)

            if str(row["manager_notes"]).strip():
                pdf.setFont("Helvetica-Bold", 9)
                pdf.drawString(left, y, "Manager Notes:")
                y -= 11
                pdf.setFont("Helvetica", 8)
                y = draw_wrapped_text(pdf, row["manager_notes"], left, y, width - 80, line_height=9, font_name="Helvetica", font_size=8)

            if str(row["contractor_notes"]).strip():
                pdf.setFont("Helvetica-Bold", 9)
                pdf.drawString(left, y, "Contractor Notes:")
                y -= 11
                pdf.setFont("Helvetica", 8)
                y = draw_wrapped_text(pdf, row["contractor_notes"], left, y, width - 80, line_height=9, font_name="Helvetica", font_size=8)

            y -= 6
            pdf.line(left, y, width - 40, y)
            y -= 12

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def get_task_trade_lookup_df() -> pd.DataFrame:
    df = get_task_lookup_df().copy()
    return df.rename(columns={"task_name": "name"})[["name", "trade_name"]].drop_duplicates().copy()


def punch_list_task_options() -> list[str]:
    df = get_task_trade_lookup_df()
    if df.empty:
        return []
    return sorted(df["name"].dropna().astype(str).unique().tolist())


def punch_list_trade_options_for_task(task_name: str) -> list[str]:
    df = get_task_trade_lookup_df()
    task_name = str(task_name or "").strip()
    if df.empty or not task_name:
        return []
    filtered = df[df["name"].fillna("").astype(str).str.strip().str.lower() == task_name.lower()]
    if filtered.empty:
        return []
    return sorted(filtered["trade_name"].dropna().astype(str).unique().tolist())



def punch_list_item_photo_row_to_dict(row) -> dict:
    photo_bytes = row.get("photo_bytes")
    if photo_bytes is not None and not isinstance(photo_bytes, (bytes, bytearray)):
        try:
            photo_bytes = bytes(photo_bytes)
        except Exception:
            photo_bytes = None
    return {
        "id": int(row["id"]) if pd.notna(row.get("id")) else None,
        "filename": str(row.get("photo_filename") or "photo"),
        "content_type": str(row.get("content_type") or "image/jpeg"),
        "storage_mode": str(row.get("storage_mode") or "database"),
        "blob_url": str(row.get("blob_url") or ""),
        "blob_name": str(row.get("blob_name") or ""),
        "bytes": photo_bytes,
        "sort_order": int(row.get("sort_order") or 0),
    }


@st.cache_data(show_spinner=False, ttl=300)
def punch_list_item_photos(item_id: int) -> list[dict]:
    df = fetch_df(
        """
        SELECT
            id,
            punch_list_item_id,
            COALESCE(photo_filename, '') AS photo_filename,
            COALESCE(content_type, 'image/jpeg') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            photo_bytes,
            COALESCE(sort_order, 0) AS sort_order
        FROM punch_list_item_photos
        WHERE punch_list_item_id = ?
        ORDER BY sort_order, id
        """,
        (item_id,),
    )
    if df.empty:
        return []
    return [punch_list_item_photo_row_to_dict(row.to_dict()) for _, row in df.iterrows()]


def save_punch_list_item_photos(project_id: int, item_id: int, uploaded_files):
    photos = normalize_uploaded_photos(uploaded_files)
    for idx, photo in enumerate(photos):
        execute(
            """
            INSERT INTO punch_list_item_photos (
                project_id, punch_list_item_id, photo_filename, content_type,
                storage_mode, blob_url, blob_name, photo_bytes, sort_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                item_id,
                str(photo.get("filename") or "photo"),
                str(photo.get("content_type") or "image/jpeg"),
                str(photo.get("storage_mode") or "database"),
                str(photo.get("blob_url") or ""),
                str(photo.get("blob_name") or ""),
                photo.get("bytes"),
                int(photo.get("sort_order", idx)),
            ),
        )




def project_status_project_options_df() -> pd.DataFrame:
    df = project_registry_active_df().copy()
    if df.empty:
        return df
    df["label"] = df.apply(lambda r: f"{int(r['id'])} | {r['project_name']}", axis=1)
    return df


@st.cache_data(show_spinner=False, ttl=300)
def project_status_entries_df(source_type: str, source_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            source_type,
            source_id,
            COALESCE(project_name, '') AS project_name,
            entry_date,
            COALESCE(note_text, '') AS note_text,
            COALESCE(created_by, '') AS created_by,
            created_at,
            modified_at
        FROM project_status_entries
        WHERE source_type = ? AND source_id = ?
        ORDER BY entry_date DESC, created_at DESC, id DESC
        """,
        (source_type, source_id),
    )


def project_status_photo_row_to_dict(row) -> dict:
    photo_bytes = row.get("photo_bytes")
    if photo_bytes is not None and not isinstance(photo_bytes, (bytes, bytearray)):
        try:
            photo_bytes = bytes(photo_bytes)
        except Exception:
            photo_bytes = None
    return {
        "id": int(row["id"]) if pd.notna(row.get("id")) else None,
        "filename": str(row.get("photo_filename") or "photo"),
        "content_type": str(row.get("content_type") or "image/jpeg"),
        "storage_mode": str(row.get("storage_mode") or "database"),
        "blob_url": str(row.get("blob_url") or ""),
        "blob_name": str(row.get("blob_name") or ""),
        "bytes": photo_bytes,
        "sort_order": int(row.get("sort_order") or 0),
    }


@st.cache_data(show_spinner=False, ttl=300)
def project_status_photos(entry_id: int) -> list[dict]:
    df = fetch_df(
        """
        SELECT
            id,
            status_entry_id,
            COALESCE(photo_filename, '') AS photo_filename,
            COALESCE(content_type, 'image/jpeg') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            photo_bytes,
            COALESCE(sort_order, 0) AS sort_order
        FROM project_status_photos
        WHERE status_entry_id = ?
        ORDER BY sort_order, id
        """,
        (entry_id,),
    )
    if df.empty:
        return []
    return [project_status_photo_row_to_dict(row.to_dict()) for _, row in df.iterrows()]


def save_project_status_photos(entry_id: int, uploaded_files):
    photos = normalize_uploaded_photos(uploaded_files)
    for idx, photo in enumerate(photos):
        execute(
            """
            INSERT INTO project_status_photos (
                status_entry_id, photo_filename, content_type, storage_mode,
                blob_url, blob_name, photo_bytes, sort_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry_id,
                str(photo.get("filename") or "photo"),
                str(photo.get("content_type") or "image/jpeg"),
                str(photo.get("storage_mode") or "database"),
                str(photo.get("blob_url") or ""),
                str(photo.get("blob_name") or ""),
                photo.get("bytes"),
                int(photo.get("sort_order", idx)),
            ),
        )


def contractor_estimates_df(contractor_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            e.id AS estimate_id,
            COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
            COALESCE(e.estimate_address, '') AS estimate_address,
            CASE WHEN COALESCE(e.active, TRUE) THEN 'Active' ELSE 'Archived' END AS status,
            COALESCE(SUM(CASE WHEN COALESCE(el.manual_repair_amount, 0) > 0 THEN 0 ELSE el.total_hours END), 0) AS contractor_total_hours,
            COALESCE(SUM(CASE WHEN COALESCE(el.manual_repair_amount, 0) > 0 THEN el.manual_repair_amount ELSE 0 END), 0) AS contractor_manual_total,
            COALESCE(SUM(el.total_labor_cost), 0) AS contractor_amount_used_total
        FROM estimates e
        JOIN estimate_lines el ON el.estimate_id = e.id
        WHERE COALESCE(el.contractor_id, 0) = ?
        GROUP BY e.id, e.estimate_name, e.estimate_address, e.active
        ORDER BY LOWER(COALESCE(e.estimate_name, '(unnamed)')), e.id DESC
        """,
        (contractor_id,),
    )


def contractor_estimate_lines_df(estimate_id: int, contractor_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            el.id,
            COALESCE(el.task_name, '') AS task_name,
            COALESCE(el.trade_name, '') AS trade_name,
            COALESCE(el.scope_description, '') AS scope_description,
            COALESCE(el.repair_quantity, 0) AS repair_quantity,
            COALESCE(el.onsite_hours, 0) AS onsite_hours,
            COALESCE(el.travel_hours, 0) AS travel_hours,
            COALESCE(el.total_hours, 0) AS total_hours,
            COALESCE(el.labor_rate, 0) AS labor_rate,
            COALESCE(el.manual_repair_amount, 0) AS manual_repair_amount,
            COALESCE(el.total_labor_cost, 0) AS amount_used
        FROM estimate_lines el
        WHERE el.estimate_id = ? AND COALESCE(el.contractor_id, 0) = ?
        ORDER BY el.id
        """,
        (estimate_id, contractor_id),
    )


def contractor_punch_list_projects_df(contractor_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            p.id,
            COALESCE(p.order_number, 'PL' || p.id::text) AS order_number,
            COALESCE(pr.project_code, '') AS project_code,
            COALESCE(p.project_name, '') AS project_name,
            COALESCE(p.project_address, '') AS project_address,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(p.status, 'Open') AS status,
            p.inspection_date,
            p.deadline_date,
            COALESCE(p.notes, '') AS notes
        FROM punch_list_projects p
        LEFT JOIN project_registry pr ON pr.id = p.project_id
        LEFT JOIN contractors c ON c.id = p.contractor_id
        WHERE COALESCE(p.contractor_id, 0) = ?
           OR EXISTS (
                SELECT 1
                FROM punch_list_items i
                WHERE i.project_id = p.id
                  AND COALESCE(i.contractor_id, 0) = ?
           )
        ORDER BY COALESCE(p.project_name, ''), p.id DESC
        """,
        (contractor_id, contractor_id),
    )



@st.cache_data(show_spinner=False, ttl=900)
def get_task_lookup_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            tasks.id,
            COALESCE(tasks.work_item_code, '') AS work_item_code,
            COALESCE(tasks.name, '') AS task_name,
            COALESCE(trades.name, '') AS trade_name
        FROM tasks
        JOIN trades ON trades.id = tasks.trade_id
        WHERE tasks.active = TRUE
        ORDER BY LOWER(tasks.name), LOWER(trades.name)
        """
    )


@st.cache_data(show_spinner=False, ttl=900)
def get_trade_list_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT id, COALESCE(name, '') AS name
        FROM trades
        ORDER BY LOWER(name)
        """
    )


@st.cache_data(show_spinner=False, ttl=900)
def get_contractor_list_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT id, COALESCE(name, '') AS name
        FROM contractors
        WHERE COALESCE(active, TRUE) = TRUE
        ORDER BY LOWER(name)
        """
    )


@st.cache_data(show_spinner=False, ttl=900)
def get_project_status_project_options_df_cached() -> pd.DataFrame:
    estimates_df = fetch_df(
        """
        SELECT
            'Estimate' AS source_type,
            id AS source_id,
            COALESCE(estimate_name, '(unnamed)') AS project_name,
            COALESCE(estimate_address, '') AS project_address
        FROM estimates
        ORDER BY LOWER(COALESCE(estimate_name, '(unnamed)')), id
        """
    )
    punch_df = fetch_df(
        """
        SELECT
            'Punch List' AS source_type,
            id AS source_id,
            COALESCE(project_name, '(unnamed)') AS project_name,
            COALESCE(project_address, '') AS project_address
        FROM punch_list_projects
        ORDER BY LOWER(COALESCE(project_name, '(unnamed)')), id
        """
    )
    combined = pd.concat([estimates_df, punch_df], ignore_index=True)
    if combined.empty:
        return combined
    combined["label"] = combined.apply(
        lambda r: f"{r['source_type']} | {int(r['source_id'])} | {r['project_name']}",
        axis=1,
    )
    return combined


@st.cache_data(show_spinner=False, ttl=900)
def get_estimate_history_df_cached() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            e.id AS estimate_id,
            COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
            COALESCE(e.estimate_address, '') AS estimate_address,
            COALESCE(STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name), '') AS contractor_name,
            CASE WHEN COALESCE(e.active, TRUE) THEN 'Active' ELSE 'Inactive' END AS status,
            e.created_at,
            e.modified_at,
            COALESCE(e.labor_rate, 0) AS labor_rate,
            COALESCE(SUM(el.total_hours), 0) AS total_hours,
            COALESCE(SUM(el.total_labor_cost), 0) AS total_labor_cost
        FROM estimates e
        LEFT JOIN estimate_lines el ON el.estimate_id = e.id
        LEFT JOIN contractors c ON c.id = el.contractor_id
        GROUP BY e.id, e.estimate_name, e.estimate_address, e.active, e.created_at, e.modified_at, e.labor_rate
        ORDER BY e.modified_at DESC NULLS LAST, e.id DESC
        """
    )


@st.cache_data(show_spinner=False, ttl=900)
def get_recent_estimates_df_cached(limit: int = 10) -> pd.DataFrame:
    return fetch_df(
        f"""
        SELECT
            e.id AS estimate_id,
            e.created_at,
            COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
            COALESCE(STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name), '') AS contractor_name,
            CASE WHEN COALESCE(e.active, TRUE) THEN 'Active' ELSE 'Inactive' END AS status,
            COALESCE(e.labor_rate, 0) AS labor_rate,
            COALESCE(SUM(el.total_hours), 0) AS total_hours,
            COALESCE(SUM(el.total_labor_cost), 0) AS total_labor_cost
        FROM estimates e
        LEFT JOIN estimate_lines el ON el.estimate_id = e.id
        LEFT JOIN contractors c ON c.id = el.contractor_id
        GROUP BY e.id, e.created_at, e.estimate_name, e.active, e.labor_rate
        ORDER BY e.modified_at DESC NULLS LAST, e.id DESC
        LIMIT {int(limit)}
        """
    )




@st.cache_data(show_spinner=False, ttl=1800)
def project_registry_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            COALESCE(project_code, '') AS project_code,
            COALESCE(project_name, '') AS project_name,
            COALESCE(project_address, '') AS project_address,
            COALESCE(notes, '') AS notes,
            COALESCE(active, TRUE) AS active,
            COALESCE(deleted, FALSE) AS deleted,
            activated_at,
            COALESCE(final_project_cost, 0) AS final_project_cost,
            COALESCE(materials_notes, '') AS materials_notes,
            created_at,
            modified_at
        FROM project_registry
        WHERE COALESCE(active, TRUE) = TRUE
        ORDER BY LOWER(COALESCE(project_name, '')), id DESC
        """
    )


def project_registry_labels() -> list[str]:
    df = project_registry_df()
    if df.empty:
        return ["Enter New Project / Repair"]
    return ["Enter New Project / Repair"] + [
        f"{int(row.id)} | {row.project_name}" for row in df.itertuples()
    ]


def get_project_registry_row_from_label(label: str):
    if not label or label == "Enter New Project / Repair":
        return None
    try:
        project_id = int(str(label).split(" | ", 1)[0])
    except Exception:
        return None
    df = project_registry_df()
    matches = df[df["id"] == project_id]
    if matches.empty:
        return None
    return matches.iloc[0]




@st.cache_data(show_spinner=False, ttl=1800)
def project_registry_all_df() -> pd.DataFrame:
    df = fetch_df(
        """
        SELECT
            id,
            COALESCE(project_code, '') AS project_code,
            COALESCE(project_name, '') AS project_name,
            COALESCE(project_address, '') AS project_address,
            COALESCE(notes, '') AS notes,
            COALESCE(active, TRUE) AS active,
            COALESCE(deleted, FALSE) AS deleted,
            activated_at,
            COALESCE(final_project_cost, 0) AS final_project_cost,
            COALESCE(materials_notes, '') AS materials_notes,
            created_at,
            modified_at
        FROM project_registry
        ORDER BY COALESCE(active, TRUE) DESC, LOWER(COALESCE(project_name, '')), id DESC
        """
    )
    if "deleted" not in df.columns:
        df["deleted"] = False
    if "active" not in df.columns:
        df["active"] = True
    return df


def project_registry_active_df() -> pd.DataFrame:
    df = project_registry_all_df()
    if df.empty:
        return df
    if "deleted" not in df.columns:
        df["deleted"] = False
    if "active" not in df.columns:
        df["active"] = True
    return df[(df["active"].fillna(True) == True) & (df["deleted"].fillna(False) == False)].copy()


def project_registry_inactive_df() -> pd.DataFrame:
    df = project_registry_all_df()
    if df.empty:
        return df
    if "deleted" not in df.columns:
        df["deleted"] = False
    if "active" not in df.columns:
        df["active"] = True
    return df[(df["active"].fillna(True) == False) & (df["deleted"].fillna(False) == False)].copy()


def project_registry_select_labels(active_only: bool = True) -> list[str]:
    df = project_registry_active_df() if active_only else project_registry_all_df()
    if not df.empty and "deleted" in df.columns:
        df = df[df["deleted"].fillna(False) == False].copy()
    if df.empty:
        return []
    labels = []
    for row in df.to_dict("records"):
        if st.session_state.get("show_shared_ids") and str(row.get("project_code") or "").strip():
            labels.append(f"{int(row['id'])} | {row['project_code']} | {row['project_name']}")
        else:
            labels.append(f"{int(row['id'])} | {row['project_name']}")
    return labels

def get_project_registry_row(project_id: int):
    df = project_registry_all_df()
    if df.empty:
        return None
    if "deleted" in df.columns:
        df = df[df["deleted"].fillna(False) == False].copy()
    match = df[df["id"] == int(project_id)]
    if match.empty:
        return None
    return match.iloc[0]


def get_project_registry_row_from_label(label: str):
    if not label:
        return None
    try:
        project_id = int(str(label).split(" | ", 1)[0])
    except Exception:
        return None
    df = project_registry_all_df()
    if df.empty:
        return None
    if "deleted" not in df.columns:
        df["deleted"] = False
    match = df[df["id"] == int(project_id)]
    if match.empty:
        return None
    return match.iloc[0]


def project_material_file_row_to_dict(row) -> dict:
    file_bytes = row.get("file_bytes")
    if file_bytes is not None and not isinstance(file_bytes, (bytes, bytearray)):
        try:
            file_bytes = bytes(file_bytes)
        except Exception:
            file_bytes = None
    return {
        "id": int(row["id"]) if pd.notna(row.get("id")) else None,
        "filename": str(row.get("file_filename") or "file"),
        "content_type": str(row.get("content_type") or "application/octet-stream"),
        "storage_mode": str(row.get("storage_mode") or "database"),
        "blob_url": str(row.get("blob_url") or ""),
        "blob_name": str(row.get("blob_name") or ""),
        "bytes": file_bytes,
        "sort_order": int(row.get("sort_order") or 0),
        "uploaded_by": str(row.get("uploaded_by") or ""),
    }


def project_material_files(project_id: int) -> list[dict]:
    df = fetch_df(
        """
        SELECT
            id,
            project_id,
            COALESCE(file_filename, '') AS file_filename,
            COALESCE(content_type, 'application/octet-stream') AS content_type,
            COALESCE(storage_mode, 'database') AS storage_mode,
            COALESCE(blob_url, '') AS blob_url,
            COALESCE(blob_name, '') AS blob_name,
            file_bytes,
            COALESCE(sort_order, 0) AS sort_order,
            COALESCE(uploaded_by, '') AS uploaded_by
        FROM project_material_files
        WHERE project_id = ?
        ORDER BY sort_order, id
        """,
        (project_id,),
    )
    if df.empty:
        return []
    return [project_material_file_row_to_dict(row.to_dict()) for _, row in df.iterrows()]


def save_project_material_files(project_id: int, uploaded_files):
    for idx, uploaded in enumerate(uploaded_files or []):
        if uploaded is None:
            continue
        raw_bytes = uploaded.getvalue()
        if not raw_bytes:
            continue
        stored_file = upload_bytes_to_blob(
            data=raw_bytes,
            filename=getattr(uploaded, "name", "file"),
            content_type=getattr(uploaded, "type", None) or "application/octet-stream",
            folder="renovation-estimator/project-material-files",
        )
        execute(
            """
            INSERT INTO project_material_files (
                project_id, file_filename, content_type, storage_mode,
                blob_url, blob_name, file_bytes, sort_order, uploaded_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                str(stored_file.get("filename") or "file"),
                str(stored_file.get("content_type") or "application/octet-stream"),
                str(stored_file.get("storage_mode") or "database"),
                str(stored_file.get("blob_url") or ""),
                str(stored_file.get("blob_name") or ""),
                stored_file.get("bytes"),
                idx,
                str(st.session_state.get("logged_in_user", "") or ""),
            ),
        )


def pretty_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    renamed = {}
    for col in df.columns:
        renamed[col] = str(col).replace("_", " " ).title()
    return df.rename(columns=renamed)


def populate_project_links():
    projects_df = project_registry_all_df()
    if projects_df.empty:
        return

    project_lookup = {}
    for _, row in projects_df.iterrows():
        key = (str(row["project_name"]).strip().lower(), str(row["project_address"]).strip().lower())
        if key not in project_lookup:
            project_lookup[key] = int(row["id"])

    estimates = fetch_df(
        """
        SELECT id, COALESCE(estimate_name, '') AS estimate_name, COALESCE(estimate_address, '') AS estimate_address
        FROM estimates
        WHERE project_id IS NULL
        """
    )
    for _, row in estimates.iterrows():
        key = (str(row["estimate_name"]).strip().lower(), str(row["estimate_address"]).strip().lower())
        pid = project_lookup.get(key)
        if pid:
            execute("UPDATE estimates SET project_id = ? WHERE id = ?", (pid, int(row["id"])))

    punches = fetch_df(
        """
        SELECT id, COALESCE(project_name, '') AS project_name, COALESCE(project_address, '') AS project_address
        FROM punch_list_projects
        WHERE project_id IS NULL
        """
    )
    for _, row in punches.iterrows():
        key = (str(row["project_name"]).strip().lower(), str(row["project_address"]).strip().lower())
        pid = project_lookup.get(key)
        if pid:
            execute("UPDATE punch_list_projects SET project_id = ? WHERE id = ?", (pid, int(row["id"])))



@st.cache_data(show_spinner=False, ttl=900)
def get_project_status_entries_for_project_cached(project_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            id,
            COALESCE(project_name,'') AS project_name,
            entry_date,
            COALESCE(note_text,'') AS note_text,
            COALESCE(created_by,'') AS created_by,
            created_at,
            modified_at
        FROM project_status_entries
        WHERE COALESCE(project_id, 0) = ?
        ORDER BY entry_date DESC, created_at DESC, id DESC
        """,
        (project_id,),
    )


def require_login():
    if st.session_state.get("logged_in", False):
        return

    st.title("Renovation Management System Login")
    st.caption("Sign in to use the estimator")

    with st.container(border=True):
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Log In", use_container_width=True):
            user = authenticate_user(username, password)
            if user:
                st.session_state.logged_in = True
                st.session_state.logged_in_user = user["username"]
                st.session_state.logged_in_user_id = int(user.get("id", 0) or 0)
                st.session_state.logged_in_role = str(user.get("role", "Other"))
                st.session_state.logged_in_contractor_id = int(user.get("contractor_id") or 0)
                st.session_state.logged_in_allowed_portfolio = str(user.get("allowed_portfolio", "") or "")
                st.success("Login successful.")
                st.rerun()
            else:
                st.error("Invalid username or password.")

    st.stop()


# -----------------------------
# Streamlit app
# -----------------------------
st.set_page_config(page_title="Renovation Management System", layout="wide")

if "app_initialized" not in st.session_state:
    st.session_state.app_initialized = False

if not st.session_state.app_initialized:
    init_db()
    seed_defaults()
    sync_task_catalog_from_excel_if_needed()
    populate_project_links()
    st.session_state.app_initialized = True

if "estimate_cart" not in st.session_state:
    st.session_state.estimate_cart = []
if "editing_estimate_id" not in st.session_state:
    st.session_state.editing_estimate_id = None
if "builder_estimate_name" not in st.session_state:
    st.session_state.builder_estimate_name = ""
if "builder_estimate_address" not in st.session_state:
    st.session_state.builder_estimate_address = ""
if "builder_selected_contractor" not in st.session_state:
    st.session_state.builder_selected_contractor = "None selected"
if "builder_contractor_address" not in st.session_state:
    st.session_state.builder_contractor_address = ""
if "builder_estimate_notes" not in st.session_state:
    st.session_state.builder_estimate_notes = ""
if "logged_in_role" not in st.session_state:
    st.session_state.logged_in_role = ""
if "logged_in_contractor_id" not in st.session_state:
    st.session_state.logged_in_contractor_id = 0
if "logged_in_user_id" not in st.session_state:
    st.session_state.logged_in_user_id = 0
if "show_shared_ids" not in st.session_state:
    st.session_state.show_shared_ids = False
# Build 15A: default to RMR Entry for field/iPhone use.
# If the URL has a ?page=... value, use that as the starting page so Safari/Home Screen
# can reopen the last page used when possible.
def _get_query_page_default() -> str:
    try:
        query_page = st.query_params.get("page", "")
        if isinstance(query_page, list):
            query_page = query_page[0] if query_page else ""
        return str(query_page or "").strip()
    except Exception:
        return ""

if "menu_page" not in st.session_state:
    st.session_state.menu_page = _get_query_page_default() or "RMR Entry"
if "pending_page" not in st.session_state:
    st.session_state.pending_page = None
if "pending_builder_reset" not in st.session_state:
    st.session_state.pending_builder_reset = False
if "pending_repair_form_reset" not in st.session_state:
    st.session_state.pending_repair_form_reset = False
if "builder_task_name_select" not in st.session_state:
    st.session_state.builder_task_name_select = "Add A Work Item"
if "builder_trade_name_select" not in st.session_state:
    st.session_state.builder_trade_name_select = ""
if "builder_selected_template_select" not in st.session_state:
    st.session_state.builder_selected_template_select = ""
if "builder_scope_description" not in st.session_state:
    st.session_state.builder_scope_description = ""
if "builder_scope_context" not in st.session_state:
    st.session_state.builder_scope_context = ("", "", "")

if st.session_state.pending_builder_reset:
    st.session_state.editing_estimate_id = None
    st.session_state.builder_estimate_name = ""
    st.session_state.builder_estimate_address = ""
    st.session_state.builder_selected_contractor = "None selected"
    st.session_state.builder_contractor_address = ""
    st.session_state.builder_estimate_notes = ""
    st.session_state.builder_task_name_select = "Add A Work Item"
    st.session_state.builder_trade_name_select = ""
    st.session_state.builder_selected_template_select = ""
    st.session_state.builder_scope_description = ""
    st.session_state.builder_scope_context = ("", "", "")
    st.session_state.pending_repair_form_reset = False
    st.session_state.estimate_cart = []
    st.session_state.pending_builder_reset = False

if st.session_state.pending_page is not None:
    st.session_state.menu_page = st.session_state.pending_page
    st.session_state.pending_page = None

st.title("Renovation Management System")
st.caption("")

require_login()

st.sidebar.write(f"Logged in as: **{st.session_state.get('logged_in_user', '')}**")
st.sidebar.caption(f"Role: {st.session_state.get('logged_in_role', '') or 'Not set'}")

if str(st.session_state.get("logged_in_role", "") or "") == "Owner":
    with st.sidebar.expander("Change Password"):
        current_password = st.text_input("Current Password", type="password", key="current_password")
        new_password = st.text_input("New Password", type="password", key="new_password")
        confirm_password = st.text_input("Confirm New Password", type="password", key="confirm_password")

        if st.button("Update Password", use_container_width=True):
            current_user = st.session_state.get("logged_in_user", "")
            user_row = get_user_account(current_user)
            if not user_row or str(user_row.get("password") or "") != current_password:
                st.error("Current password is incorrect.")
            elif len(new_password) < 6:
                st.error("New password must be at least 6 characters.")
            elif new_password != confirm_password:
                st.error("New passwords do not match.")
            else:
                update_user_password(current_user, new_password)
                st.success("Password updated.")

if st.sidebar.button("Log Out", use_container_width=True):
    st.session_state.logged_in = False
    st.session_state.logged_in_user = ""
    st.session_state.logged_in_role = ""
    st.session_state.logged_in_contractor_id = 0
    st.session_state.logged_in_user_id = 0
    st.rerun()

current_role = str(st.session_state.get("logged_in_role", "") or "")

# Build 17C: sidebar menu section headings. Headings are visual only, not selectable pages.
owner_menu_sections = [
    ("Daily Operations", [
        "RMR Entry",
        "RMR Search / Review",
        "Punch List / Inspection",
        "Work Groups",
        "Project Materials",
        "Renovation Schedule",
        "Budget Planner",
        "Project Ideas",
        "Quality Control",
    ]),
    ("Reports / Data", [
        "Master Work List",
        "Contractor Quotes",
        "Project Cost",
        "Active Projects",
        "Update Records",
    ]),
    ("Setup / Administration", [
        "Properties",
        "Addresses",
        "Contractors",
        "Work Items",
        "Categories of Labor",
        "Scope Templates",
        "Projects",
        "Admin",
    ]),
    ("Legacy Tools", [
        "Estimate Builder",
        "Estimate History",
    ]),
]

manager_menu_sections = [
    ("Daily Operations", [
        "RMR Entry",
        "RMR Search / Review",
        "Punch List / Inspection",
        "Work Groups",
        "Project Materials",
        "Renovation Schedule",
        "Budget Planner",
        "Project Ideas",
        "Quality Control",
    ]),
    ("Reports / Data", [
        "Master Work List",
        "Contractor Quotes",
        "Project Cost",
        "Active Projects",
        "Update Records",
    ]),
    ("Setup / Administration", [
        "Properties",
        "Addresses",
        "Contractors",
        "Work Items",
        "Categories of Labor",
        "Scope Templates",
        "Projects",
    ]),
    ("Legacy Tools", [
        "Estimate Builder",
        "Estimate History",
    ]),
]

contractor_menu_sections = [
    ("Menu", [
        "My Estimates",
        "My Work Groups",
        "My Punch Lists",
        "My Quality Control",
        "Project Materials",
        "Renovation Schedule",
    ])
]

property_manager_menu_sections = [("Menu", ["My Repair Requests"])]

qc_assignee_menu_sections = [("Menu", ["My Quality Control"])]

if current_role == "Owner":
    menu_sections = owner_menu_sections
elif current_role == "Renovation Manager":
    menu_sections = manager_menu_sections
elif current_role == "Contractor":
    menu_sections = contractor_menu_sections
elif current_role == "Property Manager":
    menu_sections = property_manager_menu_sections
elif current_role in ("Maintenance", "Lawn & Landscape"):
    menu_sections = qc_assignee_menu_sections
else:
    menu_sections = manager_menu_sections

page_options = [page_name for _, section_pages in menu_sections for page_name in section_pages]

# Build 15A: role-aware default page. Owners and managers land on RMR Entry,
# which is the fast iPhone field-capture screen.
role_default_page = "RMR Entry" if "RMR Entry" in page_options else page_options[0]
current_page = st.session_state.menu_page if st.session_state.menu_page in page_options else role_default_page
st.session_state.menu_page = current_page

# Build 17D: section headings with page buttons.
# Do not use one radio widget per section because Streamlit can keep an old
# radio selection in multiple sections at the same time. That caused the menu
# to show both RMR Entry and Master Work List selected while the page stayed
# on RMR Entry.
# Build 17E: compact sidebar menu styling so the menu does not consume
# excessive vertical space on Surface/iPhone screens.
st.sidebar.markdown(
    """
    <style>
    section[data-testid="stSidebar"] div.stButton > button {
        min-height: 1.65rem !important;
        height: 1.65rem !important;
        padding: 0.08rem 0.35rem !important;
        margin: 0.02rem 0 !important;
        border: 0 !important;
        background: transparent !important;
        box-shadow: none !important;
        justify-content: flex-start !important;
        text-align: left !important;
        font-size: 0.86rem !important;
        line-height: 1.1 !important;
    }
    section[data-testid="stSidebar"] div.stButton > button:hover {
        background: rgba(255, 75, 75, 0.08) !important;
    }
    section[data-testid="stSidebar"] h3 {
        margin-top: 0.75rem !important;
        margin-bottom: 0.25rem !important;
        font-size: 1.02rem !important;
        line-height: 1.15 !important;
    }
    section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        margin-bottom: 0.25rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.sidebar.markdown("Menu")

for section_title, section_pages in menu_sections:
    if section_title != "Menu":
        st.sidebar.markdown(f"### {section_title}")
    for option in section_pages:
        is_current = option == current_page
        button_label = ("● " if is_current else "○ ") + option
        button_key = (
            "menu_button_"
            + section_title.lower().replace(" ", "_").replace("/", "_")
            + "_"
            + option.lower().replace(" ", "_").replace("/", "_")
        )
        if st.sidebar.button(button_label, key=button_key, use_container_width=True):
            st.session_state.menu_page = option
            try:
                st.query_params["page"] = option
            except Exception:
                pass
            st.rerun()

page = st.session_state.menu_page


# Build 15A: remember the last selected page in the browser URL where supported.
# This lets the iPhone Home Screen shortcut reopen the last page used, while new
# sessions without a page parameter still default to RMR Entry.
try:
    if st.query_params.get("page", "") != page:
        st.query_params["page"] = page
except Exception:
    pass



QC_PRIORITY_OPTIONS = [
    "1 - Urgent",
    "2 - Normal",
    "3 - Low / When Available",
]

QC_STATUS_OPTIONS = [
    "Open",
    "Assigned",
    "In Progress",
    "Completed",
    "Verified",
    "Deferred",
    "Cancelled",
]


def qc_priority_sort_value(value: str) -> int:
    value = str(value or "")
    if value.startswith("1"):
        return 1
    if value.startswith("2"):
        return 2
    if value.startswith("3"):
        return 3
    return 9


def ensure_quality_control_schema():
    """Create/repair Quality Control tables and columns before QC pages query them."""
    with closing(get_conn()) as conn:
        try:
            conn.autocommit = True
        except Exception:
            pass
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS quality_control_items (
                    id BIGSERIAL PRIMARY KEY,
                    qc_code TEXT UNIQUE,
                    entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    property_name TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    unit_number TEXT NOT NULL DEFAULT '',
                    location_identifier TEXT NOT NULL DEFAULT '',
                    work_item_name TEXT NOT NULL DEFAULT '',
                    category_name TEXT NOT NULL DEFAULT '',
                    issue_description TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    priority TEXT NOT NULL DEFAULT '2 - Normal',
                    status TEXT NOT NULL DEFAULT 'Open',
                    due_date DATE,
                    follow_up_date DATE,
                    assignee_type TEXT NOT NULL DEFAULT '',
                    assignee_name TEXT NOT NULL DEFAULT '',
                    contractor_id BIGINT,
                    assigned_user_id BIGINT,
                    completed_date DATE,
                    verified_date DATE,
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    created_by TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS quality_control_files (
                    id BIGSERIAL PRIMARY KEY,
                    qc_item_id BIGINT NOT NULL REFERENCES quality_control_items(id) ON DELETE CASCADE,
                    file_filename TEXT,
                    content_type TEXT,
                    storage_mode TEXT NOT NULL DEFAULT 'database',
                    blob_url TEXT,
                    blob_name TEXT,
                    file_bytes BYTEA,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    uploaded_by TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS quality_control_comments (
                    id BIGSERIAL PRIMARY KEY,
                    qc_item_id BIGINT NOT NULL REFERENCES quality_control_items(id) ON DELETE CASCADE,
                    user_id BIGINT,
                    username TEXT NOT NULL DEFAULT '',
                    role TEXT NOT NULL DEFAULT '',
                    contractor_id BIGINT,
                    comment_text TEXT NOT NULL DEFAULT '',
                    status_update TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            # Repair any partial table created by an earlier build.
            alter_statements = [
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS qc_code TEXT UNIQUE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS entry_date DATE NOT NULL DEFAULT CURRENT_DATE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS property_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS address TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS unit_number TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS location_identifier TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS work_item_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS category_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS issue_description TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT '2 - Normal'",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'Open'",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS due_date DATE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS follow_up_date DATE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS assignee_type TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS assignee_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS contractor_id BIGINT",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS assigned_user_id BIGINT",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS completed_date DATE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS verified_date DATE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                "ALTER TABLE quality_control_items ADD COLUMN IF NOT EXISTS modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS qc_item_id BIGINT",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS file_filename TEXT",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS content_type TEXT",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS storage_mode TEXT NOT NULL DEFAULT 'database'",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS blob_url TEXT",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS blob_name TEXT",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS file_bytes BYTEA",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS uploaded_by TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_files ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS qc_item_id BIGINT",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS user_id BIGINT",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS username TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS contractor_id BIGINT",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS comment_text TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS status_update TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE quality_control_comments ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
            ]
            for stmt in alter_statements:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
            try:
                cur.execute("UPDATE quality_control_items SET qc_code = 'QC-' || LPAD(id::text, 6, '0') WHERE COALESCE(qc_code, '') = ''")
            except Exception:
                pass
        try:
            conn.commit()
        except Exception:
            pass


def qc_items_df(include_deleted: bool = False, contractor_id: int | None = None, assigned_user_id: int | None = None) -> pd.DataFrame:
    ensure_quality_control_schema()
    query = """
        SELECT
            qci.id,
            COALESCE(qci.qc_code, 'QC-' || LPAD(qci.id::text, 6, '0')) AS qc_code,
            qci.entry_date,
            COALESCE(qci.property_name, '') AS property_name,
            COALESCE(qci.address, '') AS address,
            COALESCE(qci.unit_number, '') AS unit_number,
            COALESCE(qci.location_identifier, '') AS location_identifier,
            COALESCE(qci.work_item_name, '') AS work_item_name,
            COALESCE(qci.category_name, '') AS category_name,
            COALESCE(qci.issue_description, '') AS issue_description,
            COALESCE(qci.notes, '') AS notes,
            COALESCE(qci.priority, '2 - Normal') AS priority,
            COALESCE(qci.status, 'Open') AS status,
            qci.due_date,
            qci.follow_up_date,
            COALESCE(qci.assignee_type, '') AS assignee_type,
            COALESCE(qci.assignee_name, '') AS assignee_name,
            COALESCE(qci.contractor_id, 0) AS contractor_id,
            COALESCE(qci.assigned_user_id, 0) AS assigned_user_id,
            qci.completed_date,
            qci.verified_date,
            COALESCE(qci.deleted, FALSE) AS deleted,
            COALESCE(qci.created_by, '') AS created_by,
            qci.created_at,
            qci.modified_at,
            qci.modified_at AS last_response_at
        FROM quality_control_items qci
        WHERE 1 = 1
    """
    params = []
    if not include_deleted:
        query += " AND COALESCE(qci.deleted, FALSE) = FALSE"
    if contractor_id:
        query += " AND COALESCE(qci.contractor_id, 0) = ?"
        params.append(int(contractor_id))
    if assigned_user_id:
        query += " AND COALESCE(qci.assigned_user_id, 0) = ?"
        params.append(int(assigned_user_id))
    query += """
        ORDER BY
            CASE
                WHEN COALESCE(qci.priority, '') LIKE '1%' THEN 1
                WHEN COALESCE(qci.priority, '') LIKE '2%' THEN 2
                WHEN COALESCE(qci.priority, '') LIKE '3%' THEN 3
                ELSE 9
            END,
            qci.due_date NULLS LAST,
            qci.entry_date DESC NULLS LAST,
            qci.id DESC
    """
    try:
        return fetch_df(query, tuple(params))
    except Exception as exc:
        # If the QC schema was partially created during a prior failed deploy, repair once and fail softly.
        try:
            ensure_quality_control_schema()
        except Exception:
            pass
        st.warning(f"Quality Control list could not be loaded yet: {exc}")
        return pd.DataFrame(columns=[
            "id", "qc_code", "entry_date", "property_name", "address", "unit_number",
            "location_identifier", "work_item_name", "category_name", "issue_description",
            "notes", "priority", "status", "due_date", "follow_up_date", "assignee_type",
            "assignee_name", "contractor_id", "assigned_user_id", "completed_date",
            "verified_date", "deleted", "created_by", "created_at", "modified_at", "last_response_at"
        ])


def create_qc_item(data: dict) -> int | None:
    ensure_quality_control_schema()
    new_id = execute_returning_id(
        """
        INSERT INTO quality_control_items (
            entry_date, property_name, address, unit_number, location_identifier,
            work_item_name, category_name, issue_description, notes, priority, status,
            due_date, follow_up_date, assignee_type, assignee_name, contractor_id,
            assigned_user_id, created_by, created_at, modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        """,
        (
            data.get("entry_date"), data.get("property_name", ""), data.get("address", ""), data.get("unit_number", ""),
            data.get("location_identifier", ""), data.get("work_item_name", ""), data.get("category_name", ""),
            data.get("issue_description", ""), data.get("notes", ""), data.get("priority", "2 - Normal"), data.get("status", "Open"),
            data.get("due_date"), data.get("follow_up_date"), data.get("assignee_type", ""), data.get("assignee_name", ""),
            data.get("contractor_id") or None, data.get("assigned_user_id") or None, data.get("created_by", ""),
        ),
    )
    if new_id:
        execute("UPDATE quality_control_items SET qc_code = ? WHERE id = ?", (f"QC-{int(new_id):06d}", int(new_id)))
    return new_id


def update_qc_item(item_id: int, data: dict):
    ensure_quality_control_schema()
    execute(
        """
        UPDATE quality_control_items
        SET entry_date = ?, property_name = ?, address = ?, unit_number = ?, location_identifier = ?,
            work_item_name = ?, category_name = ?, issue_description = ?, notes = ?, priority = ?, status = ?,
            due_date = ?, follow_up_date = ?, assignee_type = ?, assignee_name = ?, contractor_id = ?,
            assigned_user_id = ?, completed_date = ?, verified_date = ?, modified_at = NOW()
        WHERE id = ?
        """,
        (
            data.get("entry_date"), data.get("property_name", ""), data.get("address", ""), data.get("unit_number", ""),
            data.get("location_identifier", ""), data.get("work_item_name", ""), data.get("category_name", ""),
            data.get("issue_description", ""), data.get("notes", ""), data.get("priority", "2 - Normal"), data.get("status", "Open"),
            data.get("due_date"), data.get("follow_up_date"), data.get("assignee_type", ""), data.get("assignee_name", ""),
            data.get("contractor_id") or None, data.get("assigned_user_id") or None, data.get("completed_date"), data.get("verified_date"),
            int(item_id),
        ),
    )


def add_qc_comment(item_id: int, comment_text: str, status_update: str = ""):
    ensure_quality_control_schema()
    if not str(comment_text or "").strip() and not str(status_update or "").strip():
        return
    execute(
        """
        INSERT INTO quality_control_comments (
            qc_item_id, user_id, username, role, contractor_id, comment_text, status_update, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
        """,
        (
            int(item_id),
            int(st.session_state.get("logged_in_user_id") or 0) or None,
            str(st.session_state.get("logged_in_user") or ""),
            str(st.session_state.get("logged_in_role") or ""),
            int(st.session_state.get("logged_in_contractor_id") or 0) or None,
            str(comment_text or "").strip(),
            str(status_update or "").strip(),
        ),
    )


@st.cache_data(show_spinner=False, ttl=300)
def qc_comments_df(item_id: int) -> pd.DataFrame:
    ensure_quality_control_schema()
    return fetch_df(
        """
        SELECT id, qc_item_id, COALESCE(username, '') AS username, COALESCE(role, '') AS role,
               COALESCE(comment_text, '') AS comment_text, COALESCE(status_update, '') AS status_update, created_at
        FROM quality_control_comments
        WHERE qc_item_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (int(item_id),),
    )


@st.cache_data(show_spinner=False, ttl=300)
def qc_files_df(item_id: int) -> pd.DataFrame:
    ensure_quality_control_schema()
    return fetch_df(
        """
        SELECT id, qc_item_id, COALESCE(file_filename, '') AS file_filename,
               COALESCE(content_type, 'application/octet-stream') AS content_type,
               COALESCE(storage_mode, 'database') AS storage_mode,
               COALESCE(blob_url, '') AS blob_url,
               COALESCE(blob_name, '') AS blob_name,
               file_bytes, COALESCE(sort_order, 0) AS sort_order,
               COALESCE(uploaded_by, '') AS uploaded_by, created_at
        FROM quality_control_files
        WHERE qc_item_id = ?
        ORDER BY sort_order, id
        """,
        (int(item_id),),
    )


def save_qc_files(item_id: int, uploaded_files, uploaded_by: str = ""):
    ensure_quality_control_schema()
    files = []
    for sort_order, uploaded in enumerate(uploaded_files or []):
        if uploaded is None:
            continue
        data = uploaded.getvalue()
        if not data:
            continue
        filename = getattr(uploaded, "name", "file")
        content_type = getattr(uploaded, "type", None) or "application/octet-stream"
        if str(content_type).startswith("image/"):
            data, content_type, filename = optimize_image_bytes_for_upload(data, filename)
        stored_file = upload_bytes_to_blob(
            data=data,
            filename=filename,
            content_type=content_type,
            folder="renovation-estimator/quality-control",
        )
        stored_file["sort_order"] = sort_order
        files.append(stored_file)
    existing_df = qc_files_df(item_id)
    existing_count = len(existing_df) if existing_df is not None else 0
    for offset, stored_file in enumerate(files):
        execute(
            """
            INSERT INTO quality_control_files (
                qc_item_id, file_filename, content_type, storage_mode, blob_url, blob_name,
                file_bytes, sort_order, uploaded_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            """,
            (
                int(item_id), stored_file.get("filename"), stored_file.get("content_type"), stored_file.get("storage_mode"),
                stored_file.get("blob_url"), stored_file.get("blob_name"), stored_file.get("bytes"), existing_count + offset,
                uploaded_by or "",
            ),
        )


def qc_file_row_to_dict(row) -> dict:
    data = row.get("file_bytes")
    if data is not None and not isinstance(data, (bytes, bytearray)):
        try:
            data = bytes(data)
        except Exception:
            data = None
    return {
        "id": int(row["id"]) if pd.notna(row.get("id")) else None,
        "filename": str(row.get("file_filename") or "file"),
        "content_type": str(row.get("content_type") or "application/octet-stream"),
        "storage_mode": str(row.get("storage_mode") or "database"),
        "blob_url": str(row.get("blob_url") or ""),
        "blob_name": str(row.get("blob_name") or ""),
        "bytes": data,
    }


def render_qc_files(item_id: int, key_prefix: str, allow_delete: bool = False):
    files_df = qc_files_df(item_id)
    if files_df.empty:
        st.info("No photos/files saved yet.")
        return
    st.caption(f"{len(files_df)} photo/file(s) saved.")
    if st.checkbox("Load QC photo/file previews", key=f"load_qc_files_{key_prefix}_{item_id}", value=False):
        cols = st.columns(min(4, max(1, len(files_df))))
        for idx, (_, row) in enumerate(files_df.iterrows()):
            file_info = qc_file_row_to_dict(row)
            with cols[idx % len(cols)]:
                if file_info["content_type"].startswith("image/"):
                    render_photo_item(file_info)
                elif file_info.get("blob_url"):
                    st.markdown(f"[{file_info['filename']}]({file_info['blob_url']})")
                else:
                    st.write(file_info["filename"])


def qc_report_dataframe(base_df: pd.DataFrame) -> pd.DataFrame:
    if base_df.empty:
        return pd.DataFrame()
    df = base_df.copy()
    today = pd.Timestamp.today().normalize()
    entry_dates = pd.to_datetime(df["entry_date"], errors="coerce")
    due_dates = pd.to_datetime(df["due_date"], errors="coerce")
    df["Days Open"] = (today - entry_dates).dt.days.fillna(0).astype(int)
    df["Overdue"] = ((due_dates.notna()) & (due_dates < today) & (~df["status"].isin(["Completed", "Verified", "Cancelled"]))).map({True: "Yes", False: "No"})
    report = pd.DataFrame({
        "QC ID": df["qc_code"],
        "Date Entered": df["entry_date"],
        "Property": df["property_name"],
        "Address": df["address"],
        "Location": df["location_identifier"],
        "Work Item": df["work_item_name"],
        "Issue": df["issue_description"],
        "Assigned To": df["assignee_name"],
        "Priority": df["priority"],
        "Status": df["status"],
        "Due Date": df["due_date"],
        "Follow-Up": df["follow_up_date"],
        "Days Open": df["Days Open"],
        "Overdue": df["Overdue"],
        "Last Response": df["last_response_at"],
    })
    return report


def build_quality_control_pdf(report_df: pd.DataFrame, filters: dict) -> bytes:
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    width, height = landscape(letter)
    left = 28
    top = height - 28
    pdf.setFont("Helvetica-Bold", 13)
    pdf.drawString(left, top, "Quality Control Report")
    pdf.setFont("Helvetica", 8)
    pdf.drawRightString(width - left, top, f"As of {datetime.now().strftime('%m-%d-%Y %I:%M %p')}")
    y = top - 18
    filter_text = " | ".join([f"{k}: {v}" for k, v in filters.items() if str(v or '').strip()])
    pdf.drawString(left, y, filter_text[:160])
    y -= 16
    total_items = len(report_df) if report_df is not None else 0
    overdue_count = int((report_df.get("Overdue", pd.Series(dtype=str)).astype(str) == "Yes").sum()) if total_items else 0
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(left, y, f"Items: {total_items}   Overdue: {overdue_count}")
    y -= 16
    headers = ["QC ID", "Property", "Location", "Work Item", "Assigned To", "Priority", "Status", "Due Date", "Days"]
    col_widths = [58, 95, 110, 120, 85, 70, 70, 60, 35]
    def draw_header(y_pos):
        pdf.setFont("Helvetica-Bold", 7)
        x = left
        for h, w in zip(headers, col_widths):
            pdf.drawString(x, y_pos, h)
            x += w
        pdf.line(left, y_pos - 3, width - left, y_pos - 3)
        return y_pos - 12
    y = draw_header(y)
    pdf.setFont("Helvetica", 7)
    if report_df is not None and not report_df.empty:
        for _, row in report_df.iterrows():
            if y < 38:
                pdf.showPage()
                y = height - 30
                y = draw_header(y)
                pdf.setFont("Helvetica", 7)
            values = [
                row.get("QC ID", ""), row.get("Property", ""), row.get("Location", ""), row.get("Work Item", ""),
                row.get("Assigned To", ""), row.get("Priority", ""), row.get("Status", ""), row.get("Due Date", ""), row.get("Days Open", ""),
            ]
            x = left
            for value, w in zip(values, col_widths):
                text = str(value or "")
                if len(text) > int(w / 4.2):
                    text = text[:max(6, int(w / 4.2) - 3)] + "..."
                pdf.drawString(x, y, text)
                x += w
            y -= 11
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def build_quality_control_excel(report_df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        (report_df if report_df is not None else pd.DataFrame()).to_excel(writer, index=False, sheet_name="Quality Control")
    buffer.seek(0)
    return buffer.getvalue()


@st.cache_data(show_spinner=False, ttl=300)
def quotes_received_df() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            qr.id AS quote_request_id,
            COALESCE(p.project_name, e.estimate_name, '(unnamed)') AS project_name,
            COALESCE(p.project_address, e.estimate_address, '') AS project_address,
            COALESCE(e.id, 0) AS estimate_id,
            COALESCE(el.id, 0) AS work_item_id,
            COALESCE(el.task_name, '') AS work_item_name,
            COALESCE(el.trade_name, '') AS trade_name,
            COALESCE(el.scope_description, '') AS scope_description,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(qr.quote_status, 'Requested') AS quote_status,
            COALESCE(qr.quote_amount, 0) AS quote_amount,
            COALESCE(qr.quote_notes, '') AS quote_notes,
            qr.requested_at,
            qr.submitted_at,
            qr.created_at,
            qr.modified_at
        FROM quote_requests qr
        JOIN estimate_lines el ON el.id = qr.estimate_line_id
        JOIN estimates e ON e.id = el.estimate_id
        LEFT JOIN project_registry p ON p.id = e.project_id
        LEFT JOIN contractors c ON c.id = qr.contractor_id
        ORDER BY
            LOWER(COALESCE(p.project_name, e.estimate_name, '(unnamed)')),
            COALESCE(qr.submitted_at, qr.modified_at, qr.requested_at) DESC NULLS LAST,
            el.id DESC,
            qr.id DESC
        """
    )


@st.cache_data(show_spinner=False, ttl=300)
def work_item_costs_df(project_id: int | None = None, contractor_id: int | None = None) -> pd.DataFrame:
    query = """
        SELECT
            wic.id,
            COALESCE(wic.project_id, 0) AS project_id,
            COALESCE(p.project_name, '') AS project_name,
            COALESCE(wic.estimate_line_id, 0) AS estimate_line_id,
            COALESCE(wic.task_name, '') AS task_name,
            COALESCE(wic.trade_name, '') AS trade_name,
            COALESCE(wic.contractor_id, 0) AS contractor_id,
            COALESCE(c.name, '') AS contractor_name,
            COALESCE(wic.agreed_price, 0) AS agreed_price,
            wic.entered_date,
            COALESCE(wic.notes, '') AS notes,
            wic.created_at,
            wic.modified_at
        FROM work_item_costs wic
        LEFT JOIN project_registry p ON p.id = wic.project_id
        LEFT JOIN contractors c ON c.id = wic.contractor_id
    """
    params = []
    clauses = []
    if project_id:
        clauses.append("COALESCE(wic.project_id, 0) = ?")
        params.append(project_id)
    if contractor_id:
        clauses.append("COALESCE(wic.contractor_id, 0) = ?")
        params.append(contractor_id)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY COALESCE(wic.entered_date, CURRENT_DATE) DESC, wic.id DESC"
    return fetch_df(query, tuple(params))


def latest_cost_row_from_df(costs_df: pd.DataFrame, project_id: int, task_name: str, trade_name: str, estimate_line_id: int | None = None):
    if costs_df.empty:
        return None
    df = costs_df.copy()
    if "project_id" in df.columns:
        df = df[df["project_id"].fillna(0).astype(int) == int(project_id)]
    if estimate_line_id:
        exact = df[df["estimate_line_id"].fillna(0).astype(int) == int(estimate_line_id)]
        if not exact.empty:
            return exact.iloc[0]
    name = str(task_name or "").strip().lower()
    trade = str(trade_name or "").strip().lower()
    match = df[
        df["task_name"].fillna("").astype(str).str.strip().str.lower().eq(name)
        & df["trade_name"].fillna("").astype(str).str.strip().str.lower().eq(trade)
    ]
    if match.empty:
        return None
    return match.iloc[0]


@st.cache_data(show_spinner=False, ttl=300)
def project_estimate_work_items_df(project_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            el.id AS estimate_line_id,
            e.id AS estimate_id,
            COALESCE(el.task_name, '') AS task_name,
            COALESCE(el.category_name, el.trade_name, '') AS category_name,
            COALESCE(el.work_group_name, '') AS work_group_name,
            COALESCE(el.trade_name, '') AS trade_name,
            COALESCE(el.scope_description, '') AS scope_description,
            COALESCE(el.total_labor_cost, 0) AS estimated_amount,
            COALESCE(el.contractor_id, 0) AS contractor_id,
            COALESCE(c.name, '') AS contractor_name,
            el.created_at,
            el.modified_at
        FROM estimate_lines el
        JOIN estimates e ON e.id = el.estimate_id
        LEFT JOIN contractors c ON c.id = el.contractor_id
        WHERE COALESCE(e.project_id, 0) = ?
        ORDER BY e.modified_at DESC NULLS LAST, e.id DESC, el.id DESC
        """,
        (project_id,),
    )


@st.cache_data(show_spinner=False, ttl=300)
@st.cache_data(show_spinner=False, ttl=300)
def estimate_lines_for_work_group_conversion_df(estimate_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            el.id AS estimate_line_id,
            e.id AS estimate_id,
            COALESCE(e.project_id, 0) AS project_id,
            COALESCE(p.project_name, e.estimate_name, '') AS project_name,
            COALESCE(p.project_address, e.estimate_address, '') AS project_address,
            COALESCE(el.task_name, '') AS task_name,
            COALESCE(el.category_name, el.trade_name, '') AS category_name,
            COALESCE(el.work_group_name, '') AS work_group_name,
            COALESCE(el.trade_name, '') AS trade_name,
            COALESCE(el.scope_description, '') AS scope_description,
            COALESCE(el.contractor_id, 0) AS estimate_contractor_id,
            COALESCE(ec.name, '') AS estimate_contractor_name,
            COALESCE(latest_cost.contractor_id, el.contractor_id, 0) AS contractor_id,
            COALESCE(cc.name, ec.name, '') AS contractor_name,
            COALESCE(latest_cost.agreed_price, 0) AS agreed_price,
            COALESCE(el.approved_final_cost, 0) AS approved_final_cost,
            COALESCE(el.manual_repair_amount, 0) AS manual_repair_amount,
            COALESCE(el.total_labor_cost, 0) AS total_labor_cost,
            COALESCE(existing.existing_work_group_count, 0) AS existing_work_group_count
        FROM estimate_lines el
        JOIN estimates e ON e.id = el.estimate_id
        LEFT JOIN project_registry p ON p.id = e.project_id
        LEFT JOIN contractors ec ON ec.id = el.contractor_id
        LEFT JOIN LATERAL (
            SELECT
                wic.contractor_id,
                wic.agreed_price
            FROM work_item_costs wic
            WHERE wic.estimate_line_id = el.id
            ORDER BY wic.entered_date DESC NULLS LAST, wic.modified_at DESC NULLS LAST, wic.id DESC
            LIMIT 1
        ) latest_cost ON TRUE
        LEFT JOIN contractors cc ON cc.id = latest_cost.contractor_id
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS existing_work_group_count
            FROM work_groups wo
            WHERE wo.estimate_line_id = el.id
        ) existing ON TRUE
        WHERE e.id = ?
        ORDER BY el.id
        """,
        (estimate_id,),
    )


def copy_estimate_line_photos_to_work_group(estimate_line_id: int, work_group_id: int, uploaded_by: str = ""):
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql_params(
                    """
                    INSERT INTO work_group_photos (
                        work_group_id,
                        photo_filename,
                        content_type,
                        storage_mode,
                        blob_url,
                        blob_name,
                        photo_bytes,
                        sort_order,
                        uploaded_by,
                        created_at
                    )
                    SELECT
                        ?,
                        photo_filename,
                        content_type,
                        storage_mode,
                        blob_url,
                        blob_name,
                        photo_bytes,
                        sort_order,
                        ?,
                        NOW()
                    FROM estimate_line_photos
                    WHERE estimate_line_id = ?
                    ORDER BY sort_order, id
                    """
                ),
                (work_group_id, uploaded_by or "", estimate_line_id),
            )
        conn.commit()
    st.cache_data.clear()


def latest_scope_for_task_name(task_name: str) -> str:
    if not str(task_name or "").strip():
        return ""
    df = fetch_df(
        """
        SELECT
            COALESCE(st.scope_description, '') AS scope_description
        FROM scope_templates st
        JOIN tasks t ON t.id = st.task_id
        WHERE LOWER(COALESCE(t.name, '')) = LOWER(?)
        ORDER BY st.id DESC
        LIMIT 1
        """,
        (str(task_name).strip(),),
    )
    if df.empty:
        return ""
    return str(df.iloc[0]["scope_description"] or "")


@st.cache_data(show_spinner=False, ttl=300)
def work_groups_df(project_id: int | None = None, contractor_id: int | None = None) -> pd.DataFrame:
    """Return Work Groups for reports and contractor pages.

    Build 16D safety fix: this avoids brittle SQL joins/COALESCE expressions
    against older Neon schemas. It reads the Work Groups table as-is, then
    normalizes expected report columns in Python so the Master Work List does
    not crash when optional columns are missing or date/number types differ.
    """
    def _safe_table(table_name: str) -> pd.DataFrame:
        try:
            return fetch_df(f"SELECT * FROM {table_name}")
        except Exception:
            return pd.DataFrame()

    def _first(row, names, default=""):
        for name in names:
            if name in row.index:
                val = row.get(name)
                try:
                    if pd.notna(val) and val is not None:
                        return val
                except Exception:
                    if val is not None:
                        return val
        return default

    raw = _safe_table("work_groups")
    if raw.empty:
        return pd.DataFrame(columns=[
            "id", "order_number", "project_id", "project_name", "project_address",
            "estimate_line_id", "work_group_name", "task_name", "category_name",
            "trade_name", "scope_description", "contractor_id", "contractor_name",
            "agreed_price", "estimated_price", "contractor_requested_price",
            "amount_to_be_paid", "due_date", "status", "notes",
            "contractor_priority", "owner_intent", "created_at", "modified_at",
        ])

    # Optional lookup tables. If they are missing or shaped differently, the
    # Work Group list still loads with blank project/contractor names.
    projects = _safe_table("project_registry")
    contractors = _safe_table("contractors")

    project_names = {}
    project_addrs = {}
    if not projects.empty and "id" in projects.columns:
        for _, pr in projects.iterrows():
            pid = int(pr.get("id") or 0)
            project_names[pid] = str(_first(pr, ["project_name", "name", "title"], "") or "")
            project_addrs[pid] = str(_first(pr, ["project_address", "address", "street_address"], "") or "")

    contractor_names = {}
    if not contractors.empty and "id" in contractors.columns:
        for _, co in contractors.iterrows():
            cid = int(co.get("id") or 0)
            contractor_names[cid] = str(_first(co, ["name", "contractor_name"], "") or "")

    rows = []
    for _, wg in raw.iterrows():
        try:
            wid = int(_first(wg, ["id"], 0) or 0)
        except Exception:
            wid = 0
        try:
            pid = int(_first(wg, ["project_id"], 0) or 0)
        except Exception:
            pid = 0
        try:
            cid = int(_first(wg, ["contractor_id"], 0) or 0)
        except Exception:
            cid = 0
        if project_id and pid != int(project_id):
            continue
        if contractor_id and cid != int(contractor_id):
            continue

        task_name = str(_first(wg, ["task_name", "work_item", "work_item_name"], "") or "")
        wg_name = str(_first(wg, ["work_group_name", "group_name", "name"], task_name) or task_name or "")
        cat = str(_first(wg, ["category_name", "work_item_category", "trade_name", "category_of_labor"], "") or "")
        trade = str(_first(wg, ["trade_name", "category_name", "category_of_labor"], cat) or "")
        agreed = currency_value(_first(wg, ["agreed_price", "approved_price", "price"], 0))
        estimated = currency_value(_first(wg, ["estimated_price", "estimate_price"], 0))
        requested = currency_value(_first(wg, ["contractor_requested_price", "requested_price"], 0))
        amount_paid = currency_value(_first(wg, ["amount_to_be_paid", "amount_paid"], agreed or estimated or requested))
        priority = str(_first(wg, ["contractor_priority", "priority"], "3 - Quote Only") or "3 - Quote Only")

        rows.append({
            "id": wid,
            "order_number": str(_first(wg, ["order_number", "work_group_number"], f"WG{wid}") or f"WG{wid}"),
            "project_id": pid,
            "project_name": project_names.get(pid, ""),
            "project_address": project_addrs.get(pid, ""),
            "estimate_line_id": int(_first(wg, ["estimate_line_id"], 0) or 0),
            "work_group_name": wg_name,
            "task_name": task_name,
            "category_name": cat,
            "trade_name": trade,
            "scope_description": str(_first(wg, ["scope_description", "scope", "description"], "") or ""),
            "contractor_id": cid,
            "contractor_name": contractor_names.get(cid, ""),
            "agreed_price": agreed,
            "estimated_price": estimated,
            "contractor_requested_price": requested,
            "amount_to_be_paid": amount_paid,
            "due_date": _first(wg, ["due_date", "date_due"], None),
            "status": str(_first(wg, ["status"], "Open") or "Open"),
            "notes": str(_first(wg, ["notes", "work_group_notes"], "") or ""),
            "contractor_priority": priority,
            "owner_intent": str(_first(wg, ["owner_intent"], "Quote Only") or "Quote Only"),
            "created_at": _first(wg, ["created_at"], None),
            "modified_at": _first(wg, ["modified_at", "updated_at"], None),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    def _priority_sort(v):
        text = str(v or "")
        if text.startswith("1"):
            return 1
        if text.startswith("2"):
            return 2
        return 3

    out["_priority_sort"] = out["contractor_priority"].apply(_priority_sort)
    out = out.sort_values(by=["_priority_sort", "id"], ascending=[True, False]).drop(columns=["_priority_sort"])
    return out




def soft_delete_rmr_record(rmr_id: int):
    """Build 19A: remove an RMR/work item from normal queues and clear budget/cash-flow/work-group links."""
    rmr_id = int(rmr_id)
    execute("DELETE FROM rmr_group_members WHERE rmr_id = ?", (rmr_id,))
    execute(
        """
        UPDATE renovation_master_records
        SET deleted = TRUE,
            budget_status = 'Cancelled',
            info_status = 'Closed',
            work_group_id = NULL,
            cashflow_export_status = 'Not Exported',
            cashflow_last_exported_at = NULL,
            cashflow_export_signature = '',
            modified_at = NOW()
        WHERE id = ?
        """,
        (rmr_id,),
    )
    add_rmr_history(rmr_id, "Deleted", "RMR removed from active use; budget/cash-flow links and group membership cleared.")


def restore_rmr_record(rmr_id: int):
    """Build 15E: restore a soft-deleted RMR back to open queue."""
    execute(
        """
        UPDATE renovation_master_records
        SET deleted = FALSE,
            budget_status = CASE WHEN COALESCE(budget_status, '') IN ('Deleted','Cancelled') THEN 'Active' ELSE budget_status END,
            info_status = CASE WHEN COALESCE(info_status, '') IN ('Closed', 'Deleted') THEN 'Open' ELSE info_status END,
            modified_at = NOW()
        WHERE id = ?
        """,
        (int(rmr_id),),
    )
    add_rmr_history(int(rmr_id), "Restored", "RMR/work item restored to active queue.")


def currency_value(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def build_pipeline_overview_df(include_deleted: bool = True) -> pd.DataFrame:
    """Build 15E: bird's-eye view of all RMR work items with optional Work Group context."""
    rmr_df = rmr_records_df(include_deleted=include_deleted)
    if rmr_df.empty:
        return pd.DataFrame()

    wg_df = work_groups_df()
    wg_lookup = {}
    if not wg_df.empty:
        for _, wg in wg_df.iterrows():
            wg_lookup[int(wg.get("id") or 0)] = wg.to_dict()

    rows = []
    for _, r in rmr_df.iterrows():
        wg_id = int(r.get("work_group_id") or 0)
        wg = wg_lookup.get(wg_id, {})
        labor_budget = currency_value(r.get("labor_budget"))
        materials_budget = currency_value(r.get("materials_budget"))
        total_budget = labor_budget + materials_budget
        wg_label = "No Work Group Assigned"
        if wg_id:
            wg_label = str(wg.get("work_group_name") or r.get("linked_work_group_name") or f"WG{wg_id}")
        row = {
            "RMR DB ID": int(r.get("id") or 0),
            "Source Type": "RMR",
            "RMR ID": str(r.get("rmr_code") or ""),
            "Property": str(r.get("property_name") or ""),
            "Address": str(r.get("address") or ""),
            "Location": str(r.get("location_identifier") or ""),
            "Work Item": str(r.get("work_item_name") or ""),
            "Category": str(r.get("category_name") or ""),
            "Work Group ID": wg_id,
            "Work Group": wg_label,
            "Contractor": str(r.get("contractor_name") or wg.get("contractor_name") or ""),
            "Priority": str(r.get("contractor_priority") or wg.get("contractor_priority") or "3 - Quote Only"),
            "Owner Intent": str(r.get("owner_intent") or wg.get("owner_intent") or "Quote Only"),
            "Budget Timeframe": str(r.get("budget_timeframe") or "No Timeframe Yet"),
            "Budget Status": str(r.get("budget_status") or "Active"),
            "RMR Status": str(r.get("info_status") or "Open"),
            "Work Group Status": str(wg.get("status") or ""),
            "Labor Budget": labor_budget,
            "Materials Budget": materials_budget,
            "Total Budget": total_budget,
            "Cash Flow Export": display_export_status(
                r.get("cashflow_export_status", "Not Exported"),
                r.get("cashflow_export_signature", ""),
                rmr_export_signature(r),
            ),
            "Photos": int(r.get("photo_count") or 0),
            "Deleted": bool(r.get("deleted", False)) or str(r.get("budget_status") or "").lower() in ["deleted", "cancelled"],
            "Modified": r.get("modified_at"),
            "Entry Date": r.get("entry_date"),
        }
        rows.append(row)
    out = pd.DataFrame(rows)
    if not out.empty:
        out["Priority Sort"] = out["Priority"].apply(contractor_priority_sort_value)
        out = out.sort_values(["Priority Sort", "Property", "Work Group", "Budget Timeframe", "RMR ID"], kind="stable")
    return out


def filter_pipeline_overview_df(df: pd.DataFrame, status_view: str, property_filter: str, work_group_filter: str, timeframe_filter: str, contractor_filter: str, priority_filter: str, search_text: str) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()

    if status_view == "Open Work":
        work = work[
            (work["Deleted"] == False)
            & (~work["Budget Status"].astype(str).str.lower().isin(["deleted", "cancelled"]))
            & (~work["RMR Status"].astype(str).str.lower().isin(["work completed", "paid", "closed", "deleted", "cancelled"]))
            & (~work["Work Group Status"].astype(str).str.lower().isin(["completed", "cancelled", "deleted"]))
        ].copy()
    elif status_view == "Completed / Closed":
        work = work[
            (work["Deleted"] == False)
            & (
                work["RMR Status"].astype(str).str.lower().isin(["work completed", "paid", "closed"])
                | work["Work Group Status"].astype(str).str.lower().isin(["completed"])
            )
        ].copy()
    elif status_view == "Deleted":
        work = work[(work["Deleted"] == True) | (work["Budget Status"].astype(str).str.lower().isin(["deleted", "cancelled"]))].copy()

    if property_filter and property_filter != "All Properties":
        work = work[work["Property"].astype(str) == property_filter]
    if work_group_filter and work_group_filter != "All Work Groups":
        if work_group_filter == "No Work Group Assigned":
            work = work[work["Work Group ID"].fillna(0).astype(int) == 0]
        else:
            work = work[work["Work Group"].astype(str) == work_group_filter]
    if timeframe_filter and timeframe_filter != "All Timeframes":
        work = work[work["Budget Timeframe"].astype(str) == timeframe_filter]
    if contractor_filter and contractor_filter != "All Contractors":
        if contractor_filter == "Unassigned":
            work = work[work["Contractor"].astype(str).str.strip() == ""]
        else:
            work = work[work["Contractor"].astype(str) == contractor_filter]
    if priority_filter and priority_filter != "All Priorities":
        work = work[work["Priority"].astype(str) == priority_filter]
    if search_text:
        needle = str(search_text).strip().lower()
        if needle:
            searchable_cols = ["RMR ID", "Property", "Address", "Location", "Work Item", "Category", "Work Group", "Contractor", "Owner Intent", "Budget Timeframe", "RMR Status", "Work Group Status"]
            mask = False
            for col in searchable_cols:
                mask = mask | work[col].astype(str).str.lower().str.contains(re.escape(needle), na=False)
            work = work[mask]
    return work


def _pdf_safe_text(value) -> str:
    text = str(value or "")
    return text.replace("–", "-").replace("—", "-").replace("•", "-")


def _format_report_money(value) -> str:
    try:
        return f"${float(value or 0):,.0f}"
    except Exception:
        return "$0"


def _excel_safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Build 16E: make a DataFrame safe for Excel export.

    Pandas/OpenPyXL will fail if a dataframe contains timezone-aware
    datetimes, dictionaries/lists, or other object values that Excel cannot
    serialize. The on-screen Master Work List can display those values, but
    the download button evaluates immediately in Streamlit, so export data
    must be cleaned before calling to_excel().
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()

    safe = df.copy()

    # Handle pandas datetime columns, including timezone-aware columns.
    for col in safe.columns:
        try:
            if pd.api.types.is_datetime64tz_dtype(safe[col]):
                safe[col] = safe[col].dt.tz_convert(None)
            elif pd.api.types.is_datetime64_any_dtype(safe[col]):
                # Keep as naive datetime where possible.
                try:
                    safe[col] = safe[col].dt.tz_localize(None)
                except Exception:
                    pass
        except Exception:
            pass

    def clean_cell(value):
        if pd.isna(value):
            return ""
        # Pandas timestamp / Python datetime with timezone.
        if isinstance(value, pd.Timestamp):
            try:
                if value.tzinfo is not None:
                    value = value.tz_convert(None)
            except Exception:
                try:
                    value = value.tz_localize(None)
                except Exception:
                    pass
            try:
                return value.to_pydatetime().replace(tzinfo=None)
            except Exception:
                return str(value)
        try:
            from datetime import datetime as _dt_datetime, date as _dt_date
            if isinstance(value, _dt_datetime):
                return value.replace(tzinfo=None)
            if isinstance(value, _dt_date):
                return value
        except Exception:
            pass
        if isinstance(value, (list, tuple, dict, set)):
            return str(value)
        return value

    for col in safe.columns:
        if safe[col].dtype == "object":
            safe[col] = safe[col].map(clean_cell)

    return safe


def master_work_list_excel_bytes(report_df: pd.DataFrame, summary_df: pd.DataFrame | None = None) -> bytes:
    """Build 16E: export the currently filtered Master Work List to Excel."""
    output = BytesIO()
    safe_report_df = _excel_safe_dataframe(report_df)
    safe_summary_df = _excel_safe_dataframe(summary_df) if summary_df is not None else None

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        safe_report_df.to_excel(writer, index=False, sheet_name="Master Work List")
        if safe_summary_df is not None and not safe_summary_df.empty:
            safe_summary_df.to_excel(writer, index=False, sheet_name="Summary")

        # Light formatting only; avoid anything that could break export.
        try:
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                for column_cells in ws.columns:
                    header = str(column_cells[0].value or "")
                    max_len = max([len(str(c.value or "")) for c in column_cells[:100]] + [len(header)])
                    ws.column_dimensions[column_cells[0].column_letter].width = min(max(max_len + 2, 10), 45)
        except Exception:
            pass

    output.seek(0)
    return output.getvalue()


def build_master_work_list_pdf(report_df: pd.DataFrame, filters: dict | None = None, grouped: bool = False) -> bytes:
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=landscape(letter))
    width, height = landscape(letter)
    left = 32
    right = width - 32
    top = height - 34
    line_height = 11

    def new_page(title_suffix=""):
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(left, top, "Master Work List" + (f" - {title_suffix}" if title_suffix else ""))
        pdf.setFont("Helvetica", 8)
        pdf.drawRightString(right, top, f"Printed: {datetime.now().strftime('%m-%d-%Y %I:%M %p')}")
        y0 = top - 18
        if filters:
            filter_text = " | ".join([f"{k}: {v}" for k, v in filters.items() if str(v or "").strip()])
            pdf.setFont("Helvetica", 8)
            y0 = draw_wrapped_text(pdf, filter_text, left, y0, right - left, line_height=9, font_name="Helvetica", font_size=8) - 4
        pdf.line(left, y0, right, y0)
        return y0 - 12

    if report_df is None or report_df.empty:
        y = new_page()
        pdf.setFont("Helvetica", 10)
        pdf.drawString(left, y, "No items match the selected filters.")
        pdf.save()
        buffer.seek(0)
        return buffer.getvalue()

    work = report_df.copy()
    for money_col in ["Labor Budget", "Materials Budget", "Total Budget"]:
        if money_col in work.columns:
            work[money_col] = pd.to_numeric(work[money_col], errors="coerce").fillna(0)

    total_labor = work["Labor Budget"].sum() if "Labor Budget" in work.columns else 0
    total_materials = work["Materials Budget"].sum() if "Materials Budget" in work.columns else 0
    total_budget = work["Total Budget"].sum() if "Total Budget" in work.columns else total_labor + total_materials

    y = new_page("Grouped View" if grouped else "Standard View")
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(left, y, f"Rows: {len(work):,}   Labor: {_format_report_money(total_labor)}   Materials: {_format_report_money(total_materials)}   Total: {_format_report_money(total_budget)}")
    y -= 16

    def ensure_space(min_y=46):
        nonlocal y
        if y < min_y:
            pdf.showPage()
            y = new_page("Grouped View" if grouped else "Standard View")
        return y

    if grouped and "Work Group" in work.columns:
        grouped_items = []
        for group_name, group_df in work.groupby("Work Group", dropna=False, sort=False):
            group_name = _pdf_safe_text(group_name or "No Work Group Assigned")
            group_total = pd.to_numeric(group_df.get("Total Budget", 0), errors="coerce").fillna(0).sum()
            first_row = group_df.iloc[0]
            grouped_items.append((contractor_priority_sort_value(first_row.get("Priority", "3 - Quote Only")), group_name.lower(), group_name, group_df, group_total))

        for _, __, group_name, group_df, group_total in sorted(grouped_items, key=lambda x: (x[0], x[1])):
            ensure_space(72)
            first = group_df.iloc[0]
            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(left, y, f"{group_name} - {len(group_df)} item(s) - {_format_report_money(group_total)}")
            pdf.setFont("Helvetica", 8)
            contractor_text = _pdf_safe_text(first.get("Contractor", "") or "Unassigned")
            priority_text = _pdf_safe_text(first.get("Priority", ""))
            intent_text = _pdf_safe_text(first.get("Owner Intent", ""))
            pdf.drawString(left + 320, y, f"Contractor: {contractor_text}   Priority: {priority_text}   Intent: {intent_text}")
            y -= 12
            pdf.setFont("Helvetica-Bold", 7.5)
            pdf.drawString(left + 12, y, "RMR")
            pdf.drawString(left + 75, y, "Property")
            pdf.drawString(left + 165, y, "Work Item / Location")
            pdf.drawRightString(right - 120, y, "Labor")
            pdf.drawRightString(right - 65, y, "Materials")
            pdf.drawRightString(right, y, "Total")
            y -= 9
            pdf.setFont("Helvetica", 7.2)
            for _, row in group_df.iterrows():
                ensure_space(50)
                desc = _pdf_safe_text(f"{row.get('Work Item','')} / {row.get('Location','')}")
                row_line_y = y
                pdf.drawString(left + 12, row_line_y, _pdf_safe_text(row.get("RMR ID", ""))[:12])
                pdf.drawString(left + 75, row_line_y, _pdf_safe_text(row.get("Property", ""))[:22])
                y_after = draw_wrapped_text(pdf, desc, left + 165, row_line_y, 370, line_height=8, font_name="Helvetica", font_size=7.2)
                pdf.drawRightString(right - 120, row_line_y, _format_report_money(row.get("Labor Budget", 0)))
                pdf.drawRightString(right - 65, row_line_y, _format_report_money(row.get("Materials Budget", 0)))
                pdf.drawRightString(right, row_line_y, _format_report_money(row.get("Total Budget", 0)))
                y = min(y_after, row_line_y - 9)
            y -= 8
    else:
        pdf.setFont("Helvetica-Bold", 7.5)
        headers = ["RMR", "Property", "Work Item", "Work Group", "Contractor", "Pri", "Timeframe", "Status", "Total"]
        xs = [left, left + 58, left + 140, left + 300, left + 455, left + 560, left + 610, left + 695, right]
        for i, header in enumerate(headers[:-1]):
            pdf.drawString(xs[i], y, header)
        pdf.drawRightString(xs[-1], y, headers[-1])
        y -= 10
        pdf.line(left, y + 4, right, y + 4)
        pdf.setFont("Helvetica", 7)
        for _, row in work.iterrows():
            ensure_space(46)
            row_y = y
            pdf.drawString(xs[0], row_y, _pdf_safe_text(row.get("RMR ID", ""))[:11])
            pdf.drawString(xs[1], row_y, _pdf_safe_text(row.get("Property", ""))[:18])
            pdf.drawString(xs[2], row_y, _pdf_safe_text(row.get("Work Item", ""))[:38])
            pdf.drawString(xs[3], row_y, _pdf_safe_text(row.get("Work Group", ""))[:35])
            pdf.drawString(xs[4], row_y, _pdf_safe_text(row.get("Contractor", "") or "Unassigned")[:22])
            pdf.drawString(xs[5], row_y, _pdf_safe_text(row.get("Priority", ""))[:10])
            pdf.drawString(xs[6], row_y, _pdf_safe_text(row.get("Budget Timeframe", ""))[:18])
            status_value = row.get("RMR Status", "") or row.get("Work Group Status", "")
            pdf.drawString(xs[7], row_y, _pdf_safe_text(status_value)[:18])
            pdf.drawRightString(xs[8], row_y, _format_report_money(row.get("Total Budget", 0)))
            y -= 9

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()



def ensure_rmr_quote_estimate(rmr_id: int) -> int | None:
    """Create/reuse a legacy estimate container for RMR quote requests.

    The old contractor quote engine is estimate-line based. Build 19A reuses that engine
    by creating a hidden/legacy estimate tied to the RMR instead of forcing the user to
    use Estimate Builder.
    """
    rmr = rmr_row_from_id(int(rmr_id))
    if not rmr:
        return None
    existing = fetch_df("SELECT id FROM estimates WHERE source_rmr_id = ? ORDER BY id DESC LIMIT 1", (int(rmr_id),))
    if not existing.empty:
        return int(existing.iloc[0]["id"])
    estimate_name = f"{rmr.get('rmr_code')} | {rmr.get('property_name')} | {rmr.get('work_item_name')}"
    estimate_address = str(rmr.get("address") or "")
    notes = f"Created from RMR {rmr.get('rmr_code')}. {str(rmr.get('notes') or '').strip()}".strip()
    estimate_id = execute_returning_id(
        """
        INSERT INTO estimates (
            source_rmr_id, created_at, modified_at, estimate_name, estimate_address,
            contractor_id, labor_rate, active, notes, category_name, work_group_name,
            estimate_mode, source_method, status, version_no
        ) VALUES (?, NOW(), NOW(), ?, ?, NULL, 0, TRUE, ?, ?, '', 'manual', 'rmr', 'draft', 1)
        """,
        (
            int(rmr_id),
            estimate_name,
            estimate_address,
            notes,
            str(rmr.get("category_name") or ""),
        ),
    )
    if estimate_id:
        set_order_number("estimates", int(estimate_id), "Est")
    return int(estimate_id) if estimate_id else None


def create_rmr_quote_requests(rmr_id: int, contractor_ids: list[int], request_note: str = "") -> tuple[int, list[str]]:
    """Create quote/material/availability requests from an RMR using the existing quote table."""
    rmr = rmr_row_from_id(int(rmr_id))
    if not rmr:
        return 0, ["RMR not found"]
    estimate_id = ensure_rmr_quote_estimate(int(rmr_id))
    if not estimate_id:
        return 0, ["Could not create quote estimate container"]
    created = 0
    skipped = []
    for contractor_id in contractor_ids:
        contractor_id = int(contractor_id)
        contractor_name_df = fetch_df("SELECT name FROM contractors WHERE id = ?", (contractor_id,))
        contractor_name = str(contractor_name_df.iloc[0]["name"]) if not contractor_name_df.empty else f"Contractor {contractor_id}"
        existing_request_df = fetch_df(
            """
            SELECT qr.id
            FROM quote_requests qr
            WHERE COALESCE(qr.rmr_id, 0) = ? AND qr.contractor_id = ?
            LIMIT 1
            """,
            (int(rmr_id), contractor_id),
        )
        if not existing_request_df.empty:
            skipped.append(contractor_name)
            continue
        hours = float(rmr.get("user_estimated_hours") or rmr.get("ai_estimated_hours") or 0)
        labor_budget = float(rmr.get("labor_budget") or 0)
        line_id = execute_returning_id(
            """
            INSERT INTO estimate_lines (
                estimate_id, source_rmr_id, contractor_id, category_name, work_group_name,
                trade_name, task_name, scope_description, repair_quantity,
                onsite_hours_each, travel_hours_each, total_hours_each, onsite_hours, travel_hours,
                total_hours, labor_rate, onsite_cost, travel_cost, manual_repair_amount,
                total_labor_cost, created_at, modified_at
            ) VALUES (?, ?, ?, ?, '', ?, ?, ?, 1, ?, 0, ?, ?, 0, ?, 0, 0, 0, ?, ?, NOW(), NOW())
            """,
            (
                int(estimate_id),
                int(rmr_id),
                contractor_id,
                str(rmr.get("category_name") or ""),
                str(rmr.get("category_name") or ""),
                str(rmr.get("work_item_name") or ""),
                str(rmr.get("scope_description") or ""),
                hours,
                hours,
                hours,
                hours,
                0.0,
                labor_budget,
            ),
        )
        if not line_id:
            skipped.append(contractor_name)
            continue
        quote_request_id = execute_returning_id(
            """
            INSERT INTO quote_requests (
                estimate_line_id, rmr_id, contractor_id, quote_status, quote_amount,
                quote_notes, requested_at, created_at, modified_at
            ) VALUES (?, ?, ?, 'Requested', NULL, ?, NOW(), NOW(), NOW())
            """,
            (int(line_id), int(rmr_id), contractor_id, str(request_note or "").strip()),
        )
        add_rmr_communication(
            int(rmr_id),
            str(request_note or "Quote/materials/availability request created.").strip(),
            author_type="Owner",
            contractor_id=contractor_id,
            quote_request_id=int(quote_request_id) if quote_request_id else None,
        )
        created += 1
    if created:
        add_rmr_history(int(rmr_id), "Quote Requested", f"Created {created} contractor request(s).")
    return created, skipped

if page == "Admin" and not can_access_admin_page():
    st.error("You do not have permission to access the Admin page.")
    st.stop()

if page in ["Other Data", "Reports / Data", "Update Records"]:
    st.subheader(page)
    if page == "Other Data":
        st.info("Choose Estimate Builder or Estimate History below this heading.")
    elif page == "Reports / Data":
        st.info("Choose Master Work List, Contractor Quotes, Project Cost, or Active Projects below this heading.")
    else:
        st.info("Choose Properties, Addresses, Contractors, Work Items, Categories of Labor, Scope Templates, Projects, or Admin below this heading.")
    st.stop()

# -----------------------------
# Properties
# -----------------------------
if page == "Properties":
    st.subheader("Properties")
    st.caption("Master property database. A property may belong to a portfolio, but portfolio assignment is optional.")

    if not can_access_full_app():
        st.error("You do not have permission to access this page.")
        st.stop()

    property_portfolio_options = [""] + PORTFOLIO_NAMES + ["Other", "None"]
    tab_create_property, tab_manage_property = st.tabs(["Create Property", "Manage Properties"])

    with tab_create_property:
        st.markdown("### Create Property")
        with st.form("master_property_create_form"):
            p1, p2 = st.columns(2)
            new_property_name = p1.text_input("Property Name")
            new_property_portfolio = p2.selectbox("Portfolio (optional)", property_portfolio_options, key="properties_create_portfolio")
            new_property_notes = st.text_area("Property Notes", height=90)
            new_property_active = st.selectbox("Property Status", ["Active", "Inactive"], index=0, key="properties_create_active")
            submitted_property = st.form_submit_button("Save Property", type="primary")
            if submitted_property:
                cleaned_property_name = str(new_property_name or "").strip()
                cleaned_portfolio = str(new_property_portfolio or "").strip()
                if cleaned_portfolio == "None":
                    cleaned_portfolio = ""
                if not cleaned_property_name:
                    st.error("Property Name is required.")
                else:
                    duplicate_property = fetch_df(
                        """
                        SELECT id FROM portfolio_properties
                        WHERE COALESCE(deleted, FALSE) = FALSE
                          AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
                          AND LOWER(TRIM(COALESCE(portfolio_name, ''))) = LOWER(TRIM(?))
                        LIMIT 1
                        """,
                        (cleaned_property_name, cleaned_portfolio),
                    )
                    if not duplicate_property.empty:
                        st.error("That property already exists for the selected portfolio.")
                    else:
                        execute(
                            """
                            INSERT INTO portfolio_properties (portfolio_name, property_name, notes, active, deleted, created_at, modified_at)
                            VALUES (?, ?, ?, ?, FALSE, NOW(), NOW())
                            """,
                            (
                                cleaned_portfolio,
                                cleaned_property_name,
                                str(new_property_notes or "").strip(),
                                new_property_active == "Active",
                            ),
                        )
                        st.success("Property saved.")
                        st.rerun()

    with tab_manage_property:
        st.markdown("### Manage Properties")
        pf1, pf2, pf3 = st.columns(3)
        manage_portfolio_filter = pf1.selectbox("Filter Portfolio", ["All"] + PORTFOLIO_NAMES + ["Other", "None"], key="properties_filter_portfolio")
        manage_search_filter = pf2.text_input("Search Property / Notes", key="properties_filter_search")
        include_inactive_properties = pf3.checkbox("Include Inactive", value=True, key="properties_include_inactive")

        portfolio_filter_value = None
        if manage_portfolio_filter not in ["All", "None"]:
            portfolio_filter_value = manage_portfolio_filter

        properties_df = portfolio_properties_df(
            portfolio_name=portfolio_filter_value,
            include_inactive=include_inactive_properties,
            include_deleted=False,
        )
        if manage_portfolio_filter == "None" and not properties_df.empty:
            properties_df = properties_df[properties_df["portfolio_name"].astype(str).str.strip() == ""].copy()
        if manage_search_filter.strip() and not properties_df.empty:
            search_lower = manage_search_filter.strip().lower()
            properties_df = properties_df[
                properties_df["property_name"].astype(str).str.lower().str.contains(search_lower, na=False)
                | properties_df["notes"].astype(str).str.lower().str.contains(search_lower, na=False)
            ].copy()

        if properties_df.empty:
            st.info("No properties found.")
        else:
            display_properties = properties_df[["id", "portfolio_name", "property_name", "active", "notes"]].copy()
            display_properties["active"] = display_properties["active"].map(lambda x: "Yes" if bool(x) else "No")
            st.dataframe(
                display_properties.rename(columns={
                    "id": "Property ID",
                    "portfolio_name": "Portfolio",
                    "property_name": "Property",
                    "active": "Active",
                    "notes": "Notes",
                }),
                use_container_width=True,
                hide_index=True,
            )

            property_edit_labels = [
                f"{int(row.id)} | {row.property_name} | {row.portfolio_name or 'No Portfolio'}"
                for row in properties_df.itertuples()
            ]
            selected_property_edit_label = st.selectbox("Choose Property To Edit", property_edit_labels, key="properties_edit_select")
            selected_property_id = int(selected_property_edit_label.split(" | ", 1)[0])
            selected_property_row = properties_df[properties_df["id"] == selected_property_id].iloc[0]

            with st.form("master_property_edit_form"):
                ep1, ep2 = st.columns(2)
                edited_property_name = ep1.text_input("Property Name", value=str(selected_property_row.get("property_name") or ""))
                current_portfolio = str(selected_property_row.get("portfolio_name") or "")
                edited_portfolio = ep2.selectbox(
                    "Portfolio (optional)",
                    property_portfolio_options,
                    index=property_portfolio_options.index(current_portfolio) if current_portfolio in property_portfolio_options else 0,
                    key=f"properties_edit_portfolio_{selected_property_id}",
                )
                edited_active = st.selectbox("Property Status", ["Active", "Inactive"], index=0 if bool(selected_property_row.get("active")) else 1, key=f"properties_edit_active_{selected_property_id}")
                edited_notes = st.text_area("Property Notes", value=str(selected_property_row.get("notes") or ""), height=90)
                ps1, ps2 = st.columns(2)
                save_property_changes = ps1.form_submit_button("Save Property Changes", type="primary")
                delete_property_clicked = ps2.form_submit_button("Delete Property")

                if save_property_changes:
                    edited_property_name_clean = str(edited_property_name or "").strip()
                    edited_portfolio_clean = str(edited_portfolio or "").strip()
                    if edited_portfolio_clean == "None":
                        edited_portfolio_clean = ""
                    if not edited_property_name_clean:
                        st.error("Property Name is required.")
                    else:
                        execute(
                            """
                            UPDATE portfolio_properties
                            SET property_name = ?, portfolio_name = ?, notes = ?, active = ?, modified_at = NOW()
                            WHERE id = ?
                            """,
                            (
                                edited_property_name_clean,
                                edited_portfolio_clean,
                                str(edited_notes or "").strip(),
                                edited_active == "Active",
                                selected_property_id,
                            ),
                        )
                        execute(
                            """
                            UPDATE portfolio_addresses
                            SET property_name = ?, portfolio_name = ?, modified_at = NOW()
                            WHERE portfolio_property_id = ?
                            """,
                            (edited_property_name_clean, edited_portfolio_clean, selected_property_id),
                        )
                        st.success("Property updated.")
                        st.rerun()

                if delete_property_clicked:
                    execute(
                        "UPDATE portfolio_properties SET deleted = TRUE, active = FALSE, modified_at = NOW() WHERE id = ?",
                        (selected_property_id,),
                    )
                    st.success("Property deleted from active lists. Existing RMR history is not deleted.")
                    st.rerun()

# -----------------------------
# Admin
# -----------------------------
if page == "Addresses":
    st.subheader("Addresses")
    st.caption("Master address database. Addresses can be tied to a portfolio/property, or kept as general addresses.")

    if not can_access_full_app():
        st.error("You do not have permission to access this page.")
        st.stop()

    tab_create, tab_manage = st.tabs(["Create Address", "Manage Addresses"])

    with tab_create:
        st.markdown("### Create Address")
        with st.form("master_address_create_form"):
            c1, c2 = st.columns(2)
            selected_portfolio = c1.selectbox("Portfolio (optional)", [""] + PORTFOLIO_NAMES, key="addr_create_portfolio")
            property_options = [""] + (portfolio_property_labels(selected_portfolio) if selected_portfolio else portfolio_property_labels(None))
            selected_property_label = c2.selectbox("Property (optional)", property_options, key="addr_create_property")

            property_id = None
            property_name = ""
            if selected_property_label:
                property_token = selected_property_label.split(" | ", 1)[0]
                property_id = int(property_token) if property_token.isdigit() else None
                property_name = selected_property_label.split(" | ", 1)[1] if " | " in selected_property_label else selected_property_label

            a1, a2 = st.columns(2)
            new_address = a1.text_input("Address")
            new_unit_number = a2.text_input("Unit Number")
            new_notes = st.text_area("Address Notes", height=80)
            new_active = st.selectbox("Address Status", ["Active", "Inactive"], index=0)

            if st.form_submit_button("Save Address", type="primary"):
                cleaned_address = str(new_address or "").strip()
                cleaned_unit = str(new_unit_number or "").strip()
                if not cleaned_address:
                    st.error("Address is required.")
                else:
                    duplicate_df = fetch_df(
                        """
                        SELECT id FROM portfolio_addresses
                        WHERE LOWER(TRIM(COALESCE(address, ''))) = LOWER(TRIM(?))
                          AND LOWER(TRIM(COALESCE(unit_number, ''))) = LOWER(TRIM(?))
                          AND LOWER(TRIM(COALESCE(property_name, ''))) = LOWER(TRIM(?))
                          AND COALESCE(deleted, FALSE) = FALSE
                        LIMIT 1
                        """,
                        (cleaned_address, cleaned_unit, property_name),
                    )
                    if not duplicate_df.empty:
                        st.error("That address already exists for the selected property/unit.")
                    else:
                        execute(
                            """
                            INSERT INTO portfolio_addresses (
                                portfolio_property_id, portfolio_name, property_name, address, unit_number,
                                notes, active, deleted, created_at, modified_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, NOW(), NOW())
                            """,
                            (
                                property_id,
                                selected_portfolio,
                                property_name,
                                cleaned_address,
                                cleaned_unit,
                                str(new_notes or "").strip(),
                                new_active == "Active",
                            ),
                        )
                        st.success("Address saved.")
                        st.rerun()

    with tab_manage:
        st.markdown("### Manage Addresses")
        m1, m2, m3 = st.columns(3)
        filter_portfolio = m1.selectbox("Filter Portfolio", ["All"] + PORTFOLIO_NAMES, key="addr_filter_portfolio")
        filter_property = m2.text_input("Filter Property Contains", key="addr_filter_property")
        filter_search = m3.text_input("Search Address / Unit", key="addr_filter_search")

        address_df = portfolio_addresses_df(
            portfolio_name=None if filter_portfolio == "All" else filter_portfolio,
            include_inactive=True,
            include_deleted=False,
        )
        if not address_df.empty:
            filtered_df = address_df.copy()
            if filter_property.strip():
                filtered_df = filtered_df[filtered_df["property_name"].astype(str).str.lower().str.contains(filter_property.strip().lower(), na=False)].copy()
            if filter_search.strip():
                search_lower = filter_search.strip().lower()
                mask = (
                    filtered_df["address"].astype(str).str.lower().str.contains(search_lower, na=False)
                    | filtered_df["unit_number"].astype(str).str.lower().str.contains(search_lower, na=False)
                    | filtered_df["notes"].astype(str).str.lower().str.contains(search_lower, na=False)
                )
                filtered_df = filtered_df[mask].copy()

            display_df = filtered_df[["id", "portfolio_name", "property_name", "address", "unit_number", "active", "notes"]].copy()
            display_df["active"] = display_df["active"].map(lambda x: "Yes" if bool(x) else "No")
            st.dataframe(
                display_df.rename(columns={
                    "id": "Address ID",
                    "portfolio_name": "Portfolio",
                    "property_name": "Property",
                    "address": "Address",
                    "unit_number": "Unit",
                    "active": "Active",
                    "notes": "Notes",
                }),
                use_container_width=True,
                hide_index=True,
            )

            if filtered_df.empty:
                st.info("No addresses match the filters.")
            else:
                edit_labels = [
                    f"{int(row.id)} | {row.property_name or '(general)'} | {row.address}{(' | Unit ' + str(row.unit_number)) if str(row.unit_number or '').strip() else ''}"
                    for row in filtered_df.itertuples()
                ]
                selected_edit_label = st.selectbox("Choose Address To Edit", edit_labels, key="addr_edit_select")
                selected_address_id = int(selected_edit_label.split(" | ", 1)[0])
                selected_address_row = filtered_df[filtered_df["id"] == selected_address_id].iloc[0]

                with st.form("master_address_edit_form"):
                    e1, e2 = st.columns(2)
                    edit_portfolio = e1.selectbox(
                        "Portfolio (optional)",
                        [""] + PORTFOLIO_NAMES,
                        index=([""] + PORTFOLIO_NAMES).index(str(selected_address_row.get("portfolio_name") or "")) if str(selected_address_row.get("portfolio_name") or "") in ([""] + PORTFOLIO_NAMES) else 0,
                    )
                    edit_property_options = [""] + (portfolio_property_labels(edit_portfolio) if edit_portfolio else portfolio_property_labels(None))
                    current_property_name = str(selected_address_row.get("property_name") or "")
                    current_property_label = ""
                    for label in edit_property_options:
                        if label.endswith(" | " + current_property_name):
                            current_property_label = label
                            break
                    edit_property_label = e2.selectbox(
                        "Property (optional)",
                        edit_property_options,
                        index=edit_property_options.index(current_property_label) if current_property_label in edit_property_options else 0,
                    )

                    edit_property_id = None
                    edit_property_name = ""
                    if edit_property_label:
                        edit_property_token = edit_property_label.split(" | ", 1)[0]
                        edit_property_id = int(edit_property_token) if edit_property_token.isdigit() else None
                        edit_property_name = edit_property_label.split(" | ", 1)[1] if " | " in edit_property_label else edit_property_label

                    ea1, ea2 = st.columns(2)
                    edit_address = ea1.text_input("Address", value=str(selected_address_row.get("address") or ""))
                    edit_unit = ea2.text_input("Unit Number", value=str(selected_address_row.get("unit_number") or ""))
                    edit_active = st.selectbox("Address Status", ["Active", "Inactive"], index=0 if bool(selected_address_row.get("active")) else 1)
                    edit_notes = st.text_area("Address Notes", value=str(selected_address_row.get("notes") or ""), height=80)

                    save_col, delete_col = st.columns(2)
                    save_clicked = save_col.form_submit_button("Save Address Changes", type="primary")
                    delete_clicked = delete_col.form_submit_button("Delete Address")

                    if save_clicked:
                        if not str(edit_address or "").strip():
                            st.error("Address is required.")
                        else:
                            execute(
                                """
                                UPDATE portfolio_addresses
                                SET portfolio_property_id = ?, portfolio_name = ?, property_name = ?,
                                    address = ?, unit_number = ?, notes = ?, active = ?, modified_at = NOW()
                                WHERE id = ?
                                """,
                                (
                                    edit_property_id,
                                    edit_portfolio,
                                    edit_property_name,
                                    str(edit_address or "").strip(),
                                    str(edit_unit or "").strip(),
                                    str(edit_notes or "").strip(),
                                    edit_active == "Active",
                                    selected_address_id,
                                ),
                            )
                            st.success("Address updated.")
                            st.rerun()

                    if delete_clicked:
                        execute(
                            "UPDATE portfolio_addresses SET deleted = TRUE, active = FALSE, modified_at = NOW() WHERE id = ?",
                            (selected_address_id,),
                        )
                        st.success("Address deleted.")
                        st.rerun()
        else:
            st.info("No addresses found yet.")


if page == "Admin":
    st.subheader("Admin")
    st.caption("Owner-only user management")

    st.markdown("### Portfolio Property / Address Setup")
    st.caption("Use this button only when you want to load the default portfolio property and address list. It no longer runs automatically at app startup.")
    if st.button("Load Default Portfolio Properties & Addresses", key="manual_seed_portfolio_addresses_btn"):
        with st.spinner("Loading portfolio properties and addresses..."):
            seed_default_portfolio_properties_and_addresses()
        st.success("Default portfolio properties and addresses loaded.")
        st.rerun()

    contractor_name_options = ["Not linked"] + get_contractor_names()
    user_df = get_user_accounts_df()

    st.markdown("### User Management")
    st.caption("Create users, edit users, reset passwords, and delete users from one place. Passwords are owner-managed so you always know the login credentials.")

    display_users = user_df.copy()
    if not display_users.empty:
        for col in ["created_at", "modified_at"]:
            if col in display_users.columns:
                display_users[col] = pd.to_datetime(display_users[col], errors="coerce").dt.strftime("%m-%d-%Y")
        display_users["active"] = display_users["active"].map(lambda x: "Yes" if bool(x) else "No")
        st.dataframe(
            display_users[["username", "role", "allowed_portfolio", "contractor_name", "active", "created_at", "modified_at"]].rename(columns={
                "username": "Username",
                "role": "Role",
                "allowed_portfolio": "Portfolio Access",
                "contractor_name": "Linked Contractor",
                "active": "Active",
                "created_at": "Created",
                "modified_at": "Modified",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No users found yet.")

    tab1, tab2, tab3, tab4 = st.tabs(["Create User", "Edit User", "Reset Password", "Delete User"])

    with tab1:
        st.markdown("#### Create User")
        with st.form("admin_add_user_form"):
            a1, a2 = st.columns(2)
            new_username = a1.text_input("Username")
            new_password = a2.text_input("Password You Want To Assign", type="password")
            b1, b2 = st.columns(2)
            new_role = b1.selectbox("Role", USER_ROLE_OPTIONS)
            new_contractor_name = b2.selectbox(
                "Linked Contractor",
                contractor_name_options,
                help="Optional now; use this when the login should see contractor-specific pages.",
            )
            new_allowed_portfolio = st.selectbox(
                "Portfolio Access For Property Manager",
                [""] + PORTFOLIO_NAMES,
                help="Required for Property Manager logins. Leave blank for other roles.",
            )
            submitted_add_user = st.form_submit_button("Create User", type="primary")
            if submitted_add_user:
                cleaned_username = new_username.strip()
                if not cleaned_username:
                    st.error("Username is required.")
                elif len(new_password) < 6:
                    st.error("Password must be at least 6 characters.")
                elif get_user_account(cleaned_username):
                    st.error("That username already exists.")
                elif new_role == "Property Manager" and not new_allowed_portfolio:
                    st.error("Choose a portfolio for this Property Manager.")
                else:
                    linked_contractor_id = get_contractor_id_by_name(new_contractor_name)
                    add_user_account(
                        cleaned_username,
                        new_password,
                        new_role,
                        linked_contractor_id if new_contractor_name != "Not linked" else None,
                        new_allowed_portfolio if new_role == "Property Manager" else "",
                    )
                    st.success("User created.")
                    st.rerun()

    with tab2:
        st.markdown("#### Edit User")
        if user_df.empty:
            st.info("No users found.")
        else:
            edit_labels = [f"{row['id']} | {row['username']} | {row['role']}" for _, row in user_df.iterrows()]
            selected_edit_label = st.selectbox("Choose User To Edit", edit_labels, key="admin_edit_user_select")
            selected_user_id = int(selected_edit_label.split(" | ")[0])
            selected_user_row = user_df[user_df["id"] == selected_user_id].iloc[0]

            current_contractor_name = str(selected_user_row.get("contractor_name") or "")
            current_contractor_option = current_contractor_name if current_contractor_name in contractor_name_options else "Not linked"

            with st.form("admin_edit_user_form"):
                c1, c2 = st.columns(2)
                edit_username = c1.text_input("Username", value=str(selected_user_row["username"]))
                edit_role = c2.selectbox(
                    "Role",
                    USER_ROLE_OPTIONS,
                    index=USER_ROLE_OPTIONS.index(str(selected_user_row["role"])),
                )
                d1, d2 = st.columns(2)
                edit_contractor_name = d1.selectbox("Linked Contractor", contractor_name_options, index=contractor_name_options.index(current_contractor_option))
                edit_active = d2.selectbox("Active", ["Yes", "No"], index=0 if bool(selected_user_row["active"]) else 1)
                current_allowed_portfolio = str(selected_user_row.get("allowed_portfolio") or "")
                portfolio_options = [""] + PORTFOLIO_NAMES
                edit_allowed_portfolio = st.selectbox(
                    "Portfolio Access For Property Manager",
                    portfolio_options,
                    index=portfolio_options.index(current_allowed_portfolio) if current_allowed_portfolio in portfolio_options else 0,
                )

                if st.form_submit_button("Save User Changes", type="primary"):
                    cleaned_username = edit_username.strip()
                    if not cleaned_username:
                        st.error("Username is required.")
                    elif edit_role == "Property Manager" and not edit_allowed_portfolio:
                        st.error("Choose a portfolio for this Property Manager.")
                    else:
                        existing_user = get_user_account(cleaned_username)
                        if existing_user and int(existing_user["id"]) != selected_user_id:
                            st.error("That username already exists.")
                        else:
                            linked_contractor_id = get_contractor_id_by_name(edit_contractor_name)
                            update_user_account(
                                user_id=selected_user_id,
                                username=cleaned_username,
                                role=edit_role,
                                contractor_id=linked_contractor_id if edit_contractor_name != "Not linked" else None,
                                active=(edit_active == "Yes"),
                                password=None,
                                allowed_portfolio=edit_allowed_portfolio if edit_role == "Property Manager" else "",
                            )
                            st.success("User updated.")
                            st.rerun()

    with tab3:
        st.markdown("#### Reset Password")
        st.caption("Use this when you want to assign a known password yourself. The user does not create their own password.")
        if user_df.empty:
            st.info("No users found.")
        else:
            reset_labels = [f"{row['id']} | {row['username']} | {row['role']}" for _, row in user_df.iterrows()]
            selected_reset_label = st.selectbox("Choose User To Reset Password", reset_labels, key="admin_reset_user_select")
            selected_reset_user_id = int(selected_reset_label.split(" | ")[0])
            selected_reset_user_row = user_df[user_df["id"] == selected_reset_user_id].iloc[0]
            st.text_input("Selected Username", value=str(selected_reset_user_row["username"]), disabled=True)
            with st.form("admin_reset_password_form"):
                reset_password = st.text_input("New Known Password", type="password")
                confirm_reset_password = st.text_input("Confirm New Known Password", type="password")
                if st.form_submit_button("Reset Password", type="primary"):
                    if len(reset_password) < 6:
                        st.error("Password must be at least 6 characters.")
                    elif reset_password != confirm_reset_password:
                        st.error("The password entries do not match.")
                    else:
                        update_user_account(
                            user_id=selected_reset_user_id,
                            username=str(selected_reset_user_row["username"]),
                            role=str(selected_reset_user_row["role"]),
                            contractor_id=(int(selected_reset_user_row["contractor_id"]) if int(selected_reset_user_row["contractor_id"] or 0) > 0 else None),
                            active=bool(selected_reset_user_row["active"]),
                            password=reset_password,
                            allowed_portfolio=str(selected_reset_user_row.get("allowed_portfolio") or ""),
                        )
                        st.success("Password reset.")
                        st.rerun()

    with tab4:
        st.markdown("#### Delete User")
        if user_df.empty:
            st.info("No users to delete.")
        else:
            delete_labels = [f"{row['id']} | {row['username']} | {row['role']}" for _, row in user_df.iterrows()]
            selected_delete_label = st.selectbox("Choose User To Delete", delete_labels, key="admin_delete_user_select")
            selected_delete_user_id = int(selected_delete_label.split(" | ")[0])

            if st.button("Delete Selected User", type="primary", key="admin_delete_user_button"):
                if selected_delete_user_id == int(st.session_state.get("logged_in_user_id", 0) or 0):
                    st.error("You cannot delete the user you are currently logged in as.")
                else:
                    delete_user_account(selected_delete_user_id)
                    st.success("User deleted.")
                    st.rerun()

    st.markdown("---")
    st.subheader("Backup And Restore")
    st.caption("Owner-only backup and restore tools.")

    db_data = get_database_file()
    if db_data:
        st.download_button(
            label="Download Backup",
            data=db_data,
            file_name=f"renovation_estimator_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.zip",
            mime="application/zip",
            key="admin_download_backup",
        )
    else:
        st.warning("Database file not found.")

    st.markdown("### Restore Backup")
    uploaded_restore_zip = st.file_uploader(
        "Upload Backup ZIP",
        type=["zip"],
        key="admin_restore_backup_zip",
        help="Upload a backup ZIP previously downloaded from this app.",
    )
    confirm_restore = st.checkbox(
        "I understand this will replace the current app data with the uploaded backup.",
        key="admin_restore_confirm",
    )

    if st.button("Restore Backup", type="primary", key="admin_restore_backup_button"):
        if uploaded_restore_zip is None:
            st.error("Upload a backup ZIP first.")
        elif not confirm_restore:
            st.error("Check the confirmation box before restoring.")
        else:
            success, message = restore_database_from_zip(uploaded_restore_zip.getvalue())
            if success:
                st.success(message)
                st.rerun()
            else:
                st.error(message)

# -----------------------------
# My Estimates (Contractor)
# -----------------------------
if page == "My Estimates":
    if current_role != "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    contractor_id = int(st.session_state.get("logged_in_contractor_id") or 0)
    if not contractor_id:
        st.warning("This contractor user is not linked to a contractor record yet.")
    else:
        st.subheader("My Estimates")
        st.caption("Only estimate items assigned to your contractor profile are shown here.")

        st.markdown("### My RMR Quote / Materials / Availability Requests")
        my_rmr_requests = fetch_df(
            """
            SELECT qr.id AS quote_request_id, COALESCE(qr.rmr_id, 0) AS rmr_id,
                   COALESCE(r.rmr_code, 'RMR-' || LPAD(r.id::text, 6, '0')) AS rmr_code,
                   COALESCE(r.property_name, '') AS property_name,
                   COALESCE(r.address, '') AS address,
                   COALESCE(r.location_identifier, '') AS location_identifier,
                   COALESCE(r.work_item_name, '') AS work_item_name,
                   COALESCE(r.scope_description, '') AS scope_description,
                   COALESCE(r.contractor_priority, '3 - Quote Only') AS contractor_priority,
                   COALESCE(r.owner_intent, 'Quote Only') AS owner_intent,
                   COALESCE(qr.quote_status, 'Requested') AS quote_status,
                   COALESCE(qr.quote_amount, 0) AS quote_amount,
                   COALESCE(qr.quote_notes, '') AS quote_notes,
                   qr.requested_at, qr.submitted_at, qr.modified_at,
                   (SELECT COUNT(*) FROM rmr_communications rc WHERE rc.rmr_id = r.id AND COALESCE(rc.contractor_id, 0) = ? AND COALESCE(rc.is_unread_for_contractor, FALSE) = TRUE) AS unread_owner_notes
            FROM quote_requests qr
            JOIN renovation_master_records r ON r.id = qr.rmr_id
            WHERE qr.contractor_id = ? AND COALESCE(r.deleted, FALSE) = FALSE
            ORDER BY qr.requested_at DESC NULLS LAST, qr.id DESC
            """,
            (int(contractor_id), int(contractor_id)),
        )
        if my_rmr_requests.empty:
            st.info("No RMR quote/materials/availability requests are currently assigned to you.")
        else:
            rq_display = my_rmr_requests.copy()
            rq_display["requested_at"] = pd.to_datetime(rq_display["requested_at"], errors="coerce").dt.strftime("%m-%d-%Y")
            st.dataframe(
                rq_display[["rmr_code", "property_name", "location_identifier", "work_item_name", "contractor_priority", "owner_intent", "quote_status", "quote_amount", "unread_owner_notes", "requested_at"]].rename(columns={
                    "rmr_code": "RMR ID", "property_name": "Property", "location_identifier": "Location", "work_item_name": "Work Item",
                    "contractor_priority": "Priority", "owner_intent": "Owner Intent", "quote_status": "Status", "quote_amount": "Quote Amount",
                    "unread_owner_notes": "Unread Owner Notes", "requested_at": "Requested",
                }),
                use_container_width=True,
                hide_index=True,
            )
            request_labels = [f"{int(row.quote_request_id)} | {row.rmr_code} | {row.property_name} | {row.work_item_name}" for row in my_rmr_requests.itertuples()]
            selected_request_label = st.selectbox("Choose RMR Request", request_labels, key="contractor_rmr_request_select")
            selected_request_id = int(selected_request_label.split(" | ", 1)[0])
            request_row = my_rmr_requests[my_rmr_requests["quote_request_id"].astype(int) == selected_request_id].iloc[0]
            selected_rmr_id = int(request_row.get("rmr_id") or 0)
            st.write(f"**RMR:** {request_row.get('rmr_code', '')}")
            st.write(f"**Property:** {request_row.get('property_name', '')}")
            if str(request_row.get("address") or "").strip():
                st.write(f"**Address:** {request_row.get('address', '')}")
            if str(request_row.get("location_identifier") or "").strip():
                st.write(f"**Location:** {request_row.get('location_identifier', '')}")
            st.write(f"**Work Item:** {request_row.get('work_item_name', '')}")
            st.write(f"**Priority:** {request_row.get('contractor_priority', '')}")
            st.write(f"**Owner Intent:** {request_row.get('owner_intent', '')}")
            if str(request_row.get("scope_description") or "").strip():
                st.write(f"**Scope:** {request_row.get('scope_description', '')}")
            if str(request_row.get("quote_notes") or "").strip():
                st.write(f"**Owner Request Notes:** {request_row.get('quote_notes', '')}")

            st.markdown("#### Submit / Update Response")
            response_amount = st.number_input("Quote Amount", min_value=0.0, value=float(request_row.get("quote_amount") or 0.0), step=50.0, key=f"contractor_rmr_quote_amount_{selected_request_id}")
            response_note = st.text_area("Contractor Notes / Materials / Availability / Comments", height=120, key=f"contractor_rmr_response_note_{selected_request_id}")
            if st.button("Submit Contractor Response", type="primary", key=f"submit_contractor_rmr_response_{selected_request_id}"):
                note_clean = str(response_note or "").strip()
                execute(
                    """
                    UPDATE quote_requests
                    SET quote_status = 'Responded', quote_amount = ?,
                        quote_notes = CASE WHEN ? <> '' THEN ? ELSE quote_notes END,
                        submitted_at = COALESCE(submitted_at, NOW()), modified_at = NOW()
                    WHERE id = ? AND contractor_id = ?
                    """,
                    (float(response_amount or 0.0), note_clean, note_clean, int(selected_request_id), int(contractor_id)),
                )
                if note_clean:
                    add_rmr_communication(selected_rmr_id, note_clean, author_type="Contractor", contractor_id=contractor_id, quote_request_id=selected_request_id)
                else:
                    add_rmr_history(selected_rmr_id, "Contractor Responded", f"Quote response submitted by contractor. Amount: ${float(response_amount or 0):,.2f}")
                st.success("Response submitted.")
                st.rerun()

            if selected_rmr_id:
                execute("UPDATE rmr_communications SET is_unread_for_contractor = FALSE WHERE rmr_id = ? AND COALESCE(contractor_id, 0) = ?", (int(selected_rmr_id), int(contractor_id)))
                render_rmr_communication_thread(selected_rmr_id, allow_contractor_note=True, contractor_id=contractor_id, section_key="contractor_my_estimates")

        st.markdown("---")
        my_estimates = contractor_estimates_df(contractor_id)
        if my_estimates.empty:
            st.info("No estimates are currently assigned to you.")
        else:
            estimate_labels = [
                f"{int(row.estimate_id)} | {row.estimate_name} | {row.status}"
                for row in my_estimates.itertuples()
            ]
            selected_label = st.selectbox("Choose Estimate", estimate_labels, key="contractor_estimate_select")
            selected_estimate_id = int(selected_label.split(" | ", 1)[0])

            selected_estimate = my_estimates[my_estimates["estimate_id"] == selected_estimate_id].iloc[0]
            st.write(f"**Estimate:** {selected_estimate['estimate_name']}")
            if str(selected_estimate.get("estimate_address") or "").strip():
                st.write(f"**Address:** {selected_estimate['estimate_address']}")
            st.write(f"**Status:** {selected_estimate['status']}")

            lines_df = contractor_estimate_lines_df(selected_estimate_id, contractor_id)
            if lines_df.empty:
                st.info("No estimate repair items are assigned to you on this estimate.")
            else:
                display_df = lines_df.copy()
                display_df["hourly_hours_shown"] = display_df.apply(
                    lambda r: 0 if float(r["manual_repair_amount"] or 0) > 0 else float(r["total_hours"] or 0),
                    axis=1,
                )
                display_df["manual_amount_shown"] = display_df["manual_repair_amount"].fillna(0)

                st.dataframe(
                    display_df[[
                        "task_name",
                        "trade_name",
                        "scope_description",
                        "repair_quantity",
                        "hourly_hours_shown",
                        "manual_amount_shown",
                        "amount_used",
                    ]].rename(columns={
                        "task_name": "Work Item",
                        "trade_name": 'Category of Labor',
                        "scope_description": "Scope",
                        "repair_quantity": "Qty",
                        "hourly_hours_shown": "Hours",
                        "manual_amount_shown": "Manual Override Amount",
                        "amount_used": "Amount Used",
                    }),
                    use_container_width=True,
                )

                total_hours = float(display_df["hourly_hours_shown"].sum())
                total_manual = float(display_df["manual_amount_shown"].sum())
                total_used = float(display_df["amount_used"].sum())

                c1, c2, c3 = st.columns(3)
                c1.metric("Total Hours", f"{total_hours:,.2f}")
                c2.metric("Manual Override Total", f"${total_manual:,.2f}")
                c3.metric("Total Amount Used", f"${total_used:,.2f}")

# -----------------------------
# My Work Groups (Contractor)
# -----------------------------
elif page == 'My Work Groups':
    if current_role != "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    contractor_id = int(st.session_state.get("logged_in_contractor_id") or 0)
    if not contractor_id:
        st.warning("This contractor user is not linked to a contractor record yet.")
    else:
        st.subheader('My Work Groups')
        st.caption('Only work groups assigned to your contractor profile are shown here.')
        render_contractor_priority_legend()
        my_work_groups_df = contractor_work_groups_df(contractor_id)
        if my_work_groups_df.empty:
            st.info('No work groups are currently assigned to you.')
        else:
            display_df = my_work_groups_df.copy()
            for col in ["due_date", "created_at", "modified_at"]:
                display_df[col] = pd.to_datetime(display_df[col], errors="coerce").dt.strftime("%m-%d-%Y")
            display_df["Amount To Be Paid"] = display_df["amount_to_be_paid"].fillna(0).astype(float)
            display_df["Contractor Requested Price"] = display_df["contractor_requested_price"].fillna(0).astype(float)
            st.dataframe(
                display_df[[
                    "contractor_priority", "owner_intent", "order_number", "project_name", "category_name", "work_group_name", "task_name", "trade_name", "Contractor Requested Price", "Amount To Be Paid", "due_date", "status"
                ]].rename(columns={
                    "contractor_priority": "Priority",
                    "owner_intent": "Owner Intent",
                    "order_number": "Order Number",
                    "project_name": "Project",
                    "category_name": 'Category of Labor',
                    "work_group_name": 'Work Group Name',
                    "task_name": "Work Item",
                    "trade_name": 'Work Item Category of Labor',
                    "due_date": "Due Date",
                    "status": "Status",
                }),
                use_container_width=True,
                hide_index=True,
            )

            labels = [f"{row.order_number} | {int(row.id)} | {row.project_name} | {row.task_name}" for row in my_work_groups_df.itertuples()]
            selected_label = st.selectbox('Choose Work Group', labels, key="contractor_work_group_select")
            selected_id = int(selected_label.split(" | ")[1])
            selected_row = my_work_groups_df[my_work_groups_df["id"] == selected_id].iloc[0]

            st.write(f"**Work Group Number:** {selected_row.get('order_number', '')}")
            st.write(f"**Owner Priority:** {selected_row.get('contractor_priority', '3 - Quote Only')}")
            st.write(f"**Owner Intent:** {selected_row.get('owner_intent', 'Quote Only')}")
            st.write(f"**Project:** {selected_row['project_name']}")
            st.write(f"**Category:** {selected_row.get('category_name', '')}")
            st.write(f"**Work Group Name:** {selected_row.get('work_group_name', '')}")
            if str(selected_row.get("project_address") or "").strip():
                st.write(f"**Address:** {selected_row['project_address']}")
            st.write(f"**Work Item:** {selected_row['task_name']}")
            st.write(f"**Work Item Category:** {selected_row['trade_name']}")
            st.write(f"**Due Date:** {pd.to_datetime(selected_row['due_date'], errors='coerce').strftime('%m-%d-%Y') if pd.notna(pd.to_datetime(selected_row['due_date'], errors='coerce')) else ''}")
            st.write(f"**Status:** {selected_row['status']}")
            contractor_requested_value = float(selected_row.get("contractor_requested_price") or 0)
            amount_to_be_paid_value = float(selected_row.get("amount_to_be_paid") or 0)
            st.write(f"**Contractor Requested Price:** ${contractor_requested_value:,.2f}")
            st.write(f"**Amount To Be Paid For Work Group:** ${amount_to_be_paid_value:,.2f}")
            if str(selected_row.get("scope_description") or "").strip():
                st.write(f"**Scope:** {selected_row['scope_description']}")
            if str(selected_row.get("notes") or "").strip():
                st.write(f"**Owner / Manager Notes:** {selected_row['notes']}")

            st.markdown("### Add Contractor Update")
            contractor_requested_price_input = st.number_input(
                "Contractor Requested Price",
                min_value=0.0,
                value=float(selected_row.get("contractor_requested_price") or 0.0),
                step=50.0,
                key=f"contractor_requested_price_{selected_id}",
            )
            contractor_update_note = st.text_area(
                "Contractor Notes / Updates",
                height=120,
                placeholder="Enter progress updates, material requests, completion notes, or questions for the owner.",
                key=f"contractor_work_group_update_note_{selected_id}",
            )
            contractor_update_photos = st.file_uploader(
                "Upload Contractor Photos",
                type=["png", "jpg", "jpeg", "webp"],
                accept_multiple_files=True,
                key=f"contractor_work_group_update_photos_{selected_id}",
            )
            if st.button("Save Contractor Update", type="primary", key=f"save_contractor_work_group_update_{selected_id}"):
                note_text_clean = str(contractor_update_note or "").strip()
                has_photos = bool(contractor_update_photos)
                if not note_text_clean and not has_photos:
                    st.error("Enter a contractor note or upload at least one photo before saving.")
                else:
                    execute(
                        """
                        UPDATE work_groups
                        SET contractor_requested_price = ?, modified_at = NOW()
                        WHERE id = ?
                        """,
                        (
                            float(contractor_requested_price_input or 0.0) if float(contractor_requested_price_input or 0.0) > 0 else None,
                            selected_id,
                        ),
                    )
                    if note_text_clean:
                        add_work_group_contractor_note(
                            work_group_id=selected_id,
                            contractor_id=contractor_id,
                            note_text=note_text_clean,
                            entered_by=str(st.session_state.get("logged_in_user", "") or ""),
                        )
                    if has_photos:
                        save_work_group_photos(
                            selected_id,
                            contractor_update_photos,
                            uploaded_by=str(st.session_state.get("logged_in_user", "") or ""),
                        )
                    st.success("Contractor update saved.")
                    st.rerun()

            render_work_group_contractor_notes(selected_id)
            render_work_group_photos_readonly(selected_id, section_key="contractor")


# -----------------------------
# My Punch Lists (Contractor)
# -----------------------------
elif page == "My Punch Lists":
    if current_role != "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    contractor_id = int(st.session_state.get("logged_in_contractor_id") or 0)
    if not contractor_id:
        st.warning("This contractor user is not linked to a contractor record yet.")
    else:
        st.subheader("My Punch Lists")
        st.caption("Only punch list projects and items assigned to your contractor profile are shown here.")
        my_projects = contractor_punch_list_projects_df(contractor_id)
        if my_projects.empty:
            st.info("No punch lists are currently assigned to you.")
        else:
            project_labels = [
                f"{row.order_number} | {int(row.id)} | {row.project_name} | {row.status}"
                for row in my_projects.itertuples()
            ]
            selected_project_label = st.selectbox("Choose Punch List", project_labels, key="contractor_punch_list_select")
            selected_project_id = int(selected_project_label.split(" | ")[1])

            project_row = my_projects[my_projects["id"] == selected_project_id].iloc[0]
            st.write(f"**Punch List Order Number:** {project_row.get('order_number', '')}")
            st.write(f"**Project:** {project_row['project_name']}")
            if st.session_state.get("show_shared_ids") and str(project_row.get("project_code") or "").strip():
                st.write(f"**Project Code:** {project_row.get('project_code')}")
            if str(project_row.get("project_address") or "").strip():
                st.write(f"**Address:** {project_row['project_address']}")
            st.write(f"**Status:** {project_row['status']}")
            if pd.notna(project_row.get("inspection_date")):
                st.write(f"**Inspection Date:** {pd.to_datetime(project_row['inspection_date']).strftime('%m-%d-%Y')}")
            if pd.notna(project_row.get("deadline_date")):
                st.write(f"**Date To Complete:** {pd.to_datetime(project_row['deadline_date']).strftime('%m-%d-%Y')}")
            if str(project_row.get("notes") or "").strip():
                st.write(f"**Project Notes:** {project_row['notes']}")

            items_df = punch_list_items_df(selected_project_id, contractor_id=contractor_id)
            if items_df.empty:
                st.info("No punch list items are currently assigned to you on this project.")
            else:
                display_items = items_df.copy()
                for col in ["identified_date", "deadline_date", "completed_date"]:
                    display_items[col] = pd.to_datetime(display_items[col], errors="coerce").dt.strftime("%m-%d-%Y")
                st.dataframe(
                    display_items[[
                        "item_title",
                        "trade_name",
                        "item_status",
                        "identified_date",
                        "deadline_date",
                        "completed_date",
                        "quote_requested",
                    ]].rename(columns={
                        "item_title": "Punch List Item",
                        "trade_name": 'Category of Labor',
                        "item_status": "Status",
                        "identified_date": "Inspection / Identified Date",
                        "deadline_date": "Deadline",
                        "completed_date": "Completed",
                        "quote_requested": "Quote Requested",
                    }),
                    use_container_width=True,
                )

                st.markdown("### Punch List Item Detail")
                item_labels = [f"{int(row.id)} | {row.item_title}" for row in items_df.itertuples()]
                selected_item_label = st.selectbox("Choose Assigned Item", item_labels, key="contractor_punch_item_select")
                selected_item_id = int(selected_item_label.split(" | ", 1)[0])
                selected_item_row = items_df[items_df["id"] == selected_item_id].iloc[0]

                st.write(f"**Punch List Item:** {selected_item_row['item_title']}")
                if str(selected_item_row.get("scope_description") or "").strip():
                    st.write(f"**Scope / Notes:** {selected_item_row['scope_description']}")
                if str(selected_item_row.get("manager_notes") or "").strip():
                    st.write(f"**Manager Notes:** {selected_item_row['manager_notes']}")
                if str(selected_item_row.get("contractor_notes") or "").strip():
                    st.write(f"**Contractor Notes:** {selected_item_row['contractor_notes']}")

                if st.checkbox("Load photos for this punch list item", key=f"contractor_punch_photos_{selected_item_id}"):
                    existing_pl_photos = punch_list_item_photos(selected_item_id)
                    if existing_pl_photos:
                        cols = st.columns(min(4, max(1, len(existing_pl_photos))))
                        for idx, photo in enumerate(existing_pl_photos):
                            with cols[idx % len(cols)]:
                                render_photo_item(photo)

# -----------------------------
# Dashboard
# -----------------------------
elif page == "Dashboard":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Categories of Labor', len(fetch_df("SELECT * FROM trades")))
    c2.metric("Work Items", len(fetch_df("SELECT * FROM tasks WHERE active = TRUE")))
    c3.metric("Contractors", len(fetch_df("SELECT * FROM contractors")))
    c4.metric("Estimates", len(fetch_df("SELECT * FROM estimates")))

    st.subheader("Recent Estimates")
    recent = fetch_df(
        """
        SELECT
            e.id AS estimate_id,
            e.created_at,
            COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
            COALESCE(c.name, '') AS contractor_name,
            CASE WHEN COALESCE(e.active, TRUE) THEN 'Active' ELSE 'Inactive' END AS status,
            COALESCE(e.labor_rate, 0) AS labor_rate,
            COALESCE(SUM(el.total_hours), 0) AS total_hours,
            COALESCE(SUM(el.total_labor_cost), 0) AS total_labor_cost
        FROM estimates e
        LEFT JOIN contractors c ON c.id = e.contractor_id
        LEFT JOIN estimate_lines el ON el.estimate_id = e.id
        GROUP BY e.id, e.created_at, e.estimate_name, c.name, e.active, e.labor_rate
        ORDER BY LOWER(COALESCE(e.estimate_name, '(unnamed)')), e.id
        LIMIT 10
        """
    )
    if recent.empty:
        st.info("No estimates saved yet.")
    else:
        recent_display = recent.copy()
        for col in ["created_at", "modified_at"]:
            if col in recent_display.columns:
                recent_display[col] = pd.to_datetime(recent_display[col], errors="coerce").dt.strftime("%m-%d-%Y")
        recent_display = recent_display.rename(columns={
            "created_at": "Date Created",
            "modified_at": "Date Modified",
        })
        st.dataframe(recent_display, use_container_width=True)

        recent_labels = [
            f"{int(row.estimate_id)} | {row.estimate_name}" for row in recent.itertuples()
        ]
        d1, d2 = st.columns([4, 1])
        selected_recent_label = d1.selectbox(
            "Quick Edit Recent Estimate",
            recent_labels,
            key="dashboard_recent_estimate_select",
        )
        if d2.button("Edit", key="dashboard_recent_edit"):
            selected_recent_id = int(selected_recent_label.split(" | ", 1)[0])
            if load_estimate_into_editor(selected_recent_id):
                st.success("Estimate loaded into builder.")
                st.rerun()

# -----------------------------
# Manager Repair Requests
# -----------------------------
elif page in ("Residences Portfolio", "Sandstone Portfolio", "Manager Repair Requests", "My Repair Requests"):
    current_user_id = int(st.session_state.get("logged_in_user_id", 0) or 0)
    current_username = str(st.session_state.get("logged_in_user", "") or "")
    current_role = str(st.session_state.get("logged_in_role", "") or "")

    active_portfolio_name = page if page in PORTFOLIO_NAMES else ""
    assigned_portfolio_name = str(st.session_state.get("logged_in_allowed_portfolio", "") or "")

    if current_role == "Property Manager":
        if assigned_portfolio_name in PORTFOLIO_NAMES:
            active_portfolio_name = assigned_portfolio_name
        st.subheader(active_portfolio_name or "My Repair Requests")
        st.caption("Enter repair requests and review responses/comments for your assigned portfolio only.")
        manager_scope_user_id = current_user_id
        tabs = st.tabs(["Create Request", "My Ongoing Requests"])
    elif current_role in ("Owner", "Renovation Manager", "Other"):
        st.subheader(active_portfolio_name or "Manager Repair Requests")
        st.caption("Review property manager repair requests, respond, change status, manage portfolio properties/addresses, and promote items into the Project Ideas.")
        manager_scope_user_id = None
        tabs = st.tabs(["Create Request", "Review Requests", "Property Info", "Addresses"])
    else:
        st.error("You do not have permission to access this page.")
        st.stop()

    if current_role in ("Owner", "Renovation Manager", "Other"):
        with st.expander("Portfolio Address Setup / Refresh", expanded=False):
            st.caption("Use this if any default portfolio properties or addresses are missing. It adds missing items only, including ReVest Rentals, and will not create duplicates.")
            if st.button(f"Load / Refresh Default Addresses For {active_portfolio_name or 'Portfolios'}", key=f"refresh_default_addresses_{active_portfolio_name or 'all'}"):
                with st.spinner("Checking and loading missing portfolio addresses..."):
                    seed_default_portfolio_properties_and_addresses()
                st.success("Default portfolio properties and addresses refreshed, including ReVest Rentals.")
                st.rerun()

    with tabs[0]:
        st.markdown("### Create Repair Request")
        st.caption("Enter the repair request details. Photos and documents can be uploaded now or added later.")

        last_success_message = st.session_state.pop(f"mgr_req_success_message_{active_portfolio_name or 'portfolio'}", "")
        if last_success_message:
            st.success(last_success_message)
            st.info("The form is ready for the next repair request.")

        if current_role == "Property Manager":
            assigned_portfolio_name = str(st.session_state.get("logged_in_allowed_portfolio", "") or "")
            if assigned_portfolio_name in PORTFOLIO_NAMES:
                active_portfolio_name = assigned_portfolio_name
                st.text_input("Portfolio", value=active_portfolio_name, disabled=True, key=f"mgr_req_portfolio_display_{active_portfolio_name}")
            else:
                st.error("No portfolio has been assigned to this Property Manager login. Ask the Owner to assign a portfolio in Admin.")
                st.stop()
        elif not active_portfolio_name:
            active_portfolio_name = st.selectbox("Portfolio", PORTFOLIO_NAMES, key="mgr_req_create_portfolio_select")

        property_labels = portfolio_property_labels(active_portfolio_name)
        if not property_labels:
            st.warning("No properties are set up for this portfolio. Add properties on the Property Info page first.")
        else:
            with st.form(key=f"mgr_req_create_form_{active_portfolio_name}", clear_on_submit=True):
                r1, r2 = st.columns(2)
                date_requested = r1.date_input("Date Requested", value=datetime.now().date())
                selected_property_label = r2.selectbox("Property", property_labels)

                selected_property_token = selected_property_label.split(" | ", 1)[0]
                selected_property_id = int(selected_property_token) if selected_property_token.isdigit() else None
                selected_property_name = selected_property_label.split(" | ", 1)[1] if " | " in selected_property_label else selected_property_label
                property_name = selected_property_name

                address_labels = portfolio_address_options_for_property(active_portfolio_name, selected_property_label) if selected_property_label else []
                if address_labels:
                    selected_address_label = st.selectbox("Address", address_labels)
                    selected_address_row = portfolio_address_row_from_label(selected_address_label)
                else:
                    selected_address_label = ""
                    selected_address_row = None
                    st.caption("No addresses are set up for the selected property yet.")

                if selected_address_row is not None:
                    request_address = str(selected_address_row.get("address") or "")
                    default_unit_number = str(selected_address_row.get("unit_number") or "")
                    r3, r4 = st.columns(2)
                    r3.text_input("Selected Address", value=request_address, disabled=True)
                    unit_number = r4.text_input("Unit Number (optional)", value=default_unit_number)
                else:
                    request_address = ""
                    unit_number = st.text_input("Unit Number (optional)")

                priority = st.selectbox("Priority", MANAGER_REPAIR_PRIORITY_OPTIONS)
                repair_description = st.text_area("Repair Request Description", height=150)
                initial_comment = st.text_area("Manager Notes / Comments", height=100)
                uploaded_request_files = st.file_uploader(
                    "Upload Photos or Documents",
                    type=["png", "jpg", "jpeg", "webp", "pdf", "xlsx", "xls", "csv", "docx", "txt"],
                    accept_multiple_files=True,
                )

                submitted_request = st.form_submit_button("Submit Repair Request", type="primary")

            if submitted_request:
                if not str(property_name).strip():
                    st.error("Choose a property.")
                elif not str(repair_description).strip():
                    st.error("Enter a repair request description.")
                else:
                    new_request_id = execute_returning_id(
                        """
                        INSERT INTO manager_repair_requests (
                            manager_user_id, manager_username, portfolio_name, date_requested, property_name, address,
                            unit_number, repair_description, priority, status, owner_response,
                            archived, deleted, created_at, modified_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'New Request', '', FALSE, FALSE, NOW(), NOW())
                        """,
                        (
                            current_user_id,
                            current_username,
                            active_portfolio_name,
                            date_requested,
                            str(property_name).strip(),
                            str(request_address).strip(),
                            str(unit_number).strip(),
                            str(repair_description).strip(),
                            str(priority).strip(),
                        ),
                    )
                    if new_request_id:
                        save_manager_repair_request_files(
                            int(new_request_id),
                            uploaded_request_files,
                            uploaded_by=current_username,
                        )
                        if str(initial_comment).strip():
                            execute(
                                """
                                INSERT INTO manager_repair_request_comments (
                                    request_id, user_id, username, role, comment_text, created_at
                                ) VALUES (?, ?, ?, ?, ?, NOW())
                                """,
                                (
                                    int(new_request_id),
                                    current_user_id,
                                    current_username,
                                    current_role,
                                    str(initial_comment).strip(),
                                ),
                            )
                        short_description = str(repair_description).strip()
                        if len(short_description) > 90:
                            short_description = short_description[:90] + "..."
                        st.session_state[f"mgr_req_success_message_{active_portfolio_name or 'portfolio'}"] = (
                            f"Repair Request #{int(new_request_id)} saved successfully for "
                            f"{str(property_name).strip()}"
                            f"{(' - ' + str(request_address).strip()) if str(request_address).strip() else ''}. "
                            f"Priority: {str(priority).strip()}. "
                            f"Request: {short_description}"
                        )
                        st.success(st.session_state[f"mgr_req_success_message_{active_portfolio_name or 'portfolio'}"])
                        st.info("The repair request has been saved. You may enter another request now.")
                    else:
                        st.error("The repair request could not be saved.")

    with tabs[1]:
        st.markdown("### Repair Request List")
        requests_df = manager_repair_requests_df(
            manager_user_id=manager_scope_user_id,
            include_archived=True,
            include_deleted=False,
            portfolio_name=active_portfolio_name if active_portfolio_name else None,
        )

        if requests_df.empty:
            st.info("No repair requests found.")
        else:
            f1, f2, f3 = st.columns(3)
            search_text = f1.text_input("Search Requests", key=f"mgr_req_search_{current_role}")
            status_filter = f2.selectbox("Status Filter", ["All"] + MANAGER_REPAIR_STATUS_OPTIONS, key=f"mgr_req_status_filter_{current_role}")
            priority_filter = f3.selectbox("Priority Filter", ["All"] + MANAGER_REPAIR_PRIORITY_OPTIONS, key=f"mgr_req_priority_filter_{current_role}")

            filtered_df = requests_df.copy()
            if status_filter != "All":
                filtered_df = filtered_df[filtered_df["status"].astype(str) == status_filter].copy()
            if priority_filter != "All":
                filtered_df = filtered_df[filtered_df["priority"].astype(str) == priority_filter].copy()
            if search_text.strip():
                search_lower = search_text.strip().lower()
                search_cols = ["manager_username", "property_name", "address", "unit_number", "repair_description", "priority", "status", "owner_response"]
                mask = False
                for col in search_cols:
                    mask = mask | filtered_df[col].astype(str).str.lower().str.contains(search_lower, na=False)
                filtered_df = filtered_df[mask].copy()

            display_cols = ["id", "date_requested", "portfolio_name", "manager_username", "property_name", "address", "unit_number", "priority", "status", "repair_description", "modified_at"]
            if current_role == "Property Manager":
                display_cols = ["id", "date_requested", "property_name", "address", "unit_number", "priority", "status", "repair_description", "modified_at"]

            st.dataframe(
                filtered_df[display_cols].rename(columns={
                    "id": "Request ID",
                    "date_requested": "Date Requested",
                    "manager_username": "Manager",
                    "portfolio_name": "Portfolio",
                    "property_name": "Property",
                    "address": "Address",
                    "unit_number": "Unit",
                    "priority": "Priority",
                    "status": "Status",
                    "repair_description": "Repair Request",
                    "modified_at": "Last Updated",
                }),
                use_container_width=True,
                hide_index=True,
            )

            if filtered_df.empty:
                st.info("No requests match the current filters.")
            else:
                request_labels = [
                    f"{int(row.id)} | {row.property_name or '(no property)'} | Unit {row.unit_number or '-'} | {str(row.repair_description or '')[:60]}"
                    for row in filtered_df.itertuples()
                ]
                selected_request_label = st.selectbox("Choose Request To Review", request_labels, key=f"mgr_req_select_{current_role}")
                selected_request_id = int(selected_request_label.split(" | ", 1)[0])
                selected_request = filtered_df[filtered_df["id"] == selected_request_id].iloc[0]

                st.markdown("### Request Detail")
                d1, d2 = st.columns(2)
                edit_date_requested = d1.date_input(
                    "Date Requested",
                    value=pd.to_datetime(selected_request.get("date_requested"), errors="coerce").date() if pd.notna(pd.to_datetime(selected_request.get("date_requested"), errors="coerce")) else datetime.now().date(),
                    key=f"mgr_req_edit_date_{selected_request_id}",
                )
                edit_priority = d2.selectbox(
                    "Priority",
                    MANAGER_REPAIR_PRIORITY_OPTIONS,
                    index=MANAGER_REPAIR_PRIORITY_OPTIONS.index(str(selected_request.get("priority") or MANAGER_REPAIR_PRIORITY_OPTIONS[1])) if str(selected_request.get("priority") or "") in MANAGER_REPAIR_PRIORITY_OPTIONS else 1,
                    key=f"mgr_req_edit_priority_{selected_request_id}",
                )

                d3, d4 = st.columns(2)
                edit_property_name = d3.text_input("Property Name", value=str(selected_request.get("property_name") or ""), key=f"mgr_req_edit_property_{selected_request_id}")
                edit_address = d4.text_input("Address", value=str(selected_request.get("address") or ""), key=f"mgr_req_edit_address_{selected_request_id}")
                edit_unit_number = st.text_input("Unit Number", value=str(selected_request.get("unit_number") or ""), key=f"mgr_req_edit_unit_{selected_request_id}")

                edit_description = st.text_area(
                    "Repair Request Description",
                    value=str(selected_request.get("repair_description") or ""),
                    height=150,
                    key=f"mgr_req_edit_description_{selected_request_id}",
                )

                if current_role in ("Owner", "Renovation Manager", "Other"):
                    s1, s2 = st.columns(2)
                    edit_status = s1.selectbox(
                        "Status",
                        MANAGER_REPAIR_STATUS_OPTIONS,
                        index=MANAGER_REPAIR_STATUS_OPTIONS.index(str(selected_request.get("status") or "New Request")) if str(selected_request.get("status") or "") in MANAGER_REPAIR_STATUS_OPTIONS else 0,
                        key=f"mgr_req_edit_status_{selected_request_id}",
                    )
                    edit_owner_response = s2.text_area(
                        "Owner Response / Comments",
                        value=str(selected_request.get("owner_response") or ""),
                        height=100,
                        key=f"mgr_req_owner_response_{selected_request_id}",
                    )
                else:
                    edit_status = str(selected_request.get("status") or "New Request")
                    edit_owner_response = str(selected_request.get("owner_response") or "")
                    st.text_input("Status", value=edit_status, disabled=True)
                    st.text_area("Owner Response / Comments", value=edit_owner_response, height=100, disabled=True)

                save_allowed = current_role in ("Owner", "Renovation Manager", "Other") or int(selected_request.get("manager_user_id") or 0) == current_user_id
                if save_allowed and st.button("Save Request Changes", type="primary", key=f"mgr_req_save_{selected_request_id}"):
                    execute(
                        """
                        UPDATE manager_repair_requests
                        SET date_requested = ?, property_name = ?, address = ?, unit_number = ?,
                            repair_description = ?, priority = ?, status = ?, owner_response = ?, modified_at = NOW()
                        WHERE id = ?
                        """,
                        (
                            edit_date_requested,
                            str(edit_property_name).strip(),
                            str(edit_address).strip(),
                            str(edit_unit_number).strip(),
                            str(edit_description).strip(),
                            str(edit_priority).strip(),
                            str(edit_status).strip(),
                            str(edit_owner_response).strip(),
                            selected_request_id,
                        ),
                    )
                    st.success("Repair request updated.")
                    st.rerun()

                st.markdown("### Photos & Documents")
                render_manager_request_files(
                    selected_request_id,
                    section_key=f"{current_role}_{selected_request_id}",
                    allow_delete=save_allowed,
                )

                new_request_files = st.file_uploader(
                    "Add More Photos or Documents",
                    type=["png", "jpg", "jpeg", "webp", "pdf", "xlsx", "xls", "csv", "docx", "txt"],
                    accept_multiple_files=True,
                    key=f"mgr_req_add_files_{selected_request_id}",
                )
                if st.button("Save Added Files", key=f"mgr_req_save_files_{selected_request_id}"):
                    save_manager_repair_request_files(
                        selected_request_id,
                        new_request_files,
                        uploaded_by=current_username,
                    )
                    st.success("Files added.")
                    st.rerun()

                render_manager_request_conversation(selected_request_id)
                new_comment = st.text_area("Add Comment", height=90, key=f"mgr_req_new_comment_{selected_request_id}")
                if st.button("Add Comment To Conversation", key=f"mgr_req_add_comment_{selected_request_id}"):
                    add_manager_repair_request_comment(selected_request_id, new_comment)
                    st.success("Comment added.")
                    st.rerun()

                if current_role == "Property Manager" and save_allowed:
                    st.markdown("### Request Actions")
                    st.caption("Use delete only for mistakes or duplicate repair requests. Deleted requests are hidden from normal lists.")
                    if st.button("Delete This Request", key=f"mgr_req_manager_delete_{selected_request_id}"):
                        st.session_state[f"confirm_manager_delete_mgr_req_{selected_request_id}"] = True
                        st.rerun()

                    if st.session_state.get(f"confirm_manager_delete_mgr_req_{selected_request_id}", False):
                        st.warning("Delete this repair request? It will be hidden from normal lists.")
                        md1, md2 = st.columns(2)
                        if md1.button("Yes, Delete My Request", type="primary", key=f"mgr_req_manager_confirm_delete_yes_{selected_request_id}"):
                            delete_manager_repair_request(selected_request_id)
                            st.session_state[f"confirm_manager_delete_mgr_req_{selected_request_id}"] = False
                            st.success("Request deleted.")
                            st.rerun()
                        if md2.button("Cancel Delete", key=f"mgr_req_manager_confirm_delete_cancel_{selected_request_id}"):
                            st.session_state[f"confirm_manager_delete_mgr_req_{selected_request_id}"] = False
                            st.rerun()

                if current_role in ("Owner", "Renovation Manager", "Other"):
                    st.markdown("### Owner Actions")
                    a1, a2, a3 = st.columns(3)
                    if a1.button("Promote To Project Ideas", key=f"mgr_req_promote_pipeline_{selected_request_id}"):
                        pipeline_id = promote_manager_request_to_pipeline(selected_request_id)
                        if pipeline_id:
                            st.success(f"Request promoted to Project Ideas item {pipeline_id}.")
                            st.rerun()
                        else:
                            st.error("Could not promote this request.")
                    if a2.button("Archive Request", key=f"mgr_req_archive_{selected_request_id}"):
                        execute(
                            "UPDATE manager_repair_requests SET archived = TRUE, status = 'Archived', modified_at = NOW() WHERE id = ?",
                            (selected_request_id,),
                        )
                        st.success("Request archived.")
                        st.rerun()
                    if a3.button("Delete Request", key=f"mgr_req_delete_{selected_request_id}"):
                        st.session_state[f"confirm_delete_mgr_req_{selected_request_id}"] = True
                        st.rerun()

                    if st.session_state.get(f"confirm_delete_mgr_req_{selected_request_id}", False):
                        st.warning("Delete this repair request? It will be hidden from normal lists.")
                        dd1, dd2 = st.columns(2)
                        if dd1.button("Yes, Delete Request", type="primary", key=f"mgr_req_confirm_delete_yes_{selected_request_id}"):
                            delete_manager_repair_request(selected_request_id)
                            st.session_state[f"confirm_delete_mgr_req_{selected_request_id}"] = False
                            st.success("Request deleted.")
                            st.rerun()
                        if dd2.button("Cancel Delete", key=f"mgr_req_confirm_delete_cancel_{selected_request_id}"):
                            st.session_state[f"confirm_delete_mgr_req_{selected_request_id}"] = False
                            st.rerun()



    if len(tabs) > 2:
        with tabs[2]:
            st.markdown("### Property Info")
            st.caption("Create, edit, or delete property names for this portfolio.")
            portfolio_for_properties = active_portfolio_name or st.selectbox("Portfolio", PORTFOLIO_NAMES, key="property_info_portfolio_select")

            with st.form(f"create_portfolio_property_{portfolio_for_properties}"):
                pp1, pp2 = st.columns(2)
                new_property_name = pp1.text_input("New Property Name")
                new_property_active = pp2.selectbox("Property Status", ["Active", "Inactive"], index=0)
                new_property_notes = st.text_area("Property Notes", height=80)
                if st.form_submit_button("Save Property", type="primary"):
                    if not str(new_property_name).strip():
                        st.error("Enter a property name.")
                    else:
                        execute(
                            """
                            INSERT INTO portfolio_properties (
                                portfolio_name, property_name, notes, active, deleted, created_at, modified_at
                            ) VALUES (?, ?, ?, ?, FALSE, NOW(), NOW())
                            """,
                            (
                                portfolio_for_properties,
                                str(new_property_name).strip(),
                                str(new_property_notes).strip(),
                                new_property_active == "Active",
                            ),
                        )
                        st.success("Property saved.")
                        st.rerun()

            properties_df = portfolio_properties_df(portfolio_for_properties, include_inactive=True)
            if properties_df.empty:
                st.info("No properties created yet for this portfolio.")
            else:
                st.dataframe(
                    properties_df[["id", "property_name", "active", "notes"]].rename(columns={
                        "id": "Property ID",
                        "property_name": "Property Name",
                        "active": "Active",
                        "notes": "Notes",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
                property_edit_labels = [f"{int(row.id)} | {row.property_name}" for row in properties_df.itertuples()]
                selected_property_edit = st.selectbox("Choose Property To Edit", property_edit_labels, key=f"edit_property_select_{portfolio_for_properties}")
                selected_property_id = int(selected_property_edit.split(" | ", 1)[0])
                selected_property_row = properties_df[properties_df["id"] == selected_property_id].iloc[0]

                ep1, ep2 = st.columns(2)
                edit_property_name = ep1.text_input("Edit Property Name", value=str(selected_property_row.get("property_name") or ""), key=f"edit_property_name_{selected_property_id}")
                edit_property_active = ep2.selectbox("Edit Property Status", ["Active", "Inactive"], index=0 if bool(selected_property_row.get("active")) else 1, key=f"edit_property_active_{selected_property_id}")
                edit_property_notes = st.text_area("Edit Property Notes", value=str(selected_property_row.get("notes") or ""), height=80, key=f"edit_property_notes_{selected_property_id}")

                pbtn1, pbtn2 = st.columns(2)
                if pbtn1.button("Save Property Changes", type="primary", key=f"save_property_changes_{selected_property_id}"):
                    execute(
                        """
                        UPDATE portfolio_properties
                        SET property_name = ?, notes = ?, active = ?, modified_at = NOW()
                        WHERE id = ?
                        """,
                        (str(edit_property_name).strip(), str(edit_property_notes).strip(), edit_property_active == "Active", selected_property_id),
                    )
                    execute(
                        """
                        UPDATE portfolio_addresses
                        SET property_name = ?, active = ?, modified_at = NOW()
                        WHERE portfolio_property_id = ?
                        """,
                        (str(edit_property_name).strip(), edit_property_active == "Active", selected_property_id),
                    )
                    st.success("Property updated.")
                    st.rerun()
                if pbtn2.button("Delete Property", key=f"delete_property_{selected_property_id}"):
                    execute("UPDATE portfolio_properties SET deleted = TRUE, active = FALSE, modified_at = NOW() WHERE id = ?", (selected_property_id,))
                    execute("UPDATE portfolio_addresses SET deleted = TRUE, active = FALSE, modified_at = NOW() WHERE portfolio_property_id = ?", (selected_property_id,))
                    st.success("Property deleted.")
                    st.rerun()

        with tabs[3]:
            st.markdown("### Addresses")
            st.caption("Create, edit, or delete addresses and optional unit numbers for this portfolio.")
            portfolio_for_addresses = active_portfolio_name or st.selectbox("Portfolio", PORTFOLIO_NAMES, key="address_info_portfolio_select")
            property_options = portfolio_property_labels(portfolio_for_addresses)
            if not property_options:
                st.info("Create a property first on the Property Info tab.")
            else:
                st.markdown("#### Existing Addresses For Selected Property")
                selected_property_for_address_lookup = st.selectbox(
                    "Property",
                    property_options,
                    key=f"address_existing_property_lookup_{portfolio_for_addresses}",
                )
                address_property_lookup_id = int(selected_property_for_address_lookup.split(" | ", 1)[0])
                address_property_lookup_name = selected_property_for_address_lookup.split(" | ", 1)[1]
                existing_address_labels_for_property = portfolio_address_labels_for_property_id(address_property_lookup_id)
                if existing_address_labels_for_property:
                    selected_existing_address_label = st.selectbox(
                        "Existing Address Choice",
                        existing_address_labels_for_property,
                        key=f"existing_address_choice_{portfolio_for_addresses}_{address_property_lookup_id}",
                    )
                    selected_existing_address_row = portfolio_address_row_from_label(selected_existing_address_label)
                    if selected_existing_address_row is not None:
                        ea1, ea2 = st.columns(2)
                        ea1.text_input(
                            "Selected Address",
                            value=str(selected_existing_address_row.get("address") or ""),
                            disabled=True,
                            key=f"selected_existing_address_display_{portfolio_for_addresses}_{address_property_lookup_id}",
                        )
                        ea2.text_input(
                            "Selected Unit",
                            value=str(selected_existing_address_row.get("unit_number") or ""),
                            disabled=True,
                            key=f"selected_existing_unit_display_{portfolio_for_addresses}_{address_property_lookup_id}",
                        )
                else:
                    st.info("No addresses found for this selected property yet.")

                st.markdown("#### Add New Address")
                with st.form(f"create_portfolio_address_{portfolio_for_addresses}"):
                    ap1, ap2 = st.columns(2)
                    selected_property_for_address = ap1.selectbox("Property For New Address", property_options)
                    address_property_id = int(selected_property_for_address.split(" | ", 1)[0])
                    address_property_name = selected_property_for_address.split(" | ", 1)[1]
                    new_address = ap2.text_input("New Address")
                    ap3, ap4 = st.columns(2)
                    new_unit_number = ap3.text_input("New Unit Number")
                    new_address_active = ap4.selectbox("Address Status", ["Active", "Inactive"], index=0)
                    new_address_notes = st.text_area("Address Notes", height=80)
                    if st.form_submit_button("Save Address", type="primary"):
                        if not str(new_address).strip():
                            st.error("Enter an address.")
                        else:
                            execute(
                                """
                                INSERT INTO portfolio_addresses (
                                    portfolio_property_id, portfolio_name, property_name, address, unit_number,
                                    notes, active, deleted, created_at, modified_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, NOW(), NOW())
                                """,
                                (
                                    address_property_id,
                                    portfolio_for_addresses,
                                    address_property_name,
                                    str(new_address).strip(),
                                    str(new_unit_number).strip(),
                                    str(new_address_notes).strip(),
                                    new_address_active == "Active",
                                ),
                            )
                            st.success("Address saved.")
                            st.rerun()

                addresses_df = portfolio_addresses_df(portfolio_for_addresses, include_inactive=True)
                if addresses_df.empty:
                    st.info("No addresses created yet for this portfolio.")
                else:
                    st.dataframe(
                        addresses_df[["id", "property_name", "address", "unit_number", "active", "notes"]].rename(columns={
                            "id": "Address ID",
                            "property_name": "Property",
                            "address": "Address",
                            "unit_number": "Unit",
                            "active": "Active",
                            "notes": "Notes",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )
                    address_edit_labels = [
                        f"{int(row.id)} | {row.property_name} | {row.address} | Unit {row.unit_number or '-'}"
                        for row in addresses_df.itertuples()
                    ]
                    selected_address_edit = st.selectbox("Choose Address To Edit", address_edit_labels, key=f"edit_address_select_{portfolio_for_addresses}")
                    selected_address_id = int(selected_address_edit.split(" | ", 1)[0])
                    selected_address_row = addresses_df[addresses_df["id"] == selected_address_id].iloc[0]

                    ea1, ea2 = st.columns(2)
                    edit_address_value = ea1.text_input("Edit Address", value=str(selected_address_row.get("address") or ""), key=f"edit_address_value_{selected_address_id}")
                    edit_unit_value = ea2.text_input("Edit Unit Number", value=str(selected_address_row.get("unit_number") or ""), key=f"edit_unit_value_{selected_address_id}")
                    ea3, ea4 = st.columns(2)
                    edit_address_active = ea3.selectbox("Edit Address Status", ["Active", "Inactive"], index=0 if bool(selected_address_row.get("active")) else 1, key=f"edit_address_active_{selected_address_id}")
                    edit_address_notes = st.text_area("Edit Address Notes", value=str(selected_address_row.get("notes") or ""), height=80, key=f"edit_address_notes_{selected_address_id}")

                    abtn1, abtn2 = st.columns(2)
                    if abtn1.button("Save Address Changes", type="primary", key=f"save_address_changes_{selected_address_id}"):
                        execute(
                            """
                            UPDATE portfolio_addresses
                            SET address = ?, unit_number = ?, notes = ?, active = ?, modified_at = NOW()
                            WHERE id = ?
                            """,
                            (
                                str(edit_address_value).strip(),
                                str(edit_unit_value).strip(),
                                str(edit_address_notes).strip(),
                                edit_address_active == "Active",
                                selected_address_id,
                            ),
                        )
                        st.success("Address updated.")
                        st.rerun()
                    if abtn2.button("Delete Address", key=f"delete_address_{selected_address_id}"):
                        execute("UPDATE portfolio_addresses SET deleted = TRUE, active = FALSE, modified_at = NOW() WHERE id = ?", (selected_address_id,))
                        st.success("Address deleted.")
                        st.rerun()

# -----------------------------
# Project Ideas
# -----------------------------



elif page in ["Quality Control", "My Quality Control"]:
    ensure_quality_control_schema()
    current_qc_role = str(st.session_state.get("logged_in_role", "") or "")
    contractor_only = current_qc_role == "Contractor"
    assigned_user_only = page == "My Quality Control" or current_qc_role in ("Contractor", "Maintenance", "Lawn & Landscape")
    st.header("My Quality Control" if assigned_user_only else "Quality Control")
    st.caption("Capture property issues, assign them, track responses, due dates, photos, and completion.")

    contractor_id_filter = int(st.session_state.get("logged_in_contractor_id") or 0) if contractor_only else None
    assigned_user_id_filter = int(st.session_state.get("logged_in_user_id") or 0) if assigned_user_only and not contractor_only else None
    if contractor_only and not contractor_id_filter:
        st.info("No contractor is linked to this login.")
    else:
        qc_section_options = ["QC Entry", "QC Review / Update", "QC Report"] if not assigned_user_only else ["My Assigned QC", "QC Report"]
        qc_section = st.radio("Quality Control Section", qc_section_options, horizontal=True, key="quality_control_section_selector")


        if (not assigned_user_only) and qc_section == "QC Entry":
            st.subheader("QC Entry")
            st.caption("Use this for inspections, trash, landscape/mowing issues, cleanup items, small repairs, or corrections that do not need a quote.")
            props = portfolio_properties_df(include_inactive=False, include_deleted=False)
            prop_names = sorted(props["property_name"].dropna().astype(str).unique().tolist()) if not props.empty else []
            work_items = fetch_df("""
                SELECT t.name, COALESCE(tr.name, '') AS trade
                FROM tasks t
                LEFT JOIN trades tr ON tr.id = t.trade_id
                WHERE COALESCE(t.active, TRUE) = TRUE
                ORDER BY LOWER(t.name)
            """)
            work_item_names = work_items["name"].dropna().astype(str).tolist() if not work_items.empty else []
            contractors = fetch_df("SELECT id, name FROM contractors ORDER BY LOWER(name)")
            users = get_user_accounts_df(active_only=True)

            with st.form("quality_control_entry_form"):
                c1, c2 = st.columns(2)
                entry_date = c1.date_input("Date Entered", value=pd.Timestamp.today().date())
                property_name = c2.selectbox("Property", [""] + prop_names)
                address_options = [""]
                if property_name:
                    addr_df = portfolio_addresses_df(property_name=property_name, include_inactive=False, include_deleted=False)
                    if not addr_df.empty:
                        address_options += sorted(addr_df["address"].dropna().astype(str).unique().tolist())
                a1, a2 = st.columns(2)
                address = a1.selectbox("Address", address_options)
                unit_number = a2.text_input("Unit / Area", value="")
                location_identifier = st.text_input("Specific Location", placeholder="Example: north parking lot, Building 4 breezeway, dumpster area")
                wi1, wi2 = st.columns(2)
                work_item_name = wi1.selectbox("Work Item", [""] + work_item_names)
                category_name = ""
                if work_item_name and not work_items.empty:
                    match = work_items[work_items["name"].astype(str) == str(work_item_name)]
                    if not match.empty:
                        category_name = str(match.iloc[0].get("trade") or "")
                category_name = wi2.text_input("Category of Labor", value=category_name)
                issue_description = st.text_area("Issue / Request Description", height=100, placeholder="Describe what needs fixed, cleaned up, corrected, or verified.")
                notes = st.text_area("Owner / Manager Notes", height=80)
                p1, p2, p3 = st.columns(3)
                priority = p1.selectbox("Priority", QC_PRIORITY_OPTIONS, index=1)
                status = p2.selectbox("Status", QC_STATUS_OPTIONS, index=0)
                due_date = p3.date_input("Due Date", value=pd.Timestamp.today().date())
                f1, f2 = st.columns(2)
                use_followup = f1.checkbox("Add Follow-Up / Reminder Date", value=False)
                follow_up_date = f2.date_input("Follow-Up Date", value=pd.Timestamp.today().date(), disabled=not use_followup)
                st.markdown("#### Assign To")
                at1, at2 = st.columns(2)
                assignee_type = at1.selectbox("Assign To Type", QC_USER_ASSIGNMENT_TYPES)
                assignee_name = ""
                contractor_id = None
                assigned_user_id = None
                if assignee_type == "Contractor":
                    contractor_labels = [""] + [f"{int(row.id)} | {row.name}" for row in contractors.itertuples()] if not contractors.empty else [""]
                    selected_contractor = at2.selectbox("Contractor", contractor_labels)
                    if selected_contractor:
                        contractor_id = int(selected_contractor.split(" | ", 1)[0])
                        assignee_name = selected_contractor.split(" | ", 1)[1]
                elif assignee_type == "Unassigned":
                    at2.info("Leave unassigned for now.")
                else:
                    user_labels = qc_user_label_options(users, assignee_type)
                    selected_user = at2.selectbox("Assigned Person", user_labels, help=f"Only active users with role {assignee_type} are shown.")
                    assigned_user_id, assignee_name = qc_parse_user_label(selected_user)
                uploaded_files = st.file_uploader("Photos / Files", accept_multiple_files=True, key="quality_control_entry_files")
                submitted = st.form_submit_button("Save Quality Control Item", type="primary")
                if submitted:
                    missing = []
                    if not property_name:
                        missing.append("Property")
                    if not location_identifier and not address:
                        missing.append("Location or Address")
                    if not work_item_name:
                        missing.append("Work Item")
                    if not issue_description:
                        missing.append("Issue Description")
                    if missing:
                        st.error("Missing required fields: " + ", ".join(missing))
                    else:
                        new_id = create_qc_item({
                            "entry_date": entry_date,
                            "property_name": property_name,
                            "address": address,
                            "unit_number": unit_number,
                            "location_identifier": location_identifier,
                            "work_item_name": work_item_name,
                            "category_name": category_name,
                            "issue_description": issue_description,
                            "notes": notes,
                            "priority": priority,
                            "status": status,
                            "due_date": due_date,
                            "follow_up_date": follow_up_date if use_followup else None,
                            "assignee_type": "" if assignee_type == "Unassigned" else assignee_type,
                            "assignee_name": assignee_name,
                            "contractor_id": contractor_id,
                            "assigned_user_id": assigned_user_id,
                            "created_by": str(st.session_state.get("logged_in_user") or ""),
                        })
                        if new_id:
                            save_qc_files(new_id, uploaded_files, uploaded_by=str(st.session_state.get("logged_in_user") or ""))
                            st.success(f"Quality Control item QC-{int(new_id):06d} saved.")
                            st.rerun()

        if qc_section in ("QC Review / Update", "My Assigned QC"):
            st.subheader("QC Review / Update" if not contractor_only else "My Assigned Quality Control")
            df = qc_items_df(include_deleted=False, contractor_id=contractor_id_filter, assigned_user_id=assigned_user_id_filter)
            if df.empty:
                st.info("No Quality Control items found.")
            else:
                search = st.text_input("Search QC items", key="qc_review_search")
                if search:
                    mask = pd.Series(False, index=df.index)
                    for col in ["qc_code", "property_name", "address", "location_identifier", "work_item_name", "issue_description", "assignee_name", "status"]:
                        mask = mask | df[col].astype(str).str.contains(search, case=False, na=False)
                    df = df[mask].copy()
                labels = [f"{int(row.id)} | {row.qc_code} | {row.property_name} | {row.work_item_name} | {row.status}" for row in df.itertuples()]
                selected = st.selectbox("Choose QC Item", labels, key="qc_review_select")
                selected_id = int(selected.split(" | ", 1)[0]) if selected else 0
                row = df[df["id"].astype(int) == selected_id].iloc[0]
                st.markdown(f"### {row['qc_code']} — {row['work_item_name']}")
                st.write(f"**Property:** {row['property_name']}  ")
                st.write(f"**Location:** {row['address']} {row['unit_number']} — {row['location_identifier']}")
                st.write(f"**Issue:** {row['issue_description']}")

                if contractor_only:
                    c1, c2 = st.columns(2)
                    new_status = c1.selectbox("Update Status", QC_STATUS_OPTIONS, index=QC_STATUS_OPTIONS.index(row["status"]) if row["status"] in QC_STATUS_OPTIONS else 0, key=f"qc_contractor_status_{selected_id}")
                    response_note = st.text_area("Response / Completion Notes", key=f"qc_contractor_note_{selected_id}", height=100)
                    response_files = st.file_uploader("Add Response Photos / Files", accept_multiple_files=True, key=f"qc_contractor_files_{selected_id}")
                    if st.button("Save QC Response", type="primary", key=f"save_qc_contractor_response_{selected_id}"):
                        completed_date = pd.Timestamp.today().date() if new_status in ["Completed", "Verified"] else row.get("completed_date")
                        verified_date = pd.Timestamp.today().date() if new_status == "Verified" else row.get("verified_date")
                        execute("UPDATE quality_control_items SET status = ?, completed_date = ?, verified_date = ?, modified_at = NOW() WHERE id = ?", (new_status, completed_date, verified_date, selected_id))
                        add_qc_comment(selected_id, response_note, new_status)
                        save_qc_files(selected_id, response_files, uploaded_by=str(st.session_state.get("logged_in_user") or ""))
                        st.success("Quality Control response saved.")
                        st.rerun()
                else:
                    contractors = fetch_df("SELECT id, name FROM contractors ORDER BY LOWER(name)")
                    users = get_user_accounts_df(active_only=True)
                    with st.form(f"qc_edit_form_{selected_id}"):
                        e1, e2 = st.columns(2)
                        entry_date = e1.date_input("Date Entered", value=pd.to_datetime(row["entry_date"]).date() if pd.notna(row["entry_date"]) else pd.Timestamp.today().date())
                        property_name = e2.text_input("Property", value=str(row["property_name"] or ""))
                        a1, a2 = st.columns(2)
                        address = a1.text_input("Address", value=str(row["address"] or ""))
                        unit_number = a2.text_input("Unit / Area", value=str(row["unit_number"] or ""))
                        location_identifier = st.text_input("Specific Location", value=str(row["location_identifier"] or ""))
                        w1, w2 = st.columns(2)
                        work_item_name = w1.text_input("Work Item", value=str(row["work_item_name"] or ""))
                        category_name = w2.text_input("Category of Labor", value=str(row["category_name"] or ""))
                        issue_description = st.text_area("Issue / Request Description", value=str(row["issue_description"] or ""), height=100)
                        notes = st.text_area("Owner / Manager Notes", value=str(row["notes"] or ""), height=80)
                        p1, p2, p3 = st.columns(3)
                        priority = p1.selectbox("Priority", QC_PRIORITY_OPTIONS, index=QC_PRIORITY_OPTIONS.index(row["priority"]) if row["priority"] in QC_PRIORITY_OPTIONS else 1)
                        status = p2.selectbox("Status", QC_STATUS_OPTIONS, index=QC_STATUS_OPTIONS.index(row["status"]) if row["status"] in QC_STATUS_OPTIONS else 0)
                        due_date_val = pd.to_datetime(row["due_date"]).date() if pd.notna(row["due_date"]) else pd.Timestamp.today().date()
                        due_date = p3.date_input("Due Date", value=due_date_val)
                        f1, f2 = st.columns(2)
                        use_followup = f1.checkbox("Use Follow-Up Date", value=pd.notna(row["follow_up_date"]))
                        follow_up_value = pd.to_datetime(row["follow_up_date"]).date() if pd.notna(row["follow_up_date"]) else pd.Timestamp.today().date()
                        follow_up_date = f2.date_input("Follow-Up Date", value=follow_up_value, disabled=not use_followup)
                        at1, at2 = st.columns(2)
                        assignee_type_options = QC_USER_ASSIGNMENT_TYPES
                        current_type = str(row["assignee_type"] or "Unassigned")
                        if current_type in ("", "User Account", "Staff Member"):
                            # Older builds saved generic staff assignment. Keep the user, but infer the role when possible.
                            current_user_id_tmp = int(row.get("assigned_user_id") or 0)
                            if current_user_id_tmp and not users.empty:
                                user_match = users[users["id"].astype(int) == current_user_id_tmp]
                                current_type = str(user_match.iloc[0].get("role") or "Other") if not user_match.empty else "Other"
                            else:
                                current_type = "Unassigned"
                        if current_type not in assignee_type_options:
                            current_type = "Unassigned"
                        assignee_type = at1.selectbox("Assign To Type", assignee_type_options, index=assignee_type_options.index(current_type))
                        assignee_name = ""
                        contractor_id = None
                        assigned_user_id = None
                        if assignee_type == "Contractor":
                            contractor_labels = [""] + [f"{int(r.id)} | {r.name}" for r in contractors.itertuples()] if not contractors.empty else [""]
                            current_contract = int(row.get("contractor_id") or 0)
                            default_idx = 0
                            for idx, label in enumerate(contractor_labels):
                                if label and int(label.split(" | ", 1)[0]) == current_contract:
                                    default_idx = idx
                            selected_contractor = at2.selectbox("Contractor", contractor_labels, index=default_idx)
                            if selected_contractor:
                                contractor_id = int(selected_contractor.split(" | ", 1)[0])
                                assignee_name = selected_contractor.split(" | ", 1)[1]
                        elif assignee_type == "Unassigned":
                            at2.info("Unassigned")
                        else:
                            user_labels = qc_user_label_options(users, assignee_type)
                            current_user_id = int(row.get("assigned_user_id") or 0)
                            default_idx = 0
                            for idx, label in enumerate(user_labels):
                                if label and int(label.split(" | ", 1)[0]) == current_user_id:
                                    default_idx = idx
                            selected_user = at2.selectbox("Assigned Person", user_labels, index=default_idx, help=f"Only active users with role {assignee_type} are shown.")
                            assigned_user_id, assignee_name = qc_parse_user_label(selected_user)
                        completed_date = pd.to_datetime(row["completed_date"]).date() if pd.notna(row["completed_date"]) else (pd.Timestamp.today().date() if status in ["Completed", "Verified"] else None)
                        verified_date = pd.to_datetime(row["verified_date"]).date() if pd.notna(row["verified_date"]) else (pd.Timestamp.today().date() if status == "Verified" else None)
                        new_comment = st.text_area("Add Update Comment", height=80)
                        new_files = st.file_uploader("Add Photos / Files", accept_multiple_files=True, key=f"qc_owner_files_{selected_id}")
                        save_btn = st.form_submit_button("Save QC Changes", type="primary")
                        if save_btn:
                            update_qc_item(selected_id, {
                                "entry_date": entry_date,
                                "property_name": property_name,
                                "address": address,
                                "unit_number": unit_number,
                                "location_identifier": location_identifier,
                                "work_item_name": work_item_name,
                                "category_name": category_name,
                                "issue_description": issue_description,
                                "notes": notes,
                                "priority": priority,
                                "status": status,
                                "due_date": due_date,
                                "follow_up_date": follow_up_date if use_followup else None,
                                "assignee_type": "" if assignee_type == "Unassigned" else assignee_type,
                                "assignee_name": assignee_name,
                                "contractor_id": contractor_id,
                                "assigned_user_id": assigned_user_id,
                                "completed_date": completed_date,
                                "verified_date": verified_date,
                            })
                            add_qc_comment(selected_id, new_comment, status)
                            save_qc_files(selected_id, new_files, uploaded_by=str(st.session_state.get("logged_in_user") or ""))
                            st.success("Quality Control item updated.")
                            st.rerun()

                st.markdown("### Photos / Files")
                render_qc_files(selected_id, "review")
                st.markdown("### Notes / Responses")
                comments = qc_comments_df(selected_id)
                if comments.empty:
                    st.info("No responses or update notes yet.")
                else:
                    for c in comments.itertuples():
                        created_text = pd.to_datetime(c.created_at).strftime("%m-%d-%Y %I:%M %p") if pd.notna(c.created_at) else ""
                        st.markdown(f"**{created_text} — {c.username or c.role or 'User'}** {('— ' + c.status_update) if c.status_update else ''}")
                        st.write(c.comment_text)
                        st.markdown("---")

        if qc_section == "QC Report":
            st.subheader("QC Report")
            base_df = qc_items_df(include_deleted=False, contractor_id=contractor_id_filter)
            if base_df.empty:
                st.info("No Quality Control records found.")
            else:
                f1, f2, f3, f4 = st.columns(4)
                prop_filter = f1.selectbox("Property", ["All"] + sorted(base_df["property_name"].dropna().astype(str).unique().tolist()))
                assignee_type_filter = f2.selectbox("Assignee Type", ["All"] + [x for x in QC_USER_ASSIGNMENT_TYPES if x != "Unassigned"])
                assignee_filter = f3.selectbox("Assignee", ["All"] + sorted([x for x in base_df["assignee_name"].dropna().astype(str).unique().tolist() if x]))
                priority_filter = f4.selectbox("Priority", ["All"] + QC_PRIORITY_OPTIONS)
                status_filter = st.multiselect("Status", QC_STATUS_OPTIONS, default=["Open", "Assigned", "In Progress"])
                f5, f6, f7 = st.columns(3)
                overdue_only = f5.checkbox("Overdue Only", value=False)
                search_text = f6.text_input("Search", value="")
                include_completed = f7.checkbox("Include Completed / Verified", value=False)
                report_df_source = base_df.copy()
                if prop_filter != "All":
                    report_df_source = report_df_source[report_df_source["property_name"].astype(str) == prop_filter]
                if assignee_type_filter != "All":
                    report_df_source = report_df_source[report_df_source["assignee_type"].astype(str) == assignee_type_filter]
                if assignee_filter != "All":
                    report_df_source = report_df_source[report_df_source["assignee_name"].astype(str) == assignee_filter]
                if priority_filter != "All":
                    report_df_source = report_df_source[report_df_source["priority"].astype(str) == priority_filter]
                if status_filter:
                    report_df_source = report_df_source[report_df_source["status"].isin(status_filter)]
                if not include_completed:
                    report_df_source = report_df_source[~report_df_source["status"].isin(["Completed", "Verified", "Cancelled"])]
                if search_text:
                    mask = pd.Series(False, index=report_df_source.index)
                    for col in ["qc_code", "property_name", "address", "location_identifier", "work_item_name", "issue_description", "assignee_name", "notes"]:
                        mask = mask | report_df_source[col].astype(str).str.contains(search_text, case=False, na=False)
                    report_df_source = report_df_source[mask]
                report_df = qc_report_dataframe(report_df_source)
                if overdue_only and not report_df.empty:
                    report_df = report_df[report_df["Overdue"] == "Yes"].copy()
                c1, c2, c3 = st.columns(3)
                c1.metric("QC Items", len(report_df))
                c2.metric("Overdue", int((report_df.get("Overdue", pd.Series(dtype=str)).astype(str) == "Yes").sum()) if not report_df.empty else 0)
                c3.metric("Open / Active", int((report_df.get("Status", pd.Series(dtype=str)).isin(["Open", "Assigned", "In Progress"])).sum()) if not report_df.empty else 0)
                st.dataframe(report_df, use_container_width=True, hide_index=True)
                filters = {
                    "Property": prop_filter,
                    "Assignee Type": assignee_type_filter,
                    "Assignee": assignee_filter,
                    "Status": ", ".join(status_filter) if status_filter else "All",
                    "Priority": priority_filter,
                    "Overdue Only": "Yes" if overdue_only else "No",
                    "Search": search_text,
                }
                e1, e2 = st.columns(2)
                e1.download_button("Download QC Report PDF", data=build_quality_control_pdf(report_df, filters), file_name="quality_control_report.pdf", mime="application/pdf", use_container_width=True)
                e2.download_button("Download QC Report Excel", data=build_quality_control_excel(report_df), file_name="quality_control_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

elif page == "Master Work List":
    st.header("Master Work List")
    st.caption("Bird's-eye report of individual RMR work items and Work Groups. Use filters to review active, completed, deleted, property, contractor, priority, timeframe, and Work Group views.")

    full_pipeline_df = build_pipeline_overview_df(include_deleted=True)
    if full_pipeline_df.empty:
        st.info("No RMR work items have been entered yet.")
    else:
        f1, f2, f3 = st.columns(3)
        status_view = f1.selectbox(
            "Status View",
            ["Open Work", "Completed / Closed", "Deleted", "All Statuses"],
            key="master_work_list_status_view",
            help="Open Work is the normal view. It excludes completed, closed, paid, deleted, and cancelled items.",
        )
        property_options = ["All Properties"] + sorted([x for x in full_pipeline_df["Property"].dropna().astype(str).unique().tolist() if x], key=lambda x: x.lower())
        property_filter = f2.selectbox("Property", property_options, key="master_work_list_property")
        timeframe_options = ["All Timeframes"] + [x for x in RMR_BUDGET_TIMEFRAME_OPTIONS if x in full_pipeline_df["Budget Timeframe"].astype(str).unique().tolist()]
        # Keep older/custom saved timeframes visible too.
        for value in sorted([x for x in full_pipeline_df["Budget Timeframe"].dropna().astype(str).unique().tolist() if x and x not in timeframe_options], key=lambda x: x.lower()):
            timeframe_options.append(value)
        timeframe_filter = f3.selectbox("Budget Timeframe", timeframe_options, key="master_work_list_timeframe")

        f4, f5, f6 = st.columns(3)
        wg_options = ["All Work Groups", "No Work Group Assigned"] + sorted([x for x in full_pipeline_df["Work Group"].dropna().astype(str).unique().tolist() if x and x != "No Work Group Assigned"], key=lambda x: x.lower())
        work_group_filter = f4.selectbox("Work Group", wg_options, key="master_work_list_work_group")
        contractor_values = sorted([x for x in full_pipeline_df["Contractor"].dropna().astype(str).unique().tolist() if x], key=lambda x: x.lower())
        contractor_filter = f5.selectbox("Contractor", ["All Contractors", "Unassigned"] + contractor_values, key="master_work_list_contractor")
        priority_values = [p for p in CONTRACTOR_PRIORITY_OPTIONS if p in full_pipeline_df["Priority"].astype(str).unique().tolist()]
        priority_filter = f6.selectbox("Priority", ["All Priorities"] + priority_values, key="master_work_list_priority")

        search_text = st.text_input("Search work list", key="master_work_list_search", placeholder="Search RMR #, address, work item, group, contractor, notes keywords...")

        pipeline_df = filter_pipeline_overview_df(
            full_pipeline_df,
            status_view=status_view,
            property_filter=property_filter,
            work_group_filter=work_group_filter,
            timeframe_filter=timeframe_filter,
            contractor_filter=contractor_filter,
            priority_filter=priority_filter,
            search_text=search_text,
        )

        total_labor = pd.to_numeric(pipeline_df.get("Labor Budget", 0), errors="coerce").fillna(0).sum() if not pipeline_df.empty else 0
        total_materials = pd.to_numeric(pipeline_df.get("Materials Budget", 0), errors="coerce").fillna(0).sum() if not pipeline_df.empty else 0
        grouped_count = int((pipeline_df.get("Work Group ID", pd.Series(dtype=int)).fillna(0).astype(int) > 0).sum()) if not pipeline_df.empty else 0
        ungrouped_count = int((pipeline_df.get("Work Group ID", pd.Series(dtype=int)).fillna(0).astype(int) == 0).sum()) if not pipeline_df.empty else 0
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Rows", f"{len(pipeline_df):,}")
        c2.metric("Grouped Rows", f"{grouped_count:,}")
        c3.metric("Ungrouped Rows", f"{ungrouped_count:,}")
        c4.metric("Labor", f"${total_labor:,.0f}")
        c5.metric("Materials", f"${total_materials:,.0f}")
        st.metric("Total Budget", f"${(total_labor + total_materials):,.0f}")

        tab_a, tab_b, tab_c, tab_d = st.tabs(["Standard Report", "Grouped By Work Group", "Summary", "Delete / Restore"])

        with tab_a:
            st.markdown("### Standard Master Work List Report")
            display_cols = [
                "RMR ID", "Property", "Address", "Location", "Work Item", "Work Group", "Contractor",
                "Priority", "Owner Intent", "Budget Timeframe", "Budget Status", "RMR Status", "Work Group Status",
                "Labor Budget", "Materials Budget", "Total Budget", "Cash Flow Export", "Photos", "Modified"
            ]
            report_df = pipeline_df[[c for c in display_cols if c in pipeline_df.columns]].copy()
            st.dataframe(report_df, use_container_width=True, hide_index=True)
            csv_bytes = report_df.to_csv(index=False).encode("utf-8")
            e1, e2, e3 = st.columns(3)
            e1.download_button("Download Standard Report CSV", data=csv_bytes, file_name="master_work_list_standard_report.csv", mime="text/csv", use_container_width=True)
            e2.download_button(
                "Download Standard Report Excel",
                data=master_work_list_excel_bytes(report_df),
                file_name="master_work_list_standard_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
            current_filters = {
                "Status View": status_view,
                "Property": property_filter,
                "Work Group": work_group_filter,
                "Timeframe": timeframe_filter,
                "Contractor": contractor_filter,
                "Priority": priority_filter,
                "Search": search_text,
            }
            e3.download_button(
                "Download Standard Report PDF",
                data=build_master_work_list_pdf(report_df, current_filters, grouped=False),
                file_name="master_work_list_standard_report.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

        with tab_b:
            st.markdown("### Work Group View")
            st.caption("Grouped RMR work items are shown under the Work Group name. Ungrouped items are shown together under 'No Work Group Assigned'.")
            if pipeline_df.empty:
                st.info("No rows match the selected filters.")
            else:
                ordered_groups = []
                for group_name, group_df in pipeline_df.groupby("Work Group", dropna=False, sort=False):
                    group_name = str(group_name or "No Work Group Assigned")
                    group_total_labor = pd.to_numeric(group_df["Labor Budget"], errors="coerce").fillna(0).sum()
                    group_total_materials = pd.to_numeric(group_df["Materials Budget"], errors="coerce").fillna(0).sum()
                    first_row = group_df.iloc[0]
                    sort_key = (contractor_priority_sort_value(first_row.get("Priority", "3 - Quote Only")), group_name.lower())
                    ordered_groups.append((sort_key, group_name, group_df, group_total_labor, group_total_materials, first_row))
                for _, group_name, group_df, group_total_labor, group_total_materials, first_row in sorted(ordered_groups, key=lambda x: x[0]):
                    header = f"{group_name} — {len(group_df)} item(s) — ${group_total_labor + group_total_materials:,.0f}"
                    with st.expander(header, expanded=(group_name == "No Work Group Assigned" or work_group_filter not in ["All Work Groups", ""])):
                        st.write(f"**Contractor:** {first_row.get('Contractor', '') or 'Unassigned'}")
                        st.write(f"**Priority:** {first_row.get('Priority', '')} | **Owner Intent:** {first_row.get('Owner Intent', '')}")
                        st.write(f"**Labor:** ${group_total_labor:,.0f} | **Materials:** ${group_total_materials:,.0f} | **Total:** ${group_total_labor + group_total_materials:,.0f}")
                        detail_cols = ["RMR ID", "Property", "Address", "Location", "Work Item", "Budget Timeframe", "RMR Status", "Labor Budget", "Materials Budget", "Total Budget", "Cash Flow Export"]
                        st.dataframe(group_df[[c for c in detail_cols if c in group_df.columns]], use_container_width=True, hide_index=True)
                grouped_filters = {
                    "Status View": status_view,
                    "Property": property_filter,
                    "Work Group": work_group_filter,
                    "Timeframe": timeframe_filter,
                    "Contractor": contractor_filter,
                    "Priority": priority_filter,
                    "Search": search_text,
                }
                st.download_button(
                    "Download Grouped Work Group PDF",
                    data=build_master_work_list_pdf(pipeline_df, grouped_filters, grouped=True),
                    file_name="master_work_list_grouped_report.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )

        with tab_c:
            st.markdown("### Summary")
            if pipeline_df.empty:
                st.info("No rows match the selected filters.")
            else:
                s1, s2 = st.columns(2)
                prop_summary = pipeline_df.groupby("Property", dropna=False).agg(
                    Items=("RMR ID", "count"),
                    Labor=("Labor Budget", "sum"),
                    Materials=("Materials Budget", "sum"),
                    Total=("Total Budget", "sum"),
                ).reset_index().sort_values("Total", ascending=False)
                timeframe_summary = pipeline_df.groupby("Budget Timeframe", dropna=False).agg(
                    Items=("RMR ID", "count"),
                    Labor=("Labor Budget", "sum"),
                    Materials=("Materials Budget", "sum"),
                    Total=("Total Budget", "sum"),
                ).reset_index().sort_values("Total", ascending=False)
                s1.markdown("#### By Property")
                s1.dataframe(prop_summary, use_container_width=True, hide_index=True)
                s2.markdown("#### By Timeframe")
                s2.dataframe(timeframe_summary, use_container_width=True, hide_index=True)

                s3, s4 = st.columns(2)
                contractor_summary = pipeline_df.copy()
                contractor_summary["Contractor"] = contractor_summary["Contractor"].replace("", "Unassigned")
                contractor_summary = contractor_summary.groupby("Contractor", dropna=False).agg(
                    Items=("RMR ID", "count"),
                    Labor=("Labor Budget", "sum"),
                    Materials=("Materials Budget", "sum"),
                    Total=("Total Budget", "sum"),
                ).reset_index().sort_values("Total", ascending=False)
                priority_summary = pipeline_df.groupby("Priority", dropna=False).agg(
                    Items=("RMR ID", "count"),
                    Labor=("Labor Budget", "sum"),
                    Materials=("Materials Budget", "sum"),
                    Total=("Total Budget", "sum"),
                ).reset_index()
                priority_summary["Priority Sort"] = priority_summary["Priority"].apply(contractor_priority_sort_value)
                priority_summary = priority_summary.sort_values("Priority Sort").drop(columns=["Priority Sort"])
                s3.markdown("#### By Contractor")
                s3.dataframe(contractor_summary, use_container_width=True, hide_index=True)
                s4.markdown("#### By Priority")
                s4.dataframe(priority_summary, use_container_width=True, hide_index=True)

        with tab_d:
            st.markdown("### Delete / Restore RMR Work Item")
            st.caption("Delete is a soft delete. The RMR is removed from normal queues but can be restored here by changing Status View to Deleted.")
            if pipeline_df.empty:
                st.info("No rows match the selected filters.")
            else:
                labels = [
                    f"{int(row['RMR DB ID'])} | {row['RMR ID']} | {row['Property']} | {row['Work Item']} | {row['Location']}"
                    for _, row in pipeline_df.iterrows()
                ]
                selected_label = st.selectbox("Choose RMR / Work Item", labels, key="master_work_list_delete_select")
                selected_rmr_id = int(selected_label.split(" | ", 1)[0]) if selected_label else 0
                selected_row = pipeline_df[pipeline_df["RMR DB ID"].astype(int) == selected_rmr_id].iloc[0] if selected_rmr_id else None
                if selected_row is not None:
                    st.write(f"**Selected:** {selected_row['RMR ID']} — {selected_row['Work Item']} — {selected_row['Property']}")
                    is_deleted = bool(selected_row.get("Deleted", False)) or str(selected_row.get("Budget Status", "")).lower() == "deleted"
                    if not is_deleted:
                        confirm_delete = st.checkbox("Confirm delete this RMR/work item from the active queue", key="master_work_list_confirm_delete")
                        if st.button("Delete Selected RMR / Work Item", type="secondary", key="master_work_list_delete_btn"):
                            if not confirm_delete:
                                st.warning("Check the confirmation box first.")
                            else:
                                soft_delete_rmr_record(selected_rmr_id)
                                st.success("RMR/work item deleted from active queue.")
                                st.rerun()
                    else:
                        confirm_restore = st.checkbox("Confirm restore this RMR/work item", key="master_work_list_confirm_restore")
                        if st.button("Restore Selected RMR / Work Item", type="primary", key="master_work_list_restore_btn"):
                            if not confirm_restore:
                                st.warning("Check the confirmation box first.")
                            else:
                                restore_rmr_record(selected_rmr_id)
                                st.success("RMR/work item restored.")
                                st.rerun()

elif page == "RMR Entry":
    st.header("RMR Entry")
    st.caption("Create a Renovation Master Record. This is the fast field-entry page for iPhone or Surface use.")

    if st.session_state.get("rmr_saved_message"):
        st.success(st.session_state.get("rmr_saved_message"))

    source_row = None
    if st.session_state.get("rmr_duplicate_source_id"):
        source_row = rmr_row_from_id(int(st.session_state.get("rmr_duplicate_source_id")))
        if source_row:
            st.info(f"Duplicating / copying from {source_row.get('rmr_code', '')}. A new RMR ID will be created when saved.")

    with st.expander("Copy From Existing RMR", expanded=False):
        search_existing = st.text_input("Search existing RMRs to use as a template", key="rmr_copy_search_text")
        if search_existing:
            existing_matches = rmr_records_df(search_text=search_existing).head(25)
            if existing_matches.empty:
                st.info("No matching RMRs found.")
            else:
                labels = [f"{int(row.id)} | {row.rmr_code} | {row.property_name} | {row.work_item_name} | {row.location_identifier}" for row in existing_matches.itertuples()]
                selected_template = st.selectbox("Choose RMR Template", labels, key="rmr_template_select")
                if st.button("Use Selected RMR As Template", key="use_rmr_template_btn"):
                    st.session_state.rmr_duplicate_source_id = int(selected_template.split(" | ", 1)[0])
                    st.rerun()
        if st.session_state.get("rmr_duplicate_source_id"):
            if st.button("Clear Template / Duplicate Source", key="clear_rmr_template_btn"):
                st.session_state.rmr_duplicate_source_id = None
                st.rerun()

    data, uploaded_files = render_rmr_entry_form(mode="create", existing_row=source_row)

    def save_current_rmr_and_reset():
        required_missing = []
        if not data.get("property_name"):
            required_missing.append("Property")
        if not data.get("location_identifier") and not data.get("address"):
            required_missing.append("Location")
        if not data.get("work_item_name"):
            required_missing.append("Work Item")
        if not data.get("scope_description"):
            required_missing.append("Scope")
        if required_missing:
            st.error("Missing required fields: " + ", ".join(required_missing))
            return
        new_id = create_rmr_record(data)
        if new_id:
            save_rmr_files(new_id, uploaded_files, uploaded_by=str(st.session_state.get("logged_in_user", "") or ""))
            st.session_state.last_created_rmr_id = int(new_id)
            st.session_state.rmr_duplicate_source_id = None
            st.session_state.rmr_entry_form_version = int(st.session_state.get("rmr_entry_form_version", 0)) + 1
            st.session_state.rmr_saved_message = f"✅ RMR saved successfully: RMR-{int(new_id):06d}. Ready for the next entry."
            st.rerun()

    save_col1, save_col2, save_col3 = st.columns(3)
    if save_col1.button("Save RMR", type="primary", key="save_new_rmr_btn"):
        save_current_rmr_and_reset()
    if save_col2.button("Save & New", key="save_and_new_rmr_btn"):
        save_current_rmr_and_reset()
    if save_col3.button("Duplicate Last Created RMR", key="duplicate_last_rmr_btn"):
        if st.session_state.get("last_created_rmr_id"):
            st.session_state.rmr_duplicate_source_id = int(st.session_state.get("last_created_rmr_id"))
            st.rerun()
        else:
            st.warning("No last-created RMR found in this session yet.")


elif page == "RMR Search / Review":
    st.header("RMR Search / Review")
    st.caption("Find, review, edit, duplicate, and manage Renovation Master Records.")

    f1, f2, f3 = st.columns([2, 1.2, 1])
    search_text = f1.text_input("Search RMR ID, property, location, work item, scope, notes, materials", key="rmr_search_text")
    # Pull the RMR search property filter from the master property list, not only from
    # properties that already have RMR records. This keeps RMR aligned with the
    # Renovation Management System master data. Also include any legacy/general
    # property names that may exist only on saved RMRs.
    master_property_df = portfolio_properties_df(include_inactive=True, include_deleted=False)
    master_property_names = []
    if not master_property_df.empty:
        master_property_names = [
            str(v).strip()
            for v in master_property_df["property_name"].dropna().astype(str).tolist()
            if str(v).strip()
        ]

    rmr_property_df = rmr_records_df(include_deleted=False)
    rmr_property_names = []
    if not rmr_property_df.empty:
        rmr_property_names = [
            str(v).strip()
            for v in rmr_property_df["property_name"].dropna().astype(str).tolist()
            if str(v).strip()
        ]

    property_options = ["All"] + sorted(set(master_property_names + rmr_property_names), key=lambda x: x.lower())
    property_filter = f2.selectbox("Property", property_options, key="rmr_search_property")
    status_filter = f3.selectbox("Status", ["All"] + RMR_INFO_STATUS_OPTIONS, key="rmr_search_status")

    results_df = rmr_records_df(search_text=search_text, property_name=property_filter, status=status_filter)
    st.write(f"**{len(results_df)} RMR record(s) found.**")

    if results_df.empty:
        st.info("No RMRs found yet.")
    else:
        display_df = results_df[[
            "rmr_code", "entry_date", "property_name", "address", "unit_number", "location_identifier",
            "work_item_name", "category_name", "contractor_priority", "owner_intent", "unread_contractor_notes", "last_contractor_update", "labor_budget", "materials_budget", "budget_timeframe", "budget_status", "info_status", "photo_count", "modified_at"
        ]].copy()
        display_df = display_df.rename(columns={
            "rmr_code": "RMR ID", "entry_date": "Date", "property_name": "Property", "address": "Address",
            "unit_number": "Unit", "location_identifier": "Location", "work_item_name": "Work Item",
            "category_name": "Category", "contractor_priority": "Priority", "owner_intent": "Owner Intent", "unread_contractor_notes": "Unread Notes", "last_contractor_update": "Last Contractor Update", "labor_budget": "Labor Budget", "materials_budget": "Materials Budget",
            "budget_timeframe": "Budget Timeframe", "budget_status": "Budget Status", "info_status": "Status",
            "photo_count": "Photos", "modified_at": "Modified"
        })
        if "Last Contractor Update" in display_df.columns:
            display_df["Last Contractor Update"] = pd.to_datetime(display_df["Last Contractor Update"], errors="coerce").dt.strftime("%m-%d-%Y %H:%M")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        with st.expander("Create / Assign RMR Group", expanded=False):
            st.caption("Use this on the Surface review screen after entering several RMRs. Select multiple RMRs and place them into a group for quoting or review.")
            group_candidate_labels = [
                f"{int(row.id)} | {row.rmr_code} | {row.property_name} | {row.work_item_name} | {row.location_identifier}"
                for row in results_df.itertuples()
            ]
            selected_group_candidates = st.multiselect(
                "Select RMRs To Group",
                group_candidate_labels,
                key="rmr_group_candidate_multiselect",
            )
            selected_group_ids = [int(label.split(" | ", 1)[0]) for label in selected_group_candidates]
            if selected_group_ids:
                selected_rows = results_df[results_df["id"].astype(int).isin(selected_group_ids)].copy()
                suggested_property = str(selected_rows["property_name"].mode().iloc[0]) if not selected_rows.empty and not selected_rows["property_name"].mode().empty else ""
                suggested_work_item = str(selected_rows["work_item_name"].mode().iloc[0]) if not selected_rows.empty and not selected_rows["work_item_name"].mode().empty else ""
                suggested_group_name = f"{suggested_property} {suggested_work_item} Group".strip()
                g1, g2 = st.columns(2)
                group_action = g1.radio("Group Action", ["Create New RMR Review Group", "Assign To Existing RMR Review Group"], key="rmr_group_action")
                if group_action == "Create New RMR Review Group":
                    new_group_name = g2.text_input("New RMR Review Group Name", value=suggested_group_name, key="rmr_new_group_name")
                    new_group_notes = st.text_area("Group Notes", key="rmr_new_group_notes", height=80)
                    if st.button("Create RMR Review Group Only", type="primary", key="create_rmr_group_btn"):
                        group_id = create_rmr_group(new_group_name, suggested_property, new_group_notes)
                        if group_id:
                            assign_rmrs_to_rmr_group(selected_group_ids, group_id)
                            st.success(f"Created RMR review group and assigned {len(selected_group_ids)} RMR(s).")
                            st.rerun()
                        else:
                            st.error("Enter a group name.")
                else:
                    existing_group_labels = rmr_group_labels(suggested_property) or rmr_group_labels()
                    if not existing_group_labels:
                        st.info("No existing RMR groups found yet. Create a new group first.")
                    else:
                        selected_existing_group = g2.selectbox("Existing RMR Review Group", existing_group_labels, key="rmr_existing_group_select")
                        if st.button("Assign Selected RMRs To Existing RMR Review Group", type="primary", key="assign_existing_rmr_group_btn"):
                            group_id = int(selected_existing_group.split(" | ", 1)[0])
                            assign_rmrs_to_rmr_group(selected_group_ids, group_id)
                            st.success(f"Assigned {len(selected_group_ids)} RMR(s) to RMR review group.")
                            st.rerun()

            st.markdown("---")
            st.markdown("### Send Selected RMRs To Contractor Portal")
            st.caption("This creates a real Work Group from the selected RMRs. The assigned contractor will see it under My Work Groups after login.")
            if not selected_group_ids:
                st.info("Select one or more RMRs above to create a contractor Work Group.")
            else:
                selected_rows = results_df[results_df["id"].astype(int).isin(selected_group_ids)].copy()
                suggested_property = str(selected_rows["property_name"].mode().iloc[0]) if not selected_rows.empty and not selected_rows["property_name"].mode().empty else ""
                suggested_work_item = str(selected_rows["work_item_name"].mode().iloc[0]) if not selected_rows.empty and not selected_rows["work_item_name"].mode().empty else ""
                suggested_location = str(selected_rows["location_identifier"].mode().iloc[0]) if not selected_rows.empty and not selected_rows["location_identifier"].mode().empty else ""
                suggested_work_group_name = f"{suggested_property} {suggested_work_item}".strip()
                if suggested_location and len(selected_group_ids) == 1:
                    suggested_work_group_name = f"{suggested_location} {suggested_work_item}".strip()

                project_choice_mode = st.radio("Project Link", ["Use Existing Project", "Create New Project"], horizontal=True, key="rmr_to_wg_project_mode")
                p1, p2 = st.columns(2)
                project_id_for_wg = None
                if project_choice_mode == "Use Existing Project":
                    project_labels_for_wg = project_registry_select_labels(active_only=True)
                    if project_labels_for_wg:
                        selected_project_label_for_wg = p1.selectbox("Project", project_labels_for_wg, key="rmr_to_wg_existing_project")
                        selected_project_row_for_wg = get_project_registry_row_from_label(selected_project_label_for_wg)
                        if selected_project_row_for_wg is not None:
                            project_id_for_wg = int(selected_project_row_for_wg["id"])
                    else:
                        p1.info("No active projects found. Choose Create New Project.")
                else:
                    new_project_name_for_wg = p1.text_input("New Project Name", value=suggested_work_group_name, key="rmr_to_wg_new_project_name")
                    new_project_address_for_wg = p2.text_input("New Project Address", value=str(selected_rows["address"].mode().iloc[0]) if not selected_rows.empty and not selected_rows["address"].mode().empty else "", key="rmr_to_wg_new_project_address")

                c1, c2 = st.columns(2)
                contractor_names = get_contractor_names()
                contractor_choice = c1.selectbox("Assign Contractor", ["None selected"] + contractor_names, key="rmr_to_wg_contractor")
                contractor_id_for_wg = get_contractor_id_by_name(contractor_choice)
                contractor_due_date = c2.date_input("Due Date", value=datetime.now().date(), key="rmr_to_wg_due_date")
                c3, c4 = st.columns(2)
                contractor_status = c3.selectbox("Work Group Status", ["Open", "Quote Requested", "Approved", "In Progress", "Completed"], key="rmr_to_wg_status")
                contractor_work_group_name = c4.text_input("Contractor Work Group Name", value=suggested_work_group_name, key="rmr_to_wg_name")
                render_contractor_priority_legend()
                pcol1, pcol2 = st.columns(2)
                default_priority_for_wg = "3 - Quote Only"
                if "contractor_priority" in selected_rows.columns and not selected_rows.empty:
                    priorities_for_wg = sorted(selected_rows["contractor_priority"].fillna("3 - Quote Only").astype(str).tolist(), key=contractor_priority_sort_value)
                    default_priority_for_wg = priorities_for_wg[0] if priorities_for_wg else "3 - Quote Only"
                selected_work_group_priority = pcol1.selectbox(
                    "Contractor Priority",
                    CONTRACTOR_PRIORITY_OPTIONS,
                    index=CONTRACTOR_PRIORITY_OPTIONS.index(default_priority_for_wg) if default_priority_for_wg in CONTRACTOR_PRIORITY_OPTIONS else 2,
                    key="rmr_to_wg_priority",
                )
                default_intent_for_wg = "Quote Only"
                if "owner_intent" in selected_rows.columns and not selected_rows.empty:
                    intents_for_wg = [str(x).strip() for x in selected_rows["owner_intent"].fillna("Quote Only").astype(str).tolist() if str(x).strip()]
                    default_intent_for_wg = intents_for_wg[0] if intents_for_wg else "Quote Only"
                selected_work_group_intent = pcol2.selectbox(
                    "Owner Intent",
                    OWNER_INTENT_OPTIONS,
                    index=OWNER_INTENT_OPTIONS.index(default_intent_for_wg) if default_intent_for_wg in OWNER_INTENT_OPTIONS else 0,
                    key="rmr_to_wg_intent",
                )
                contractor_notes = st.text_area("Owner Notes For Contractor", value="", height=90, key="rmr_to_wg_notes")
                copy_rmr_photos = st.checkbox("Copy RMR photos into Work Group photos", value=True, key="rmr_to_wg_copy_photos")

                if st.button("Create Contractor Work Group From Selected RMRs", type="primary", key="rmr_create_contractor_work_group_btn"):
                    if project_choice_mode == "Create New Project":
                        project_id_for_wg = find_or_create_project_simple(new_project_name_for_wg, new_project_address_for_wg, contractor_notes)
                    if not project_id_for_wg:
                        st.error("Choose or create a project before sending to the contractor portal.")
                    else:
                        new_wg_id = create_work_group_from_rmrs(
                            selected_group_ids,
                            int(project_id_for_wg),
                            contractor_work_group_name,
                            contractor_id_for_wg,
                            contractor_due_date,
                            contractor_status,
                            contractor_notes,
                            copy_rmr_photos,
                        )
                        if new_wg_id:
                            execute("UPDATE work_groups SET contractor_priority = ?, owner_intent = ?, modified_at = NOW() WHERE id = ?", (selected_work_group_priority, selected_work_group_intent, int(new_wg_id)))
                            st.success(f"Created contractor Work Group WG{new_wg_id}. It will appear in the assigned contractor's My Work Groups screen.")
                            st.rerun()
                        else:
                            st.error("Could not create contractor Work Group from selected RMRs.")

        labels = [f"{int(row.id)} | {row.rmr_code} | {row.property_name} | {row.work_item_name} | {row.location_identifier}" for row in results_df.itertuples()]
        selected_label = st.selectbox("Open RMR", labels, key="rmr_open_select")
        selected_id = int(selected_label.split(" | ", 1)[0])
        selected_row = rmr_row_from_id(selected_id)

        if selected_row:
            st.markdown("---")
            st.subheader(f"{selected_row.get('rmr_code')} — {selected_row.get('work_item_name')}")
            top_a, top_b, top_c, top_d = st.columns(4)
            top_a.metric("Labor Budget", format_money(selected_row.get("labor_budget")))
            top_b.metric("Materials Budget", format_money(selected_row.get("materials_budget")))
            top_c.metric("My Hours", f"{float(selected_row.get('user_estimated_hours') or 0):.2f}")
            top_d.metric("Photos", int(selected_row.get("photo_count") or 0))
            render_rmr_progress_panel(selected_row)

            tabs = st.tabs(["Overview / Edit", "Photos / Files", "History", "Actions / Contractor Requests"])
            with tabs[0]:
                data, uploaded_files = render_rmr_entry_form(mode="edit", existing_row=selected_row)
                linked_wg_id = int(selected_row.get("work_group_id") or 0)
                if linked_wg_id:
                    st.markdown("### Linked Contractor / Work Group Updates")
                    wg_df = work_groups_df()
                    wg_match = wg_df[wg_df["id"].astype(int) == linked_wg_id] if not wg_df.empty else pd.DataFrame()
                    if not wg_match.empty:
                        wg = wg_match.iloc[0]
                        cinfo1, cinfo2, cinfo3 = st.columns(3)
                        cinfo1.metric("Contractor Requested", format_money(wg.get("contractor_requested_price")))
                        cinfo2.metric("Amount To Be Paid", format_money(wg.get("amount_to_be_paid")))
                        cinfo3.write(f"**Status:** {wg.get('status', '')}")
                        st.caption(f"Priority: {wg.get('contractor_priority', '3 - Quote Only')} | Owner Intent: {wg.get('owner_intent', 'Quote Only')}")
                        render_work_group_contractor_notes(linked_wg_id)
                        render_work_group_photos_readonly(linked_wg_id, section_key=f"rmr_linked_{selected_id}")
                if st.button("Save Changes To RMR", type="primary", key=f"save_rmr_changes_{selected_id}"):
                    update_rmr_record(selected_id, data)
                    save_rmr_files(selected_id, uploaded_files, uploaded_by=str(st.session_state.get("logged_in_user", "") or ""))
                    st.success("RMR updated.")
                    st.rerun()
            with tabs[1]:
                st.markdown("### RMR Photos / Files")
                render_rmr_file_section(selected_id, section_key="review", allow_delete=True)
            with tabs[2]:
                history_df = rmr_history_df(selected_id)
                if history_df.empty:
                    st.info("No history yet.")
                else:
                    for row in history_df.itertuples():
                        created_display = pd.to_datetime(row.created_at, errors="coerce")
                        created_text = created_display.strftime("%m-%d-%Y %I:%M %p") if pd.notna(created_display) else ""
                        st.markdown(f"**{created_text} — {row.action_type} — {row.changed_by}**")
                        if str(row.action_notes or "").strip():
                            st.write(row.action_notes)
                        st.markdown("---")
            with tabs[3]:
                st.markdown("### Contractor Requests / Quotes")
                st.caption("Engage contractors from the RMR. Contractor Quotes remains the review page for submitted responses.")
                contractor_names = get_contractor_names()
                default_contractors = []
                if selected_row.get("contractor_id"):
                    cdf = fetch_df("SELECT name FROM contractors WHERE id = ?", (int(selected_row.get("contractor_id")),))
                    if not cdf.empty:
                        default_contractors = [str(cdf.iloc[0]["name"])]
                selected_quote_contractors = st.multiselect(
                    "Request From Contractor(s)",
                    contractor_names,
                    default=[x for x in default_contractors if x in contractor_names],
                    key=f"rmr_request_quote_contractors_{selected_id}",
                )
                qc1, qc2, qc3 = st.columns(3)
                request_quote = qc1.checkbox("Quote", value=True, key=f"rmr_action_quote_{selected_id}")
                request_materials = qc2.checkbox("Materials List", value=(selected_row.get("owner_intent") in ["Quote + Materials", "Move Forward"]), key=f"rmr_action_materials_{selected_id}")
                request_availability = qc3.checkbox("Availability / Comments", value=str(selected_row.get("contractor_priority") or "").startswith("1"), key=f"rmr_action_avail_{selected_id}")
                parts = []
                if request_quote:
                    parts.append("Quote requested")
                if request_materials:
                    parts.append("materials list requested")
                if request_availability:
                    parts.append("availability/comments requested")
                default_quote_note = f"Priority: {selected_row.get('contractor_priority', '3 - Quote Only')}. Intent: {selected_row.get('owner_intent', 'Quote Only')}. " + (", ".join(parts) + "." if parts else "")
                rmr_quote_notes = st.text_area("Request Notes For Contractor", value=default_quote_note.strip(), height=90, key=f"rmr_request_quote_notes_{selected_id}")
                if st.button("Create Quote / Materials / Availability Request(s)", type="primary", key=f"rmr_create_quote_requests_{selected_id}"):
                    contractor_ids = [get_contractor_id_by_name(name) for name in selected_quote_contractors]
                    contractor_ids = [int(x) for x in contractor_ids if x]
                    if not contractor_ids:
                        st.error("Select at least one contractor.")
                    elif not (request_quote or request_materials or request_availability):
                        st.error("Select at least one request type: Quote, Materials List, or Availability / Comments.")
                    else:
                        created_count, skipped_names = create_rmr_quote_requests(selected_id, contractor_ids, rmr_quote_notes)
                        if created_count:
                            st.success(f"Created {created_count} contractor request(s).")
                        if skipped_names:
                            st.warning("Skipped existing request(s): " + ", ".join(skipped_names))
                        st.rerun()

                existing_rmr_quotes = fetch_df(
                    """
                    SELECT qr.id, COALESCE(c.name, '') AS contractor, COALESCE(qr.quote_status, 'Requested') AS quote_status,
                           COALESCE(qr.quote_amount, 0) AS quote_amount, COALESCE(qr.quote_notes, '') AS quote_notes,
                           qr.requested_at, qr.submitted_at, qr.modified_at
                    FROM quote_requests qr
                    LEFT JOIN contractors c ON c.id = qr.contractor_id
                    WHERE COALESCE(qr.rmr_id, 0) = ?
                    ORDER BY qr.requested_at DESC NULLS LAST, qr.id DESC
                    """,
                    (selected_id,),
                )
                if not existing_rmr_quotes.empty:
                    show_q = existing_rmr_quotes.copy()
                    for col in ["requested_at", "submitted_at", "modified_at"]:
                        show_q[col] = pd.to_datetime(show_q[col], errors="coerce").dt.strftime("%m-%d-%Y %H:%M")
                    st.markdown("#### Existing Contractor Requests / Quotes For This RMR")
                    st.dataframe(show_q.rename(columns={"contractor":"Contractor", "quote_status":"Status", "quote_amount":"Quote Amount", "quote_notes":"Notes", "requested_at":"Requested", "submitted_at":"Submitted", "modified_at":"Updated"}), use_container_width=True, hide_index=True)
                st.markdown("---")
                execute("UPDATE rmr_communications SET is_unread_for_owner = FALSE WHERE rmr_id = ?", (int(selected_id),))
                render_rmr_communication_thread(selected_id, allow_owner_note=True, section_key="owner_action")
                st.markdown("---")
                st.markdown("### RMR Actions")
                a1, a2 = st.columns(2)
                if a1.button("Duplicate This RMR", key=f"duplicate_this_rmr_{selected_id}"):
                    st.session_state.rmr_duplicate_source_id = selected_id
                    st.session_state.pending_page = "RMR Entry"
                    st.rerun()
                if a2.button("Delete This RMR", key=f"delete_this_rmr_{selected_id}"):
                    st.session_state[f"confirm_delete_rmr_{selected_id}"] = True
                    st.rerun()
                if st.session_state.get(f"confirm_delete_rmr_{selected_id}"):
                    st.warning("Confirm delete this RMR? It will be hidden from normal searches.")
                    c1, c2 = st.columns(2)
                    if c1.button("Yes, Delete RMR", type="primary", key=f"confirm_delete_rmr_yes_{selected_id}"):
                        delete_rmr_record(selected_id)
                        st.session_state[f"confirm_delete_rmr_{selected_id}"] = False
                        st.success("RMR deleted.")
                        st.rerun()
                    if c2.button("Cancel", key=f"confirm_delete_rmr_cancel_{selected_id}"):
                        st.session_state[f"confirm_delete_rmr_{selected_id}"] = False
                        st.rerun()



elif page == "Budget Planner":
    if current_role == "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    st.header("Budget Planner")
    st.caption("Review RMR budgets, move items between timeframes, defer items, and summarize labor/materials cash needs.")

    b1, b2, b3, b4 = st.columns([1.4, 1.2, 1.2, 1.2])
    all_rmrs_for_filters = rmr_records_df(include_deleted=False)

    property_names = ["All"]
    if not all_rmrs_for_filters.empty:
        property_names += sorted([p for p in all_rmrs_for_filters["property_name"].dropna().astype(str).unique().tolist() if p], key=lambda x: x.lower())
    budget_property_filter = b1.selectbox("Property", property_names, key="budget_planner_property")

    timeframe_options = ["All"] + RMR_BUDGET_TIMEFRAME_OPTIONS
    budget_timeframe_filter = b2.selectbox("Budget Timeframe", timeframe_options, key="budget_planner_timeframe")

    status_options = ["All"] + RMR_BUDGET_STATUS_OPTIONS
    budget_status_filter = b3.selectbox("Budget Status", status_options, key="budget_planner_status")

    budget_search = b4.text_input("Search", key="budget_planner_search")

    budget_df = rmr_records_df(include_deleted=False, search_text=budget_search, property_name=budget_property_filter)
    if budget_timeframe_filter != "All" and not budget_df.empty:
        budget_df = budget_df[budget_df["budget_timeframe"].astype(str) == budget_timeframe_filter].copy()
    if budget_status_filter != "All" and not budget_df.empty:
        budget_df = budget_df[budget_df["budget_status"].astype(str) == budget_status_filter].copy()

    st.markdown("### Budget Totals")
    if budget_df.empty:
        st.info("No RMR budget items found for the selected filters.")
    else:
        budget_df["labor_budget"] = pd.to_numeric(budget_df["labor_budget"], errors="coerce").fillna(0.0)
        budget_df["materials_budget"] = pd.to_numeric(budget_df["materials_budget"], errors="coerce").fillna(0.0)
        labor_total = budget_df["labor_budget"].sum()
        materials_total = budget_df["materials_budget"].sum()
        active_df = budget_df[budget_df["budget_status"].astype(str) == "Active"].copy()
        active_labor = pd.to_numeric(active_df["labor_budget"], errors="coerce").fillna(0).sum() if not active_df.empty else 0
        active_materials = pd.to_numeric(active_df["materials_budget"], errors="coerce").fillna(0).sum() if not active_df.empty else 0
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("RMR Count", len(budget_df))
        m2.metric("Labor Budget", format_money(labor_total))
        m3.metric("Materials Budget", format_money(materials_total))
        m4.metric("Total Budget", format_money(labor_total + materials_total))
        st.caption(f"Active-only total: Labor {format_money(active_labor)} | Materials {format_money(active_materials)} | Total {format_money(active_labor + active_materials)}")

        # Incomplete-budget warning section.
        missing_df = budget_df[
            (budget_df["budget_status"].astype(str) == "Active")
            & (
                (budget_df["labor_budget"].fillna(0) <= 0)
                | (budget_df["materials_budget"].fillna(0) <= 0)
                | (budget_df["budget_timeframe"].fillna("No Timeframe Yet").astype(str) == "No Timeframe Yet")
            )
        ].copy()
        if not missing_df.empty:
            st.warning(f"{len(missing_df)} active RMR budget item(s) need budget/timeframe review.")
            missing_display = missing_df[["rmr_code", "property_name", "work_item_name", "labor_budget", "materials_budget", "budget_timeframe"]].copy()
            missing_display["Missing Items"] = missing_display.apply(
                lambda r: ", ".join([
                    item for item, missing in [
                        ("Labor Budget", float(r.get("labor_budget") or 0) <= 0),
                        ("Materials Budget", float(r.get("materials_budget") or 0) <= 0),
                        ("Budget Timeframe", str(r.get("budget_timeframe") or "No Timeframe Yet") == "No Timeframe Yet"),
                    ] if missing
                ]),
                axis=1,
            )
            missing_display = missing_display.rename(columns={
                "rmr_code": "RMR ID",
                "property_name": "Property",
                "work_item_name": "Work Item",
                "labor_budget": "Labor Budget",
                "materials_budget": "Materials Budget",
                "budget_timeframe": "Budget Timeframe",
            })
            st.dataframe(missing_display, use_container_width=True, hide_index=True)

        st.markdown("### Edit Budget Items")
        st.caption("Use this grid to move items earlier/later, defer them, update materials budgets, and select rows for bulk moves or cash-flow export.")

        edit_df = budget_df[[
            "id", "rmr_code", "entry_date", "property_name", "project_name", "work_group_id", "linked_work_group_name",
            "work_item_name", "contractor_name", "contractor_priority", "owner_intent", "labor_budget", "materials_budget", "budget_timeframe",
            "budget_start_date", "budget_end_date", "budget_status", "cashflow_export_status", "cashflow_last_exported_at", "cashflow_export_signature"
        ]].copy()
        edit_df = edit_df.rename(columns={
            "id": "RMR Internal ID",
            "rmr_code": "RMR ID",
            "entry_date": "RMR Date",
            "property_name": "Property",
            "project_name": "Project",
            "work_group_id": "Work Group Internal ID",
            "linked_work_group_name": "Work Group",
            "work_item_name": "Work Item",
            "contractor_name": "Contractor",
            "contractor_priority": "Contractor Priority",
            "owner_intent": "Owner Intent",
            "labor_budget": "Labor Budget",
            "materials_budget": "Materials Budget",
            "budget_timeframe": "Budget Timeframe",
            "budget_start_date": "Budget Start Date",
            "budget_end_date": "Budget End Date",
            "budget_status": "Budget Status",
            "cashflow_export_status": "Cash Flow Export Status",
            "cashflow_last_exported_at": "Last Cash Flow Export",
            "cashflow_export_signature": "Cash Flow Export Signature",
        })
        edit_df.insert(0, "Select", False)
        edit_df["Labor Budget"] = pd.to_numeric(edit_df["Labor Budget"], errors="coerce").fillna(0.0)
        edit_df["Materials Budget"] = pd.to_numeric(edit_df["Materials Budget"], errors="coerce").fillna(0.0)
        edit_df["Total Budget"] = edit_df["Labor Budget"] + edit_df["Materials Budget"]
        edit_df["Budget Start Date"] = pd.to_datetime(edit_df["Budget Start Date"], errors="coerce").dt.date
        edit_df["Budget End Date"] = pd.to_datetime(edit_df["Budget End Date"], errors="coerce").dt.date
        edit_df["Last Cash Flow Export"] = pd.to_datetime(edit_df["Last Cash Flow Export"], errors="coerce")
        edit_df["Cash Flow Export Status"] = edit_df.apply(
            lambda r: display_export_status(r.get("Cash Flow Export Status"), r.get("Cash Flow Export Signature"), rmr_export_signature(r)),
            axis=1,
        )

        # Put the active budgeting controls near the left side so they are visible without horizontal scrolling.
        edit_df = edit_df[[
            "Select", "RMR ID", "Budget Status", "Budget Timeframe", "Budget Start Date", "Budget End Date",
            "Labor Budget", "Materials Budget", "Total Budget", "Cash Flow Export Status", "Last Cash Flow Export",
            "Property", "Work Item", "Contractor Priority", "Owner Intent", "RMR Date", "Project", "Work Group", "Contractor",
            "RMR Internal ID", "Work Group Internal ID", "Cash Flow Export Signature"
        ]]

        edited_budget_df = st.data_editor(
            edit_df,
            use_container_width=True,
            hide_index=True,
            key="budget_planner_editor",
            column_config={
                "Select": st.column_config.CheckboxColumn("Select"),
                "RMR Internal ID": st.column_config.NumberColumn("RMR Internal ID", disabled=True),
                "RMR ID": st.column_config.TextColumn("RMR ID", disabled=True, width="small"),
                "RMR Date": st.column_config.DateColumn("RMR Date", disabled=True),
                "Property": st.column_config.TextColumn("Property", disabled=True, width="medium"),
                "Project": st.column_config.TextColumn("Project", disabled=True, width="medium"),
                "Work Group": st.column_config.TextColumn("Work Group", disabled=True, width="medium"),
                "Work Group Internal ID": st.column_config.NumberColumn("Work Group Internal ID", disabled=True),
                "Work Item": st.column_config.TextColumn("Work Item", disabled=True, width="large"),
                "Contractor": st.column_config.TextColumn("Contractor", disabled=True, width="medium"),
                "Contractor Priority": st.column_config.SelectboxColumn("Contractor Priority", options=CONTRACTOR_PRIORITY_OPTIONS, required=True, width="medium"),
                "Owner Intent": st.column_config.SelectboxColumn("Owner Intent", options=OWNER_INTENT_OPTIONS, required=True, width="medium"),
                "Labor Budget": st.column_config.NumberColumn("Labor Budget", min_value=0.0, format="$%.2f", width="small"),
                "Materials Budget": st.column_config.NumberColumn("Materials Budget", min_value=0.0, format="$%.2f", width="small"),
                "Total Budget": st.column_config.NumberColumn("Total Budget", min_value=0.0, format="$%.2f", disabled=True, width="small"),
                "Budget Timeframe": st.column_config.SelectboxColumn("Budget Timeframe", options=RMR_BUDGET_TIMEFRAME_OPTIONS, required=True, width="medium"),
                "Budget Start Date": st.column_config.DateColumn("Budget Start Date", width="small"),
                "Budget End Date": st.column_config.DateColumn("Budget End Date", width="small"),
                "Budget Status": st.column_config.SelectboxColumn("Budget Status", options=RMR_BUDGET_STATUS_OPTIONS, required=True, width="small"),
                "Cash Flow Export Status": st.column_config.TextColumn("Cash Flow Export Status", disabled=True, width="medium"),
                "Last Cash Flow Export": st.column_config.DatetimeColumn("Last Cash Flow Export", disabled=True, width="medium"),
                "Cash Flow Export Signature": st.column_config.TextColumn("Cash Flow Export Signature", disabled=True),
            },
        )

        selected_budget_rows = edited_budget_df[edited_budget_df["Select"].fillna(False).astype(bool)].copy()
        st.caption(f"Selected rows: {len(selected_budget_rows)}")
        st.caption("Editable fields are now placed at the left of the grid: Budget Status, Timeframe, Dates, Labor Budget, and Materials Budget.")

        st.markdown("#### Bulk Move / Defer Selected Items")
        bulk_c1, bulk_c2, bulk_c3 = st.columns([1.2, 1.2, 1.4])
        bulk_timeframe = bulk_c1.selectbox("Move Selected To Timeframe", RMR_BUDGET_TIMEFRAME_OPTIONS, key="budget_bulk_timeframe")
        bulk_status = bulk_c2.selectbox("Set Selected Status", RMR_BUDGET_STATUS_OPTIONS, key="budget_bulk_status")
        bulk_apply = bulk_c3.button("Apply To Selected Rows", type="secondary", key="budget_bulk_apply")
        if bulk_apply:
            if selected_budget_rows.empty:
                st.warning("Check at least one row in the Select column first.")
            else:
                changed_count = 0
                for _, row in selected_budget_rows.iterrows():
                    try:
                        rmr_id = int(row["RMR Internal ID"])
                    except Exception:
                        continue
                    if bulk_timeframe != "Custom Dates":
                        budget_start, budget_end = calculate_budget_dates(row.get("RMR Date"), bulk_timeframe)
                    else:
                        budget_start, budget_end = row.get("Budget Start Date"), row.get("Budget End Date")
                    update_rmr_budget_fields(
                        rmr_id,
                        float(row.get("Labor Budget") or 0),
                        float(row.get("Materials Budget") or 0),
                        bulk_timeframe,
                        budget_start,
                        budget_end,
                        bulk_status,
                    )
                    changed_count += 1
                st.success(f"Updated {changed_count} selected budget item(s).")
                st.rerun()

        if st.button("Save Budget Planner Changes", type="primary", key="save_budget_planner_changes"):
            saved_count = 0
            for _, row in edited_budget_df.iterrows():
                try:
                    rmr_id = int(row["RMR Internal ID"])
                except Exception:
                    continue
                timeframe = str(row.get("Budget Timeframe") or "No Timeframe Yet")
                custom_start = row.get("Budget Start Date")
                custom_end = row.get("Budget End Date")
                if timeframe != "Custom Dates":
                    budget_start, budget_end = calculate_budget_dates(row.get("RMR Date"), timeframe)
                else:
                    budget_start, budget_end = custom_start, custom_end
                update_rmr_budget_fields(
                    rmr_id,
                    float(row.get("Labor Budget") or 0),
                    float(row.get("Materials Budget") or 0),
                    timeframe,
                    budget_start,
                    budget_end,
                    str(row.get("Budget Status") or "Active"),
                )
                execute("UPDATE renovation_master_records SET contractor_priority = ?, owner_intent = ?, modified_at = NOW() WHERE id = ?", (str(row.get("Contractor Priority") or "3 - Quote Only"), str(row.get("Owner Intent") or "Quote Only"), rmr_id))
                saved_count += 1
            st.success(f"Saved budget changes for {saved_count} RMR(s).")
            st.rerun()

        st.markdown("#### Cash Flow Cloud Export")
        st.caption("Build 15B: export selected individual RMRs and/or Work Groups into the separate Cash Flow Cloud database. Re-exporting updates/replaces prior exported rows for the same source instead of creating duplicates.")

        export_level = st.radio(
            "Export Level",
            ["Individual RMRs", "Work Groups"],
            horizontal=True,
            key="budget_cashflow_export_level",
            help="Individual RMRs exports each selected budget item separately. Work Groups consolidates selected rows with the same Work Group; ungrouped rows still export as individual RMRs.",
        )
        export_source = selected_budget_rows.copy()
        if export_source.empty:
            export_source = edited_budget_df[edited_budget_df["Budget Status"].astype(str) == "Active"].copy()
            st.caption("No rows are selected, so the export preview uses all filtered Active rows.")
        else:
            st.caption("The export preview uses the selected rows only.")

        export_rows, source_updates = selected_budget_rows_to_cash_flow_rows(export_source, export_level)
        export_columns = CASH_FLOW_COLUMNS + CASH_FLOW_SOURCE_COLUMNS
        export_df = pd.DataFrame(export_rows, columns=export_columns)
        if export_df.empty:
            st.info("No labor or materials budget amounts are available to export yet.")
        else:
            st.dataframe(export_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download Cash Flow Export CSV",
                data=export_df[CASH_FLOW_COLUMNS].to_csv(index=False).encode("utf-8"),
                file_name="rmr_budget_cash_flow_export.csv",
                mime="text/csv",
                key="download_rmr_budget_cash_flow_export_csv",
            )
            if not cash_flow_connection_available():
                st.warning("Direct Cash Flow export is not configured yet. Add cash_flow_database_url to this Renovation Management app's Streamlit secrets. The CSV download still works.")
            else:
                confirm_export = st.checkbox("Confirm export/update these rows in Cash Flow Cloud", key="confirm_direct_cashflow_export")
                if st.button("Export / Update Cash Flow Cloud", type="primary", key="direct_cashflow_export_button"):
                    if not confirm_export:
                        st.warning("Check the confirmation box before exporting to Cash Flow Cloud.")
                    else:
                        try:
                            inserted_count = upsert_cash_flow_rows(export_rows)
                            update_cashflow_export_status(source_updates)
                            st.success(f"Cash Flow Cloud updated with {inserted_count} forecast row(s). Existing exported rows for the same RMR/Work Group were replaced.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Cash Flow export failed: {e}")
        st.markdown("---")
        st.markdown("### Budget Summaries")
        active_budget_df = budget_df[budget_df["budget_status"].astype(str) == "Active"].copy()
        st.caption("Summaries below use Active budget items only. Deferred and Cancelled items stay in the database but are excluded from these totals.")
        summary_tabs = st.tabs(["By Property", "By Timeframe", "By Project", "By Work Group", "By Work Item", "By Contractor"])
        with summary_tabs[0]:
            st.dataframe(budget_summary_table(active_budget_df, ["property_name"]).rename(columns={"property_name": "Property"}), use_container_width=True, hide_index=True)
        with summary_tabs[1]:
            st.dataframe(budget_summary_table(active_budget_df, ["budget_timeframe"]).rename(columns={"budget_timeframe": "Budget Timeframe"}), use_container_width=True, hide_index=True)
        with summary_tabs[2]:
            st.dataframe(budget_summary_table(active_budget_df, ["project_name"]).rename(columns={"project_name": "Project"}), use_container_width=True, hide_index=True)
        with summary_tabs[3]:
            st.dataframe(budget_summary_table(active_budget_df, ["linked_work_group_name"]).rename(columns={"linked_work_group_name": "Work Group"}), use_container_width=True, hide_index=True)
        with summary_tabs[4]:
            st.dataframe(budget_summary_table(active_budget_df, ["work_item_name"]).rename(columns={"work_item_name": "Work Item"}), use_container_width=True, hide_index=True)
        with summary_tabs[5]:
            st.dataframe(budget_summary_table(active_budget_df, ["contractor_name"]).rename(columns={"contractor_name": "Contractor"}), use_container_width=True, hide_index=True)


elif page == "Project Ideas":
    if current_role == "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    st.subheader("Project Ideas")
    st.caption('Separate idea box for possible renovation work before it becomes an active RMR, estimate, work group, or project.')

    tab1, tab2, tab3 = st.tabs(["Create Project Idea", "Search / Review Ideas", "Draft Cash Flow Forecast"])

    with tab1:
        st.markdown("### Create Project Idea")
        st.caption("Enter as much information as you know now. Most fields can be edited later.")

        project_labels = ["Not linked to existing project"] + project_registry_select_labels(active_only=False)
        linked_project_label = st.selectbox("Link Existing Project (optional)", project_labels, key="pipeline_create_project_link")
        linked_project_row = None
        linked_project_id = None
        if linked_project_label != "Not linked to existing project":
            linked_project_row = get_project_registry_row_from_label(linked_project_label)
            if linked_project_row is not None:
                linked_project_id = int(linked_project_row["id"])

        default_project_name = str(linked_project_row.get("project_name") or "") if linked_project_row is not None else ""
        default_project_address = str(linked_project_row.get("project_address") or "") if linked_project_row is not None else ""
        pipeline_project_name = st.text_input("Possible Project / Property Name", value=default_project_name, key="pipeline_create_project_name")

        st.markdown("#### Project / Property Address")
        ap1, ap2 = st.columns(2)
        pipeline_selected_property = ap1.selectbox(
            "Property (optional)",
            master_property_labels(),
            key="pipeline_create_property_choice",
            help="Optional. Choose a property to narrow the address list.",
        )
        pipeline_address_options = master_address_labels(pipeline_selected_property)
        pipeline_selected_address = ap2.selectbox(
            "Choose Address",
            ["Type New Address"] + pipeline_address_options,
            key="pipeline_create_address_choice",
        )
        pipeline_selected_address_row = portfolio_address_row_from_label(pipeline_selected_address) if pipeline_selected_address != "Type New Address" else None
        if pipeline_selected_address_row is not None:
            pipeline_project_address = str(pipeline_selected_address_row.get("address") or "")
            pipeline_project_unit_number = str(pipeline_selected_address_row.get("unit_number") or "")
            ad1, ad2 = st.columns(2)
            ad1.text_input("Selected Address", value=pipeline_project_address, disabled=True, key="pipeline_create_selected_address_display")
            pipeline_project_unit_number = ad2.text_input("Unit Number (optional)", value=pipeline_project_unit_number, key="pipeline_create_unit_optional")
        else:
            ad1, ad2 = st.columns(2)
            pipeline_project_address = ad1.text_input("Type New Address", value=default_project_address, key="pipeline_create_new_address")
            pipeline_project_unit_number = ad2.text_input("Unit Number (optional)", key="pipeline_create_new_unit_optional")
        if str(pipeline_project_unit_number or "").strip():
            pipeline_project_address = f"{pipeline_project_address} Unit {pipeline_project_unit_number}".strip()

        c1, c2 = st.columns(2)
        category_options = [""] + get_category_names()
        pipeline_category = c1.selectbox('Category of Labor', category_options, key="pipeline_create_category")
        pipeline_work_group_name = c2.text_input('Possible Work Group Name', key="pipeline_create_work_group_name")

        w1, w2 = st.columns(2)
        pipeline_work_item_name = w1.text_input("Possible Work Item / Repair Idea", key="pipeline_create_work_item_name")
        pipeline_target_timing = w2.text_input("Target Timing", placeholder="Example: Summer 2026, next vacancy, Q3, ASAP", key="pipeline_create_target_timing")

        b1, b2, b3 = st.columns(3)
        pipeline_priority = b1.selectbox("Priority", PIPELINE_PRIORITY_OPTIONS, index=1, key="pipeline_create_priority")
        pipeline_status = b2.selectbox("Status", PIPELINE_STATUS_OPTIONS, index=0, key="pipeline_create_status")
        pipeline_rough_budget = b3.number_input("Rough Budget Estimate", min_value=0.0, value=0.0, step=100.0, key="pipeline_create_rough_budget")

        l1, l2 = st.columns(2)
        pipeline_rough_labor_hours = l1.number_input("Rough Labor Hours", min_value=0.0, value=0.0, step=1.0, key="pipeline_create_labor_hours")
        pipeline_rough_duration = l2.text_input("Rough Duration", placeholder="Example: 3 days, 2 weeks, 4 weekends", key="pipeline_create_duration")

        pipeline_scope = st.text_area("Rough Scope / Description", height=120, key="pipeline_create_scope")
        pipeline_notes = st.text_area("Notes", height=100, key="pipeline_create_notes")
        pipeline_cash_flow_notes = st.text_area(
            "Rough Cash Flow / Payment Timing Notes",
            height=90,
            placeholder="Example: $300/week for 3 weeks, 50% deposit and 50% completion, materials first week.",
            key="pipeline_create_cash_flow_notes",
        )

        st.markdown("#### Draft Cash Flow Schedule")
        st.caption("Optional rough schedule for brainstorming future cash outflows if this work moves forward.")
        cf1, cf2, cf3 = st.columns(3)
        pipeline_cash_flow_start = cf1.date_input("First Payment Date", value=datetime.now().date(), key="pipeline_create_cash_flow_start")
        pipeline_cash_flow_pattern = cf2.selectbox(
            "Payment Pattern",
            ["No Structured Schedule", "One Payment", "Weekly Payments", "Every 2 Weeks", "Deposit / Completion"],
            key="pipeline_create_cash_flow_pattern",
        )
        pipeline_cash_flow_count = cf3.number_input(
            "Number of Payments",
            min_value=1,
            value=3,
            step=1,
            disabled=(pipeline_cash_flow_pattern not in ["Weekly Payments", "Every 2 Weeks"]),
            key="pipeline_create_cash_flow_count",
        )

        uploaded_pipeline_files = st.file_uploader(
            "Upload Photos or Documents",
            type=["png", "jpg", "jpeg", "webp", "pdf", "xlsx", "xls", "csv", "docx", "txt"],
            accept_multiple_files=True,
            key="pipeline_create_files",
        )

        if st.button("Save Project Idea", type="primary", key="save_pipeline_item_btn"):
            if not str(pipeline_project_name).strip() and not str(pipeline_work_item_name).strip() and not str(pipeline_scope).strip():
                st.error("Enter at least a project/property name, work item idea, or rough scope.")
            else:
                new_pipeline_id = execute_returning_id(
                    """
                    INSERT INTO renovation_pipeline_items (
                        project_id, project_name, project_address, category_name, work_group_name,
                        work_item_name, priority, status, target_timing, rough_budget, rough_labor_hours,
                        rough_duration, cash_flow_notes, scope_description, notes, created_by,
                        created_at, modified_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                    """,
                    (
                        linked_project_id,
                        str(pipeline_project_name).strip(),
                        str(pipeline_project_address).strip(),
                        str(pipeline_category).strip(),
                        str(pipeline_work_group_name).strip(),
                        str(pipeline_work_item_name).strip(),
                        str(pipeline_priority).strip(),
                        str(pipeline_status).strip(),
                        str(pipeline_target_timing).strip(),
                        float(pipeline_rough_budget or 0),
                        float(pipeline_rough_labor_hours or 0),
                        str(pipeline_rough_duration).strip(),
                        str(pipeline_cash_flow_notes).strip(),
                        str(pipeline_scope).strip(),
                        str(pipeline_notes).strip(),
                        str(st.session_state.get("logged_in_user", "") or ""),
                    ),
                )
                if new_pipeline_id:
                    save_renovation_pipeline_files(
                        int(new_pipeline_id),
                        uploaded_pipeline_files,
                        uploaded_by=str(st.session_state.get("logged_in_user", "") or ""),
                    )
                    if pipeline_cash_flow_pattern != "No Structured Schedule":
                        draft_cash_flow_rows = build_cash_flow_rows_from_pattern(
                            pipeline_cash_flow_start,
                            float(pipeline_rough_budget or 0),
                            pipeline_cash_flow_pattern,
                            int(pipeline_cash_flow_count or 1),
                            str(pipeline_cash_flow_notes or "").strip(),
                        )
                        save_pipeline_cash_flow_rows(int(new_pipeline_id), draft_cash_flow_rows)
                st.success("Project idea saved.")
                st.rerun()

    with tab2:
        st.markdown("### Search / Review Ideas")
        pipeline_df = renovation_pipeline_items_df(include_archived=True, include_deleted=False)

        if pipeline_df.empty:
            st.info("No project ideas have been created yet.")
        else:
            f1, f2, f3, f4 = st.columns(4)
            search_text = f1.text_input("Search Ideas", key="pipeline_search_text")
            show_filter = f2.selectbox(
                "Show",
                ["Active Ideas Only", "Converted To RMR", "Archived", "All"],
                key="project_ideas_show_filter",
                help="Converted and archived ideas are saved but hidden from the normal active idea view.",
            )
            status_filter = f3.selectbox("Status Filter", ["All"] + PIPELINE_STATUS_OPTIONS, key="pipeline_status_filter")
            category_values = sorted([v for v in pipeline_df["category_name"].dropna().astype(str).unique().tolist() if v.strip()])
            category_filter = f4.selectbox('Category of Labor Filter', ["All"] + category_values, key="pipeline_category_filter")

            filtered_df = pipeline_df.copy()
            if show_filter == "Active Ideas Only":
                filtered_df = filtered_df[
                    filtered_df["status"].astype(str).isin(ACTIVE_PROJECT_IDEA_STATUSES)
                    & (~filtered_df["archived"].astype(bool))
                ].copy()
            elif show_filter == "Converted To RMR":
                filtered_df = filtered_df[filtered_df["status"].astype(str) == "Converted To RMR"].copy()
            elif show_filter == "Archived":
                filtered_df = filtered_df[filtered_df["archived"].astype(bool) | (filtered_df["status"].astype(str) == "Archived")].copy()
            if status_filter != "All":
                filtered_df = filtered_df[filtered_df["status"].astype(str) == status_filter].copy()
            if category_filter != "All":
                filtered_df = filtered_df[filtered_df["category_name"].astype(str) == category_filter].copy()
            if search_text.strip():
                search_lower = search_text.strip().lower()
                search_cols = [
                    "project_name", "project_address", "category_name", "work_group_name",
                    "work_item_name", "priority", "status", "target_timing", "rough_duration",
                    "cash_flow_notes", "scope_description", "notes"
                ]
                mask = False
                for col in search_cols:
                    mask = mask | filtered_df[col].astype(str).str.lower().str.contains(search_lower, na=False)
                filtered_df = filtered_df[mask].copy()

            display_cols = [
                "id", "priority", "status", "project_name", "category_name", "work_group_name",
                "work_item_name", "rough_budget", "rough_labor_hours", "target_timing", "modified_at"
            ]
            st.dataframe(
                filtered_df[display_cols].rename(columns={
                    "id": "Idea ID",
                    "priority": "Priority",
                    "status": "Status",
                    "project_name": "Project / Property",
                    "category_name": 'Category of Labor',
                    "work_group_name": 'Work Group Name',
                    "work_item_name": "Work Item / Idea",
                    "rough_budget": "Rough Budget",
                    "rough_labor_hours": "Rough Labor Hours",
                    "target_timing": "Target Timing",
                    "modified_at": "Last Updated",
                }),
                use_container_width=True,
                hide_index=True,
            )

            item_labels = [
                f"{int(row.id)} | {row.project_name or '(no project)'} | {row.work_group_name or '(no work group)'} | {row.work_item_name or '(no work item)'}"
                for row in filtered_df.itertuples()
            ]

            if item_labels:
                selected_pipeline_label = st.selectbox("Choose Project Idea To Review / Edit", item_labels, key="pipeline_review_select")
                selected_pipeline_id = int(selected_pipeline_label.split(" | ", 1)[0])
                selected_row = filtered_df[filtered_df["id"] == selected_pipeline_id].iloc[0]

                st.markdown("### Edit Project Idea")
                edit_project_name = st.text_input("Possible Project / Property Name", value=str(selected_row.get("project_name") or ""), key=f"pipeline_edit_project_name_{selected_pipeline_id}")

                st.markdown("#### Project / Property Address")
                eap1, eap2 = st.columns(2)
                edit_selected_property = eap1.selectbox(
                    "Property (optional)",
                    master_property_labels(),
                    key=f"pipeline_edit_property_choice_{selected_pipeline_id}",
                    help="Optional. Choose a property to narrow the address list.",
                )
                edit_address_options = master_address_labels(edit_selected_property)
                edit_selected_address = eap2.selectbox(
                    "Choose Address",
                    ["Keep Current Address"] + edit_address_options + ["Type New Address"],
                    key=f"pipeline_edit_address_choice_{selected_pipeline_id}",
                )
                if edit_selected_address == "Keep Current Address":
                    edit_project_address = str(selected_row.get("project_address") or "")
                    ead1, ead2 = st.columns(2)
                    ead1.text_input("Current Address", value=edit_project_address, disabled=True, key=f"pipeline_edit_current_address_{selected_pipeline_id}")
                    edit_project_unit_number = ead2.text_input("Unit Number (optional)", key=f"pipeline_edit_current_unit_optional_{selected_pipeline_id}")
                elif edit_selected_address == "Type New Address":
                    ead1, ead2 = st.columns(2)
                    edit_project_address = ead1.text_input("Type New Address", value=str(selected_row.get("project_address") or ""), key=f"pipeline_edit_new_address_{selected_pipeline_id}")
                    edit_project_unit_number = ead2.text_input("Unit Number (optional)", key=f"pipeline_edit_new_unit_optional_{selected_pipeline_id}")
                else:
                    edit_address_row = portfolio_address_row_from_label(edit_selected_address)
                    edit_project_address = str(edit_address_row.get("address") or "") if edit_address_row is not None else str(selected_row.get("project_address") or "")
                    edit_project_unit_number = str(edit_address_row.get("unit_number") or "") if edit_address_row is not None else ""
                    ead1, ead2 = st.columns(2)
                    ead1.text_input("Selected Address", value=edit_project_address, disabled=True, key=f"pipeline_edit_selected_address_{selected_pipeline_id}")
                    edit_project_unit_number = ead2.text_input("Unit Number (optional)", value=edit_project_unit_number, key=f"pipeline_edit_unit_optional_{selected_pipeline_id}")
                if str(edit_project_unit_number or "").strip():
                    edit_project_address = f"{edit_project_address} Unit {edit_project_unit_number}".strip()

                e3, e4 = st.columns(2)
                edit_category_options = [""] + get_category_names()
                current_category = str(selected_row.get("category_name") or "")
                edit_category = e3.selectbox(
                    'Category of Labor',
                    edit_category_options,
                    index=edit_category_options.index(current_category) if current_category in edit_category_options else 0,
                    key=f"pipeline_edit_category_{selected_pipeline_id}",
                )
                edit_work_group_name = e4.text_input('Possible Work Group Name', value=str(selected_row.get("work_group_name") or ""), key=f"pipeline_edit_work_group_name_{selected_pipeline_id}")

                e5, e6 = st.columns(2)
                edit_work_item_name = e5.text_input("Possible Work Item / Repair Idea", value=str(selected_row.get("work_item_name") or ""), key=f"pipeline_edit_work_item_name_{selected_pipeline_id}")
                edit_target_timing = e6.text_input("Target Timing", value=str(selected_row.get("target_timing") or ""), key=f"pipeline_edit_target_timing_{selected_pipeline_id}")

                e7, e8, e9 = st.columns(3)
                current_priority = str(selected_row.get("priority") or "Medium")
                edit_priority = e7.selectbox(
                    "Priority",
                    PIPELINE_PRIORITY_OPTIONS,
                    index=PIPELINE_PRIORITY_OPTIONS.index(current_priority) if current_priority in PIPELINE_PRIORITY_OPTIONS else 1,
                    key=f"pipeline_edit_priority_{selected_pipeline_id}",
                )
                current_status = str(selected_row.get("status") or "Idea")
                edit_status = e8.selectbox(
                    "Status",
                    PIPELINE_STATUS_OPTIONS,
                    index=PIPELINE_STATUS_OPTIONS.index(current_status) if current_status in PIPELINE_STATUS_OPTIONS else 0,
                    key=f"pipeline_edit_status_{selected_pipeline_id}",
                )
                edit_rough_budget = e9.number_input(
                    "Rough Budget Estimate",
                    min_value=0.0,
                    value=float(selected_row.get("rough_budget") or 0),
                    step=100.0,
                    key=f"pipeline_edit_rough_budget_{selected_pipeline_id}",
                )

                e10, e11 = st.columns(2)
                edit_rough_labor_hours = e10.number_input(
                    "Rough Labor Hours",
                    min_value=0.0,
                    value=float(selected_row.get("rough_labor_hours") or 0),
                    step=1.0,
                    key=f"pipeline_edit_labor_hours_{selected_pipeline_id}",
                )
                edit_rough_duration = e11.text_input("Rough Duration", value=str(selected_row.get("rough_duration") or ""), key=f"pipeline_edit_duration_{selected_pipeline_id}")

                edit_scope = st.text_area("Rough Scope / Description", value=str(selected_row.get("scope_description") or ""), height=120, key=f"pipeline_edit_scope_{selected_pipeline_id}")
                edit_notes = st.text_area("Notes", value=str(selected_row.get("notes") or ""), height=100, key=f"pipeline_edit_notes_{selected_pipeline_id}")
                edit_cash_flow_notes = st.text_area(
                    "Rough Cash Flow / Payment Timing Notes",
                    value=str(selected_row.get("cash_flow_notes") or ""),
                    height=90,
                    key=f"pipeline_edit_cash_flow_notes_{selected_pipeline_id}",
                )

                b1, b2, b3 = st.columns(3)
                if b1.button("Save Project Idea Changes", type="primary", key=f"save_pipeline_changes_{selected_pipeline_id}"):
                    execute(
                        """
                        UPDATE renovation_pipeline_items
                        SET project_name = ?, project_address = ?, category_name = ?, work_group_name = ?,
                            work_item_name = ?, priority = ?, status = ?, target_timing = ?, rough_budget = ?,
                            rough_labor_hours = ?, rough_duration = ?, cash_flow_notes = ?, scope_description = ?,
                            notes = ?, modified_at = NOW()
                        WHERE id = ?
                        """,
                        (
                            str(edit_project_name).strip(),
                            str(edit_project_address).strip(),
                            str(edit_category).strip(),
                            str(edit_work_group_name).strip(),
                            str(edit_work_item_name).strip(),
                            str(edit_priority).strip(),
                            str(edit_status).strip(),
                            str(edit_target_timing).strip(),
                            float(edit_rough_budget or 0),
                            float(edit_rough_labor_hours or 0),
                            str(edit_rough_duration).strip(),
                            str(edit_cash_flow_notes).strip(),
                            str(edit_scope).strip(),
                            str(edit_notes).strip(),
                            selected_pipeline_id,
                        ),
                    )
                    st.success("Project idea updated.")
                    st.rerun()

                if b2.button("Archive / Unarchive", key=f"archive_pipeline_{selected_pipeline_id}"):
                    archive_renovation_pipeline_item(selected_pipeline_id, archived=not bool(selected_row.get("archived")))
                    st.success("Project idea archive status updated.")
                    st.rerun()

                if b3.button("Delete Project Idea", key=f"delete_pipeline_{selected_pipeline_id}"):
                    st.session_state[f"confirm_delete_pipeline_{selected_pipeline_id}"] = True
                    st.rerun()

                if st.session_state.get(f"confirm_delete_pipeline_{selected_pipeline_id}", False):
                    st.warning("Delete this Project idea? This removes it from active review.")
                    d1, d2 = st.columns(2)
                    if d1.button("Yes, Delete Project Idea", type="primary", key=f"confirm_delete_pipeline_yes_{selected_pipeline_id}"):
                        delete_renovation_pipeline_item(selected_pipeline_id)
                        st.session_state[f"confirm_delete_pipeline_{selected_pipeline_id}"] = False
                        st.success("Project idea deleted.")
                        st.rerun()
                    if d2.button("Cancel Delete", key=f"confirm_delete_pipeline_cancel_{selected_pipeline_id}"):
                        st.session_state[f"confirm_delete_pipeline_{selected_pipeline_id}"] = False
                        st.rerun()

                st.markdown("### Draft Cash Flow Schedule")
                existing_cash_flow_df = renovation_pipeline_cash_flows_df(selected_pipeline_id)
                if existing_cash_flow_df.empty:
                    st.info("No structured draft cash flow rows have been created for this Project idea yet.")
                else:
                    st.dataframe(
                        existing_cash_flow_df[["scheduled_date", "amount", "payment_type", "status", "notes"]].rename(columns={
                            "scheduled_date": "Scheduled Date",
                            "amount": "Amount",
                            "payment_type": "Payment Type",
                            "status": "Status",
                            "notes": "Notes",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )
                    delete_cash_flow_labels = [
                        f"{int(row.id)} | {pd.to_datetime(row.scheduled_date).strftime('%m-%d-%Y') if pd.notna(pd.to_datetime(row.scheduled_date, errors='coerce')) else ''} | ${float(row.amount or 0):,.2f}"
                        for row in existing_cash_flow_df.itertuples()
                    ]
                    if delete_cash_flow_labels:
                        selected_cash_flow_delete = st.selectbox(
                            "Choose Draft Cash Flow Row To Delete",
                            delete_cash_flow_labels,
                            key=f"pipeline_cash_flow_delete_select_{selected_pipeline_id}",
                        )
                        selected_cash_flow_id = int(selected_cash_flow_delete.split(" | ", 1)[0])
                        if st.button("Delete Selected Draft Cash Flow Row", key=f"pipeline_cash_flow_delete_btn_{selected_pipeline_id}_{selected_cash_flow_id}"):
                            delete_pipeline_cash_flow_row(selected_cash_flow_id)
                            st.success("Draft cash flow row deleted.")
                            st.rerun()

                st.markdown("#### Add Draft Cash Flow Rows")
                ac1, ac2, ac3 = st.columns(3)
                add_cash_flow_start = ac1.date_input("First Payment Date", value=datetime.now().date(), key=f"pipeline_add_cash_flow_start_{selected_pipeline_id}")
                add_cash_flow_pattern = ac2.selectbox(
                    "Payment Pattern",
                    ["One Payment", "Weekly Payments", "Every 2 Weeks", "Deposit / Completion"],
                    key=f"pipeline_add_cash_flow_pattern_{selected_pipeline_id}",
                )
                add_cash_flow_count = ac3.number_input(
                    "Number of Payments",
                    min_value=1,
                    value=3,
                    step=1,
                    disabled=(add_cash_flow_pattern not in ["Weekly Payments", "Every 2 Weeks"]),
                    key=f"pipeline_add_cash_flow_count_{selected_pipeline_id}",
                )
                add_cash_flow_amount = st.number_input(
                    "Total Amount To Schedule",
                    min_value=0.0,
                    value=float(selected_row.get("rough_budget") or 0),
                    step=100.0,
                    key=f"pipeline_add_cash_flow_amount_{selected_pipeline_id}",
                )
                add_cash_flow_notes = st.text_input(
                    "Draft Cash Flow Row Notes",
                    value=str(selected_row.get("cash_flow_notes") or ""),
                    key=f"pipeline_add_cash_flow_notes_{selected_pipeline_id}",
                )
                if st.button("Add Draft Cash Flow Schedule", key=f"pipeline_add_cash_flow_btn_{selected_pipeline_id}"):
                    rows_to_add = build_cash_flow_rows_from_pattern(
                        add_cash_flow_start,
                        float(add_cash_flow_amount or 0),
                        add_cash_flow_pattern,
                        int(add_cash_flow_count or 1),
                        str(add_cash_flow_notes or "").strip(),
                    )
                    save_pipeline_cash_flow_rows(selected_pipeline_id, rows_to_add)
                    st.success("Draft cash flow schedule added.")
                    st.rerun()

                st.markdown("### Files / Photos / Documents")
                item_files_df = renovation_pipeline_files_df(selected_pipeline_id)
                if item_files_df.empty:
                    st.info("No files uploaded for this Project Idea yet.")
                else:
                    st.caption(f"{len(item_files_df)} file(s) saved.")
                    file_labels = []
                    for file_row in item_files_df.itertuples():
                        created_display = pd.to_datetime(file_row.created_at, errors="coerce")
                        created_text = created_display.strftime("%m-%d-%Y") if pd.notna(created_display) else ""
                        file_labels.append(f"{int(file_row.id)} | {file_row.file_filename or 'file'} | {created_text}")

                    for _, file_row in item_files_df.iterrows():
                        file_name = str(file_row.get("file_filename") or "file")
                        content_type = str(file_row.get("content_type") or "")
                        file_bytes = pipeline_file_bytes(file_row)
                        if content_type.startswith("image/") and st.checkbox(f"Preview {file_name}", key=f"pipeline_file_preview_{selected_pipeline_id}_{int(file_row['id'])}", value=False):
                            try:
                                if file_bytes:
                                    st.image(file_bytes, caption=file_name, use_container_width=True)
                                elif str(file_row.get("blob_url") or ""):
                                    st.image(str(file_row.get("blob_url")), caption=file_name, use_container_width=True)
                            except Exception:
                                st.warning(f"Could not preview {file_name}.")
                        if file_bytes:
                            st.download_button(
                                f"Download {file_name}",
                                data=file_bytes,
                                file_name=file_name,
                                mime=content_type or "application/octet-stream",
                                key=f"download_pipeline_file_{selected_pipeline_id}_{int(file_row['id'])}",
                            )

                    selected_file_label = st.selectbox("Choose File To Delete", file_labels, key=f"pipeline_delete_file_select_{selected_pipeline_id}")
                    selected_file_id = int(selected_file_label.split(" | ", 1)[0])
                    if st.button("Delete Selected File", key=f"pipeline_delete_file_btn_{selected_pipeline_id}_{selected_file_id}"):
                        delete_renovation_pipeline_file(selected_file_id)
                        st.success("File deleted.")
                        st.rerun()

                new_files = st.file_uploader(
                    "Add More Photos or Documents",
                    type=["png", "jpg", "jpeg", "webp", "pdf", "xlsx", "xls", "csv", "docx", "txt"],
                    accept_multiple_files=True,
                    key=f"pipeline_add_files_{selected_pipeline_id}",
                )
                if st.button("Save Added Files", key=f"pipeline_save_added_files_{selected_pipeline_id}"):
                    save_renovation_pipeline_files(
                        selected_pipeline_id,
                        new_files,
                        uploaded_by=str(st.session_state.get("logged_in_user", "") or ""),
                    )
                    st.success("Files added.")
                    st.rerun()

                st.markdown("### Move Project Idea Forward")
                st.caption("Use Convert To RMR when this idea becomes real work you want to track. Converted ideas are saved but hidden from the default active idea view.")
                p1, p2, p3, p4 = st.columns(4)
                if p1.button("Convert To RMR", type="primary", key=f"project_idea_convert_rmr_{selected_pipeline_id}"):
                    rmr_id = project_idea_to_rmr(selected_pipeline_id)
                    if rmr_id:
                        st.success(f"Project Idea converted to RMR-{int(rmr_id):06d}.")
                        st.rerun()
                    else:
                        st.error("Could not convert this Project Idea to an RMR. Enter at least a project/property name or work item.")

                if p2.button("Promote To Project", key=f"pipeline_promote_project_{selected_pipeline_id}"):
                    new_project_id = find_or_create_project_from_pipeline(selected_row.to_dict(), active=True)
                    if new_project_id:
                        execute(
                            """
                            UPDATE renovation_pipeline_items
                            SET promoted_project_id = ?, status = 'Converted To Project', modified_at = NOW()
                            WHERE id = ?
                            """,
                            (int(new_project_id), selected_pipeline_id),
                        )
                        st.success("Project idea promoted to active Project.")
                        st.rerun()
                    else:
                        st.error("Enter a project/property name before promoting to Project.")

                if p3.button("Promote To Estimate", key=f"pipeline_promote_estimate_{selected_pipeline_id}"):
                    estimate_id = pipeline_item_to_estimate(selected_pipeline_id)
                    if estimate_id:
                        st.success(f"Project idea copied to Estimate Est{estimate_id}.")
                        st.rerun()
                    else:
                        st.error("Could not promote this item to an Estimate. Check that it has at least a project/property name.")

                if p4.button('Promote To Work Group', key=f"pipeline_promote_work_group_{selected_pipeline_id}"):
                    work_group_id = pipeline_item_to_work_group(selected_pipeline_id)
                    if work_group_id:
                        st.success(f"Project idea copied to Work Group WG{work_group_id}.")
                        st.rerun()
                    else:
                        st.error('Could not promote this item to a Work Group. Check that it has at least a project/property name.')




    with tab3:
        st.markdown("### Draft Cash Flow Forecast")
        st.caption("This report uses rough draft Project Ideas cash flow rows for brainstorming possible future cash needs.")

        forecast_df = renovation_pipeline_cash_flows_df()
        if forecast_df.empty:
            st.info("No draft cash flow rows have been created yet. Add them when creating or editing Project Ideas.")
        else:
            fc1, fc2, fc3 = st.columns(3)
            forecast_start = fc1.date_input("Forecast Start Date", value=datetime.now().date(), key="pipeline_forecast_start")
            forecast_end = fc2.date_input("Forecast End Date", value=datetime.now().date() + __import__("datetime").timedelta(days=180), key="pipeline_forecast_end")
            forecast_group = fc3.selectbox("Group Forecast By", ["Week", "Month"], key="pipeline_forecast_group")

            forecast_df = forecast_df.copy()
            forecast_df["scheduled_date"] = pd.to_datetime(forecast_df["scheduled_date"], errors="coerce")
            forecast_df = forecast_df[
                (forecast_df["scheduled_date"].dt.date >= forecast_start)
                & (forecast_df["scheduled_date"].dt.date <= forecast_end)
            ].copy()

            if forecast_df.empty:
                st.info("No draft cash flow rows match this date range.")
            else:
                if forecast_group == "Week":
                    forecast_df["Period"] = forecast_df["scheduled_date"].dt.to_period("W").apply(lambda p: f"{p.start_time.strftime('%m-%d-%Y')} to {p.end_time.strftime('%m-%d-%Y')}")
                else:
                    forecast_df["Period"] = forecast_df["scheduled_date"].dt.to_period("M").astype(str)

                summary_df = (
                    forecast_df.groupby("Period", dropna=False)["amount"]
                    .sum()
                    .reset_index()
                    .rename(columns={"amount": "Draft Cash Outflow"})
                )

                st.markdown("#### Forecast Summary")
                st.dataframe(summary_df, use_container_width=True, hide_index=True)

                st.markdown("#### Forecast Detail")
                detail_cols = [
                    "scheduled_date", "amount", "project_name", "category_name", "work_group_name",
                    "work_item_name", "payment_type", "status", "priority", "pipeline_status", "notes"
                ]
                st.dataframe(
                    forecast_df[detail_cols].rename(columns={
                        "scheduled_date": "Scheduled Date",
                        "amount": "Amount",
                        "project_name": "Project / Property",
                        "category_name": 'Category of Labor',
                        "work_group_name": 'Work Group Name',
                        "work_item_name": "Work Item / Idea",
                        "payment_type": "Payment Type",
                        "status": "Cash Flow Status",
                        "priority": "Priority",
                        "pipeline_status": "Project Idea Status",
                        "notes": "Notes",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

                csv_bytes = forecast_df[detail_cols].to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download Draft Cash Flow Forecast CSV",
                    data=csv_bytes,
                    file_name=f"renovation_pipeline_draft_cash_flow_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key="download_pipeline_cash_flow_forecast_csv",
                )


# -----------------------------
# Projects
# -----------------------------
elif page == "Projects":
    if current_role == "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    st.subheader("Projects")
    st.caption("Master project list. Create, search, edit, and activate/inactivate projects here. Other pages select only from this list.")

    tab1, tab2 = st.tabs(["Create Project", "Manage Projects"])

    with tab1:
        with st.form("project_registry_create_form"):
            st.markdown("#### Optional Portfolio Address Lookup")
            pa1, pa2 = st.columns(2)
            selected_project_portfolio = pa1.selectbox(
                "Portfolio Address Source",
                ["None"] + PORTFOLIO_NAMES,
                key="project_create_portfolio_source",
            )
            address_lookup_labels = ["Type manually"]
            if selected_project_portfolio != "None":
                address_lookup_labels += portfolio_address_labels(selected_project_portfolio)
            selected_project_address_label = pa2.selectbox(
                "Address Choice",
                address_lookup_labels,
                key="project_create_address_choice",
            )
            selected_project_address_row = portfolio_address_row_from_label(selected_project_address_label) if selected_project_address_label != "Type manually" else None

            c1, c2 = st.columns(2)
            project_name_default = str(selected_project_address_row.get("property_name") or "") if selected_project_address_row is not None else ""
            project_address_default = str(selected_project_address_row.get("address") or "") if selected_project_address_row is not None else ""
            project_unit_default = str(selected_project_address_row.get("unit_number") or "") if selected_project_address_row is not None else ""
            if project_unit_default:
                project_address_default = f"{project_address_default} Unit {project_unit_default}"
            project_name = c1.text_input("Project Name", value=project_name_default)
            project_address = c2.text_input("Project Address", value=project_address_default)
            c3, c4 = st.columns(2)
            active_choice_new = c3.selectbox("Project Status", ["Inactive", "Active"], index=0)
            activated_at_text = c4.text_input("Date Activated (MM/DD/YYYY if active)", value=datetime.now().strftime("%m/%d/%Y"), disabled=(active_choice_new != "Active"))
            project_notes = st.text_area("Project Description / Notes", height=100)
            create_project = st.form_submit_button("Save Project", type="primary")
            if create_project:
                if not str(project_name).strip():
                    st.error("Enter a project or repair name.")
                else:
                    exists_df = fetch_df(
                        """
                        SELECT id
                        FROM project_registry
                        WHERE LOWER(COALESCE(project_name, '')) = LOWER(?)
                        LIMIT 1
                        """,
                        (str(project_name).strip(),),
                    )
                    if not exists_df.empty:
                        st.error("That project name already exists. Project names must be unique.")
                    else:
                        is_active = (active_choice_new == "Active")
                        new_project_id = execute_returning_id(
                            """
                            INSERT INTO project_registry (
                                project_name, project_address, active, deleted, activated_at, created_at, modified_at, notes
                            ) VALUES (?, ?, ?, FALSE, CASE WHEN ? THEN NOW() ELSE NULL END, NOW(), NOW(), ?)
                            """,
                            (
                                str(project_name).strip(),
                                str(project_address).strip(),
                                is_active,
                                is_active,
                                str(project_notes).strip(),
                            ),
                        )
                        execute(
                            "UPDATE project_registry SET project_code = 'PRJ-' || LPAD(id::text, 6, '0') WHERE id = ?",
                            (int(new_project_id),),
                        )
                        st.cache_data.clear()
                        st.success("Project saved.")
                        st.rerun()

    with tab2:
        all_projects_df = project_registry_all_df()
        if all_projects_df.empty:
            st.info("No projects have been saved yet.")
        else:
            filter_col, search_col = st.columns([1, 2])
            filter_options = ["Active", "Inactive", "All"]
            if current_role == "Owner":
                filter_options.append("Deleted")
            status_filter = filter_col.selectbox("Project List", filter_options, key="projects_status_filter")
            search_term = search_col.text_input("Search by project name", key="projects_search_term", placeholder="Search projects...").strip().lower()

            filtered_df = all_projects_df.copy()
            if "deleted" not in filtered_df.columns:
                filtered_df["deleted"] = False
            if "active" not in filtered_df.columns:
                filtered_df["active"] = True
            if status_filter == "Deleted":
                filtered_df = filtered_df[filtered_df["deleted"].fillna(False) == True]
            else:
                filtered_df = filtered_df[filtered_df["deleted"].fillna(False) == False]
            if status_filter == "Active":
                filtered_df = filtered_df[filtered_df["active"].fillna(True) == True]
            elif status_filter == "Inactive":
                filtered_df = filtered_df[filtered_df["active"].fillna(True) == False]

            if search_term:
                filtered_df = filtered_df[
                    filtered_df["project_name"].fillna("").astype(str).str.lower().str.contains(search_term, na=False)
                ]

            display_df = filtered_df.copy()
            if "notes" not in display_df.columns:
                display_df["notes"] = ""
            if "active" not in display_df.columns:
                display_df["active"] = True
            for col in ["activated_at", "created_at", "modified_at"]:
                if col not in display_df.columns:
                    display_df[col] = ""
                else:
                    display_df[col] = pd.to_datetime(display_df[col], errors="coerce").dt.strftime("%m-%d-%Y %H:%M")
            display_df["status"] = display_df.apply(
                lambda r: "Deleted" if bool(r.get("deleted", False)) else ("Active" if bool(r.get("active", True)) else "Inactive"),
                axis=1,
            )

            st.dataframe(
                display_df[["id", "project_name", "project_address", "notes", "status", "activated_at", "created_at", "modified_at"]].rename(columns={
                    "id": "Project ID",
                    "project_name": "Project Name",
                    "project_address": "Address",
                    "notes": "Project Description",
                    "status": "Status",
                    "activated_at": "Date Activated",
                    "created_at": "Created",
                    "modified_at": "Modified",
                }),
                use_container_width=True,
            )

            project_labels = [f"{int(row.id)} | {row.project_name}" for row in filtered_df.itertuples()]
            if project_labels:
                selected_project_label = st.selectbox("Choose Project To Edit", project_labels, key="projects_edit_select")
                selected_project_row = get_project_registry_row_from_label(selected_project_label)

                if selected_project_row is not None:
                    if st.session_state.get("show_shared_ids") and str(selected_project_row.get("project_code") or "").strip():
                        st.write(f"**Project Code:** {selected_project_row.get('project_code')}")
                    with st.form("project_registry_edit_form"):
                        e1, e2 = st.columns(2)
                        edit_name = e1.text_input("Edit Project Name", value=str(selected_project_row["project_name"] or ""))
                        edit_address = e2.text_input("Edit Address", value=str(selected_project_row["project_address"] or ""))
                        edit_notes = st.text_area("Project Description / Notes", value=str(selected_project_row.get("notes") or ""), height=100)
                        current_active = bool(selected_project_row["active"])
                        is_deleted_project = bool(selected_project_row.get("deleted", False))
                        active_choice = st.selectbox(
                            "Project Status",
                            ["Active", "Inactive"],
                            index=0 if current_active else 1,
                            disabled=is_deleted_project,
                        )
                        save_project = st.form_submit_button("Save Project Changes", type="primary")
                        if save_project:
                            if bool(selected_project_row.get("deleted", False)):
                                st.error("Deleted projects cannot be edited.")
                            elif not str(edit_name).strip():
                                st.error("Project name cannot be blank.")
                            else:
                                duplicate_name_df = fetch_df(
                                    """
                                    SELECT id
                                    FROM project_registry
                                    WHERE LOWER(COALESCE(project_name, '')) = LOWER(?)
                                      AND id <> ?
                                    LIMIT 1
                                    """,
                                    (str(edit_name).strip(), int(selected_project_row["id"])),
                                )
                                if not duplicate_name_df.empty:
                                    st.error("That project name already exists. Project names must be unique.")
                                else:
                                    execute(
                                    """
                                    UPDATE project_registry
                                    SET project_name = ?, project_address = ?, notes = ?, active = ?,
                                        activated_at = CASE
                                            WHEN ? = TRUE AND activated_at IS NULL THEN NOW()
                                            WHEN ? = FALSE THEN NULL
                                            ELSE activated_at
                                        END,
                                        modified_at = NOW()
                                    WHERE id = ?
                                    """,
                                    (
                                        str(edit_name).strip(),
                                        str(edit_address).strip(),
                                        str(edit_notes).strip(),
                                        True if active_choice == "Active" else False,
                                        True if active_choice == "Active" else False,
                                        True if active_choice == "Active" else False,
                                        int(selected_project_row["id"]),
                                    ),
                                )
                                # keep child records names aligned
                                execute("UPDATE estimates SET estimate_name = ?, estimate_address = ? WHERE project_id = ?", (str(edit_name).strip(), str(edit_address).strip(), int(selected_project_row["id"])))
                                execute("UPDATE punch_list_projects SET project_name = ?, project_address = ? WHERE project_id = ?", (str(edit_name).strip(), str(edit_address).strip(), int(selected_project_row["id"])))
                                execute("UPDATE project_status_entries SET project_name = ? WHERE project_id = ?", (str(edit_name).strip(), int(selected_project_row["id"])))
                                st.cache_data.clear()
                                st.success("Project updated.")
                                st.rerun()

                    if current_role == "Owner":
                        st.markdown("---")
                        if bool(selected_project_row.get("deleted", False)):
                            st.subheader("Restore Project")
                            st.caption("Restore this deleted project if you want to use it again instead of retyping it.")
                            if st.button("Restore Project", type="primary", key=f"restore_project_btn_{int(selected_project_row['id'])}"):
                                execute(
                                    "UPDATE project_registry SET deleted = FALSE, active = TRUE, modified_at = NOW(), activated_at = COALESCE(activated_at, NOW()) WHERE id = ?",
                                    (int(selected_project_row["id"]),),
                                )
                                st.success("Project restored.")
                                st.rerun()
                        else:
                            st.subheader("Delete Project")
                            delete_project_key = f"confirm_delete_project_{int(selected_project_row['id'])}"
                            if delete_project_key not in st.session_state:
                                st.session_state[delete_project_key] = False

                            if not st.session_state[delete_project_key]:
                                if st.button("Delete Project", type="secondary", key=f"delete_project_btn_{int(selected_project_row['id'])}"):
                                    st.session_state[delete_project_key] = True
                                    st.rerun()
                            else:
                                st.warning("Delete this project? This is for Owner only and cannot be undone.")
                                d1, d2 = st.columns(2)
                                if d1.button("Yes, Delete Project", type="primary", key=f"confirm_delete_project_yes_{int(selected_project_row['id'])}"):
                                    project_id_to_delete = int(selected_project_row["id"])
                                    execute(
                                        "UPDATE project_registry SET deleted = TRUE, active = FALSE, modified_at = NOW() WHERE id = ?",
                                        (project_id_to_delete,),
                                    )
                                    st.session_state[delete_project_key] = False
                                    st.success("Project deleted.")
                                    st.rerun()
                                if d2.button("Cancel Delete", key=f"confirm_delete_project_cancel_{int(selected_project_row['id'])}"):
                                    st.session_state[delete_project_key] = False
                                    st.rerun()


# -----------------------------
# Estimate Builder
# -----------------------------
elif page == "Estimate Builder":
    st.subheader("Estimate Builder")

    if st.session_state.editing_estimate_id is not None:
        st.info(f"Editing Estimate ID: {st.session_state.editing_estimate_id}")
        if st.button("Cancel Edit Mode"):
            reset_estimate_editor()
            st.rerun()

    selected_contractor = "None selected"

    top1, top2 = st.columns(2)
    with top1:
        if "builder_project_select" not in st.session_state:
            st.session_state.builder_project_select = ""
        if "builder_project_select_applied" not in st.session_state:
            st.session_state.builder_project_select_applied = ""

        project_labels = project_registry_select_labels(active_only=True)
        if not project_labels:
            st.warning("No active projects exist yet. Please create a project first on the Projects page.")
            estimate_name = ""
            estimate_address = st.text_input("Project Address", value="", disabled=True)
        else:
            if st.session_state.builder_project_select not in project_labels:
                st.session_state.builder_project_select = project_labels[0]

            selected_project_label = st.selectbox(
                "Choose An Existing Project",
                project_labels,
                key="builder_project_select",
                help="Choose an existing active project from the Projects page.",
            )

            selected_project_row = get_project_registry_row_from_label(selected_project_label)
            if selected_project_row is not None:
                st.session_state.builder_estimate_name = str(selected_project_row.get("project_name") or "")
                st.session_state.builder_estimate_address = str(selected_project_row.get("project_address") or "")
                st.session_state.builder_project_select_applied = selected_project_label
                st.session_state.builder_project_id = int(selected_project_row["id"])
                estimate_name = st.session_state.builder_estimate_name
                estimate_address, estimate_unit_number = render_shared_address_picker(
                    "Estimate Address",
                    "builder_estimate_address_picker",
                    default_address=st.session_state.builder_estimate_address,
                )
                if str(estimate_unit_number or "").strip():
                    estimate_address = f"{estimate_address} Unit {estimate_unit_number}".strip()
            else:
                estimate_name = ""
                estimate_address = st.text_input("Estimate Address", key="builder_estimate_address", value="")

    with top2:
        category_options = [""] + get_category_names()
        if "builder_category_select" not in st.session_state:
            st.session_state.builder_category_select = ""
        if st.session_state.builder_category_select not in category_options:
            st.session_state.builder_category_select = ""
        estimate_category = st.selectbox(
            'Category of Labor (optional)',
            category_options,
            key="builder_category_select",
            help='Optional category of labor used for grouping estimates and aligning costs with cash flow.',
        )
        work_group_name_for_estimate = st.text_input(
            'Work Group Name',
            key="builder_work_group_name",
            help='Optional grouping name. Multiple Work Items can later be converted under this Work Group name.',
        )
        estimate_notes = st.text_area("Estimate Notes", height=95, key="builder_estimate_notes")

    st.markdown("---")
    st.subheader("Add Repair To The Estimate")

    tasks_df = fetch_df(
        """
        SELECT
            tasks.id,
            tasks.name,
            COALESCE(tasks.notes, '') AS notes,
            trades.id AS trade_id,
            trades.name AS trade_name
        FROM tasks
        JOIN trades ON trades.id = tasks.trade_id
        WHERE tasks.active = TRUE
        ORDER BY LOWER(tasks.name), LOWER(trades.name)
        """
    )

    if tasks_df.empty:
        st.warning("No active tasks found.")
    else:
        if st.session_state.pending_repair_form_reset:
            st.session_state.builder_task_name_select = "Add A Work Item"
            st.session_state.builder_trade_name_select = ""
            st.session_state.builder_selected_template_select = ""
            st.session_state.builder_scope_description = ""
            st.session_state.builder_scope_context = ("", "", "")
            st.session_state.pending_repair_form_reset = False

        task_options = sorted(tasks_df["name"].dropna().unique().tolist())
        task_select_options = ["Add A Work Item"] + task_options
        if st.session_state.builder_task_name_select not in task_select_options:
            st.session_state.builder_task_name_select = "Add A Work Item"

        task_name = st.selectbox(
            "Add A Work Item",
            task_select_options,
            key="builder_task_name_select",
            help="Choose an existing task from the Tasks page.",
        )

        # AUTO-FILL SCOPE FROM WORK ITEM
        selected_task_df = fetch_df(
            '''
            SELECT st.scope_description
            FROM scope_templates st
            JOIN tasks t ON t.id = st.task_id
            WHERE LOWER(t.name) = LOWER(?)
            ORDER BY st.id DESC
            LIMIT 1
            ''',
            (task_name,)
        )

        if not selected_task_df.empty:
            st.session_state.builder_scope_description = str(selected_task_df.iloc[0]["scope_description"] or "")


        if task_name == "Add A Work Item":
            st.session_state.builder_trade_name_select = ""
            st.session_state.builder_selected_template_select = ""
            st.session_state.builder_scope_description = ""
            st.session_state.builder_scope_context = ("", "", "")
            st.selectbox('Category of Labor', [""], index=0, disabled=True, format_func=lambda _: "")
            st.selectbox("Scope Template", [""], index=0, disabled=True, format_func=lambda _: "")
            scope_description = st.text_area(
                "Scope description",
                key="builder_scope_description",
                height=150,
                placeholder="Select a task to auto-populate trade and scope.",
                disabled=True,
            )
            st.info("Select an existing task to auto-populate the trade and scope.")
            task_row = None
            trade_name = ""
        else:
            task_matches = tasks_df[tasks_df["name"] == task_name].copy()
            trade_options = sorted(task_matches["trade_name"].dropna().unique().tolist())
            if st.session_state.builder_trade_name_select not in trade_options:
                st.session_state.builder_trade_name_select = trade_options[0] if trade_options else ""
            trade_name = st.selectbox('Category of Labor', trade_options, key="builder_trade_name_select")
            task_row = task_matches[task_matches["trade_name"] == trade_name].iloc[0]

            if str(task_row["notes"]).strip():
                st.write(f"**Task notes:** {task_row['notes']}")

            template_df = fetch_df(
                """
                SELECT st.id, st.template_name AS name, st.scope_description
                FROM scope_templates st
                JOIN tasks t ON t.id = st.task_id
                JOIN trades tr ON tr.id = t.trade_id
                WHERE st.active = TRUE AND t.name = ? AND tr.name = ?
                ORDER BY st.template_name
                """,
                (task_name, trade_name),
            )
            template_options = template_df["name"].tolist() if not template_df.empty else []

            if template_options:
                if st.session_state.builder_selected_template_select not in template_options:
                    st.session_state.builder_selected_template_select = template_options[0]
                selected_template = st.selectbox(
                    "Scope Template",
                    template_options,
                    key="builder_selected_template_select",
                    help="The first saved scope auto-loads, but you can choose another saved scope template for this task.",
                )
                template_scope_value = str(
                    template_df.loc[template_df["name"] == selected_template, "scope_description"].iloc[0]
                )
            else:
                st.session_state.builder_selected_template_select = ""
                selected_template = ""
                template_scope_value = ""
                st.selectbox(
                    "Scope Template",
                    ["No scope template found"],
                    index=0,
                    disabled=True,
                    help="No saved scope templates were found for this task and trade.",
                )

            scope_context = (task_name, trade_name, selected_template)
            if st.session_state.builder_scope_context != scope_context:
                st.session_state.builder_scope_description = template_scope_value
                st.session_state.builder_scope_context = scope_context

            scope_description = st.text_area(
                "Scope description",
                key="builder_scope_description",
                height=150,
                placeholder="Scope auto-populates from the selected task and can be edited for this estimate.",
            )

        h1, h2, h3 = st.columns(3)
        onsite_hours_each = h1.number_input("Man Hours On Site (Each Repair)", min_value=0.0, value=0.0, step=0.25)
        travel_hours_each = h2.number_input("Travel Time Man Hours (Each Repair)", min_value=0.0, value=0.0, step=0.25)
        labor_rate = h3.number_input("Labor Rate For This Repair", min_value=0.0, value=30.0, step=1.0)

        q1, q2, q3 = st.columns(3)
        repair_quantity = q1.number_input("Number of Times This Repair Is Needed", min_value=1, value=1, step=1)

        total_hours_each = onsite_hours_each + travel_hours_each
        total_onsite_hours = onsite_hours_each * repair_quantity
        total_travel_hours = travel_hours_each * repair_quantity
        total_hours = total_hours_each * repair_quantity

        q2.metric("Total Man Hours (Each Repair)", f"{total_hours_each:.2f}")
        q3.metric("Total Man Hours (All Repairs)", f"{total_hours:.2f}")

        onsite_cost_each = onsite_hours_each * labor_rate
        travel_cost_each = travel_hours_each * labor_rate
        total_labor_cost_each = total_hours_each * labor_rate

        onsite_cost = onsite_cost_each * repair_quantity
        travel_cost = travel_cost_each * repair_quantity
        hourly_calculated_amount = total_labor_cost_each * repair_quantity

        manual_repair_amount = st.number_input(
            "Manual Repair Amount Override",
            min_value=0.0,
            value=0.0,
            step=1.0,
            help="If you enter an amount here, this number will be used for the repair total instead of the hourly calculation.",
        )

        total_labor_cost = manual_repair_amount if manual_repair_amount > 0 else hourly_calculated_amount

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("On Site Labor Cost", f"${onsite_cost:,.2f}")
        m2.metric("Travel Labor Cost", f"${travel_cost:,.2f}")
        m3.metric("Hourly Calculated Amount", f"${hourly_calculated_amount:,.2f}")
        m4.metric("Amount Used", f"${total_labor_cost:,.2f}")

        uploaded_repair_photos = st.file_uploader(
            "Photos For This Repair",
            type=["png", "jpg", "jpeg", "webp"],
            accept_multiple_files=True,
            key=f"repair_photos_{len(st.session_state.estimate_cart)}",
            help="Attach one or more photos to this repair. These photos will stay tied to this repair and will also appear in an overall job photo section.",
        )

        add_repair_submitted = st.button("Add Repair Item To The Estimate", type="primary")

        if add_repair_submitted:
            if task_name == "Add A Work Item":
                st.error("Select an existing task from the task list before adding the repair item.")
            else:
                st.session_state.estimate_cart.append(
                    {
                        "trade_name": trade_name,
                        "category_name": estimate_category if str(estimate_category or "").strip() else trade_name,
                        "work_group_name": work_group_name_for_estimate.strip(),
                        "task_name": task_name,
                        "scope_description": scope_description,
                        "repair_quantity": int(repair_quantity),
                        "onsite_hours_each": onsite_hours_each,
                        "travel_hours_each": travel_hours_each,
                        "total_hours_each": total_hours_each,
                        "onsite_hours": total_onsite_hours,
                        "travel_hours": total_travel_hours,
                        "total_hours": total_hours,
                        "labor_rate": labor_rate,
                        "onsite_cost": onsite_cost,
                        "travel_cost": travel_cost,
                        "manual_repair_amount": manual_repair_amount,
                        "hourly_calculated_amount": hourly_calculated_amount,
                        "total_labor_cost": total_labor_cost,
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "modified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "photos": normalize_uploaded_photos(uploaded_repair_photos),
                    }
                )
                st.session_state.pending_repair_form_reset = True
                st.success("Repair item added. The task, trade, and scope are now cleared and ready for the next repair.")
                st.rerun()
    st.markdown("---")
    st.subheader("Current Estimate")

    if not st.session_state.estimate_cart:
        st.info("No lines added yet.")
    else:
        cart_df = pd.DataFrame(st.session_state.estimate_cart)
        if "hourly_calculated_amount" not in cart_df.columns:
            cart_df["hourly_calculated_amount"] = cart_df.get("onsite_cost", 0).fillna(0) + cart_df.get("travel_cost", 0).fillna(0)
        if "manual_repair_amount" not in cart_df.columns:
            cart_df["manual_repair_amount"] = 0.0
        cart_df["photo_count"] = cart_df["photos"].apply(lambda x: len(x) if isinstance(x, list) else 0)
        display_df = cart_df[
            [
                "work_group_name",
                "category_name",
                "task_name",
                "trade_name",
                "scope_description",
                "repair_quantity",
                "photo_count",
                "onsite_hours_each",
                "travel_hours_each",
                "total_hours_each",
                "labor_rate",
                "hourly_calculated_amount",
                "manual_repair_amount",
                "onsite_hours",
                "travel_hours",
                "total_hours",
                "total_labor_cost",
            ]
        ].copy()
        display_df.columns = [
            'Work Group Name',
            'Category of Labor',
            "Work Item",
            'Work Item Category of Labor',
            "Scope Description",
            "Repair Qty",
            "Photos",
            "On Site Hrs Each",
            "Travel Hrs Each",
            "Total Hrs Each",
            "Labor Rate",
            "Hourly Calc Amount",
            "Manual Amount",
            "Total On Site Hrs",
            "Total Travel Hrs",
            "Total Man Hours",
            "Amount Used",
        ]
        display_df.index = range(1, len(display_df) + 1)
        st.dataframe(display_df, use_container_width=True)
        render_line_photo_sections(st.session_state.estimate_cart, load_key_prefix="estimate_builder_cart")

        total_onsite_hours = cart_df["onsite_hours"].sum()
        total_travel_hours = cart_df["travel_hours"].sum()
        total_hours = cart_df["total_hours"].sum()
        total_labor_cost = cart_df["total_labor_cost"].sum()

        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Total On Site Man Hours", f"{total_onsite_hours:.2f}")
        t2.metric("Total Travel Time Man Hours", f"{total_travel_hours:.2f}")
        t3.metric("Total Man Hours", f"{total_hours:.2f}")
        t4.metric("Total Labor Cost", f"${total_labor_cost:,.2f}")

        remove_options = [
            f"Line {i + 1}: {row['task_name']} | {row['trade_name']}"
            for i, row in cart_df.iterrows()
        ]
        r1, r2 = st.columns([3, 1])
        selected_remove = r1.selectbox("Remove a line", remove_options)
        if r2.button("Remove Selected Line"):
            remove_index = remove_options.index(selected_remove)
            st.session_state.estimate_cart.pop(remove_index)
            st.rerun()

        s1, s2 = st.columns(2)
        save_label = "Save Changes" if st.session_state.editing_estimate_id is not None else "Save Estimate"
        if s1.button(save_label, type="primary"):
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if st.session_state.editing_estimate_id is not None:
                estimate_id = int(st.session_state.editing_estimate_id)
                contractor_id = get_contractor_id_by_name(selected_contractor)
                execute(
                    """
                    UPDATE estimates
                    SET modified_at = ?, estimate_name = ?, estimate_address = ?,
                        contractor_id = ?, labor_rate = ?, notes = ?, category_name = ?, work_group_name = ?
                    WHERE id = ?
                    """,
                    (
                        now_text,
                        estimate_name.strip(),
                        estimate_address.strip(),
                        contractor_id,
                        0,
                        estimate_notes.strip(),
                        str(estimate_category or "").strip(),
                        str(work_group_name_for_estimate or "").strip(),
                        estimate_id,
                    ),
                )
                execute("DELETE FROM estimate_lines WHERE estimate_id = ?", (estimate_id,))
            else:
                contractor_id = get_contractor_id_by_name(selected_contractor)
                estimate_id = execute_returning_id(
                    """
                    INSERT INTO estimates (
                        created_at, modified_at, estimate_name, estimate_address,
                        contractor_id, labor_rate, active, notes, category_name, work_group_name, estimate_mode, source_method, status, version_no
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual', 'typed', 'draft', 1)
                    """,
                    (
                        now_text,
                        now_text,
                        estimate_name.strip(),
                        estimate_address.strip(),
                        contractor_id,
                        0,
                        True,
                        estimate_notes.strip(),
                        str(estimate_category or "").strip(),
                        str(work_group_name_for_estimate or "").strip(),
                    ),
                )
                set_order_number("estimates", int(estimate_id), "Est")

            for line in st.session_state.estimate_cart:
                line_id = execute_returning_id(
                    """
                    INSERT INTO estimate_lines (
                        estimate_id,
                        category_name,
                        work_group_name,
                        trade_name,
                        task_name,
                        scope_description,
                        repair_quantity,
                        onsite_hours_each,
                        travel_hours_each,
                        total_hours_each,
                        onsite_hours,
                        travel_hours,
                        total_hours,
                        labor_rate,
                        onsite_cost,
                        travel_cost,
                        manual_repair_amount,
                        total_labor_cost,
                        created_at,
                        modified_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        estimate_id,
                        str(line.get("category_name") or line.get("trade_name") or ""),
                        str(line.get("work_group_name") or ""),
                        line["trade_name"],
                        line["task_name"],
                        line["scope_description"],
                        int(line.get("repair_quantity", 1)),
                        float(line.get("onsite_hours_each", line["onsite_hours"])),
                        float(line.get("travel_hours_each", line["travel_hours"])),
                        float(line.get("total_hours_each", line["total_hours"])),
                        line["onsite_hours"],
                        line["travel_hours"],
                        line["total_hours"],
                        line["labor_rate"],
                        line["onsite_cost"],
                        line["travel_cost"],
                        float(line.get("manual_repair_amount", 0) or 0),
                        line["total_labor_cost"],
                        line.get("created_at") or now_text,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
                for photo_idx, photo in enumerate(line.get("photos", [])):
                    execute(
                        """
                        INSERT INTO estimate_line_photos (
                            estimate_id,
                            estimate_line_id,
                            photo_filename,
                            content_type,
                            storage_mode,
                            blob_url,
                            blob_name,
                            photo_bytes,
                            sort_order
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            estimate_id,
                            line_id,
                            str(photo.get("filename") or f"photo_{photo_idx + 1}.jpg"),
                            str(photo.get("content_type") or "image/jpeg"),
                            str(photo.get("storage_mode") or "database"),
                            str(photo.get("blob_url") or ""),
                            str(photo.get("blob_name") or ""),
                            photo.get("bytes"),
                            int(photo.get("sort_order", photo_idx)),
                        ),
                    )

            if st.session_state.editing_estimate_id is not None:
                reset_estimate_editor()
                st.success(f"Estimate updated. Estimate ID: {estimate_id}")
                st.rerun()
            else:
                reset_estimate_editor()
                st.success(f"Estimate saved. Estimate ID: {estimate_id}")
                st.rerun()

        if s2.button("Clear Entire Estimate"):
            st.session_state.estimate_cart = []
            st.warning("Current estimate cleared.")

# -----------------------------
# Punch List / Inspection
# -----------------------------
elif page == "Punch List / Inspection":
    st.subheader("Punch List / Inspection")
    st.caption("Inspection and completion tracking for projects already awarded. Use Estimate Builder for out-of-scope pricing.")

    current_role = str(st.session_state.get("logged_in_role", "") or "")
    linked_contractor_id = int(st.session_state.get("logged_in_contractor_id") or 0) if current_role == "Contractor" else None
    contractor_options = ["None selected"] + get_contractor_list_df()["name"].tolist()
    project_df = punch_list_projects_df(contractor_id=linked_contractor_id)

    tab1, tab2 = st.tabs(["Create Punch List Project", "Open Punch List Project"])

    with tab1:
        if current_role == "Contractor":
            st.info("Contractors cannot create new punch list projects.")
        else:
            with st.form("create_punch_list_project_form"):
                c1, c2 = st.columns(2)
                active_project_labels = project_registry_select_labels(active_only=True)
                selected_project_label = c1.selectbox("Existing Project", active_project_labels if active_project_labels else ["No active projects"], key="pl_project_select")
                selected_project_row = get_project_registry_row_from_label(selected_project_label) if active_project_labels else None
                project_name = str(selected_project_row["project_name"]) if selected_project_row is not None else ""
                project_address_default = str(selected_project_row["project_address"]) if selected_project_row is not None else ""
                c2.text_input("Selected Project", value=project_name, disabled=True)

                st.markdown("#### Punch List Address")
                pl_selected_property = st.selectbox(
                    "Property (optional)",
                    master_property_labels(),
                    key="pl_property_choice",
                    help="Optional. Choose a property to narrow the address list, or leave it as a general address.",
                )
                pl_address_options = master_address_labels(pl_selected_property)
                pl_selected_address = st.selectbox(
                    "Choose Address",
                    ["Type New Address"] + pl_address_options,
                    key="pl_address_choice",
                )
                pl_row = portfolio_address_row_from_label(pl_selected_address) if pl_selected_address != "Type New Address" else None
                if pl_row is not None:
                    project_address = str(pl_row.get("address") or "")
                    project_unit_number = str(pl_row.get("unit_number") or "")
                    pa1, pa2 = st.columns(2)
                    pa1.text_input("Selected Address", value=project_address, disabled=True, key="pl_selected_address")
                    pa2.text_input("Selected Unit Number", value=project_unit_number, disabled=True, key="pl_selected_unit")
                else:
                    pa1, pa2 = st.columns(2)
                    project_address = pa1.text_input("Type New Address", value=project_address_default, key="pl_new_address")
                    project_unit_number = pa2.text_input("Type New Unit Number", key="pl_new_unit")

                c3, c4, c5 = st.columns(3)
                project_contractor = c3.selectbox("Primary Contractor", contractor_options)
                inspection_date = c4.date_input("Inspection Date", value=datetime.now().date())
                deadline_date = c5.date_input("Due Date", value=datetime.now().date())
                project_notes = st.text_area("Project Notes", height=100)
                submit_project = st.form_submit_button("Create Punch List Project", type="primary")
                if submit_project:
                    if selected_project_row is None:
                        st.error("Create an active project first on the Projects page.")
                    else:
                        new_punch_list_id = execute_returning_id(
                            """
                            INSERT INTO punch_list_projects (
                                project_id, project_name, project_address, contractor_id, status,
                                inspection_date, deadline_date, notes, created_by, created_at, modified_at
                            ) VALUES (?, ?, ?, ?, 'Open', ?, ?, ?, ?, NOW(), NOW())
                            """,
                            (
                                int(selected_project_row["id"]),
                                project_name.strip(),
                                (project_address + (f" Unit {project_unit_number}" if str(project_unit_number or "").strip() else "")).strip(),
                                get_contractor_id_by_name(project_contractor),
                                str(inspection_date),
                                str(deadline_date),
                                project_notes.strip(),
                                st.session_state.get("logged_in_user", ""),
                            ),
                        )
                        set_order_number("punch_list_projects", int(new_punch_list_id), "PL")
                        st.cache_data.clear()
                        st.success("Punch list project created.")
                        st.rerun()

    with tab2:
        if project_df.empty:
            st.info("No punch list projects found.")
        else:
            project_labels = [f"{row.order_number} | {int(row.id)} | {row.project_name} | {row.status}" for row in project_df.itertuples()]
            selected_project_label = st.selectbox("Select Punch List Project", project_labels)
            selected_project_id = int(selected_project_label.split(" | ")[1])
            project_row = project_df[project_df["id"] == selected_project_id].iloc[0]
            items_df = punch_list_items_df(selected_project_id, contractor_id=linked_contractor_id)

            st.markdown(f"### {project_row['project_name']}")
            st.write(f"**Punch List Order Number:** {project_row.get('order_number', '')}")
            st.write(f"**Address:** {project_row['project_address']}")
            st.write(f"**Contractor:** {project_row['contractor_name']}")
            st.write(f"**Status:** {project_row['status']}")
            if pd.notna(project_row["inspection_date"]):
                st.write(f"**Inspection Date:** {pd.to_datetime(project_row['inspection_date']).strftime('%m-%d-%Y')}")
            if pd.notna(project_row["deadline_date"]):
                st.write(f"**Date To Complete:** {pd.to_datetime(project_row['deadline_date']).strftime('%m-%d-%Y')}")
            if str(project_row["notes"]).strip():
                st.write(f"**Project Notes:** {project_row['notes']}")

            d1, d2, d3 = st.columns(3)
            full_pdf = build_punch_list_report_pdf(selected_project_id, status_filter="all")
            open_pdf = build_punch_list_report_pdf(selected_project_id, status_filter="open")
            completed_pdf = build_punch_list_report_pdf(selected_project_id, status_filter="completed")
            file_base = str(project_row["project_name"]).strip().replace(" ", "_") or f"punch_list_{selected_project_id}"
            if full_pdf:
                d1.download_button("Download Full Punch List", full_pdf, file_name=f"{file_base}_full_punch_list.pdf", mime="application/pdf", key=f"pl_full_{selected_project_id}")
            if open_pdf:
                d2.download_button("Download Open Punch List", open_pdf, file_name=f"{file_base}_open_punch_list.pdf", mime="application/pdf", key=f"pl_open_{selected_project_id}")
            if completed_pdf:
                d3.download_button("Download Completed Punch List", completed_pdf, file_name=f"{file_base}_completed_punch_list.pdf", mime="application/pdf", key=f"pl_completed_{selected_project_id}")

            if current_role != "Contractor":
                st.markdown("---")
                st.subheader('Add Punch List Work Group')
                task_options = punch_list_task_options()
                item_choice_options = ["Add Work Item"] + task_options

                if "pl_item_choice" not in st.session_state:
                    st.session_state.pl_item_choice = "Add Work Item"
                if "pl_custom_item_title" not in st.session_state:
                    st.session_state.pl_custom_item_title = ""
                if "pl_trade_name_value" not in st.session_state:
                    st.session_state.pl_trade_name_value = ""
                if st.session_state.get("reset_punch_list_item_form"):
                    st.session_state.pl_item_choice = "Add Work Item"
                    st.session_state.pl_custom_item_title = ""
                    st.session_state.pl_trade_name_value = ""
                    st.session_state.reset_punch_list_item_form = False

                with st.form("add_punch_list_item_form"):
                    st.caption('A Punch List Work Group is a correction or closeout item assigned to a contractor for completion.')
                    i1, i2 = st.columns(2)

                    item_choice = i1.selectbox(
                        "Work Item",
                        item_choice_options,
                        key="pl_item_choice",
                    )

                    if item_choice == "Add Work Item":
                        item_title = i1.text_input(
                            "Add Work Item",
                            key="pl_custom_item_title",
                            placeholder="Add Work Item",
                        ).strip()
                    else:
                        item_title = item_choice.strip()

                    trade_options = punch_list_trade_options_for_task(item_title) if item_title else []
                    default_trade_value = trade_options[0] if trade_options else ""
                    current_trade_value = st.session_state.get("pl_trade_name_value", "")
                    if default_trade_value and (not current_trade_value or current_trade_value not in trade_options):
                        st.session_state.pl_trade_name_value = default_trade_value
                    elif not item_title and not st.session_state.get("pl_trade_name_value", ""):
                        st.session_state.pl_trade_name_value = ""

                    selected_trade_name = i2.text_input(
                        'Category of Labor',
                        key="pl_trade_name_value",
                        placeholder="Add A Trade",
                    ).strip()

                    scope_description = st.text_area(
                        'Punch List Work Group Scope',
                        height=120,
                        placeholder="Enter the unique punch list scope for this item.",
                    )
                    uploaded_punch_photos = st.file_uploader(
                        'Upload Photos For This Punch List Work Group (optimized)',
                        type=["png", "jpg", "jpeg", "webp"],
                        accept_multiple_files=True,
                        key="pl_item_photos_upload",
                    )
                    i3, i4, i5 = st.columns(3)
                    item_contractor = i3.selectbox("Assigned Contractor", contractor_options, key="pl_item_contractor")
                    item_status = i4.selectbox('Punch List Work Group Status', ["Open", "In Progress", "Ready for Review", "Complete"])
                    quote_requested = i5.selectbox("Request Quote?", ["No", "Yes"])
                    i6, i7 = st.columns(2)
                    date_reference = i6.date_input("Inspection / Identified Date", value=datetime.now().date(), key="pl_date_reference")
                    default_deadline = pd.to_datetime(project_row["deadline_date"]).date() if pd.notna(project_row["deadline_date"]) else datetime.now().date()
                    item_deadline = i7.date_input("Due Date", value=default_deadline, key="pl_item_deadline")
                    manager_notes = st.text_area("Owner / Manager Instructions", height=100)
                    submit_item = st.form_submit_button('Add Punch List Work Group', type="primary")
                    if submit_item:
                        if not item_title:
                            st.error("Choose an existing work item or enter your own.")
                        elif not selected_trade_name:
                            st.error("Enter or confirm a trade.")
                        else:
                            item_id = execute_returning_id(
                                """
                                INSERT INTO punch_list_items (
                                    project_id, item_title, trade_name, scope_description, contractor_id,
                                    item_status, identified_date, deadline_date, completed_date, quote_requested,
                                    manager_notes, contractor_notes, created_at, modified_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', NOW(), NOW())
                                """,
                                (
                                    selected_project_id,
                                    item_title,
                                    selected_trade_name,
                                    scope_description.strip(),
                                    get_contractor_id_by_name(item_contractor),
                                    item_status,
                                    str(date_reference),
                                    str(item_deadline),
                                    str(datetime.now().date()) if item_status == "Complete" else None,
                                    True if quote_requested == "Yes" else False,
                                    manager_notes.strip(),
                                ),
                            )
                            set_order_number("punch_list_items", int(item_id), "PLWG")
                            save_punch_list_item_photos(selected_project_id, item_id, uploaded_punch_photos)
                            st.session_state.reset_punch_list_item_form = True
                            st.cache_data.clear()
                            st.success('Punch list work group added.')
                            st.rerun()

            st.markdown("---")
            st.subheader('Current Punch List Work Groups')
            if items_df.empty:
                st.info("No punch list items yet.")
            else:
                display_items = items_df.copy()
                for col in ["identified_date", "deadline_date", "completed_date"]:
                    display_items[col] = pd.to_datetime(display_items[col], errors="coerce").dt.strftime("%m-%d-%Y")
                st.dataframe(
                    display_items[["order_number", "item_title", "trade_name", "contractor_name", "item_status", "identified_date", "deadline_date", "completed_date", "quote_requested"]].rename(columns={
                        "order_number": 'Punch List Work Group #',
                        "item_title": "Work Item",
                        "trade_name": 'Category of Labor',
                        "contractor_name": "Contractor",
                        "item_status": "Status",
                        "identified_date": "Identified Date",
                        "deadline_date": "Due Date",
                        "completed_date": "Completed Date",
                        "quote_requested": "Quote Requested",
                    }),
                    use_container_width=True,
                )

                item_labels = [f"{row.order_number} | {int(row.id)} | {row.item_title}" for row in items_df.itertuples()]
                selected_item_label = st.selectbox("Update Punch List Work Group", item_labels)
                selected_item_id = int(selected_item_label.split(" | ")[1])
                selected_item_row = items_df[items_df["id"] == selected_item_id].iloc[0]

                if st.checkbox('Load photos for selected punch list work group', key=f"manager_punch_photos_{selected_item_id}"):
                    existing_pl_photos = punch_list_item_photos(selected_item_id)
                    if existing_pl_photos:
                        st.markdown('#### Photos For Selected Punch List Work Group')
                        cols = st.columns(min(4, max(1, len(existing_pl_photos))))
                        for idx, photo in enumerate(existing_pl_photos):
                            with cols[idx % len(cols)]:
                                render_photo_item(photo)

                with st.form("update_punch_list_item_form"):
                    st.text_input(
                        'Punch List Work Group Number',
                        value=str(selected_item_row.get("order_number") or ""),
                        disabled=True,
                    )
                    st.text_input(
                        "Work Item",
                        value=str(selected_item_row.get("item_title") or ""),
                        disabled=True,
                    )
                    st.text_input(
                        'Category of Labor',
                        value=str(selected_item_row.get("trade_name") or ""),
                        disabled=True,
                    )
                    st.text_area(
                        'Punch List Work Group Scope',
                        value=str(selected_item_row.get("scope_description") or ""),
                        height=100,
                        disabled=True,
                    )
                    u1, u2 = st.columns(2)
                    statuses = ["Open", "In Progress", "Ready for Review", "Complete"]
                    current_status = selected_item_row["item_status"] if selected_item_row["item_status"] in statuses else "Open"
                    update_status = u1.selectbox('Punch List Work Group Status', statuses, index=statuses.index(current_status))
                    update_quote = u2.selectbox("Quote Requested?", ["No", "Yes"], index=1 if bool(selected_item_row["quote_requested"]) else 0)
                    current_deadline = pd.to_datetime(selected_item_row["deadline_date"], errors="coerce")
                    update_deadline = st.date_input("Due Date", value=current_deadline.date() if pd.notna(current_deadline) else datetime.now().date())
                    manager_note_update = st.text_area("Owner / Manager Instructions", value=str(selected_item_row["manager_notes"] or ""), height=100)
                    contractor_note_update = st.text_area("Contractor Notes", value=str(selected_item_row["contractor_notes"] or ""), height=100)
                    additional_photos = st.file_uploader(
                        'Add Photos For This Punch List Work Group (optimized)',
                        type=["png", "jpg", "jpeg", "webp"],
                        accept_multiple_files=True,
                        key=f"pl_update_photos_{selected_item_id}",
                    )
                    submit_update = st.form_submit_button('Save Punch List Work Group Changes', type="primary")
                    if submit_update:
                        execute(
                            """
                            UPDATE punch_list_items
                            SET item_status = ?, deadline_date = ?, completed_date = ?, quote_requested = ?,
                                manager_notes = ?, contractor_notes = ?, modified_at = NOW()
                            WHERE id = ?
                            """,
                            (
                                update_status,
                                str(update_deadline),
                                str(datetime.now().date()) if update_status == "Complete" else None,
                                True if update_quote == "Yes" else False,
                                manager_note_update.strip(),
                                contractor_note_update.strip(),
                                selected_item_id,
                            ),
                        )
                        save_punch_list_item_photos(selected_project_id, selected_item_id, additional_photos)
                        st.cache_data.clear()
                        st.success('Punch list work group updated.')
                        st.rerun()

            st.markdown("---")
            st.subheader("Delete Punch List")
            delete_punch_key = f"confirm_delete_punch_list_{selected_project_id}"
            if delete_punch_key not in st.session_state:
                st.session_state[delete_punch_key] = False

            if not st.session_state[delete_punch_key]:
                if st.button("Delete Punch List", type="secondary", key=f"delete_punch_list_btn_{selected_project_id}"):
                    st.session_state[delete_punch_key] = True
                    st.rerun()
            else:
                st.warning("Delete this Punch List permanently? This will also delete all related punch list items and photos.")
                p1, p2 = st.columns(2)
                if p1.button("Yes, Delete Punch List", type="primary", key=f"confirm_delete_punch_list_yes_{selected_project_id}"):
                    delete_punch_list_project(selected_project_id)
                    st.session_state[delete_punch_key] = False
                    st.success("Punch List deleted.")
                    st.rerun()
                if p2.button("Cancel Delete", key=f"confirm_delete_punch_list_cancel_{selected_project_id}"):
                    st.session_state[delete_punch_key] = False
                    st.rerun()


# -----------------------------
# Active Projects
# -----------------------------
elif page == "Active Projects":
    st.subheader("Active Projects")
    st.caption("Review active projects and manage project status updates here.")

    active_projects_df = project_registry_active_df()
    if active_projects_df.empty:
        st.info("No active projects exist yet. Activate a project on the Projects page first.")
    else:
        display_df = active_projects_df.copy()
        for col in ["activated_at", "created_at", "modified_at"]:
            if col in display_df.columns:
                display_df[col] = pd.to_datetime(display_df[col], errors="coerce").dt.strftime("%m-%d-%Y")
        st.dataframe(
            display_df[["id", "project_name", "project_address", "activated_at", "notes"]].rename(columns={
                "id": "Project ID",
                "project_name": "Project Name",
                "project_address": "Address",
                "activated_at": "Date Activated",
                "notes": "Project Description",
            }),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")
        st.markdown("### Project Status")

        project_labels = [f"{int(row.id)} | {row.project_name}" for row in active_projects_df.itertuples()]
        selected_project_label = st.selectbox("Project Name", project_labels, key="active_project_select")
        selected_project_row = get_project_registry_row_from_label(selected_project_label)

        if selected_project_row is not None:
            selected_project_id = int(selected_project_row["id"])
            status_mode = st.radio(
                "Project Status Action",
                ["Review Previous Project Status", "Enter New Project Status"],
                horizontal=True,
                key="active_project_status_mode",
            )

            info1, info2 = st.columns(2)
            info1.text_input("Project Name Selected", value=str(selected_project_row.get("project_name") or ""), disabled=True)
            info2.text_input("Project ID", value=str(selected_project_id), disabled=True)
            st.text_input("Project Address", value=str(selected_project_row.get("project_address") or ""), disabled=True)
            st.text_area("Project Description", value=str(selected_project_row.get("notes") or ""), disabled=True, height=80)

            status_entries = get_project_status_entries_for_project_cached(selected_project_id)

            if status_mode == "Enter New Project Status":
                with st.form("active_project_status_entry_form"):
                    s1, s2 = st.columns(2)
                    entry_date_text = s1.text_input("Date Of Inspection (MM/DD/YYYY)", value=datetime.now().strftime("%m/%d/%Y"))
                    uploaded_status_photos = s2.file_uploader(
                        "Upload Status Photos (optimized)",
                        type=["png", "jpg", "jpeg", "webp"],
                        accept_multiple_files=True,
                        key="active_project_status_photos_upload",
                    )
                    note_text = st.text_area("Status Update Notes", height=160, placeholder="Enter project status notes, progress updates, observations, or issues.")
                    submit_status_entry = st.form_submit_button("Save Project Status Update", type="primary")
                    if submit_status_entry:
                        try:
                            parsed_date = datetime.strptime(str(entry_date_text).strip(), "%m/%d/%Y").date()
                        except Exception:
                            st.error("Enter the inspection date in MM/DD/YYYY format.")
                        else:
                            if not str(note_text).strip() and not uploaded_status_photos:
                                st.error("Enter notes or upload photos before saving.")
                            else:
                                entry_id = execute_returning_id(
                                    """
                                    INSERT INTO project_status_entries (
                                        source_type, source_id, project_id, project_name, entry_date, note_text,
                                        created_by, created_at, modified_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                                    """,
                                    (
                                        "Project",
                                        selected_project_id,
                                        selected_project_id,
                                        str(selected_project_row.get("project_name") or ""),
                                        str(parsed_date),
                                        str(note_text).strip(),
                                        st.session_state.get("logged_in_user", ""),
                                    ),
                                )
                                save_project_status_photos(entry_id, uploaded_status_photos)
                                st.cache_data.clear()
                                st.success("Project status update saved.")
                                st.rerun()
            else:
                st.subheader("Previous Project Status")
                if status_entries.empty:
                    st.info("No project status updates have been saved yet for this project.")
                else:
                    status_display_df = status_entries.copy()
                    status_display_df["project_name"] = str(selected_project_row.get("project_name") or "")
                    status_display_df["entry_date_sort"] = pd.to_datetime(status_display_df["entry_date"], errors="coerce")
                    status_display_df = status_display_df.sort_values(
                        by=["project_name", "entry_date_sort", "created_at"],
                        ascending=[True, False, False],
                    )
                    status_display_df["entry_date"] = pd.to_datetime(status_display_df["entry_date"], errors="coerce").dt.strftime("%m-%d-%Y")
                    status_display_df["created_at"] = pd.to_datetime(status_display_df["created_at"], errors="coerce").dt.strftime("%m-%d-%Y %H:%M")
                    st.dataframe(
                        status_display_df[["project_name", "entry_date", "created_by", "created_at", "note_text"]].rename(columns={
                            "project_name": "Project Name",
                            "entry_date": "Inspection Date",
                            "created_by": "Entered By",
                            "created_at": "Saved",
                            "note_text": "Notes",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )

# -----------------------------
# Project Cost
# -----------------------------
elif page == "Project Cost":
    st.subheader("Project Cost")
    st.caption('Enter and review agreed prices by Project, Category of Labor, Work Group, and Work Item.')

    all_projects_df = project_registry_all_df()
    if not all_projects_df.empty and "deleted" in all_projects_df.columns:
        all_projects_df = all_projects_df[all_projects_df["deleted"].fillna(False) == False].copy()

    if all_projects_df.empty:
        st.info("No projects found yet.")
    else:
        project_labels = [f"{int(row.id)} | {row.project_name}" for row in all_projects_df.itertuples()]
        selected_project_label = st.selectbox("Choose Project", project_labels, key="project_cost_select")
        selected_project_row = get_project_registry_row_from_label(selected_project_label)

        if selected_project_row is not None:
            project_id = int(selected_project_row["id"])
            costs_df = work_item_costs_df(project_id=project_id)
            lines_df = project_estimate_work_items_df(project_id)

            c1, c2 = st.columns(2)
            c1.text_input("Project Name", value=str(selected_project_row.get("project_name") or ""), disabled=True)
            c2.text_input("Project ID", value=str(project_id), disabled=True)
            st.text_input("Project Address", value=str(selected_project_row.get("project_address") or ""), disabled=True)

            st.markdown("### Work Item Cost Entries")
            if lines_df.empty:
                st.info("No estimate-based Work Items were found yet for this project.")
            else:
                display_rows = []
                for row in lines_df.itertuples():
                    cost_row = latest_cost_row_from_df(costs_df, project_id, row.task_name, row.trade_name, int(row.estimate_line_id))
                    category_label = str(getattr(row, "category_name", "") or getattr(row, "trade_name", "") or "")
                    work_group_label = str(getattr(row, "work_group_name", "") or "")
                    agreed_price_value = float(cost_row["agreed_price"]) if cost_row is not None else 0.0
                    display_rows.append({
                        "Work Item ID": int(row.estimate_line_id),
                        'Category of Labor': category_label,
                        'Work Group Name': work_group_label,
                        "Work Item": row.task_name,
                        'Work Item Category of Labor': row.trade_name,
                        "Current Contractor": (str(cost_row["contractor_name"]) if cost_row is not None and str(cost_row.get("contractor_name") or "").strip() else row.contractor_name),
                        "Agreed Price For Work Item": agreed_price_value,
                        "Date Entered": pd.to_datetime(cost_row["entered_date"], errors="coerce").strftime("%m-%d-%Y") if cost_row is not None and pd.notna(cost_row.get("entered_date")) else "",
                    })

                display_df = pd.DataFrame(display_rows)
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                st.markdown("### Cost Summary")
                s1, s2 = st.columns(2)
                category_summary_df = (
                    display_df.groupby('Category of Labor', dropna=False)["Agreed Price For Work Item"]
                    .sum()
                    .reset_index()
                    .rename(columns={"Agreed Price For Work Item": "Total Agreed Price"})
                )
                work_group_summary_df = (
                    display_df.groupby(['Category of Labor', 'Work Group Name'], dropna=False)["Agreed Price For Work Item"]
                    .sum()
                    .reset_index()
                    .rename(columns={"Agreed Price For Work Item": "Total Agreed Price"})
                )
                s1.markdown('**By Category of Labor**')
                s1.dataframe(category_summary_df, use_container_width=True, hide_index=True)
                s2.markdown('**By Work Group Name**')
                s2.dataframe(work_group_summary_df, use_container_width=True, hide_index=True)

                item_labels = [
                    f"{int(row.estimate_line_id)} | {getattr(row, 'category_name', '') or getattr(row, 'trade_name', '')} | {getattr(row, 'work_group_name', '') or ''} | {row.task_name}"
                    for row in lines_df.itertuples()
                ]
                selected_item_label = st.selectbox("Choose Work Item", item_labels, key="project_cost_work_item_select")
                selected_item_id = int(selected_item_label.split(" | ", 1)[0])
                selected_item_row = lines_df[lines_df["estimate_line_id"] == selected_item_id].iloc[0]
                latest_cost_row = latest_cost_row_from_df(costs_df, project_id, selected_item_row["task_name"], selected_item_row["trade_name"], selected_item_id)

                st.markdown("### Agreed Price Entry")
                d1, d2 = st.columns(2)
                d1.text_input('Category of Labor', value=str(selected_item_row.get("category_name") or selected_item_row.get("trade_name") or ""), disabled=True)
                d2.text_input('Work Group Name', value=str(selected_item_row.get("work_group_name") or ""), disabled=True)
                d3, d4 = st.columns(2)
                d3.text_input("Work Item", value=str(selected_item_row["task_name"]), disabled=True)
                d4.text_input('Work Item Category of Labor', value=str(selected_item_row["trade_name"]), disabled=True)
                st.text_area("Scope Description", value=str(selected_item_row.get("scope_description") or ""), disabled=True, height=100)

                contractor_names = get_contractor_names()
                contractor_options = ["None selected"] + contractor_names
                latest_contractor_name = ""
                if latest_cost_row is not None and str(latest_cost_row.get("contractor_name") or "").strip():
                    latest_contractor_name = str(latest_cost_row.get("contractor_name") or "")
                elif str(selected_item_row.get("contractor_name") or "").strip():
                    latest_contractor_name = str(selected_item_row.get("contractor_name") or "")
                contractor_index = contractor_options.index(latest_contractor_name) if latest_contractor_name in contractor_options else 0

                p1, p2, p3 = st.columns(3)
                agreed_price_default = float(latest_cost_row["agreed_price"]) if latest_cost_row is not None else float(selected_item_row.get("estimated_amount") or 0.0)
                entered_date_default = pd.to_datetime(latest_cost_row["entered_date"], errors="coerce").date() if latest_cost_row is not None and pd.notna(latest_cost_row.get("entered_date")) else datetime.now().date()

                agreed_price = p1.number_input(
                    "Agreed Price For Work Item",
                    min_value=0.0,
                    value=agreed_price_default,
                    step=50.0,
                    key=f"work_item_agreed_price_{selected_item_id}",
                )
                contractor_name = p2.selectbox(
                    "Contractor",
                    contractor_options,
                    index=contractor_index,
                    key=f"work_item_cost_contractor_{selected_item_id}",
                )
                entered_date = p3.date_input(
                    "Date Entered",
                    value=entered_date_default,
                    key=f"work_item_cost_date_{selected_item_id}",
                )
                cost_notes = st.text_area(
                    "Cost Notes",
                    value=str(latest_cost_row.get("notes") or "") if latest_cost_row is not None else "",
                    key=f"work_item_cost_notes_{selected_item_id}",
                    height=80,
                )

                if st.button("Save Work Item Cost", type="primary", key=f"save_work_item_cost_{selected_item_id}"):
                    contractor_id_value = get_contractor_id_by_name(contractor_name)
                    execute(
                        """
                        INSERT INTO work_item_costs (
                            project_id, estimate_line_id, task_name, trade_name, contractor_id,
                            agreed_price, entered_date, notes, created_at, modified_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                        """,
                        (
                            project_id,
                            selected_item_id,
                            str(selected_item_row["task_name"]),
                            str(selected_item_row["trade_name"]),
                            contractor_id_value,
                            agreed_price,
                            entered_date,
                            str(cost_notes).strip(),
                        ),
                    )
                    execute(
                        """
                        UPDATE estimate_lines
                        SET approved_final_cost = ?, contractor_id = ?
                        WHERE id = ?
                        """,
                        (agreed_price, contractor_id_value, selected_item_id),
                    )
                    st.success("Work Item cost saved.")
                    st.rerun()


# -----------------------------
# Work Groups
# -----------------------------
elif page == 'Work Groups':
    st.subheader('Work Groups')
    st.caption('Create contractor work groups by project and review work group reports.')

    if current_role == "Contractor":
        st.error("You do not have permission to access this page.")
        st.stop()

    active_projects_df = project_registry_active_df()
    if active_projects_df.empty:
        st.info("No active projects found yet.")
    else:
        tab1, tab_rmr, tab2, tab3, tab4 = st.tabs(['Create Work Groups', 'Assign RMRs To Work Group', 'Edit Work Groups', 'Estimates → Convert To Work Groups', 'Work Groups Report'])

        with tab1:
            project_labels = [f"{int(row.id)} | {row.project_name}" for row in active_projects_df.itertuples()]
            selected_project_label = st.selectbox("Choose Project", project_labels, key="work_group_project_select")
            selected_project_row = get_project_registry_row_from_label(selected_project_label)

            if selected_project_row is not None:
                project_id = int(selected_project_row["id"])
                lines_df = project_estimate_work_items_df(project_id)
                costs_df = work_item_costs_df(project_id=project_id)
                task_lookup_df = get_task_lookup_df()

                st.text_input("Project Name", value=str(selected_project_row.get("project_name") or ""), disabled=True)
                work_group_address, work_group_unit_number = render_shared_address_picker(
                    'Work Group Address',
                    f"work_group_address_picker_{project_id}",
                    default_address=str(selected_project_row.get("project_address") or ""),
                )

                source_mode = st.radio(
                    "Work Item Source",
                    ["Existing Project Work Item", "Existing Master Work Item", "Type New Work Item"],
                    key=f"work_group_source_mode_{project_id}",
                    horizontal=True,
                )

                estimate_line_id = None
                task_name = ""
                trade_name = ""
                category_name = ""
                work_group_name = ""
                scope_description = ""
                contractor_name_default = ""
                estimated_price_default = 0.0
                latest_cost_row = None

                if source_mode == "Existing Project Work Item":
                    if lines_df.empty:
                        st.info("No estimate-based Work Items exist yet for this project.")
                    else:
                        source_labels = [f"{int(row.estimate_line_id)} | {row.task_name} | {row.trade_name}" for row in lines_df.itertuples()]
                        selected_source_label = st.selectbox("Choose Existing Project Work Item", source_labels, key=f"work_group_existing_line_{project_id}")
                        estimate_line_id = int(selected_source_label.split(" | ", 1)[0])
                        selected_line_row = lines_df[lines_df["estimate_line_id"] == estimate_line_id].iloc[0]
                        task_name = str(selected_line_row["task_name"])
                        trade_name = str(selected_line_row["trade_name"])
                        category_name = str(selected_line_row.get("category_name") or trade_name)
                        work_group_name = str(selected_line_row.get("work_group_name") or task_name)
                        scope_description = str(selected_line_row.get("scope_description") or "")
                        contractor_name_default = str(selected_line_row.get("contractor_name") or "")
                        estimated_price_default = float(selected_line_row.get("estimated_amount") or 0.0)
                        latest_cost_row = latest_cost_row_from_df(costs_df, project_id, task_name, trade_name, estimate_line_id)
                elif source_mode == "Existing Master Work Item":
                    if task_lookup_df.empty:
                        st.info("No master Work Items exist yet.")
                    else:
                        master_labels = [f"{int(row.id)} | {row.task_name} | {row.trade_name}" for row in task_lookup_df.itertuples()]
                        selected_master_label = st.selectbox("Choose Existing Master Work Item", master_labels, key=f"work_group_master_item_{project_id}")
                        selected_master_id = int(selected_master_label.split(" | ", 1)[0])
                        selected_master_row = task_lookup_df[task_lookup_df["id"] == selected_master_id].iloc[0]
                        task_name = str(selected_master_row["task_name"])
                        trade_name = str(selected_master_row["trade_name"])
                        category_name = trade_name
                        work_group_name = task_name
                        scope_description = latest_scope_for_task_name(task_name)
                        latest_cost_row = latest_cost_row_from_df(costs_df, project_id, task_name, trade_name, None)
                else:
                    wo1, wo2 = st.columns(2)
                    work_group_name = wo1.text_input('Work Group Name', key=f"work_group_custom_work_group_name_{project_id}")
                    trade_options = get_category_names()
                    trade_name = wo2.selectbox('Category of Labor', trade_options if trade_options else [""], key=f"work_group_custom_trade_{project_id}")
                    category_name = trade_name
                    task_name = st.text_input("New Work Item Name", key=f"work_group_custom_task_{project_id}")
                    scope_description = st.text_area("Scope Description", key=f"work_group_custom_scope_{project_id}", height=120)
                    latest_cost_row = latest_cost_row_from_df(costs_df, project_id, task_name, trade_name, None)

                if str(task_name).strip():
                    st.markdown('### Work Group Detail')
                    if source_mode == "Type New Work Item":
                        work_group_name_input = work_group_name or task_name
                        category_name_input = category_name or trade_name
                    else:
                        z0, zcat = st.columns(2)
                        work_group_name_input = z0.text_input(
                            'Work Group Name',
                            value=work_group_name or task_name,
                            key=f"work_group_name_input_{project_id}_{task_name}_{estimate_line_id or 0}",
                        )
                        category_options_for_wo = [""] + get_category_names()
                        category_default_for_wo = category_name if category_name in category_options_for_wo else ""
                        category_name_input = zcat.selectbox(
                            'Category of Labor',
                            category_options_for_wo,
                            index=category_options_for_wo.index(category_default_for_wo),
                            key=f"work_group_category_input_{project_id}_{task_name}_{estimate_line_id or 0}",
                        )
                    z1, z2 = st.columns(2)
                    z1.text_input("Work Item", value=task_name, disabled=True)
                    z2.text_input('Work Item Category of Labor', value=trade_name, disabled=True)
                    st.text_area("Scope", value=scope_description, disabled=(source_mode != "Type New Work Item"), key=f"work_group_scope_display_{project_id}", height=120)

                    contractor_names = get_contractor_names()
                    contractor_options = ["None selected"] + contractor_names
                    if latest_cost_row is not None and str(latest_cost_row.get("contractor_name") or "").strip():
                        contractor_name_default = str(latest_cost_row.get("contractor_name") or "")
                    contractor_index = contractor_options.index(contractor_name_default) if contractor_name_default in contractor_options else 0

                    latest_agreed_price = float(latest_cost_row["agreed_price"]) if latest_cost_row is not None else 0.0
                    latest_price_date = pd.to_datetime(latest_cost_row["entered_date"], errors="coerce") if latest_cost_row is not None else pd.NaT

                    w1, w2, w3 = st.columns(3)
                    contractor_name = w1.selectbox("Assigned Contractor", contractor_options, index=contractor_index, key=f"work_group_contractor_{project_id}_{task_name}")
                    due_date = w2.date_input("Due Date", value=datetime.now().date(), key=f"work_group_due_date_{project_id}_{task_name}")
                    status_value = w3.selectbox("Status", ["Open", "In Progress", "Complete"], index=0, key=f"work_group_status_{project_id}_{task_name}")

                    if latest_agreed_price > 0:
                        if pd.notna(latest_price_date):
                            st.caption(f"Prior agreed price entered on {latest_price_date.strftime('%m-%d-%Y')}.")
                        default_amount_to_be_paid = latest_agreed_price
                        estimated_price = 0.0
                    else:
                        default_amount_to_be_paid = float(estimated_price_default or 0.0)
                        estimated_price = float(estimated_price_default or 0.0)

                    amount_to_be_paid_input = st.number_input(
                        'Amount To Be Paid For Work Group',
                        min_value=0.0,
                        value=float(default_amount_to_be_paid or 0.0),
                        step=50.0,
                        key=f"work_group_amount_to_be_paid_{project_id}_{task_name}",
                    )
                    agreed_price_to_save = float(amount_to_be_paid_input or 0.0) if float(amount_to_be_paid_input or 0.0) > 0 else None

                    work_group_notes = st.text_area('Work Group Notes', key=f"work_group_notes_{project_id}_{task_name}", height=100)

                    st.markdown('### Work Group Photos')
                    uploaded_work_group_photos = st.file_uploader(
                        'Upload Work Group Photos',
                        type=["png", "jpg", "jpeg", "webp"],
                        accept_multiple_files=True,
                        key=f"work_group_create_photos_{project_id}_{task_name}_{estimate_line_id or 0}",
                    )

                    if st.button('Create Work Group', type="primary", key=f"create_work_group_btn_{project_id}_{task_name}_{estimate_line_id or 0}"):
                        contractor_id_value = get_contractor_id_by_name(contractor_name)
                        if not contractor_id_value:
                            st.error("Select a contractor for this work order.")
                        elif work_group_duplicate_exists(
                            project_id,
                            task_name,
                            trade_name,
                            category_name=str(category_name_input or category_name or trade_name),
                            work_group_name=str(work_group_name_input or task_name),
                        ):
                            st.error("A Work Group with this same Project, Category of Labor, Work Group Name, Work Item, and Work Item Category already exists. Open the existing Work Group or delete it before creating another one.")
                        else:
                            new_work_group_id = execute_returning_id(
                                """
                                INSERT INTO work_groups (
                                    project_id, estimate_line_id, work_group_name, category_name, task_name, trade_name, scope_description,
                                    contractor_id, agreed_price, estimated_price, contractor_requested_price, amount_to_be_paid, due_date, status, notes,
                                    work_group_address, work_group_unit_number, created_at, modified_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                                """,
                                (
                                    project_id,
                                    estimate_line_id,
                                    str(work_group_name_input or task_name).strip(),
                                    str(category_name_input or category_name or trade_name).strip(),
                                    task_name,
                                    trade_name,
                                    scope_description,
                                    contractor_id_value,
                                    agreed_price_to_save,
                                    estimated_price if agreed_price_to_save is None else None,
                                    None,
                                    agreed_price_to_save,
                                    due_date,
                                    status_value,
                                    str(work_group_notes).strip(),
                                    str(work_group_address or "").strip(),
                                    str(work_group_unit_number or "").strip(),
                                ),
                            )
                            if new_work_group_id:
                                set_order_number("work_groups", int(new_work_group_id), "WG")
                                save_work_group_photos(
                                    int(new_work_group_id),
                                    uploaded_work_group_photos,
                                    uploaded_by=str(st.session_state.get("logged_in_user", "") or ""),
                                )
                            st.success("Work order created.")
                            st.rerun()


        with tab_rmr:
            st.markdown("### Assign Existing RMRs To Work Group")
            st.caption("Use this tab when RMR work items have already been created and you want to package them into one contractor Work Group.")

            all_rmrs_for_grouping = rmr_records_df(include_deleted=False, property_name="All", status="All")
            if all_rmrs_for_grouping.empty:
                st.info("No RMR records found yet.")
            else:
                rg1, rg2, rg3 = st.columns(3)
                rmr_property_options = ["All Properties"] + sorted([x for x in all_rmrs_for_grouping["property_name"].dropna().astype(str).unique().tolist() if x], key=lambda x: x.lower())
                rmr_group_property_filter = rg1.selectbox("Filter RMR Property", rmr_property_options, key="assign_rmr_to_wg_property")
                rmr_group_filter_mode = rg2.selectbox("Show", ["Unassigned RMRs", "All RMRs", "Already Assigned RMRs"], key="assign_rmr_to_wg_show")
                rmr_group_search_text = rg3.text_input("Search RMRs", key="assign_rmr_to_wg_search")

                rmr_candidates = all_rmrs_for_grouping.copy()
                if rmr_group_property_filter != "All Properties":
                    rmr_candidates = rmr_candidates[rmr_candidates["property_name"].astype(str) == rmr_group_property_filter].copy()
                if rmr_group_filter_mode == "Unassigned RMRs":
                    rmr_candidates = rmr_candidates[pd.to_numeric(rmr_candidates.get("work_group_id", 0), errors="coerce").fillna(0).astype(int) == 0].copy()
                elif rmr_group_filter_mode == "Already Assigned RMRs":
                    rmr_candidates = rmr_candidates[pd.to_numeric(rmr_candidates.get("work_group_id", 0), errors="coerce").fillna(0).astype(int) > 0].copy()
                if rmr_group_search_text.strip():
                    search_lower = rmr_group_search_text.strip().lower()
                    searchable = (
                        rmr_candidates.get("rmr_code", "").astype(str) + " "
                        + rmr_candidates.get("property_name", "").astype(str) + " "
                        + rmr_candidates.get("address", "").astype(str) + " "
                        + rmr_candidates.get("location_identifier", "").astype(str) + " "
                        + rmr_candidates.get("work_item_name", "").astype(str) + " "
                        + rmr_candidates.get("scope_description", "").astype(str) + " "
                        + rmr_candidates.get("notes", "").astype(str)
                    ).str.lower()
                    rmr_candidates = rmr_candidates[searchable.str.contains(search_lower, na=False)].copy()

                if rmr_candidates.empty:
                    st.info("No RMRs match the current filters.")
                else:
                    rmr_select_display = rmr_candidates[[
                        "id", "rmr_code", "property_name", "address", "location_identifier", "work_item_name",
                        "category_name", "linked_work_group_name", "contractor_name", "labor_budget", "materials_budget", "budget_timeframe", "budget_status"
                    ]].copy()
                    rmr_select_display["Select"] = False
                    rmr_select_display = rmr_select_display.rename(columns={
                        "id": "RMR ID",
                        "rmr_code": "RMR #",
                        "property_name": "Property",
                        "address": "Address",
                        "location_identifier": "Location",
                        "work_item_name": "Work Item",
                        "category_name": "Category",
                        "linked_work_group_name": "Current Work Group",
                        "contractor_name": "Contractor",
                        "labor_budget": "Labor Budget",
                        "materials_budget": "Materials Budget",
                        "budget_timeframe": "Timeframe",
                        "budget_status": "Budget Status",
                    })
                    ordered_cols = ["Select"] + [c for c in rmr_select_display.columns if c != "Select"]
                    rmr_select_display = rmr_select_display[ordered_cols]
                    edited_rmr_selection = st.data_editor(
                        rmr_select_display,
                        use_container_width=True,
                        hide_index=True,
                        key="assign_rmr_to_work_group_editor",
                        column_config={
                            "Select": st.column_config.CheckboxColumn("Select", default=False),
                            "RMR ID": st.column_config.NumberColumn("RMR ID", disabled=True),
                        },
                        disabled=[c for c in rmr_select_display.columns if c != "Select"],
                    )
                    selected_rmr_ids = edited_rmr_selection.loc[edited_rmr_selection["Select"] == True, "RMR ID"].astype(int).tolist()

                    st.markdown("### Create Work Group From Selected RMRs")
                    if selected_rmr_ids:
                        selected_summary_df = rmr_candidates[rmr_candidates["id"].astype(int).isin(selected_rmr_ids)].copy()
                        st.success(f"{len(selected_rmr_ids)} RMR(s) selected.")
                        st.caption(
                            f"Selected Budget: Labor ${pd.to_numeric(selected_summary_df['labor_budget'], errors='coerce').fillna(0).sum():,.2f} | "
                            f"Materials ${pd.to_numeric(selected_summary_df['materials_budget'], errors='coerce').fillna(0).sum():,.2f}"
                        )
                    else:
                        st.info("Select one or more RMRs above to create a Work Group.")

                    if selected_rmr_ids:
                        project_labels_for_rmr = [f"{int(row.id)} | {row.project_name}" for row in active_projects_df.itertuples()]
                        rwm1, rwm2 = st.columns(2)
                        selected_rmr_project_label = rwm1.selectbox("Project For Work Group", project_labels_for_rmr, key="assign_rmr_wg_project")
                        selected_rmr_project_row = get_project_registry_row_from_label(selected_rmr_project_label)
                        selected_rmr_project_id = int(selected_rmr_project_row["id"]) if selected_rmr_project_row is not None else None
                        rmr_work_group_name = rwm2.text_input("New Work Group Name", key="assign_rmr_wg_name")

                        rwm3, rwm4, rwm5 = st.columns(3)
                        contractor_options_for_rmr = ["None selected"] + get_contractor_names()
                        selected_rmr_contractor = rwm3.selectbox("Assigned Contractor", contractor_options_for_rmr, key="assign_rmr_wg_contractor")
                        selected_rmr_contractor_id = get_contractor_id_by_name(selected_rmr_contractor) if selected_rmr_contractor != "None selected" else None
                        selected_rmr_due_date = rwm4.date_input("Due Date", value=datetime.now().date(), key="assign_rmr_wg_due_date")
                        selected_rmr_status = rwm5.selectbox("Status", ["Open", "In Progress", "Complete"], key="assign_rmr_wg_status")

                        rmr_wg_notes = st.text_area("Work Group Notes", key="assign_rmr_wg_notes", height=90)
                        copy_rmr_photos = st.checkbox("Copy RMR photos into Work Group", value=True, key="assign_rmr_wg_copy_photos")

                        if st.button("Create Work Group From Selected RMRs", type="primary", key="assign_rmr_wg_create_btn"):
                            if not selected_rmr_project_id:
                                st.error("Choose a project for this Work Group.")
                            elif not str(rmr_work_group_name or "").strip():
                                st.error("Enter a Work Group Name.")
                            else:
                                new_rmr_work_group_id = create_work_group_from_rmrs(
                                    selected_rmr_ids,
                                    int(selected_rmr_project_id),
                                    str(rmr_work_group_name or "").strip(),
                                    contractor_id=int(selected_rmr_contractor_id) if selected_rmr_contractor_id else None,
                                    due_date=selected_rmr_due_date,
                                    status=selected_rmr_status,
                                    notes=str(rmr_wg_notes or "").strip(),
                                    copy_photos=copy_rmr_photos,
                                )
                                if new_rmr_work_group_id:
                                    st.success(f"Work Group WG{int(new_rmr_work_group_id)} created and selected RMRs were assigned.")
                                    st.rerun()
                                else:
                                    st.error("Could not create Work Group from selected RMRs.")


        with tab2:
            st.markdown('### Edit Work Group')
            st.caption("Select an existing Work Group and update contractor, due date, status, price, or notes.")

            edit_filter_mode = st.radio(
                'Edit Work Group Filter',
                ["By Project", "By Contractor"],
                horizontal=True,
                key="edit_work_group_filter_mode",
            )
            edit_df = pd.DataFrame()

            if edit_filter_mode == "By Project":
                edit_project_labels = [f"{int(row.id)} | {row.project_name}" for row in active_projects_df.itertuples()]
                selected_edit_project_label = st.selectbox(
                    "Choose Project",
                    edit_project_labels,
                    key="edit_work_group_project_select",
                )
                selected_edit_project_row = get_project_registry_row_from_label(selected_edit_project_label)
                if selected_edit_project_row is not None:
                    edit_df = work_groups_df(project_id=int(selected_edit_project_row["id"]))
            else:
                edit_contractor_names = get_contractor_names()
                if edit_contractor_names:
                    selected_edit_contractor_name = st.selectbox(
                        "Choose Contractor",
                        edit_contractor_names,
                        key="edit_work_group_contractor_select",
                    )
                    selected_edit_contractor_id = get_contractor_id_by_name(selected_edit_contractor_name)
                    edit_df = work_groups_df(contractor_id=selected_edit_contractor_id)
                else:
                    st.info("No contractors found.")

            if edit_df.empty:
                st.info('No work groups found to edit.')
            else:
                edit_labels = [
                    f"{row.order_number} | {int(row.id)} | {row.project_name} | {row.work_group_name} | {row.task_name}"
                    for row in edit_df.itertuples()
                ]
                selected_edit_label = st.selectbox(
                    'Choose Work Group To Edit',
                    edit_labels,
                    key="edit_work_group_select",
                )
                selected_work_group_id = int(selected_edit_label.split(" | ")[1])
                selected_work_group_row = edit_df[edit_df["id"] == selected_work_group_id].iloc[0]

                st.text_input(
                    'Work Group Number',
                    value=str(selected_work_group_row.get("order_number") or ""),
                    disabled=True,
                    key=f"edit_work_group_order_number_{selected_work_group_id}",
                )

                st.text_input(
                    "Project",
                    value=str(selected_work_group_row.get("project_name") or ""),
                    disabled=True,
                    key=f"edit_work_group_project_name_{selected_work_group_id}",
                )
                e0, ecat = st.columns(2)
                updated_work_group_name = e0.text_input(
                    'Work Group Name',
                    value=str(selected_work_group_row.get("work_group_name") or ""),
                    key=f"edit_work_group_name_{selected_work_group_id}",
                )
                category_options_edit = [""] + get_category_names()
                current_category_edit = str(selected_work_group_row.get("category_name") or "")
                updated_category_name = ecat.selectbox(
                    'Category of Labor',
                    category_options_edit,
                    index=category_options_edit.index(current_category_edit) if current_category_edit in category_options_edit else 0,
                    key=f"edit_work_group_category_{selected_work_group_id}",
                )
                e1, e2 = st.columns(2)
                e1.text_input(
                    "Work Item",
                    value=str(selected_work_group_row.get("task_name") or ""),
                    disabled=True,
                    key=f"edit_work_group_task_name_{selected_work_group_id}",
                )
                e2.text_input(
                    'Work Item Category of Labor',
                    value=str(selected_work_group_row.get("trade_name") or ""),
                    disabled=True,
                    key=f"edit_work_group_trade_name_{selected_work_group_id}",
                )

                current_scope = str(selected_work_group_row.get("scope_description") or "")
                updated_scope = st.text_area(
                    "Scope",
                    value=current_scope,
                    height=120,
                    key=f"edit_work_group_scope_{selected_work_group_id}",
                )

                contractor_names = get_contractor_names()
                contractor_options = ["None selected"] + contractor_names
                current_contractor = str(selected_work_group_row.get("contractor_name") or "").strip()
                contractor_index = contractor_options.index(current_contractor) if current_contractor in contractor_options else 0

                c1, c2, c3 = st.columns(3)
                updated_contractor = c1.selectbox(
                    "Assigned Contractor",
                    contractor_options,
                    index=contractor_index,
                    key=f"edit_work_group_contractor_{selected_work_group_id}",
                )

                current_due_date = pd.to_datetime(selected_work_group_row.get("due_date"), errors="coerce")
                updated_due_date = c2.date_input(
                    "Due Date",
                    value=current_due_date.date() if pd.notna(current_due_date) else datetime.now().date(),
                    key=f"edit_work_group_due_date_{selected_work_group_id}",
                )

                status_options = ["Open", "In Progress", "Complete"]
                current_status = str(selected_work_group_row.get("status") or "Open")
                status_index = status_options.index(current_status) if current_status in status_options else 0
                updated_status = c3.selectbox(
                    "Status",
                    status_options,
                    index=status_index,
                    key=f"edit_work_group_status_{selected_work_group_id}",
                )
                render_contractor_priority_legend()
                pri_col, intent_col = st.columns(2)
                current_wg_priority = str(selected_work_group_row.get("contractor_priority") or "3 - Quote Only")
                if current_wg_priority not in CONTRACTOR_PRIORITY_OPTIONS:
                    current_wg_priority = "3 - Quote Only"
                updated_contractor_priority = pri_col.selectbox(
                    "Contractor Priority",
                    CONTRACTOR_PRIORITY_OPTIONS,
                    index=CONTRACTOR_PRIORITY_OPTIONS.index(current_wg_priority),
                    key=f"edit_work_group_priority_{selected_work_group_id}",
                )
                current_wg_intent = str(selected_work_group_row.get("owner_intent") or "Quote Only")
                if current_wg_intent not in OWNER_INTENT_OPTIONS:
                    current_wg_intent = "Quote Only"
                updated_owner_intent = intent_col.selectbox(
                    "Owner Intent",
                    OWNER_INTENT_OPTIONS,
                    index=OWNER_INTENT_OPTIONS.index(current_wg_intent),
                    key=f"edit_work_group_owner_intent_{selected_work_group_id}",
                )

                price_col1, price_col2 = st.columns(2)
                current_requested_price = float(selected_work_group_row.get("contractor_requested_price") or 0.0)
                current_amount_to_be_paid = float(selected_work_group_row.get("amount_to_be_paid") or selected_work_group_row.get("agreed_price") or 0.0)
                updated_requested_price = price_col1.number_input(
                    "Contractor Requested Price",
                    min_value=0.0,
                    value=current_requested_price,
                    step=50.0,
                    disabled=True,
                    key=f"edit_work_group_contractor_requested_price_{selected_work_group_id}",
                )
                updated_amount_to_be_paid = price_col2.number_input(
                    'Amount To Be Paid For Work Group',
                    min_value=0.0,
                    value=current_amount_to_be_paid,
                    step=50.0,
                    key=f"edit_work_group_amount_to_be_paid_{selected_work_group_id}",
                )

                updated_notes = st.text_area(
                    'Work Group Notes',
                    value=str(selected_work_group_row.get("notes") or ""),
                    height=120,
                    key=f"edit_work_group_notes_{selected_work_group_id}",
                )

                render_work_group_contractor_notes(selected_work_group_id)

                st.markdown("### Work Group Contractor Quote Request")
                st.caption("Use this when the RMRs have been grouped and you want the contractor to quote the entire Work Group instead of quoting each RMR separately.")
                group_quote_note_default = (
                    f"Please quote this entire Work Group: {updated_work_group_name or selected_work_group_row.get('work_group_name', '')}. "
                    f"Priority: {updated_contractor_priority}. Intent: {updated_owner_intent}."
                )
                group_quote_note = st.text_area(
                    "Group Quote Request Notes",
                    value=group_quote_note_default,
                    height=90,
                    key=f"edit_work_group_quote_request_note_{selected_work_group_id}",
                )
                if st.button("Request Quote For Entire Work Group", type="primary", key=f"request_group_quote_{selected_work_group_id}"):
                    chosen_contractor_id = get_contractor_id_by_name(updated_contractor) if updated_contractor != "None selected" else None
                    if not chosen_contractor_id:
                        st.error("Choose an Assigned Contractor before requesting a Work Group quote.")
                    else:
                        execute(
                            """
                            UPDATE work_groups
                            SET contractor_id = ?, contractor_requested_price = NULL, modified_at = NOW()
                            WHERE id = ?
                            """,
                            (int(chosen_contractor_id), int(selected_work_group_id)),
                        )
                        add_work_group_contractor_note(
                            work_group_id=int(selected_work_group_id),
                            contractor_id=int(chosen_contractor_id),
                            note_text=str(group_quote_note or "Please quote this entire Work Group.").strip(),
                            entered_by=str(st.session_state.get("logged_in_user", "") or "Owner"),
                        )
                        st.success("Work Group quote request saved. The assigned contractor can see this Work Group in the contractor portal and enter a Contractor Requested Price.")
                        st.rerun()

                st.markdown('### Work Group Photos')
                additional_work_group_photos = st.file_uploader(
                    'Add Work Group Photos',
                    type=["png", "jpg", "jpeg", "webp"],
                    accept_multiple_files=True,
                    key=f"edit_work_group_photos_{selected_work_group_id}",
                )
                render_work_group_photos_section(selected_work_group_id, section_key="edit")

                if st.button('Save Work Group Changes', type="primary", key=f"save_work_group_changes_{selected_work_group_id}"):
                    updated_contractor_id = get_contractor_id_by_name(updated_contractor)
                    if not updated_contractor_id:
                        st.error("Select a contractor for this work order.")
                    elif work_group_duplicate_exists(
                        int(selected_work_group_row.get("project_id") or 0),
                        str(selected_work_group_row.get("task_name") or ""),
                        str(selected_work_group_row.get("trade_name") or ""),
                        exclude_work_group_id=selected_work_group_id,
                        category_name=str(updated_category_name or selected_work_group_row.get("category_name") or selected_work_group_row.get("trade_name") or ""),
                        work_group_name=str(updated_work_group_name or selected_work_group_row.get("work_group_name") or selected_work_group_row.get("task_name") or ""),
                    ):
                        st.error('Another Work Group already exists with this same Project, Category of Labor, Work Group Name, Work Item, and Work Item Category of Labor.')
                    else:
                        save_work_group_photos(
                            selected_work_group_id,
                            additional_work_group_photos,
                            uploaded_by=str(st.session_state.get("logged_in_user", "") or ""),
                        )
                        execute(
                            """
                            UPDATE work_groups
                            SET contractor_id = ?,
                                due_date = ?,
                                status = ?,
                                agreed_price = ?,
                                estimated_price = ?,
                                amount_to_be_paid = ?,
                                work_group_name = ?,
                                category_name = ?,
                                scope_description = ?,
                                notes = ?,
                                contractor_priority = ?,
                                owner_intent = ?,
                                modified_at = NOW()
                            WHERE id = ?
                            """,
                            (
                                updated_contractor_id,
                                str(updated_due_date),
                                updated_status,
                                float(updated_amount_to_be_paid or 0.0) if float(updated_amount_to_be_paid or 0.0) > 0 else None,
                                None,
                                float(updated_amount_to_be_paid or 0.0) if float(updated_amount_to_be_paid or 0.0) > 0 else None,
                                str(updated_work_group_name or "").strip(),
                                str(updated_category_name or "").strip(),
                                str(updated_scope).strip(),
                                str(updated_notes).strip(),
                                str(updated_contractor_priority or "3 - Quote Only"),
                                str(updated_owner_intent or "Quote Only"),
                                selected_work_group_id,
                            ),
                        )
                        st.success("Work order updated.")
                        st.rerun()

                st.markdown("---")
                st.subheader("Delete This Work Group")
                st.warning('Use this only for mistakes or duplicate Work Groups.')
                edit_delete_key = f"confirm_delete_work_group_edit_{selected_work_group_id}"
                if edit_delete_key not in st.session_state:
                    st.session_state[edit_delete_key] = False

                if not st.session_state[edit_delete_key]:
                    if st.button("Delete This Work Group", type="secondary", key=f"delete_work_group_edit_btn_{selected_work_group_id}"):
                        st.session_state[edit_delete_key] = True
                        st.rerun()
                else:
                    st.warning("Delete this Work Group permanently? This will also delete related photos and contractor notes.")
                    ed1, ed2 = st.columns(2)
                    if ed1.button("Yes, Delete Work Group", type="primary", key=f"confirm_delete_work_group_edit_yes_{selected_work_group_id}"):
                        delete_work_group(selected_work_group_id)
                        st.session_state[edit_delete_key] = False
                        st.success('Work Group deleted.')
                        st.rerun()
                    if ed2.button("Cancel Delete", key=f"confirm_delete_work_group_edit_cancel_{selected_work_group_id}"):
                        st.session_state[edit_delete_key] = False
                        st.rerun()


        with tab3:
            st.markdown('### Estimates → Convert To Work Groups')
            st.caption('Choose an existing estimate and convert selected estimate Work Items into actual Work Groups.')

            project_labels = [f"{int(row.id)} | {row.project_name}" for row in active_projects_df.itertuples()]
            selected_convert_project_label = st.selectbox(
                "Choose Project",
                project_labels,
                key="convert_work_group_project_select",
            )
            selected_convert_project_row = get_project_registry_row_from_label(selected_convert_project_label)

            if selected_convert_project_row is not None:
                convert_project_id = int(selected_convert_project_row["id"])
                estimates_to_convert_df = fetch_df(
                    """
                    SELECT
                        e.id AS estimate_id,
                        COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
                        COALESCE(e.estimate_address, '') AS estimate_address,
                        e.created_at,
                        e.modified_at
                    FROM estimates e
                    WHERE COALESCE(e.project_id, 0) = ?
                      AND COALESCE(e.active, TRUE) = TRUE
                    ORDER BY e.modified_at DESC NULLS LAST, e.id DESC
                    """,
                    (convert_project_id,),
                )

                if estimates_to_convert_df.empty:
                    st.info("No active estimates found for this project.")
                else:
                    estimate_labels = [
                        f"{int(row.estimate_id)} | {row.estimate_name}"
                        for row in estimates_to_convert_df.itertuples()
                    ]
                    selected_convert_estimate_label = st.selectbox(
                        "Choose Estimate To Convert",
                        estimate_labels,
                        key="convert_work_group_estimate_select",
                    )
                    selected_convert_estimate_id = int(selected_convert_estimate_label.split(" | ", 1)[0])
                    conversion_lines_df = estimate_lines_for_work_group_conversion_df(selected_convert_estimate_id)

                    if conversion_lines_df.empty:
                        st.info("This estimate has no Work Items to convert.")
                    else:
                        display_conversion_df = conversion_lines_df.copy()
                        display_conversion_df['Already Work Grouped'] = display_conversion_df["existing_work_group_count"].apply(lambda v: "Yes" if int(v or 0) > 0 else "No")
                        display_conversion_df["Price"] = display_conversion_df.apply(
                            lambda r: float(r["agreed_price"]) if float(r.get("agreed_price") or 0) > 0
                            else float(r["approved_final_cost"]) if float(r.get("approved_final_cost") or 0) > 0
                            else float(r["manual_repair_amount"]) if float(r.get("manual_repair_amount") or 0) > 0
                            else float(r.get("total_labor_cost") or 0),
                            axis=1,
                        )
                        st.dataframe(
                            display_conversion_df[[
                                "estimate_line_id",
                                "work_group_name",
                                "task_name",
                                "category_name",
                                "contractor_name",
                                "Price",
                                'Already Work Grouped',
                            ]].rename(columns={
                                "estimate_line_id": "Estimate Line ID",
                                "work_group_name": 'Work Group Name',
                                "task_name": "Work Item",
                                "category_name": 'Category of Labor',
                                "contractor_name": "Contractor",
                            }),
                            use_container_width=True,
                            hide_index=True,
                        )

                        available_lines_df = conversion_lines_df[conversion_lines_df["existing_work_group_count"].fillna(0).astype(int) == 0].copy()
                        if available_lines_df.empty:
                            st.info('All Work Items on this estimate already have Work Groups.')
                        else:
                            selectable_labels = [
                                f"{int(row.estimate_line_id)} | {row.work_group_name or row.task_name} | {row.task_name} | {row.category_name or row.trade_name}"
                                for row in available_lines_df.itertuples()
                            ]
                            selected_line_labels = st.multiselect(
                                "Choose Estimate Work Items To Convert",
                                selectable_labels,
                                default=selectable_labels,
                                key=f"convert_selected_lines_{selected_convert_estimate_id}",
                            )

                            c1, c2 = st.columns(2)
                            default_due_date = c1.date_input(
                                'Due Date For Created Work Groups',
                                value=datetime.now().date(),
                                key=f"convert_default_due_date_{selected_convert_estimate_id}",
                            )
                            default_status = c2.selectbox(
                                'Status For Created Work Groups',
                                ["Open", "In Progress", "Complete"],
                                index=0,
                                key=f"convert_default_status_{selected_convert_estimate_id}",
                            )

                            conversion_notes = st.text_area(
                                'Work Group Notes',
                                placeholder="Optional note added to all Work Groups created from this estimate.",
                                height=100,
                                key=f"convert_work_group_notes_{selected_convert_estimate_id}",
                            )

                            copy_photos = st.checkbox(
                                'Copy estimate photos to Work Groups',
                                value=True,
                                key=f"convert_copy_photos_{selected_convert_estimate_id}",
                            )

                            if st.button("Create Work Groups From Selected Estimate Items", type="primary", key=f"convert_estimate_to_work_groups_btn_{selected_convert_estimate_id}"):
                                if not selected_line_labels:
                                    st.error("Select at least one estimate Work Item to convert.")
                                else:
                                    created_count = 0
                                    skipped_count = 0
                                    missing_contractor_count = 0
                                    selected_line_ids = [int(label.split(" | ", 1)[0]) for label in selected_line_labels]

                                    for line_id in selected_line_ids:
                                        line_rows = available_lines_df[available_lines_df["estimate_line_id"] == line_id]
                                        if line_rows.empty:
                                            skipped_count += 1
                                            continue
                                        line = line_rows.iloc[0]
                                        contractor_id_value = int(line.get("contractor_id") or 0)
                                        if contractor_id_value <= 0:
                                            missing_contractor_count += 1
                                            continue
                                        if work_group_duplicate_exists(
                                            convert_project_id,
                                            str(line.get("task_name") or ""),
                                            str(line.get("trade_name") or ""),
                                            category_name=str(line.get("category_name") or line.get("trade_name") or ""),
                                            work_group_name=str(line.get("work_group_name") or line.get("task_name") or ""),
                                        ):
                                            skipped_count += 1
                                            continue

                                        agreed_price_value = float(line.get("agreed_price") or 0.0)
                                        approved_final_value = float(line.get("approved_final_cost") or 0.0)
                                        manual_value = float(line.get("manual_repair_amount") or 0.0)
                                        total_labor_value = float(line.get("total_labor_cost") or 0.0)
                                        estimated_price_value = approved_final_value if approved_final_value > 0 else manual_value if manual_value > 0 else total_labor_value

                                        new_work_group_id = execute_returning_id(
                                            """
                                            INSERT INTO work_groups (
                                                project_id,
                                                estimate_line_id,
                                                work_group_name,
                                                category_name,
                                                task_name,
                                                trade_name,
                                                scope_description,
                                                contractor_id,
                                                agreed_price,
                                                estimated_price,
                                                contractor_requested_price,
                                                amount_to_be_paid,
                                                due_date,
                                                status,
                                                notes,
                                                created_at,
                                                modified_at
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                                            """,
                                            (
                                                convert_project_id,
                                                int(line["estimate_line_id"]),
                                                str(line.get("work_group_name") or line.get("task_name") or ""),
                                                str(line.get("category_name") or line.get("trade_name") or ""),
                                                str(line.get("task_name") or ""),
                                                str(line.get("trade_name") or ""),
                                                str(line.get("scope_description") or ""),
                                                contractor_id_value,
                                                agreed_price_value if agreed_price_value > 0 else estimated_price_value if estimated_price_value > 0 else None,
                                                estimated_price_value if agreed_price_value <= 0 and estimated_price_value > 0 else None,
                                                None,
                                                agreed_price_value if agreed_price_value > 0 else estimated_price_value if estimated_price_value > 0 else None,
                                                str(default_due_date),
                                                default_status,
                                                str(conversion_notes or "").strip(),
                                            ),
                                        )
                                        if new_work_group_id:
                                            set_order_number("work_groups", int(new_work_group_id), "WG")
                                            created_count += 1
                                            if copy_photos:
                                                copy_estimate_line_photos_to_work_group(
                                                    estimate_line_id=int(line["estimate_line_id"]),
                                                    work_group_id=int(new_work_group_id),
                                                    uploaded_by=str(st.session_state.get("logged_in_user", "") or ""),
                                                )

                                    if created_count:
                                        st.success(f"Created {created_count} Work Group(s) from the selected estimate items.")
                                    if missing_contractor_count:
                                        st.warning(f"{missing_contractor_count} item(s) were skipped because no contractor was assigned.")
                                    if skipped_count:
                                        st.info(f"{skipped_count} item(s) were skipped because they were no longer available.")
                                    st.rerun()


        with tab4:
            filter_mode = st.radio("Report Filter", ["By Project", "By Contractor"], horizontal=True, key="work_group_report_mode")
            report_df = pd.DataFrame()

            if filter_mode == "By Project":
                project_labels = [f"{int(row.id)} | {row.project_name}" for row in active_projects_df.itertuples()]
                selected_project_label = st.selectbox("Choose Project For Report", project_labels, key="work_group_report_project")
                selected_project_row = get_project_registry_row_from_label(selected_project_label)
                if selected_project_row is not None:
                    report_df = work_groups_df(project_id=int(selected_project_row["id"]))
            else:
                contractor_names = get_contractor_names()
                if contractor_names:
                    selected_contractor_name = st.selectbox("Choose Contractor For Report", contractor_names, key="work_group_report_contractor")
                    selected_contractor_id = get_contractor_id_by_name(selected_contractor_name)
                    report_df = work_groups_df(contractor_id=selected_contractor_id)
                else:
                    st.info("No contractors found.")

            if report_df.empty:
                st.info('No work groups found for this report.')
            else:
                display_df = report_df.copy()
                for col in ["due_date", "created_at", "modified_at"]:
                    display_df[col] = pd.to_datetime(display_df[col], errors="coerce").dt.strftime("%m-%d-%Y")
                display_df["Contractor Requested Price"] = display_df["contractor_requested_price"].fillna(0).astype(float)
                display_df["Amount To Be Paid"] = display_df["amount_to_be_paid"].fillna(0).astype(float)
                st.dataframe(
                    display_df[[
                        "order_number", "id", "project_name", "category_name", "work_group_name", "task_name", "trade_name", "contractor_name",
                        "Contractor Requested Price", "Amount To Be Paid", "due_date", "status", "notes", "created_at", "modified_at"
                    ]].rename(columns={
                        "order_number": "Order Number",
                        "id": 'Work Group ID',
                        "project_name": "Project",
                        "category_name": 'Category of Labor',
                        "work_group_name": 'Work Group Name',
                        "task_name": "Work Item",
                        "trade_name": 'Work Item Category of Labor',
                        "contractor_name": "Contractor",
                        "due_date": "Due Date",
                        "status": "Status",
                        "notes": "Notes",
                        "created_at": "Created",
                        "modified_at": "Modified",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

                st.markdown("---")
                st.subheader("Delete Work Group")
                delete_work_group_labels = [
                    f"{row.order_number} | {int(row.id)} | {row.project_name} | {row.work_group_name} | {row.task_name}"
                    for row in report_df.itertuples()
                ]
                if delete_work_group_labels:
                    selected_delete_work_group_label = st.selectbox(
                        'Choose Work Group To Delete',
                        delete_work_group_labels,
                        key="work_groups_delete_select",
                    )
                    selected_delete_work_group_id = int(selected_delete_work_group_label.split(" | ")[1])
                    delete_work_group_confirm_key = f"confirm_delete_work_group_{selected_delete_work_group_id}"
                    if delete_work_group_confirm_key not in st.session_state:
                        st.session_state[delete_work_group_confirm_key] = False

                    if not st.session_state[delete_work_group_confirm_key]:
                        if st.button("Delete Work Group", type="secondary", key=f"delete_work_group_btn_{selected_delete_work_group_id}"):
                            st.session_state[delete_work_group_confirm_key] = True
                            st.rerun()
                    else:
                        st.warning("Delete this Work Group permanently?")
                        d1, d2 = st.columns(2)
                        if d1.button("Yes, Delete Work Group", type="primary", key=f"confirm_delete_work_group_yes_{selected_delete_work_group_id}"):
                            delete_work_group(selected_delete_work_group_id)
                            st.session_state[delete_work_group_confirm_key] = False
                            st.success('Work Group deleted.')
                            st.rerun()
                        if d2.button("Cancel Delete", key=f"confirm_delete_work_group_cancel_{selected_delete_work_group_id}"):
                            st.session_state[delete_work_group_confirm_key] = False
                            st.rerun()


# -----------------------------
# Project Materials
# -----------------------------
elif page == "Project Materials":
    st.subheader("Project Materials")
    st.caption("Store materials notes and files for each project.")
    if current_role == "Contractor":
        projects_df = project_registry_active_df()
    else:
        projects_df = project_registry_all_df()
    if projects_df.empty:
        st.info("No projects found yet.")
    else:
        project_labels = [f"{int(row.id)} | {row.project_name}" for row in projects_df.itertuples()]
        selected_project_label = st.selectbox("Choose Project", project_labels, key="project_materials_select")
        selected_project_row = get_project_registry_row_from_label(selected_project_label)
        if selected_project_row is not None:
            project_id = int(selected_project_row["id"])
            c1, c2 = st.columns(2)
            c1.text_input("Project Name", value=str(selected_project_row.get("project_name") or ""), disabled=True)
            c2.text_input("Project ID", value=str(project_id), disabled=True)
            st.text_input("Project Address", value=str(selected_project_row.get("project_address") or ""), disabled=True)
            materials_notes = st.text_area("Materials List / Notes", value=str(selected_project_row.get("materials_notes") or ""), height=180)
            uploaded_material_files = st.file_uploader(
                "Upload Materials Files",
                accept_multiple_files=True,
                key=f"project_materials_upload_{project_id}",
            )
            if st.button("Save Project Materials", type="primary"):
                execute("UPDATE project_registry SET materials_notes = ?, modified_at = NOW() WHERE id = ?", (str(materials_notes).strip(), project_id))
                save_project_material_files(project_id, uploaded_material_files)
                st.cache_data.clear()
                st.success("Project materials saved.")
                st.rerun()

            existing_material_files = project_material_files(project_id)
            if existing_material_files:
                st.markdown("### Saved Material Files")
                file_rows = []
                for file_item in existing_material_files:
                    data = file_item.get("bytes")
                    if not data and file_item.get("blob_name"):
                        data = download_blob_bytes(str(file_item.get("blob_name")))
                    st.download_button(
                        label=f"Download {file_item.get('filename')}",
                        data=data or b"",
                        file_name=str(file_item.get("filename") or "file"),
                        mime=str(file_item.get("content_type") or "application/octet-stream"),
                        key=f"project_material_download_{file_item.get('id')}",
                    )

# -----------------------------
# Tasks
# -----------------------------
elif page == "Work Items":
    st.subheader("Work Item Manager")

    trades_df = fetch_df("SELECT id, name FROM trades ORDER BY LOWER(name)")

    tab1, tab2, tab3, tab4 = st.tabs(["Add Work Item", "Edit Work Item", "Delete Work Item", "Review Work Item Scopes"])

    with tab1:
        scope_template_source_df = fetch_df(
            """
            SELECT
                st.id,
                COALESCE(st.template_name, '') AS template_name,
                COALESCE(st.scope_description, '') AS scope_description,
                COALESCE(t.name, '') AS source_work_item,
                COALESCE(tr.name, '') AS trade_name
            FROM scope_templates st
            LEFT JOIN tasks t ON t.id = st.task_id
            LEFT JOIN trades tr ON tr.id = t.trade_id
            WHERE COALESCE(st.active, TRUE) = TRUE
            ORDER BY LOWER(COALESCE(st.template_name, '')), LOWER(COALESCE(t.name, '')), LOWER(COALESCE(tr.name, ''))
            """
        )

        with st.form("add_task_form"):
            c1, c2 = st.columns(2)
            task_name = c1.text_input("Work Item name")
            trade_name = c2.selectbox('Category of Labor', trades_df["name"].tolist(), key="add_task_trade")

            scope_options = ["Select Scope"]
            scope_option_map = {"Select Scope": None}
            if not scope_template_source_df.empty:
                for _, row in scope_template_source_df.iterrows():
                    label = f"{row['template_name']} | {row['source_work_item']} | {row['trade_name']}"
                    scope_options.append(label)
                    scope_option_map[label] = int(row["id"])

            selected_scope_label = st.selectbox(
                "Scope To Attach To This Work Item",
                scope_options,
                key="add_task_scope_template",
                help="Choose the scope template that should be tied to this new Work Item.",
            )

            selected_scope_description = ""
            selected_scope_template_id = scope_option_map.get(selected_scope_label)
            if selected_scope_template_id:
                scope_row = scope_template_source_df[scope_template_source_df["id"] == selected_scope_template_id]
                if not scope_row.empty:
                    selected_scope_description = str(scope_row.iloc[0]["scope_description"] or "")
            st.text_area(
                "Selected Scope Preview",
                value=selected_scope_description,
                height=180,
                disabled=True,
                key="add_task_scope_preview",
            )

            notes = st.text_area("Default work item notes", key="add_task_notes")
            active = st.selectbox("Active", ["Yes", "No"], index=0)

            if st.form_submit_button("Add Work Item"):
                selected_trade_id = int(trades_df.loc[trades_df["name"] == trade_name, "id"].iloc[0])
                cleaned_work_item_name = task_name.strip()
                existing_work_item_df = fetch_df(
                    """
                    SELECT id
                    FROM tasks
                    WHERE trade_id = ? AND LOWER(COALESCE(name, '')) = LOWER(?)
                    LIMIT 1
                    """,
                    (selected_trade_id, cleaned_work_item_name),
                )
                if not cleaned_work_item_name:
                    st.error("Enter a Work Item name.")
                elif not selected_scope_template_id:
                    st.error("Select a scope to attach to this Work Item.")
                elif not existing_work_item_df.empty:
                    st.error("That work item already exists for this trade.")
                else:
                    try:
                        new_work_item_id = execute_returning_id(
                            "INSERT INTO tasks (trade_id, name, active, notes) VALUES (?, ?, ?, ?)",
                            (
                                selected_trade_id,
                                cleaned_work_item_name,
                                True if active == "Yes" else False,
                                notes.strip(),
                            ),
                        )
                        execute(
                            "UPDATE tasks SET work_item_code = 'WI-' || LPAD(id::text, 6, '0') WHERE id = ?",
                            (int(new_work_item_id),),
                        )

                        source_template_df = fetch_df(
                            """
                            SELECT
                                COALESCE(template_name, '') AS template_name,
                                COALESCE(scope_description, '') AS scope_description,
                                COALESCE(active, TRUE) AS active,
                                COALESCE(template_type, 'detailed') AS template_type,
                                COALESCE(audience, 'all') AS audience,
                                COALESCE(notes, '') AS notes
                            FROM scope_templates
                            WHERE id = ?
                            LIMIT 1
                            """,
                            (int(selected_scope_template_id),),
                        )
                        if not source_template_df.empty:
                            source_row = source_template_df.iloc[0]
                            execute(
                                """
                                INSERT INTO scope_templates (
                                    task_id, template_name, template_type, audience, scope_description, active, notes
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    int(new_work_item_id),
                                    str(source_row["template_name"]),
                                    str(source_row["template_type"]),
                                    str(source_row["audience"]),
                                    str(source_row["scope_description"]),
                                    bool(source_row["active"]),
                                    str(source_row["notes"]),
                                ),
                            )

                        st.cache_data.clear()
                        st.success("Work Item added with scope attached.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not add work item. Database error: {e}")

    with tab2:
        task_edit_df = fetch_df(
            """
            SELECT
                tasks.id,
                tasks.trade_id,
                trades.name AS trade_name,
                tasks.name,
                tasks.active,
                COALESCE(tasks.notes, '') AS notes
            FROM tasks
            JOIN trades ON trades.id = tasks.trade_id
            ORDER BY LOWER(tasks.name), LOWER(trades.name)
            """
        )

        if task_edit_df.empty:
            st.info("No work items found.")
        else:
            task_options = [
                f"{row['id']} | {row['name']} | {row['trade_name']}"
                for _, row in task_edit_df.iterrows()
            ]
            selected_task_label = st.selectbox("Select work item to edit", task_options)
            selected_task_id = int(selected_task_label.split(" | ")[0])
            selected_row = task_edit_df[task_edit_df["id"] == selected_task_id].iloc[0]

            with st.form("edit_task_form"):
                c1, c2 = st.columns(2)
                edit_task_name = c1.text_input("Work Item name", value=selected_row["name"])
                edit_trade_name = c2.selectbox(
                    'Category of Labor',
                    trades_df["name"].tolist(),
                    index=trades_df["name"].tolist().index(selected_row["trade_name"]),
                    key="edit_task_trade",
                )
                edit_notes = st.text_area("Default work item notes", value=selected_row["notes"], key="edit_task_notes")
                edit_active = st.selectbox(
                    "Active",
                    ["Yes", "No"],
                    index=0 if int(selected_row["active"]) == 1 else 1,
                )

                if st.form_submit_button("Update Work Item"):
                    selected_trade_id = int(trades_df.loc[trades_df["name"] == edit_trade_name, "id"].iloc[0])
                    cleaned_work_item_name = edit_task_name.strip()
                    existing_work_item_df = fetch_df(
                        """
                        SELECT id
                        FROM tasks
                        WHERE trade_id = ? AND LOWER(COALESCE(name, '')) = LOWER(?) AND id <> ?
                        LIMIT 1
                        """,
                        (selected_trade_id, cleaned_work_item_name, selected_task_id),
                    )
                    if not cleaned_work_item_name:
                        st.error("Enter a Work Item name.")
                    elif not existing_work_item_df.empty:
                        st.error("That work item already exists for this trade.")
                    else:
                        try:
                            execute(
                                """
                                UPDATE tasks
                                SET trade_id = ?, name = ?, active = ?, notes = ?
                                WHERE id = ?
                                """,
                                (
                                    selected_trade_id,
                                    cleaned_work_item_name,
                                    True if edit_active == "Yes" else False,
                                    edit_notes.strip(),
                                    selected_task_id,
                                ),
                            )
                            st.cache_data.clear()
                            st.success("Work Item updated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not update work item. Database error: {e}")

    with tab3:
        task_delete_df = fetch_df(
            """
            SELECT
                tasks.id,
                trades.name AS trade_name,
                tasks.name
            FROM tasks
            JOIN trades ON trades.id = tasks.trade_id
            ORDER BY LOWER(tasks.name), LOWER(trades.name)
            """
        )

        if task_delete_df.empty:
            st.info("No work items to delete.")
        else:
            delete_options = [
                f"{row['id']} | {row['name']} | {row['trade_name']}"
                for _, row in task_delete_df.iterrows()
            ]
            selected_delete_task = st.selectbox("Select work item to delete", delete_options)

            if st.button("Delete Selected Work Item"):
                delete_task_id = int(selected_delete_task.split(" | ")[0])
                execute("DELETE FROM tasks WHERE id = ?", (delete_task_id,))
                st.cache_data.clear()
                st.success("Work Item deleted.")
                st.rerun()

    task_view = fetch_df(
        """
        SELECT
            tasks.id,
            COALESCE(tasks.work_item_code, '') AS work_item_code,
            tasks.name AS "Work Item",
            trades.name AS trade,
            CASE WHEN tasks.active THEN 'Yes' ELSE 'No' END AS active,
            COALESCE(tasks.notes, '') AS notes
        FROM tasks
        JOIN trades ON trades.id = tasks.trade_id
        ORDER BY LOWER(tasks.name), LOWER(trades.name)
        """
    )
    st.markdown("---")
    if st.session_state.get("show_shared_ids"):
        st.dataframe(
            task_view.rename(columns={"work_item_code": "Work Item Code", "trade": 'Category of Labor', "active": "Active", "notes": "Notes"}),
            use_container_width=True,
        )
    else:
        st.dataframe(
            task_view.drop(columns=["work_item_code"]).rename(columns={"trade": 'Category of Labor', "active": "Active", "notes": "Notes"}),
            use_container_width=True,
        )


    with tab4:
        st.markdown("### Review Work Item Scopes")
        st.caption("Search Work Items and review the full scope text currently used for each Work Item.")

        review_df = fetch_df(
            """
            SELECT
                t.id AS work_item_id,
                COALESCE(t.name, '') AS work_item_name,
                COALESCE(tr.name, '') AS trade_name,
                COALESCE(st.template_name, '') AS scope_name,
                COALESCE(st.scope_description, '') AS scope_description,
                COALESCE(t.active, TRUE) AS active
            FROM tasks t
            JOIN trades tr ON tr.id = t.trade_id
            LEFT JOIN scope_templates st
                ON st.task_id = t.id
            ORDER BY LOWER(COALESCE(t.name, '')), LOWER(COALESCE(tr.name, '')), LOWER(COALESCE(st.template_name, ''))
            """
        )

        if review_df.empty:
            st.info("No Work Items or scopes found.")
        else:
            search_col1, search_col2 = st.columns([2, 1])
            search_text = search_col1.text_input(
                "Search Work Item, Trade, Scope Name, or Scope Text",
                key="work_item_scope_review_search",
                placeholder="Search scopes...",
            ).strip().lower()
            active_filter = search_col2.selectbox(
                "Show",
                ["Active Only", "All"],
                key="work_item_scope_review_filter",
            )

            filtered_df = review_df.copy()

            if active_filter == "Active Only":
                filtered_df = filtered_df[filtered_df["active"].fillna(True) == True]

            if search_text:
                filtered_df = filtered_df[
                    filtered_df["work_item_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                    | filtered_df["trade_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                    | filtered_df["scope_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                    | filtered_df["scope_description"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                ]

            if filtered_df.empty:
                st.info("No matching Work Item scopes found.")
            else:
                display_df = filtered_df.rename(columns={
                    "work_item_id": "Work Item ID",
                    "work_item_name": "Work Item",
                    "trade_name": 'Category of Labor',
                    "scope_name": "Scope Name",
                    "scope_description": "Full Scope",
                    "active": "Active",
                })[["Work Item ID", "Work Item", 'Category of Labor', "Scope Name", "Full Scope", "Active"]]

                display_df["Active"] = display_df["Active"].apply(lambda x: "Yes" if bool(x) else "No")
                st.dataframe(display_df, use_container_width=True, hide_index=True)


# -----------------------------
# Trades
# -----------------------------
elif page == 'Categories of Labor':
    st.subheader('Categories of Labor')

    tab1, tab2, tab3 = st.tabs(["Add Trade", "Edit Trade", "Delete Trade"])

    with tab1:
        with st.form("add_trade_form"):
            new_trade = st.text_input("New trade name")
            if st.form_submit_button("Add Trade"):
                try:
                    execute("INSERT INTO trades (name) VALUES (?)", (new_trade.strip(),))
                    st.success("Trade added.")
                    st.rerun()
                except Exception:
                    st.error("Trade already exists.")

    with tab2:
        trades_view = fetch_df("SELECT * FROM trades ORDER BY LOWER(name)")
        if trades_view.empty:
            st.info("No trades found.")
        else:
            selected_trade_name = st.selectbox("Select trade to edit", trades_view["name"].tolist(), key="edit_trade_select")
            selected_row = trades_view[trades_view["name"] == selected_trade_name].iloc[0]

            with st.form("edit_trade_form"):
                edit_trade_name = st.text_input("Trade name", value=selected_row["name"])
                if st.form_submit_button("Update Trade"):
                    try:
                        execute(
                            "UPDATE trades SET name = ? WHERE id = ?",
                            (edit_trade_name.strip(), int(selected_row["id"])),
                        )
                        st.success("Trade updated.")
                        st.rerun()
                    except Exception:
                        st.error("Trade already exists.")

    with tab3:
        trades_view = fetch_df("SELECT * FROM trades ORDER BY LOWER(name)")
        if trades_view.empty:
            st.info("No trades to delete.")
        else:
            selected_delete_trade = st.selectbox("Select trade to delete", trades_view["name"].tolist(), key="delete_trade_select")
            delete_row = trades_view[trades_view["name"] == selected_delete_trade].iloc[0]

            related_tasks = fetch_df("SELECT COUNT(*) AS cnt FROM tasks WHERE trade_id = ?", (int(delete_row["id"]),))
            task_count = int(related_tasks.iloc[0]["cnt"])

            if task_count > 0:
                st.warning(f"This trade has {task_count} task(s). Delete those tasks first.")
            else:
                if st.button("Delete Selected Trade"):
                    execute("DELETE FROM trades WHERE id = ?", (int(delete_row["id"]),))
                    st.success("Trade deleted.")
                    st.rerun()

    st.markdown("---")
    st.dataframe(fetch_df("SELECT * FROM trades ORDER BY LOWER(name)"), use_container_width=True)

# -----------------------------
# Contractors
# -----------------------------
elif page == "Contractors":
    st.subheader("Contractors")

    tab1, tab2, tab3 = st.tabs(["Add Contractor", "Edit Contractor", "Delete Contractor"])

    with tab1:
        with st.form("add_contractor_form"):
            c1, c2 = st.columns(2)
            contractor_name = c1.text_input("Contractor name")
            phone = c2.text_input("Phone")
            address = st.text_input("Address")
            notes = st.text_area("Notes")

            if st.form_submit_button("Add Contractor"):
                try:
                    execute(
                        "INSERT INTO contractors (name, address, phone, notes) VALUES (?, ?, ?, ?)",
                        (contractor_name.strip(), address.strip(), phone.strip(), notes.strip()),
                    )
                    st.success("Contractor added.")
                    st.rerun()
                except Exception:
                    st.error("Contractor already exists.")

    with tab2:
        contractors_view = fetch_df("SELECT * FROM contractors ORDER BY LOWER(name)")
        if contractors_view.empty:
            st.info("No contractors found.")
        else:
            selected_contractor_name = st.selectbox(
                "Select contractor to edit",
                contractors_view["name"].tolist(),
                key="edit_contractor_select",
            )
            selected_row = contractors_view[contractors_view["name"] == selected_contractor_name].iloc[0]

            with st.form("edit_contractor_form"):
                c1, c2 = st.columns(2)
                edit_contractor_name = c1.text_input("Contractor name", value=selected_row["name"])
                edit_phone = c2.text_input("Phone", value="" if pd.isna(selected_row["phone"]) else str(selected_row["phone"]))
                edit_address = st.text_input("Address", value="" if pd.isna(selected_row["address"]) else str(selected_row["address"]))
                edit_notes = st.text_area(
                    "Notes",
                    value="" if pd.isna(selected_row["notes"]) else str(selected_row["notes"]),
                )

                if st.form_submit_button("Update Contractor"):
                    try:
                        execute(
                            """
                            UPDATE contractors
                            SET name = ?, address = ?, phone = ?, notes = ?
                            WHERE id = ?
                            """,
                            (
                                edit_contractor_name.strip(),
                                edit_address.strip(),
                                edit_phone.strip(),
                                edit_notes.strip(),
                                int(selected_row["id"]),
                            ),
                        )
                        st.success("Contractor updated.")
                        st.rerun()
                    except Exception:
                        st.error("Contractor already exists.")

    with tab3:
        contractors_view = fetch_df("SELECT * FROM contractors ORDER BY LOWER(name)")
        if contractors_view.empty:
            st.info("No contractors to delete.")
        else:
            selected_delete_contractor = st.selectbox(
                "Select contractor to delete",
                contractors_view["name"].tolist(),
                key="delete_contractor_select",
            )
            delete_row = contractors_view[contractors_view["name"] == selected_delete_contractor].iloc[0]

            if st.button("Delete Selected Contractor"):
                execute("DELETE FROM contractors WHERE id = ?", (int(delete_row["id"]),))
                st.success("Contractor deleted.")
                st.rerun()

    st.markdown("---")
    st.dataframe(fetch_df("SELECT * FROM contractors ORDER BY LOWER(name)"), use_container_width=True)


# -----------------------------
# Scope Templates
# -----------------------------
elif page == "Scope Templates":
    st.subheader("Scope Templates")
    st.caption("Create a default scope description for each task. In Estimate Builder, the first saved template for the selected task/trade auto-fills into the scope box and can be edited per estimate.")

    trades_df = fetch_df("SELECT name FROM trades ORDER BY LOWER(name)")
    tasks_df = fetch_df(
        """
        SELECT tasks.id AS task_id, tasks.name AS task_name, trades.name AS trade_name
        FROM tasks
        JOIN trades ON trades.id = tasks.trade_id
        WHERE tasks.active = TRUE
        ORDER BY LOWER(tasks.name), LOWER(trades.name), trades.name
        """
    )

    tab1, tab2, tab3 = st.tabs(["Add Template", "Edit Template", "Delete Template"])

    with tab1:
        with st.form("add_scope_template_form"):
            template_name = st.text_input("Template name")
            c1, c2 = st.columns(2)
            task_name = c1.selectbox("Work Item", tasks_df["task_name"].tolist() if not tasks_df.empty else [])
            trade_options = (
                sorted(tasks_df[tasks_df["task_name"] == task_name]["trade_name"].unique().tolist())
                if not tasks_df.empty and task_name else []
            )
            trade_name = c2.selectbox('Category of Labor', trade_options if trade_options else [])
            scope_description = st.text_area("Scope description", height=220)
            active = st.selectbox("Active", ["Yes", "No"], index=0)

            if st.form_submit_button("Save Template"):
                selected_task_lookup = tasks_df[(tasks_df["task_name"] == task_name) & (tasks_df["trade_name"] == trade_name)]
                selected_task_id = int(selected_task_lookup.iloc[0]["task_id"]) if not selected_task_lookup.empty else None
                try:
                    execute(
                        """
                        INSERT INTO scope_templates (task_id, template_name, template_type, audience, scope_description, active, notes)
                        VALUES (?, ?, 'detailed', 'all', ?, ?, '')
                        """,
                        (
                            selected_task_id,
                            template_name.strip(),
                            scope_description.strip(),
                            True if active == "Yes" else False,
                        ),
                    )
                    st.success("Scope template saved.")
                    st.rerun()
                except Exception:
                    st.error("That template already exists for this task.")

    with tab2:
        template_edit_df = fetch_df(
            """
            SELECT st.id, st.template_name AS name, tr.name AS trade_name, t.name AS task_name, st.scope_description, st.active
            FROM scope_templates st
            JOIN tasks t ON t.id = st.task_id
            JOIN trades tr ON tr.id = t.trade_id
            ORDER BY LOWER(st.template_name), LOWER(t.name), LOWER(tr.name)
            """
        )
        if template_edit_df.empty:
            st.info("No scope templates found.")
        else:
            edit_options = [
                f"{row['id']} | {row['name']} | {row['task_name']} | {row['trade_name']}"
                for _, row in template_edit_df.iterrows()
            ]
            selected_label = st.selectbox("Select template to edit", edit_options)
            selected_id = int(selected_label.split(" | ")[0])
            selected_row = template_edit_df[template_edit_df["id"] == selected_id].iloc[0]

            with st.form("edit_scope_template_form"):
                edit_template_name = st.text_input("Template name", value=selected_row["name"])
                c1, c2 = st.columns(2)
                edit_task_name = c1.selectbox(
                    "Work Item",
                    tasks_df["task_name"].tolist() if not tasks_df.empty else [],
                    index=(tasks_df["task_name"].tolist().index(selected_row["task_name"]) if not tasks_df.empty and selected_row["task_name"] in tasks_df["task_name"].tolist() else 0),
                )
                edit_trade_options = (
                    sorted(tasks_df[tasks_df["task_name"] == edit_task_name]["trade_name"].unique().tolist())
                    if not tasks_df.empty and edit_task_name else []
                )
                edit_trade_name = c2.selectbox(
                    'Category of Labor',
                    edit_trade_options if edit_trade_options else [],
                    index=(edit_trade_options.index(selected_row["trade_name"]) if edit_trade_options and selected_row["trade_name"] in edit_trade_options else 0),
                )
                edit_scope_description = st.text_area("Scope description", value=selected_row["scope_description"], height=220)
                edit_active = st.selectbox("Active", ["Yes", "No"], index=0 if int(selected_row["active"]) == 1 else 1)

                if st.form_submit_button("Update Template"):
                    try:
                        selected_task_lookup = tasks_df[(tasks_df["task_name"] == edit_task_name) & (tasks_df["trade_name"] == edit_trade_name)]
                        selected_task_id = int(selected_task_lookup.iloc[0]["task_id"]) if not selected_task_lookup.empty else None
                        execute(
                            """
                            UPDATE scope_templates
                            SET task_id = ?, template_name = ?, scope_description = ?, active = ?
                            WHERE id = ?
                            """,
                            (
                                selected_task_id,
                                edit_template_name.strip(),
                                edit_scope_description.strip(),
                                True if edit_active == "Yes" else False,
                                selected_id,
                            ),
                        )
                        st.success("Scope template updated.")
                        st.rerun()
                    except Exception:
                        st.error("That template already exists for this task.")

    with tab3:
        template_delete_df = fetch_df(
            """
            SELECT st.id, st.template_name AS name, tr.name AS trade_name, t.name AS task_name
            FROM scope_templates st
            JOIN tasks t ON t.id = st.task_id
            JOIN trades tr ON tr.id = t.trade_id
            ORDER BY LOWER(st.template_name), LOWER(t.name), LOWER(tr.name)
            """
        )
        if template_delete_df.empty:
            st.info("No scope templates to delete.")
        else:
            delete_options = [
                f"{row['id']} | {row['name']} | {row['task_name']} | {row['trade_name']}"
                for _, row in template_delete_df.iterrows()
            ]
            selected_delete = st.selectbox("Select template to delete", delete_options)
            if st.button("Delete Selected Template"):
                delete_id = int(selected_delete.split(" | ")[0])
                execute("DELETE FROM scope_templates WHERE id = ?", (delete_id,))
                st.success("Scope template deleted.")
                st.rerun()

    template_view = fetch_df(
        """
        SELECT
            st.id,
            st.template_name,
            t.name AS task_name,
            tr.name AS trade_name,
            CASE WHEN st.active THEN 'Yes' ELSE 'No' END AS active,
            st.scope_description
        FROM scope_templates st
        JOIN tasks t ON t.id = st.task_id
        JOIN trades tr ON tr.id = t.trade_id
        ORDER BY LOWER(st.template_name), LOWER(t.name), LOWER(tr.name)
        """
    )
    st.markdown("---")
    st.dataframe(template_view, use_container_width=True)


# -----------------------------
# Crew Schedule
# -----------------------------
elif page == "Renovation Schedule":
    st.subheader("Renovation Schedule")
    st.caption("Place saved estimate projects onto a visual calendar for renovation scheduling.")

    if current_role == "Contractor":
        schedule_tab3 = st.tabs(["My Weekly Plan"])[0]
    else:
        schedule_tab1, schedule_tab2, schedule_tab3 = st.tabs(["Add To Schedule", "Weekly Calendar View", "Contractor Weekly Plans"])

        estimates_for_schedule = fetch_df(
            """
            SELECT
                id AS estimate_id,
                COALESCE(estimate_name, '(unnamed)') AS estimate_name,
                COALESCE(estimate_address, '') AS estimate_address,
                created_at
            FROM estimates
            WHERE COALESCE(active, TRUE) = TRUE
            ORDER BY LOWER(COALESCE(estimate_name, '(unnamed)')), id
            """
        )

        with schedule_tab1:
            st.markdown("### Add Project To Renovation Schedule")
            if estimates_for_schedule.empty:
                st.info("No active estimates found. Save an estimate first, then place it on the schedule.")
            else:
                estimate_labels = [
                    f"{row.estimate_name} | ID {int(row.estimate_id)}"
                    for row in estimates_for_schedule.itertuples()
                ]
                selected_schedule_label = st.selectbox("Project or Repair Estimate", estimate_labels, key="schedule_estimate_select")
                selected_schedule_id = int(selected_schedule_label.rsplit("ID ", 1)[1])
                selected_estimate = estimates_for_schedule[
                    estimates_for_schedule["estimate_id"] == selected_schedule_id
                ].iloc[0]

                crew_name_options = ["None selected"] + get_contractor_names()

                s1, s2 = st.columns(2)
                with s1:
                    crew_name = st.selectbox(
                        "Crew Leader / Crew Name",
                        crew_name_options,
                        index=0,
                        key="schedule_crew_name",
                    )
                with s2:
                    schedule_day_count = st.number_input(
                        "Number of Scheduled Day Entries",
                        min_value=1,
                        max_value=14,
                        value=1,
                        step=1,
                        key="schedule_day_count",
                    )

                st.markdown("#### Schedule Each Day Needed")
                schedule_day_entries = []
                default_start = datetime.now().date()
                for i in range(int(schedule_day_count)):
                    d1, d2 = st.columns(2)
                    with d1:
                        day_date = st.date_input(
                            f"Scheduled Date {i + 1}",
                            value=default_start,
                            key=f"scheduled_date_{i}",
                        )
                    with d2:
                        day_block = st.selectbox(
                            f"Time Block {i + 1}",
                            ["Full Day", "Morning", "Afternoon"],
                            key=f"time_block_{i}",
                        )
                    schedule_day_entries.append((day_date, day_block))

                schedule_notes = st.text_area(
                    "Schedule Notes",
                    height=100,
                    placeholder="Optional notes for the renovation schedule entry.",
                )

                if st.button("Add Project To Calendar", type="primary"):
                    for day_date, day_block in schedule_day_entries:
                        execute(
                            """
                            INSERT INTO schedule_entries (
                                estimate_id,
                                project_name,
                                estimate_address,
                                scheduled_date,
                                time_block,
                                crew_name,
                                notes,
                                created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                selected_schedule_id,
                                str(selected_estimate["estimate_name"]),
                                str(selected_estimate["estimate_address"]),
                                day_date.strftime("%Y-%m-%d"),
                                day_block,
                                "" if crew_name == "None selected" else crew_name.strip(),
                                schedule_notes.strip(),
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
                    st.success("Project added to renovation schedule.")
                    st.rerun()

            st.markdown("---")
            st.markdown("### Scheduled Projects")
            all_schedule_entries = schedule_entries_df()
            if all_schedule_entries.empty:
                st.info("No projects scheduled yet.")
            else:
                display_entries = all_schedule_entries.copy()
                display_entries["scheduled_date"] = pd.to_datetime(display_entries["scheduled_date"], errors="coerce").dt.strftime("%m-%d-%Y")
                st.dataframe(display_entries[["id", "project_name", "estimate_address", "scheduled_date", "time_block", "crew_name", "notes"]], use_container_width=True)

                st.markdown("#### Edit Scheduled Project Crew Name")
                edit_options = [
                    f"{int(row.id)} | {row.project_name} | {pd.to_datetime(row.scheduled_date).strftime('%m-%d-%Y')} | {row.time_block}"
                    for row in all_schedule_entries.itertuples()
                ]
                selected_schedule_edit = st.selectbox("Select schedule entry to edit", edit_options, key="schedule_edit_select")
                selected_edit_id = int(selected_schedule_edit.split(" | ")[0])
                selected_edit_row = all_schedule_entries[all_schedule_entries["id"] == selected_edit_id].iloc[0]

                crew_name_options = ["None selected"] + get_contractor_names()
                current_edit_crew = str(selected_edit_row["crew_name"]).strip()
                current_edit_display = current_edit_crew if current_edit_crew and current_edit_crew in crew_name_options else "None selected"

                e1, e2 = st.columns([4, 1])
                updated_crew_name = e1.selectbox(
                    "Crew Leader / Crew Name",
                    crew_name_options,
                    index=crew_name_options.index(current_edit_display),
                    key="schedule_edit_crew_name",
                )
                if e2.button("Save Crew Name Change"):
                    execute(
                        "UPDATE schedule_entries SET crew_name = ? WHERE id = ?",
                        ("" if updated_crew_name == "None selected" else updated_crew_name, selected_edit_id),
                    )
                    st.success("Schedule crew name updated.")
                    st.rerun()

                st.markdown("#### Delete Scheduled Project")
                delete_options = [
                    f"{int(row.id)} | {row.project_name} | {pd.to_datetime(row.scheduled_date).strftime('%m-%d-%Y')} | {row.time_block}"
                    for row in all_schedule_entries.itertuples()
                ]
                d1, d2 = st.columns([4, 1])
                selected_schedule_delete = d1.selectbox("Remove schedule entry", delete_options)
                if d2.button("Delete Entry"):
                    delete_id = int(selected_schedule_delete.split(" | ")[0])
                    execute("DELETE FROM schedule_entries WHERE id = ?", (delete_id,))
                    st.success("Schedule entry deleted.")
                    st.rerun()

        with schedule_tab2:
            st.markdown("### Renovation Calendar View")

            v1, v2, v3, v4 = st.columns([2, 2, 1.6, 1.6])
            with v1:
                week_date = st.date_input("Select a date in the schedule to view", key="week_view_date")
            week_start = week_date - __import__("datetime").timedelta(days=week_date.weekday())

            contractor_options = ["All"]
            all_schedule_for_filter = schedule_entries_df()
            if not all_schedule_for_filter.empty:
                unique_crews = sorted([c for c in all_schedule_for_filter["crew_name"].dropna().astype(str).unique().tolist() if c.strip()])
                contractor_options += unique_crews

            with v2:
                selected_contractor_filter = st.selectbox(
                    "Show Schedule For",
                    contractor_options,
                    key="schedule_contractor_filter",
                )
            with v3:
                calendar_span = st.selectbox(
                    "Calendar View",
                    ["One Week", "Two Weeks"],
                    index=1,
                    key="calendar_span_view",
                )
            with v4:
                enlarged_schedule = st.checkbox("Enlarge Schedule View", value=True, key="enlarge_schedule_view")

            total_days = 14 if calendar_span == "Two Weeks" else 7
            view_end = week_start + __import__("datetime").timedelta(days=total_days - 1)

            st.write(
                f"**Showing {calendar_span.lower()} from {week_start.strftime('%m-%d-%Y')} through {view_end.strftime('%m-%d-%Y')}**"
            )

            span_entries = schedule_entries_df(
                start_date=week_start.strftime("%Y-%m-%d"),
                end_date=view_end.strftime("%Y-%m-%d"),
                crew_name=selected_contractor_filter if selected_contractor_filter != "All" else None,
            )

            schedule_title = (
                "Overall Renovation Schedule"
                if selected_contractor_filter == "All"
                else f"Renovation Schedule - {selected_contractor_filter}"
            )

            first_week_end = week_start + __import__("datetime").timedelta(days=6)
            first_week_entries = span_entries.copy()
            if not first_week_entries.empty:
                temp_dates = pd.to_datetime(first_week_entries["scheduled_date"], errors="coerce")
                first_week_entries = first_week_entries[(temp_dates.dt.date >= week_start) & (temp_dates.dt.date <= first_week_end)]

            schedule_html_week_1 = render_week_schedule_html(
                schedule_df=first_week_entries,
                week_start=week_start,
                enlarged=enlarged_schedule,
                title=f"{schedule_title} - Week 1",
            )
            st.markdown(schedule_html_week_1, unsafe_allow_html=True)

            if calendar_span == "Two Weeks":
                second_week_start = week_start + __import__("datetime").timedelta(days=7)
                second_week_end = second_week_start + __import__("datetime").timedelta(days=6)
                second_week_entries = span_entries.copy()
                if not second_week_entries.empty:
                    temp_dates_2 = pd.to_datetime(second_week_entries["scheduled_date"], errors="coerce")
                    second_week_entries = second_week_entries[(temp_dates_2.dt.date >= second_week_start) & (temp_dates_2.dt.date <= second_week_end)]

                schedule_html_week_2 = render_week_schedule_html(
                    schedule_df=second_week_entries,
                    week_start=second_week_start,
                    enlarged=enlarged_schedule,
                    title=f"{schedule_title} - Week 2",
                )
                st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)
                st.markdown(schedule_html_week_2, unsafe_allow_html=True)

            p1, p2 = st.columns(2)
            overall_week_entries = schedule_entries_df(
                start_date=week_start.strftime("%Y-%m-%d"),
                end_date=first_week_end.strftime("%Y-%m-%d"),
            )

            p1.download_button(
                "Download Printable Overall Schedule PDF",
                data=build_printable_schedule_pdf(
                    schedule_df=overall_week_entries,
                    week_start=week_start,
                    enlarged=True,
                    title="Overall Renovation Schedule",
                ),
                file_name=f"overall_renovation_schedule_{week_start.strftime('%Y-%m-%d')}.pdf",
                mime="application/pdf",
            )

            contractor_pdf_options = [c for c in contractor_options if c != "All"]
            selected_contractor_pdf = None
            if contractor_pdf_options:
                selected_contractor_pdf = st.selectbox(
                    "Select Contractor For Contractor PDF",
                    contractor_pdf_options,
                    key="contractor_pdf_selector",
                )
                contractor_pdf_entries = schedule_entries_df(
                    start_date=week_start.strftime("%Y-%m-%d"),
                    end_date=first_week_end.strftime("%Y-%m-%d"),
                    crew_name=selected_contractor_pdf,
                )
                safe_contractor_name = "".join(
                    ch if ch.isalnum() or ch in ("-", "_") else "_"
                    for ch in str(selected_contractor_pdf).strip()
                ).strip("_") or "contractor"

                p2.download_button(
                    "Download Contractor Schedule PDF",
                    data=build_printable_schedule_pdf(
                        schedule_df=contractor_pdf_entries,
                        week_start=week_start,
                        enlarged=True,
                        title=f"Renovation Schedule - {selected_contractor_pdf}",
                    ),
                    file_name=f"{safe_contractor_name}_schedule_{week_start.strftime('%Y-%m-%d')}.pdf",
                    mime="application/pdf",
                )
            else:
                p2.info("Add schedule entries with a contractor name to generate a contractor PDF.")

            detail_label = "Detailed Entries For These Two Weeks" if calendar_span == "Two Weeks" else "Detailed Entries For This Week"
            st.markdown(f"### {detail_label}")
            if span_entries.empty:
                st.info("No scheduled projects for this selected period.")
            else:
                display_span_entries = span_entries.copy()
                display_span_entries["scheduled_date"] = pd.to_datetime(display_span_entries["scheduled_date"], errors="coerce").dt.strftime("%m-%d-%Y")
                st.dataframe(
                    display_span_entries[["id", "project_name", "estimate_address", "scheduled_date", "time_block", "crew_name", "notes"]],
                    use_container_width=True,
                )

    with schedule_tab3:
        if current_role == "Contractor":
            contractor_id = int(st.session_state.get("logged_in_contractor_id") or 0)
            contractor_name = str(st.session_state.get("logged_in_contractor_name") or "").strip()
            if not contractor_name and contractor_id:
                contractor_df = fetch_df("SELECT COALESCE(name, '') AS name FROM contractors WHERE id = ? LIMIT 1", (contractor_id,))
                contractor_name = str(contractor_df.iloc[0]["name"]) if not contractor_df.empty else ""
            if not contractor_id:
                st.warning("This contractor user is not linked to a contractor record yet.")
            else:
                render_contractor_weekly_schedule_form(
                    contractor_id=contractor_id,
                    contractor_name=contractor_name or str(st.session_state.get("logged_in_user", "")),
                    owner_view=False,
                )
        else:
            st.markdown("### Contractor Weekly Plans")
            st.caption("Review or update each contractor's fixed weekly AM/PM plan.")

            contractor_df = get_contractor_list_df()
            if contractor_df.empty:
                st.info("No contractors found.")
            else:
                contractor_labels = [f"{int(row.id)} | {row.name}" for row in contractor_df.itertuples()]
                selected_contractor_label = st.selectbox(
                    "Choose Contractor",
                    contractor_labels,
                    key="owner_contractor_weekly_plan_select",
                )
                selected_contractor_id = int(selected_contractor_label.split(" | ", 1)[0])
                selected_contractor_name = selected_contractor_label.split(" | ", 1)[1]

                render_contractor_weekly_schedule_form(
                    contractor_id=selected_contractor_id,
                    contractor_name=selected_contractor_name,
                    owner_view=True,
                )

                st.markdown("### Recently Submitted Contractor Weekly Plans")
                recent_plans = contractor_weekly_schedule_df()
                if recent_plans.empty:
                    st.info("No contractor weekly plans saved yet.")
                else:
                    display_recent = recent_plans.copy()
                    display_recent["week_start_date"] = pd.to_datetime(display_recent["week_start_date"], errors="coerce").dt.strftime("%m-%d-%Y")
                    display_recent["modified_at"] = pd.to_datetime(display_recent["modified_at"], errors="coerce").dt.strftime("%m-%d-%Y %I:%M %p")
                    st.dataframe(
                        display_recent[[
                            "contractor_name",
                            "week_start_date",
                            "day_name",
                            "am_project_name",
                            "am_crew_members",
                            "pm_project_name",
                            "pm_crew_members",
                            "notes",
                            "submitted_by",
                            "modified_at",
                        ]].rename(columns={
                            "contractor_name": "Contractor",
                            "week_start_date": "Week Start",
                            "day_name": "Day",
                            "am_project_name": "AM Project",
                            "am_crew_members": "AM Crew",
                            "pm_project_name": "PM Project",
                            "pm_crew_members": "PM Crew",
                            "notes": "Notes",
                            "submitted_by": "Submitted By",
                            "modified_at": "Modified",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )

                    st.markdown("#### Delete Saved Contractor Schedule Entry")
                    delete_recent_labels = [
                        f"{int(row.id)} | {row.contractor_name} | {pd.to_datetime(row.week_start_date).strftime('%m-%d-%Y')} | {row.day_name}"
                        for row in recent_plans.itertuples()
                    ]
                    selected_recent_delete_label = st.selectbox(
                        "Choose Saved Contractor Schedule Day To Delete",
                        delete_recent_labels,
                        key="delete_recent_contractor_schedule_day_select",
                    )
                    selected_recent_delete_id = int(selected_recent_delete_label.split(" | ", 1)[0])
                    confirm_recent_day_key = f"confirm_delete_recent_contractor_schedule_day_{selected_recent_delete_id}"
                    if confirm_recent_day_key not in st.session_state:
                        st.session_state[confirm_recent_day_key] = False

                    if not st.session_state[confirm_recent_day_key]:
                        if st.button("Delete Saved Contractor Schedule Day", type="secondary", key=f"delete_recent_contractor_schedule_day_btn_{selected_recent_delete_id}"):
                            st.session_state[confirm_recent_day_key] = True
                            st.rerun()
                    else:
                        st.warning("Delete this saved contractor schedule day?")
                        r1, r2 = st.columns(2)
                        if r1.button("Yes, Delete Day", type="primary", key=f"confirm_delete_recent_contractor_schedule_day_yes_{selected_recent_delete_id}"):
                            execute("DELETE FROM contractor_weekly_schedules WHERE id = ?", (selected_recent_delete_id,))
                            st.session_state[confirm_recent_day_key] = False
                            st.success("Saved contractor schedule day deleted.")
                            st.rerun()
                        if r2.button("Cancel", key=f"confirm_delete_recent_contractor_schedule_day_cancel_{selected_recent_delete_id}"):
                            st.session_state[confirm_recent_day_key] = False
                            st.rerun()


# -----------------------------
# Estimate History
# -----------------------------
elif page == "Estimate History":
    st.subheader("Estimate History")

    estimates = fetch_df(
        """
        SELECT
            COALESCE(e.order_number, 'Est' || e.id::text) AS order_number,
            COALESCE(e.estimate_name, '(unnamed)') AS estimate_name,
            COALESCE(e.estimate_address, '') AS estimate_address,
            e.id AS estimate_id,
            e.project_id,
            e.created_at,
            e.modified_at,
            COALESCE(c.name, '') AS contractor_name,
            CASE WHEN COALESCE(e.active, TRUE) THEN 'Active' ELSE 'Inactive' END AS status,
            COALESCE(e.labor_rate, 0) AS labor_rate,
            COALESCE(e.notes, '') AS notes,
            COALESCE(e.contractor_quote, 0) AS contractor_quote,
            COALESCE(SUM(el.onsite_hours), 0) AS total_onsite_hours,
            COALESCE(SUM(el.travel_hours), 0) AS total_travel_hours,
            COALESCE(SUM(el.total_hours), 0) AS total_hours,
            COALESCE(SUM(el.total_labor_cost), 0) AS total_labor_cost
        FROM estimates e
        LEFT JOIN contractors c ON c.id = e.contractor_id
        LEFT JOIN estimate_lines el ON el.estimate_id = e.id
        GROUP BY
            e.id, e.order_number, e.project_id, e.created_at, e.modified_at, e.estimate_name, e.estimate_address,
            c.name, e.active, e.labor_rate, e.contractor_quote, e.notes
        ORDER BY LOWER(COALESCE(e.estimate_name, '(unnamed)')), e.id DESC
        """
    )

    estimate_view = st.selectbox(
        "Estimate List",
        ["Active", "Archived", "All"],
        key="estimate_history_status_filter",
    )

    filtered_estimates = estimates.copy()
    if estimate_view == "Active":
        filtered_estimates = filtered_estimates[filtered_estimates["status"] == "Active"].copy()
    elif estimate_view == "Archived":
        filtered_estimates = filtered_estimates[filtered_estimates["status"] == "Archived"].copy()

    if filtered_estimates.empty:
        st.info("No estimates found for this filter.")
    else:
        st.markdown("### Edit An Existing Estimate")
        st.caption("Choose a project and load its estimate into Estimate Builder so you can add repairs as needed.")

        edit_labels = [
            f"{row.order_number} | {int(row.estimate_id)} | {row.estimate_name}"
            for row in filtered_estimates.itertuples()
        ]
        selected_edit_label = st.selectbox("Project Name", edit_labels, key="history_edit_estimate_select")
        selected_edit_id = int(selected_edit_label.split(" | ")[1])

        e1, e2 = st.columns([1, 1])
        if e1.button("Load Estimate Into Builder", type="primary"):
            if load_estimate_into_editor(selected_edit_id):
                st.success("Estimate loaded into Estimate Builder.")
                st.rerun()

        selected_estimate_row = filtered_estimates[filtered_estimates["estimate_id"] == selected_edit_id].iloc[0]
        current_status = selected_estimate_row["status"]

        toggle_label = "Archive Estimate" if current_status == "Active" else "Make Estimate Active"
        if e2.button(toggle_label):
            new_active = False if current_status == "Active" else True
            execute(
                "UPDATE estimates SET active = ?, modified_at = ? WHERE id = ?",
                (new_active, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), selected_edit_id),
            )
            st.success("Estimate status updated.")
            st.rerun()

        delete_confirm_key = f"confirm_delete_estimate_{selected_edit_id}"
        if delete_confirm_key not in st.session_state:
            st.session_state[delete_confirm_key] = False

        d1, d2 = st.columns([1, 1])
        if not st.session_state[delete_confirm_key]:
            if d1.button("Delete Estimate"):
                st.session_state[delete_confirm_key] = True
                st.rerun()
        else:
            st.warning("Delete this estimate permanently? This will delete the estimate and related quotes, costs, work orders, and estimate photos tied to it.")
            if d1.button("Yes, Delete Estimate", type="primary"):
                delete_estimate(selected_edit_id)
                st.session_state[delete_confirm_key] = False
                st.success("Estimate deleted.")
                st.rerun()
            if d2.button("Cancel Delete"):
                st.session_state[delete_confirm_key] = False
                st.rerun()

        st.markdown("---")
        st.markdown("### Review An Existing Estimate")
        st.caption("Choose a project name and review the itemized list of repairs under that estimate.")

        review_labels = [
            f"{row.order_number} | {int(row.estimate_id)} | {row.estimate_name}"
            for row in filtered_estimates.itertuples()
        ]
        selected_review_label = st.selectbox("Project Name ", review_labels, key="history_review_estimate_select")
        selected_review_id = int(selected_review_label.split(" | ")[1])

        review_estimate_row = filtered_estimates[filtered_estimates["estimate_id"] == selected_review_id].iloc[0]
        r1, r2, r3 = st.columns(3)
        r1.text_input("Project Name", value=str(review_estimate_row["estimate_name"]), disabled=True)
        r2.text_input("Project Address", value=str(review_estimate_row["estimate_address"]), disabled=True)
        r3.text_input("Estimate ID", value=str(selected_review_id), disabled=True)

        lines = estimate_lines_df(selected_review_id)
        if lines.empty:
            st.info("No repairs have been saved under this estimate yet.")
        else:
            display_lines = lines.copy()
            display_lines = display_lines[[
                "id",
                "task_name",
                "trade_name",
                "scope_description",
                "repair_quantity",
                "onsite_hours",
                "travel_hours",
                "total_hours",
                "total_labor_cost",
            ]].rename(columns={
                "id": "Work Item ID",
                "task_name": "Work Item",
                "trade_name": 'Category of Labor',
                "scope_description": "Scope Description",
                "repair_quantity": "Repair Qty",
                "onsite_hours": "On Site Man Hours",
                "travel_hours": "Travel Man Hours",
                "total_hours": "Total Man Hours",
                "total_labor_cost": "Amount Used",
            })
            st.dataframe(display_lines, use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("### Request Quotes For A Work Item")
            st.caption("Choose a saved Work Item from this estimate and assign one or more contractors to request quotes.")

            work_item_labels = [
                f"{int(row.id)} | {row.task_name} | {row.trade_name}"
                for row in lines.itertuples()
            ]
            selected_work_item_label = st.selectbox(
                "Choose Work Item",
                work_item_labels,
                key=f"quote_request_work_item_select_{selected_review_id}",
            )
            selected_work_item_id = int(selected_work_item_label.split(" | ", 1)[0])
            selected_work_item_row = lines[lines["id"] == selected_work_item_id].iloc[0]

            q1, q2 = st.columns(2)
            q1.text_input("Selected Work Item", value=str(selected_work_item_row["task_name"]), disabled=True)
            q2.text_input('Category of Labor', value=str(selected_work_item_row["trade_name"]), disabled=True)
            st.text_area(
                "Scope For This Work Item",
                value=str(selected_work_item_row.get("scope_description") or ""),
                disabled=True,
                height=120,
            )

            contractor_names = get_contractor_names()
            contractor_select_options = contractor_names if contractor_names else []
            selected_contractors = st.multiselect(
                "Request Quotes From Contractors",
                contractor_select_options,
                key=f"quote_request_contractors_{selected_review_id}_{selected_work_item_id}",
                help="Choose one or more contractors already saved on the Contractors page.",
            )

            request_note = st.text_area(
                "Quote Request Notes",
                key=f"quote_request_notes_{selected_review_id}_{selected_work_item_id}",
                height=100,
                placeholder="Optional internal note for this quote request.",
            )

            if st.button("Create Quote Requests", type="primary", key=f"create_quote_requests_{selected_review_id}_{selected_work_item_id}"):
                if not selected_contractors:
                    st.error("Select at least one contractor to request quotes from.")
                else:
                    created_count = 0
                    skipped_names = []
                    for contractor_name in selected_contractors:
                        contractor_id = get_contractor_id_by_name(contractor_name)
                        if not contractor_id:
                            skipped_names.append(contractor_name)
                            continue

                        existing_request_df = fetch_df(
                            """
                            SELECT id
                            FROM quote_requests
                            WHERE estimate_line_id = ? AND contractor_id = ?
                            LIMIT 1
                            """,
                            (selected_work_item_id, contractor_id),
                        )
                        if existing_request_df.empty:
                            execute(
                                """
                                INSERT INTO quote_requests (
                                    estimate_line_id,
                                    contractor_id,
                                    quote_status,
                                    quote_amount,
                                    quote_notes,
                                    requested_at,
                                    created_at,
                                    modified_at
                                ) VALUES (?, ?, 'Requested', NULL, ?, NOW(), NOW(), NOW())
                                """,
                                (
                                    selected_work_item_id,
                                    contractor_id,
                                    str(request_note).strip(),
                                ),
                            )
                            created_count += 1
                        else:
                            skipped_names.append(contractor_name)

                    st.cache_data.clear()
                    if created_count > 0:
                        st.success(f"{created_count} quote request(s) created.")
                    if skipped_names:
                        st.warning("Skipped contractors already requested or not found: " + ", ".join(skipped_names))
                    st.rerun()

            existing_requests_df = fetch_df(
                """
                SELECT
                    qr.id,
                    COALESCE(c.name, '') AS contractor_name,
                    COALESCE(qr.quote_status, 'Requested') AS quote_status,
                    COALESCE(qr.quote_amount, 0) AS quote_amount,
                    COALESCE(qr.quote_notes, '') AS quote_notes,
                    qr.requested_at,
                    qr.submitted_at,
                    qr.modified_at
                FROM quote_requests qr
                LEFT JOIN contractors c ON c.id = qr.contractor_id
                WHERE qr.estimate_line_id = ?
                ORDER BY COALESCE(qr.submitted_at, qr.modified_at, qr.requested_at) DESC NULLS LAST, qr.id DESC
                """,
                (selected_work_item_id,),
            )

            st.markdown("#### Existing Quote Requests For This Work Item")
            if existing_requests_df.empty:
                st.info("No quote requests have been created for this Work Item yet.")
            else:
                for col in ["requested_at", "submitted_at", "modified_at"]:
                    existing_requests_df[col] = pd.to_datetime(existing_requests_df[col], errors="coerce").dt.strftime("%m-%d-%Y %H:%M")
                st.dataframe(
                    existing_requests_df.rename(columns={
                        "contractor_name": "Contractor",
                        "quote_status": "Quote Status",
                        "quote_amount": "Quote Amount",
                        "quote_notes": "Quote Notes",
                        "requested_at": "Requested At",
                        "submitted_at": "Submitted At",
                        "modified_at": "Updated",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

        photo_map = line_photo_map(selected_review_id)
        line_photo_rows = []
        for _, row in lines.iterrows():
            line_photo_rows.append(
                {
                    "task_name": row["task_name"],
                    "trade_name": row["trade_name"],
                    "photos": photo_map.get(int(row["id"]), []),
                }
            )
        render_line_photo_sections(
            line_photo_rows,
            section_title="Photos By Repair",
            gallery_title="All Pictures For This Job",
            load_key_prefix=f"estimate_history_{selected_review_id}",
        )

        st.markdown("---")
        st.subheader("Printable PDF Reports")

        pdf_internal = build_estimate_pdf(selected_review_id, report_type="internal")
        pdf_contractor = build_estimate_pdf(selected_review_id, report_type="contractor")

        c1, c2 = st.columns(2)

        if pdf_internal:
            c1.download_button(
                "Download Internal PDF",
                pdf_internal,
                file_name=f"estimate_{selected_review_id}_internal.pdf",
                mime="application/pdf",
            )

        if pdf_contractor:
            c2.download_button(
                "Download Contractor PDF Report",
                pdf_contractor,
                file_name=f"estimate_{selected_review_id}_contractor.pdf",
                mime="application/pdf",
            )

# -----------------------------
# Contractor Quotes
# -----------------------------
elif page == "Contractor Quotes":
    st.subheader("Quotes Received")
    st.caption("Review contractor quotes that have been submitted for Work Items. This is the Stage 1 review page.")

    quotes_df = quotes_received_df()
    if quotes_df.empty:
        st.info("No quote requests or submitted quotes found yet.")
    else:
        top1, top2 = st.columns([2, 1])
        search_text = top1.text_input(
            "Search Project, Work Item, Contractor, or Quote Notes",
            key="quotes_received_search",
            placeholder="Search quotes...",
        ).strip().lower()
        status_filter = top2.selectbox(
            "Quote Status",
            ["Responded Only", "All"],
            key="quotes_received_status_filter",
        )

        filtered_df = quotes_df.copy()

        if status_filter == "Responded Only":
            filtered_df = filtered_df[
                filtered_df["quote_status"].fillna("").astype(str).str.lower().isin(["responded", "approved"])
            ]

        if search_text:
            filtered_df = filtered_df[
                filtered_df["project_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                | filtered_df["work_item_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                | filtered_df["trade_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                | filtered_df["contractor_name"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
                | filtered_df["quote_notes"].fillna("").astype(str).str.lower().str.contains(search_text, na=False)
            ]

        if filtered_df.empty:
            st.info("No matching quotes found.")
        else:
            grouped_projects = filtered_df["project_name"].dropna().astype(str).unique().tolist()
            for project_name in grouped_projects:
                project_df = filtered_df[filtered_df["project_name"] == project_name].copy()
                project_address = str(project_df.iloc[0]["project_address"] or "")
                st.markdown(f"### {project_name}")
                if project_address.strip():
                    st.caption(project_address)

                display_df = project_df.copy()
                for col in ["requested_at", "submitted_at", "modified_at"]:
                    display_df[col] = pd.to_datetime(display_df[col], errors="coerce").dt.strftime("%m-%d-%Y %H:%M")

                display_df = display_df.rename(columns={
                    "work_item_id": "Work Item ID",
                    "work_item_name": "Work Item",
                    "trade_name": 'Category of Labor',
                    "contractor_name": "Contractor",
                    "quote_status": "Quote Status",
                    "quote_amount": "Quote Amount",
                    "quote_notes": "Quote Notes",
                    "requested_at": "Requested At",
                    "submitted_at": "Submitted At",
                    "modified_at": "Updated",
                })[
                    [
                        "Work Item ID",
                        "Work Item",
                        'Category of Labor',
                        "Contractor",
                        "Quote Status",
                        "Quote Amount",
                        "Quote Notes",
                        "Requested At",
                        "Submitted At",
                        "Updated",
                    ]
                ]

                st.dataframe(display_df, use_container_width=True, hide_index=True)

                with st.expander(f"View Full Scope Details For {project_name}"):
                    for row in project_df.itertuples():
                        st.markdown(
                            f"**Work Item:** {row.work_item_name} | **Contractor:** {row.contractor_name or 'Not assigned'}"
                        )
                        st.write(f"**Trade:** {row.trade_name}")
                        st.write(f"**Scope:** {row.scope_description}")
                        st.write(f"**Quote Status:** {row.quote_status}")
                        st.write(f"**Quote Amount:** ${float(row.quote_amount or 0):,.2f}")
                        if str(row.quote_notes or "").strip():
                            st.write(f"**Quote Notes:** {row.quote_notes}")
                        st.markdown("---")