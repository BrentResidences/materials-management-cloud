
from __future__ import annotations

from contextlib import closing
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import psycopg
from psycopg.rows import dict_row
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


# -----------------------------
# Database helpers
# -----------------------------

def get_conn() -> psycopg.Connection:
    try:
        conn = psycopg.connect(
            host=st.secrets["connections"]["postgresql"]["host"],
            dbname=st.secrets["connections"]["postgresql"]["database"],
            user=st.secrets["connections"]["postgresql"]["username"],
            password=st.secrets["connections"]["postgresql"]["password"],
            port=st.secrets["connections"]["postgresql"]["port"],
            sslmode=st.secrets["connections"]["postgresql"].get("sslmode", "require"),
            row_factory=dict_row,
        )
        return conn
    except Exception as exc:
        st.error("Could not connect to Neon / PostgreSQL. Check your Streamlit secrets.")
        raise exc


def execute(sql: str, params: tuple = ()) -> None:
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            columns = [desc.name if hasattr(desc, "name") else desc[0] for desc in (cur.description or [])]
            if not rows:
                return pd.DataFrame(columns=columns)
            return pd.DataFrame(rows, columns=columns)


def query_one(sql: str, params: tuple = ()) -> Optional[dict[str, Any]]:
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


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
    with closing(get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
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
    with closing(get_conn()) as conn:
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


# -----------------------------
# Utility functions
# -----------------------------

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
        ),
    )


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
        story.append(Paragraph(f"Work Item: {work_item_name}", styles["Heading2"]))
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

        story.append(Paragraph(f"Work Item Total: {money(work_item_total)}", styles["Heading3"]))
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


# -----------------------------
# UI pages
# -----------------------------

def page_dashboard() -> None:
    st.header("Dashboard")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Categories", int(query_one("SELECT COUNT(*) AS c FROM company_categories WHERE active = 1")["c"]))
    c2.metric("Materials", int(query_one("SELECT COUNT(*) AS c FROM materials WHERE active = 1")["c"]))
    c3.metric("Projects", int(query_one("SELECT COUNT(*) AS c FROM projects")["c"]))
    c4.metric("Work Items", int(query_one("SELECT COUNT(*) AS c FROM project_work_items")["c"]))

    st.subheader("Recent Materials")
    df_mat = query_df(
        "SELECT material_id, material_name, manufacturer, model_number, dimension_display, date_created FROM materials ORDER BY material_id DESC LIMIT 10"
    )
    df_mat = format_dates(df_mat)
    st.dataframe(df_mat, use_container_width=True)

    st.subheader("Recent Projects")
    df_proj = query_df(
        "SELECT project_id, project_name, property_name, unit_or_location, status, date_created FROM projects ORDER BY project_id DESC LIMIT 10"
    )
    df_proj = format_dates(df_proj)
    st.dataframe(df_proj, use_container_width=True)


def page_setup() -> None:
    st.header("Setup")
    tab1, tab2, tab3 = st.tabs(["Categories", "Sub-categories", "Units"])

    with tab1:
        with st.form("add_category_form"):
            col1, col2 = st.columns([3, 1])
            category_name = col1.text_input("Category name")
            sort_order = col2.number_input("Sort order", min_value=0, step=1, value=0)
            notes = st.text_area("Notes")
            submitted = st.form_submit_button("Add category")
            if submitted and category_name.strip():
                try:
                    execute(
                        "INSERT INTO company_categories (category_name, sort_order, notes) VALUES (%s, %s, %s)",
                        (category_name.strip(), sort_order, notes.strip()),
                    )
                    st.success("Category added.")
                except Exception as exc:
                    st.error(f"Could not add category: {exc}")

        st.dataframe(query_df("SELECT * FROM company_categories ORDER BY category_name"), use_container_width=True)

    with tab2:
        cats = query_df("SELECT category_id, category_name FROM company_categories WHERE active = 1 ORDER BY category_name")
        if cats.empty:
            st.info("Add a category first.")
        else:
            cat_map = {str(name): int(cat_id) for name, cat_id in zip(cats["category_name"], cats["category_id"])}
            with st.form("add_subcategory_form"):
                selected_cat = st.selectbox("Category", list(cat_map.keys()))
                subcategory_name = st.text_input("Sub-category name")
                sort_order = st.number_input("Sort order ", min_value=0, step=1, value=0, key="sub_sort")
                notes = st.text_area("Notes ")
                submitted = st.form_submit_button("Add sub-category")
                if submitted and subcategory_name.strip():
                    execute(
                        """
                        INSERT INTO company_subcategories (category_id, subcategory_name, sort_order, notes)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (category_id, subcategory_name) DO NOTHING
                        """,
                        (cat_map[selected_cat], subcategory_name.strip(), sort_order, notes.strip()),
                    )
                    st.success("Sub-category added.")

        st.dataframe(
            query_df(
                """
                SELECT s.subcategory_id, c.category_name, s.subcategory_name, s.active, s.sort_order, s.notes
                FROM company_subcategories s
                JOIN company_categories c ON s.category_id = c.category_id
                ORDER BY c.category_name, s.subcategory_name
                """
            ),
            use_container_width=True,
        )

    with tab3:
        with st.form("add_unit_form"):
            c1, c2, c3, c4 = st.columns(4)
            unit_name = c1.text_input("Unit name")
            unit_abbreviation = c2.text_input("Abbreviation")
            measurement_system = c3.text_input("Measurement system")
            unit_type = c4.text_input("Unit type")
            notes = st.text_area("Notes", key="unit_notes")
            submitted = st.form_submit_button("Add unit")
            if submitted and unit_name.strip():
                execute(
                    """
                    INSERT INTO units_of_measure (unit_name, unit_abbreviation, measurement_system, unit_type, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (unit_name) DO NOTHING
                    """,
                    (unit_name.strip(), unit_abbreviation.strip(), measurement_system.strip(), unit_type.strip(), notes.strip()),
                )
                st.success("Unit added.")

        st.dataframe(query_df("SELECT * FROM units_of_measure ORDER BY unit_name"), use_container_width=True)


def page_materials() -> None:
    st.header("Materials")
    tab1, tab2, tab3, tab4 = st.tabs(["Add Material", "Add Vendor Info", "Search / Review", "Vendor Master Reports"])

    with tab1:
        cats = query_df("SELECT category_id, category_name FROM company_categories WHERE active = 1 ORDER BY category_name")
        units = query_df("SELECT unit_id, unit_name FROM units_of_measure WHERE active = 1 ORDER BY unit_name")
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

    with tab2:
        mats = query_df("SELECT material_id, material_name FROM materials WHERE active = 1 ORDER BY material_name")
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

    with tab3:
        c1, c2, c3 = st.columns(3)
        search = c1.text_input("Search materials")
        categories = query_df("SELECT category_id, category_name FROM company_categories WHERE active = 1 ORDER BY category_name")
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

        st.subheader("Current Vendor Records")
        df_vendor = query_df("SELECT * FROM material_vendor_current ORDER BY vendor_name, vendor_item_number")
        df_vendor = format_dates(df_vendor)
        st.dataframe(df_vendor, use_container_width=True)

    with tab4:
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
            vendor_pdf = build_vendor_master_pdf(selected_vendor)
            col1.download_button(
                "Download Vendor Master PDF",
                data=vendor_pdf.getvalue(),
                file_name=f"vendor_master_{selected_vendor.replace(' ', '_')}.pdf",
                mime="application/pdf",
            )
            vendor_xlsx = build_vendor_master_excel(selected_vendor)
            col2.download_button(
                "Download Vendor Master Excel",
                data=vendor_xlsx.getvalue(),
                file_name=f"vendor_master_{selected_vendor.replace(' ', '_')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


def page_projects() -> None:
    st.header("Projects")
    tab1, tab2, tab3, tab4 = st.tabs([
        "Create Project",
        "Add Work Item",
        "Add Materials to Work Item",
        "Project Detail / Reports",
    ])

    with tab1:
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
            if submitted and project_name.strip():
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

    with tab2:
        projects = query_df("SELECT project_id, project_name FROM projects ORDER BY project_name")
        if projects.empty:
            st.info("Create a project first.")
        else:
            proj_map = dict(zip(projects["project_name"], projects["project_id"]))
            with st.form("add_work_item_form"):
                project_name = st.selectbox("Project", list(proj_map.keys()))
                work_item_name = st.text_input("Work Item name")
                work_item_description = st.text_area("Work Item description")
                sort_order = st.number_input("Sort order", min_value=0, step=1, value=0, key="wi_sort")
                notes = st.text_input("Notes", key="wi_notes")
                submitted = st.form_submit_button("Add Work Item")
                if submitted and work_item_name.strip():
                    execute(
                        """
                        INSERT INTO project_work_items (project_id, work_item_name, work_item_description, sort_order, notes, date_modified)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (proj_map[project_name], work_item_name.strip(), work_item_description.strip(), sort_order, notes.strip(), now_ts()),
                    )
                    st.success("Work Item added.")

    with tab3:
        work_items = query_df(
            """
            SELECT wi.work_item_id, p.project_name || ' | ' || wi.work_item_name AS label
            FROM project_work_items wi
            JOIN projects p ON wi.project_id = p.project_id
            ORDER BY p.project_name, wi.sort_order NULLS LAST, wi.work_item_name
            """
        )
        materials = query_df(
            """
            SELECT m.material_id, m.material_name, u.unit_id AS default_unit_id
            FROM materials m
            LEFT JOIN units_of_measure u ON m.default_unit_id = u.unit_id
            WHERE m.active = 1
            ORDER BY m.material_name
            """
        )
        units = query_df("SELECT unit_id, unit_name FROM units_of_measure WHERE active = 1 ORDER BY unit_name")
        if work_items.empty or materials.empty or units.empty:
            st.info("You need at least one project, work item, material, and unit.")
        else:
            wi_map = dict(zip(work_items["label"], work_items["work_item_id"]))
            mat_map = dict(zip(materials["material_name"], materials["material_id"]))
            mat_default_unit_map = {str(name): (int(uid) if pd.notna(uid) else None) for name, uid in zip(materials["material_name"], materials["default_unit_id"])}
            unit_map = {str(name): int(uid) for name, uid in zip(units["unit_name"], units["unit_id"])}
            unit_name_by_id = {int(uid): str(name) for uid, name in zip(units["unit_id"], units["unit_name"])}

            selected_material_name = st.selectbox("Material", list(mat_map.keys()), key="add_line_material_select")
            selected_material_id = mat_map[selected_material_name]
            vendor_df = query_df(
                """
                SELECT material_vendor_id,
                       vendor_name || COALESCE(' | ' || vendor_item_number, '') AS label,
                       latest_retail_price,
                       latest_quoted_price
                FROM material_vendor_current
                WHERE material_id = %s AND active = 1
                ORDER BY vendor_name
                """,
                (selected_material_id,),
            )
            vendor_options = {"None": None}
            vendor_price_lookup = {"None": 0.0}
            if not vendor_df.empty:
                for _, row in vendor_df.iterrows():
                    label = row["label"]
                    vendor_options[label] = row["material_vendor_id"]
                    preferred_price = row["latest_quoted_price"] if pd.notna(row["latest_quoted_price"]) and float(row["latest_quoted_price"] or 0) > 0 else row["latest_retail_price"]
                    vendor_price_lookup[label] = float(preferred_price or 0.0)

            default_unit_id = mat_default_unit_map.get(selected_material_name)
            default_unit_name = unit_name_by_id.get(default_unit_id)
            unit_choices = list(unit_map.keys())
            default_unit_index = unit_choices.index(default_unit_name) if default_unit_name in unit_choices else 0

            with st.form("add_material_to_work_item_form"):
                work_item_label = st.selectbox("Work Item", list(wi_map.keys()))
                selected_vendor = st.selectbox("Vendor record", list(vendor_options.keys()))
                c1, c2, c3 = st.columns(3)
                quantity = c1.number_input("Quantity", min_value=0.0, step=1.0, value=1.0)
                unit_name = c2.selectbox("Unit", unit_choices, index=default_unit_index)
                default_price = vendor_price_lookup.get(selected_vendor, 0.0)
                unit_price = c3.number_input("Unit price", min_value=0.0, step=0.01, value=float(default_price))
                notes = st.text_input("Line notes")
                submitted = st.form_submit_button("Add Material Line")
                if submitted:
                    add_material_line_from_master(
                        wi_map[work_item_label],
                        selected_material_id,
                        quantity,
                        unit_map[unit_name],
                        unit_price,
                        vendor_options[selected_vendor],
                        notes.strip(),
                    )
                    st.success("Material line added.")

    with tab4:
        projects = query_df("SELECT project_id, project_name FROM projects ORDER BY project_name")
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
            st.info("This project has no Work Items yet.")
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

        st.subheader("Work Items")
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
                    with st.form(f"edit_line_form_{wi_id}"):
                        e1, e2, e3 = st.columns(3)
                        edit_quantity = e1.number_input("Quantity", min_value=0.0, step=1.0, value=float(line_row["quantity"]), key=f"edit_qty_{wi_id}")
                        edit_unit_name = e2.selectbox("Unit", unit_choices_edit, index=current_unit_index, key=f"edit_unit_{wi_id}")
                        edit_unit_price = e3.number_input("Unit price", min_value=0.0, step=0.01, value=float(line_row["unit_price"]), key=f"edit_price_{wi_id}")
                        edit_notes = st.text_input("Notes", value=line_row["notes"] or "", key=f"edit_notes_{wi_id}")
                        col_save, col_delete = st.columns(2)
                        save_clicked = col_save.form_submit_button("Save Line Changes")
                        delete_clicked = col_delete.form_submit_button("Delete Line")

                        if save_clicked:
                            execute(
                                """
                                UPDATE work_item_materials
                                SET quantity = %s,
                                    unit_id = %s,
                                    unit_price = %s,
                                    line_total = %s,
                                    notes = %s,
                                    date_modified = %s
                                WHERE work_item_material_id = %s
                                """,
                                (
                                    edit_quantity,
                                    unit_map_edit[edit_unit_name],
                                    edit_unit_price,
                                    edit_quantity * edit_unit_price,
                                    edit_notes.strip(),
                                    now_ts(),
                                    selected_line_id,
                                ),
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
            project_pdf = build_project_report_pdf(project_id)
            col_pdf1.download_button(
                "Download Project PDF",
                data=project_pdf.getvalue(),
                file_name=f"project_report_{project_id}.pdf",
                mime="application/pdf",
            )

            vendor_pdf = build_vendor_report_pdf(project_id)
            col_pdf2.download_button(
                "Download Vendor PDF",
                data=vendor_pdf.getvalue(),
                file_name=f"vendor_report_{project_id}.pdf",
                mime="application/pdf",
            )

            vendor_xlsx = build_vendor_report_excel(project_id)
            col_xlsx.download_button(
                "Download Vendor Excel",
                data=vendor_xlsx.getvalue(),
                file_name=f"vendor_report_{project_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


# -----------------------------
# Main app
# -----------------------------

def main() -> None:
    st.set_page_config(page_title="Materials Management V1 - Neon Cloud", layout="wide")
    init_db()

    st.title("Materials Management V1 - Neon Cloud")
    st.caption("Master materials database + project materials quoting")

    with st.sidebar:
        st.markdown("### Navigation")
        page = st.radio("Go to", ["Dashboard", "Setup", "Materials", "Projects"])
        st.markdown("---")
        st.markdown("### Neon Setup")
        st.code(
            "[connections.postgresql]\n"
            'host = "YOUR_HOST"\n'
            'database = "YOUR_DATABASE"\n'
            'username = "YOUR_USERNAME"\n'
            'password = "YOUR_PASSWORD"\n'
            'port = 5432\n'
            'sslmode = "require"',
            language="toml",
        )

    if page == "Dashboard":
        page_dashboard()
    elif page == "Setup":
        page_setup()
    elif page == "Materials":
        page_materials()
    elif page == "Projects":
        page_projects()


if __name__ == "__main__":
    main()
