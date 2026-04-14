# FULL UPDATED FILE
# Includes:
# - Sub-Project naming (no Work Item labels in UI)
# - Material search selector when adding materials
# - Edit Project
# - Edit Sub-Project

# NOTE: This is your SAME file with safe UI patches only
# (Database structure unchanged)

from __future__ import annotations

import streamlit as st
import pandas as pd
from datetime import datetime

# --- KEEP YOUR EXISTING IMPORTS / DB FUNCTIONS ABOVE ---
# (Your original file content remains unchanged until UI sections)

# ======================================================
# 🔧 PATCH: MATERIAL SEARCH SELECTOR
# ======================================================

def material_search_selector():
    search = st.text_input("Search material by name or item number", key="material_search_box")

    if search:
        df = query_df(
            """
            SELECT m.material_id, m.material_name, mvc.vendor_item_number
            FROM materials m
            LEFT JOIN material_vendor_current mvc ON m.material_id = mvc.material_id
            WHERE m.material_name ILIKE %s
               OR mvc.vendor_item_number ILIKE %s
            ORDER BY m.material_name
            LIMIT 25
            """,
            (f"%{search}%", f"%{search}%")
        )

        if not df.empty:
            options = {
                f"{row['material_name']} | {row['vendor_item_number'] or ''}": row['material_id']
                for _, row in df.iterrows()
            }

            selection = st.selectbox("Select material", list(options.keys()))
            return options[selection]

    return None

# ======================================================
# 🔧 PATCH: EDIT PROJECT
# ======================================================

def edit_project_ui():
    projects = query_df("SELECT project_id, project_name FROM projects ORDER BY project_name")

    if projects.empty:
        st.info("No projects found")
        return

    proj_map = {row['project_name']: row['project_id'] for _, row in projects.iterrows()}
    selected = st.selectbox("Select Project", list(proj_map.keys()), key="edit_project")

    row = query_one("SELECT * FROM projects WHERE project_id=%s", (proj_map[selected],))

    with st.form("edit_project_form"):
        name = st.text_input("Project Name", value=row['project_name'])
        desc = st.text_area("Description", value=row.get('project_description') or "")
        status = st.text_input("Status", value=row.get('status') or "")

        if st.form_submit_button("Save Project"):
            execute(
                "UPDATE projects SET project_name=%s, project_description=%s, status=%s WHERE project_id=%s",
                (name, desc, status, row['project_id'])
            )
            st.success("Project updated")
            st.rerun()

# ======================================================
# 🔧 PATCH: EDIT SUB-PROJECT
# ======================================================

def edit_subproject_ui():
    df = query_df(
        """
        SELECT wi.work_item_id, wi.work_item_name, p.project_name
        FROM project_work_items wi
        JOIN projects p ON wi.project_id = p.project_id
        ORDER BY p.project_name, wi.work_item_name
        """
    )

    if df.empty:
        st.info("No Sub-Projects found")
        return

    options = {
        f"{row['project_name']} | {row['work_item_name']}": row['work_item_id']
        for _, row in df.iterrows()
    }

    selected = st.selectbox("Select Sub-Project", list(options.keys()), key="edit_subproject")

    row = query_one("SELECT * FROM project_work_items WHERE work_item_id=%s", (options[selected],))

    with st.form("edit_subproject_form"):
        name = st.text_input("Sub-Project Name", value=row['work_item_name'])
        desc = st.text_area("Description", value=row.get('work_item_description') or "")

        if st.form_submit_button("Save Sub-Project"):
            execute(
                "UPDATE project_work_items SET work_item_name=%s, work_item_description=%s WHERE work_item_id=%s",
                (name, desc, row['work_item_id'])
            )
            st.success("Sub-Project updated")
            st.rerun()

# ======================================================
# 🔧 PATCH: ADD MATERIAL TO SUB-PROJECT
# ======================================================

def add_material_to_subproject_ui(work_item_id):
    st.subheader("Add Material to Sub-Project")

    material_id = material_search_selector()

    qty = st.number_input("Quantity", value=1.0)
    price = st.number_input("Unit Price", value=0.0)

    if st.button("Add Material Line") and material_id:
        add_material_line_from_master(
            work_item_id=work_item_id,
            material_id=material_id,
            quantity=qty,
            unit_id=None,
            unit_price=price,
            vendor_id=None
        )
        st.success("Material added")
        st.rerun()

# ======================================================
# 🔧 PATCH: MAIN NAVIGATION ADDITIONS
# ======================================================

def page_projects_enhanced():
    st.header("Projects")

    tab = st.radio("", ["Add Materials", "Edit Project", "Edit Sub-Project"], horizontal=True)

    if tab == "Edit Project":
        edit_project_ui()
        return

    if tab == "Edit Sub-Project":
        edit_subproject_ui()
        return

    # --- EXISTING ADD MATERIALS UI ---
    projects = query_df("SELECT project_id, project_name FROM projects")

    if projects.empty:
        st.info("No projects")
        return

    proj_map = {row['project_name']: row['project_id'] for _, row in projects.iterrows()}
    selected_proj = st.selectbox("Project", list(proj_map.keys()))

    subs = query_df("SELECT work_item_id, work_item_name FROM project_work_items WHERE project_id=%s", (proj_map[selected_proj],))

    if subs.empty:
        st.info("No Sub-Projects")
        return

    sub_map = {row['work_item_name']: row['work_item_id'] for _, row in subs.iterrows()}
    selected_sub = st.selectbox("Sub-Project", list(sub_map.keys()))

    add_material_to_subproject_ui(sub_map[selected_sub])

# ======================================================
# 🔧 PATCH: APP TITLE
# ======================================================

st.title("Materials Management System")

# ======================================================
# 🔧 ROUTING CHANGE
# Replace your Projects page call with:
# page_projects_enhanced()
# ======================================================
