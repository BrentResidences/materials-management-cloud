
import re

FILE = "Mapp.py"

with open(FILE, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Update app title
content = content.replace("Materials Management", "Materials Management System")

# 2. Replace Work Item -> Sub-Project (UI only safe replacement)
content = re.sub(r"Work Items", "Sub-Projects", content)
content = re.sub(r"Work Item", "Sub-Project", content)

# DO NOT change database table names
# Restore if accidentally changed
content = content.replace("project_sub_projects", "project_work_items")

with open(FILE, "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied successfully to Mapp.py")
